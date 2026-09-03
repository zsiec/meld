package flow

// bitrateAdvisor turns the transport's total-rate budget into a stable source
// bitrate request. Recovery and source share one wire budget: when the current
// source consumes so much of it that the admitted recovery allowance cannot fit,
// the encoder is the only layer that can create capacity without allowing a
// deadline queue to grow.
//
// The request is deliberately sticky. Once an encoder complies, its lower
// observed rate must not be mistaken for spare capacity and immediately clear the
// request. The pre-reduction offer is remembered as resumeRate; only sustained
// capacity sufficient for that offer relaxes the advisory.
type bitrateAdvisor struct {
	target     int64
	resumeRate int64
	overload   int
	clear      int
}

const (
	bitrateAdviceActivateReports = 2
	bitrateAdviceClearReports    = 8
	bitrateAdviceMarginPercent   = 5
	// Recovery may not purchase packet completeness by collapsing picture
	// quality. Even when a burst-tail set-point is very large, reserve at least
	// 60% of the aggregate byte budget for source wire bytes; the ordinary
	// source-first token ledger sheds the remaining unaffordable repair.
	bitrateAdviceMaxRecoveryShare = 0.40
)

// observe consumes the currently offered payload rate and the largest payload
// rate that leaves room for the bounded recovery allowance. A zero affordable
// rate means the transport has not measured enough state to advise yet.
func (a *bitrateAdvisor) observe(offered, affordable int64) {
	if offered <= 0 || affordable <= 0 {
		return
	}
	overloaded := offered > affordable*(100+bitrateAdviceMarginPercent)/100
	if a.target == 0 {
		if !overloaded {
			a.overload = 0
			return
		}
		a.overload++
		if a.overload < bitrateAdviceActivateReports {
			return
		}
		a.target = affordable
		a.resumeRate = offered
		a.overload = 0
		return
	}

	if offered > a.resumeRate {
		a.resumeRate = offered
	}
	if affordable < a.target {
		// Congestion/loss got worse: reduce immediately so queue growth stops.
		a.target = affordable
	} else if affordable > a.target {
		// Probe an improved envelope slowly; a one-report spike must not make the
		// encoder seesaw between rates.
		a.target += (affordable - a.target) / 8
	}
	if a.target < 1 {
		a.target = 1
	}

	if affordable >= a.resumeRate*(100+bitrateAdviceMarginPercent)/100 {
		a.clear++
		if a.clear >= bitrateAdviceClearReports {
			a.target, a.resumeRate, a.overload, a.clear = 0, 0, 0, 0
		}
	} else {
		a.clear = 0
	}
}

func (a *bitrateAdvisor) control() int64 { return a.target }

// affordableSourceBitrate prices one source packet plus its expected share of
// recovery against the live aggregate budget. SourceWireBytes includes transport
// headers while SourcePayloadBytes is what the encoder controls. Keeping the
// packet cadence fixed makes the header term explicit; reducing payload cannot
// pretend those bytes disappear.
func affordableSourceBitrate(budgetBytesPerSec, interMicros, sourceWireBytes,
	sourcePayloadBytes, repairBytes int64, repairPerSource float64) (offered, affordable int64) {
	if budgetBytesPerSec <= 0 || interMicros <= 0 || sourceWireBytes <= 0 ||
		sourcePayloadBytes <= 0 || repairBytes <= 0 || repairPerSource < 0 {
		return 0, 0
	}
	offered = sourcePayloadBytes * 8_000_000 / interMicros
	headerBytes := sourceWireBytes - sourcePayloadBytes
	if headerBytes < 0 {
		headerBytes = 0
	}
	bytesPerInterval := float64(budgetBytesPerSec) * float64(interMicros) / 1e6
	recoveryPerInterval := repairPerSource * float64(repairBytes)
	if maxRecovery := bitrateAdviceMaxRecoveryShare * bytesPerInterval; recoveryPerInterval > maxRecovery {
		recoveryPerInterval = maxRecovery
	}
	payloadPerInterval := bytesPerInterval - float64(headerBytes) - recoveryPerInterval
	if payloadPerInterval <= 0 {
		return offered, 1
	}
	affordable = int64(payloadPerInterval * 8e6 / float64(interMicros))
	if affordable < 1 {
		affordable = 1
	}
	return offered, affordable
}

func (s *Sender) updateBitrateAdvice() {
	if s.fbCount < coldStartFeedbacks || s.interMicros <= 0 || s.sourceWireBytes <= 0 ||
		s.sourcePayloadBytes <= 0 {
		return
	}
	n := s.curGenWidth
	if n <= 0 {
		n = s.genWidthNow()
	}
	repairRate := float64(s.repairCountForPolicy(n, false)) / float64(n)
	repairBytes := int64(symHeaderBytes + 8 + codedSymbolSize(s.cfg.SymbolSize))
	offered, affordable := affordableSourceBitrate(s.bucket.bytesPerSec, s.interMicros,
		s.sourceWireBytes, s.sourcePayloadBytes, repairBytes, repairRate)
	s.bitrateAdvice.observe(offered, affordable)
}

func (s *SlidingSender) updateBitrateAdvice() {
	if s.fbCount < coldStartFeedbacks || s.interMicros <= 0 || s.sourceWireBytes <= 0 ||
		s.sourcePayloadBytes <= 0 || !s.crValid {
		return
	}
	// The normal write path memoizes its pre-budget set-point in crRate before
	// applying source-headroom and flood caps. Advice consumes that established
	// observation; it MUST NOT call codeRate here, because polling an advisory may
	// not advance or re-time the transport policy state.
	rate := s.crRate
	repairBytes := int64(repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize))
	offered, affordable := affordableSourceBitrate(s.bucket.bytesPerSec, s.interMicros,
		s.sourceWireBytes, s.sourcePayloadBytes, repairBytes, rate)
	s.bitrateAdvice.observe(offered, affordable)
}
