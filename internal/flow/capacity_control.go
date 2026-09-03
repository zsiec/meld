package flow

import (
	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// Capacity control converts delivery and timing observations into bounded
// proactive-rate ceilings. Emission remains in the sender integration layer.

// rttMinHalfWindowMicros is the min filter's half-window: two halves roll so a
// valid minimum always spans 1.5-3 s of samples. Long enough that a transient
// self-flood (which drains once the band stops clipping) cannot poison the
// estimate; short enough that a genuine route change is adopted within ~3 s.
const rttMinHalfWindowMicros = 1_500_000

// Flood-breaker constants: the trigger threshold is scaled by (1−pEst) so honest
// heavy loss (arrivals below offer BECAUSE the wire dropped them) never trips it —
// only the wire-overrun signature (arrivals far below what loss explains) does.
const (
	floodTriggerQ8    = 218  // 0.85 in Q8: trigger when arrivals < 0.85×(1−pEst)×offered
	floodTriggerMinQ8 = 128  // …but never above-trigger below half the offer (deep-loss floor)
	floodCapMin       = 0.25 // the cap never starves protection entirely
	floodClearReports = 3    // consecutive clean reports before the cap relaxes
)

// updateFloodBreaker folds one feedback report into the wire-overrun breaker: the
// receiver's source-arrival rate (ΔHighestSeen/Δt) against the offered source rate
// (1/interMicros), EWMA'd in Q8. Sustained arrivals far below what the reported
// loss explains mean the sender itself is overrunning the wire. The response is
// AIMD on the proactive-rate cap only.
func (s *SlidingSender) updateFloodBreaker(now clock.Timestamp, fb wire.Feedback) {
	defer func() {
		s.lastFBAt, s.lastFBHighest, s.lastFBSource, s.lastFBRepair =
			now, fb.HighestSeen, s.stats.Source, s.stats.Repair
	}()
	if s.lastFBAt == 0 || fb.HighestSeen <= s.lastFBHighest {
		return
	}
	offered := s.stats.Source - s.lastFBSource // exact source symbols offered since the last report
	if offered == 0 {
		return
	}
	arrived := int64(fb.HighestSeen-s.lastFBHighest) * 256 / int64(offered) // Q8 ratio vs offer
	if arrived > 512 {
		arrived = 512 // in-flight skew can transiently exceed the window's offer; clamp
	}
	s.arriveRatioQ8 += (arrived - s.arriveRatioQ8) / 4
	s.updateHeadroom(fb, offered, arrived, now.Sub(s.lastFBAt))
	thresh := int64(float64(floodTriggerQ8) * (1 - s.pEst))
	if thresh < floodTriggerMinQ8 {
		thresh = floodTriggerMinQ8
	}
	if s.arriveRatioQ8 < thresh {
		s.floodClear = 0
		if cap := s.floodCap * 0.7; cap >= floodCapMin {
			s.floodCap = cap
		} else {
			s.floodCap = floodCapMin
		}
		return
	}
	if s.floodCap >= maxRepairFactor {
		return // inactive
	}
	if s.floodClear++; s.floodClear >= floodClearReports {
		if s.floodCap *= 1.25; s.floodCap > maxRepairFactor {
			s.floodCap = maxRepairFactor
		}
	}
}

// Headroom thresholds use separate saturation and clear points so the controller
// has hysteresis around the estimated wire capacity.
const (
	headroomSatF        = 0.90 // saturation evidence: passed-through fraction below this tightens
	headroomClearF      = 0.97 // arrivals track offer above this: eligible to probe upward
	headroomProbePerSec = 0.50 // additive probe rate (time-based: 10 ms event feedbacks must not multiply it)
	headroomSafety      = 0.90 // discount on the measured affordable rate
)

// updateHeadroom folds one report into the affordable-rate ceiling: with the wire
// passing fraction f of the offered (1+r) mix, the affordable proactive rate is
// f·(1+r)−1 — offered load equal to what the wire demonstrably serves. Tighten to
// that (discounted) on saturation evidence; probe upward per unit time only when
// arrivals track offer and the RTT sample shows no standing queue. The
// instantaneous interval ratio avoids stale EWMA state during queue drain. A
// queue witness is required because low arrival rate alone is ambiguous under
// burst loss. The floor preserves at least the reported mean-loss replacement
// rate plus margin.
func (s *SlidingSender) updateHeadroom(fb wire.Feedback, offered uint64, arrivedQ8, dtMicros int64) {
	if !s.cfg.HeadroomAwareSizing {
		return // opt-in (Config.HeadroomAwareSizing): the cap stays inactive
	}
	repOffered := float64(s.stats.Repair-s.lastFBRepair) / float64(offered)
	denom := 1 - float64(fb.LossRate)/65535
	if denom < 0.5 {
		denom = 0.5 // deep-loss floor, as the breaker's trigger: attribution saturates
	}
	f := (float64(arrivedQ8) / 256) / denom
	if f > 1 {
		f = 1
	}
	// The queue witness is the INSTANTANEOUS sample, not the min-filter: the
	// min-window (1.5 s halves) is built to exclude queueing, so it cannot see a
	// sub-window boom (gating on it un-broke the sim limit cycle). The 1/6 bar
	// clears glass natural release wobble (~±13%).
	switch {
	case f < headroomSatF && s.rttSample > s.rttMicros+s.rttMicros/6:
		if cap := headroomSafety * (f*(1+repOffered) - 1); cap < s.headroomCap {
			s.headroomCap = cap
			s.stats.HeadroomTightens++
		}
	case f >= headroomClearF && s.rttMinCur <= s.rttMicros+s.rttMicros/4:
		s.headroomCap += headroomProbePerSec * float64(dtMicros) / 1e6
	}
	if s.headroomCap < floodCapMin {
		s.headroomCap = floodCapMin
	}
	if pFloor := 1.3 * float64(fb.LossRate) / 65535; s.headroomCap < pFloor {
		s.headroomCap = pFloor
	}
	if s.headroomCap > maxRepairFactor {
		s.headroomCap = maxRepairFactor // inactive
	}
}

// proactiveCap combines the continuous headroom estimate with the AIMD backstop.
func (s *SlidingSender) proactiveCap() float64 {
	if s.headroomCap < s.floodCap {
		return s.headroomCap
	}
	return s.floodCap
}

func (s *SlidingSender) updateRTT(now clock.Timestamp, fb wire.Feedback) {
	if fb.HighestSeen == 0 {
		return
	}
	dl, ok := s.deadlines[fb.HighestSeen-1]
	if !ok {
		return
	}
	sample := now.Sub(dl.Add(-s.cfg.BufferMicros))
	if sample <= 0 {
		return
	}
	s.rttSample = sample // per-report instantaneous RTT: the queue-delay witness (updateHeadroom)
	if s.cc != nil {
		// The raw RTT sample drives delay control; the receiver's admitted-packet CE
		// fraction layers the L4S response on top. Coding never masks either signal.
		wasPrimed := s.cc.primed
		s.cc.onSample(now, sample, float64(fb.EcnCE)/65535)
		if !wasPrimed && s.cc.primed {
			s.cc.seedRate(s.slidingObservedRate())
		}
	}
	if s.rttWinStart == 0 {
		s.rttWinStart, s.rttMinCur, s.rttMinPrev = now, sample, sample
	} else if now.Sub(s.rttWinStart) > rttMinHalfWindowMicros {
		s.rttMinPrev, s.rttMinCur, s.rttWinStart = s.rttMinCur, sample, now
	} else if sample < s.rttMinCur {
		s.rttMinCur = sample
	}
	if s.rttMicros = s.rttMinCur; s.rttMinPrev < s.rttMicros {
		s.rttMicros = s.rttMinPrev
	}
}
