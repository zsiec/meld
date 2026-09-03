package flow

import "github.com/zsiec/meld/internal/clock"

// This file implements a Copa-style delay-based congestion controller
// (Arun & Balakrishnan, NSDI'18) adapted to Meld's per-feedback RTT samples, with the
// L4S/DCTCP ECN response layered on. The controller owns the total send-rate budget; the
// redundancy sizer then allocates repair WITHIN that budget (CC sets the budget, FEC fits
// inside — never repair on top). Pure arithmetic, time as explicit timestamps. The
// arithmetic is float (a rate controller, not an oracle-scored value); its outputs need
// only converge, not be bit-identical, unlike the fixed-point burst sizer.
//
// It reacts to congestion via DELAY + ECN, never LOSS — deliberately, and satisfying RFC
// 9265 (FEC and congestion control): the standing-queue delay and CE marks are exactly the
// signals coding cannot mask, and §5.3 endorses a delay/ECN controller as sufficient. Loss
// is NOT a CC input even though the receiver reports the pre-recovery wire loss
// (Feedback.CongestionLoss, which the redundancy SIZER consumes) — because loss is AMBIGUOUS:
// a policer's congestion and a wireless link's corruption are indistinguishable here (both
// show a silent queue + silent ECN + drops). A loss backstop would therefore cut the rate
// on lossy-but-uncongested paths. The pre-recovery wire-loss counter sizes FEC but never
// throttles the rate; MaxBitrate and application adaptation bound hard policers.

// Congestion-control tuning. δ is the throughput/latency knob: rate ≈ 1/(δ·d_q), so a
// smaller δ tolerates a larger standing queue for more throughput (live video favors
// goodput — Meta runs Copa at δ≈0.04). The min filters separate the propagation RTT
// (long window) from the standing-queue RTT (short window); their difference is d_q.
const (
	ccDefaultDelta     = 0.1
	ccRTTMinWindow     = 10_000_000 // 10 s — the propagation-delay baseline
	ccStandingWindow   = 100_000    // 100 ms — captures the standing queue (a few feedbacks)
	ccMinQueueMicros   = 1          // floor on d_q so the target rate stays finite
	ccInitWindowFactor = 2          // initial cwnd = this many MSS
	ccMaxRTTSeconds    = 2.0        // cwnd ≤ ceiling-rate × this, bounding queue-free growth
	// Slow start exits when the RAW queue (not the min-filtered standing) first exceeds
	// rttMin/4 + this — the fast ramp's own queue, which the min filter would lag.
	ccSlowStartQueueMicros = 2_000
	// Min-RTT re-baseline (path change UP). A windowed min cannot track an INCREASE in
	// propagation delay (failover to a longer path), so it would read the new baseline
	// as a permanent queue and starve. When the controller has converged (cwnd not
	// growing) yet the standing RTT stays this far above rttMin for this long, the floor
	// has risen, not a queue formed (the controller would have drained a queue) — so
	// re-anchor rttMin up to the standing RTT.
	ccReBaselineMarginMicros = 8_000
	ccReBaselineMicros       = 600_000
	// L4S / DCTCP ECN response (RFC 9330/9331; Alizadeh et al., SIGCOMM'10). α is an EWMA
	// of the CE-marked fraction with gain g = 1/16; cwnd is reduced by α/2 per RTT while
	// marks persist, layered on Copa's delay rule. Below the floor α snaps to 0, so a path
	// that has never marked runs as pure Copa (no ECN drag).
	ccECNAlphaGain  = 1.0 / 16
	ccECNAlphaFloor = 1e-3
)

// minFilter is a windowed minimum via the double-bucket (periodic-reset) trick: it
// reports the min over roughly [window, 2·window) at O(1) per sample, no ring buffer.
type minFilter struct {
	cur, prev int64
	curStart  clock.Timestamp
	primed    bool
}

func (m *minFilter) observe(now clock.Timestamp, v, window int64) int64 {
	if !m.primed {
		m.primed, m.cur, m.prev, m.curStart = true, v, v, now
		return v
	}
	if now.Sub(m.curStart) >= window {
		m.prev, m.cur, m.curStart = m.cur, v, now
	} else if v < m.cur {
		m.cur = v
	}
	return m.value()
}

func (m *minFilter) value() int64 {
	if m.prev < m.cur {
		return m.prev
	}
	return m.cur
}

// reset forces the filter's minimum to v (a re-baseline after a detected path change,
// where the true floor has risen and the windowed min would otherwise lag).
func (m *minFilter) reset(v int64, now clock.Timestamp) {
	m.primed, m.cur, m.prev, m.curStart = true, v, v, now
}

// congestionController holds the queueing delay near a target by pacing the send rate
// to ≈ 1/(δ·d_q). It consumes RTT samples and emits a rate budget in bytes/sec.
type congestionController struct {
	delta          float64
	mss            float64 // bytes per paced unit (symbol + header)
	maxBytesPerSec float64 // budget ceiling (the static MaxBitrate; CC only reduces below it)
	cwndCap        float64 // absolute cwnd bound, so a queue-free path can't grow it unbounded
	cwndBytes      float64
	primed         bool
	slowStart      bool
	alpha          float64 // EWMA of the CE-marked fraction (L4S/DCTCP); 0 ⇒ pure Copa
	lastSample     clock.Timestamp
	elevatedSince  clock.Timestamp // when the standing RTT first stayed above the baseline at a converged cwnd

	rttMin      minFilter // propagation RTT (long window)
	rttStanding minFilter // standing-queue RTT (short window)
}

func newCongestionController(delta float64, mss int, maxBitrate int64) *congestionController {
	if delta <= 0 {
		delta = ccDefaultDelta
	}
	maxBps := float64(maxBitrate) / 8
	return &congestionController{
		delta: delta, mss: float64(mss),
		maxBytesPerSec: maxBps,
		cwndCap:        maxBps * ccMaxRTTSeconds, // cwnd ≤ ceiling × a generous RTT
	}
}

// onSample folds one RTT measurement (microseconds) into the controller. It opens in
// SLOW START — cwnd doubles per RTT — so it reaches the operating point in log(BDP)
// round trips instead of the slow linear ramp the additive rule alone gives off a
// fast/empty path; it exits the first time a queue forms. Thereafter cwnd moves toward
// the BDP at the target rate 1/(δ·d_q) by Copa's additive rule, RTT-paced (per-RTT
// change v/δ packets, scaled by the elapsed fraction of an RTT so it is independent of
// the irregular feedback cadence). reBaseline handles the propagation floor RISING
// (path change), which a windowed min cannot track on its own.
//
// The additive step is fixed. A velocity multiplier would trade a faster response to
// newly available bandwidth for a larger standing-queue excursion. Fixed-rate media
// cannot use capacity beyond its encode rate, and slow start already covers startup,
// so the controller favors the bounded-queue behavior pinned by its convergence tests.
func (cc *congestionController) onSample(now clock.Timestamp, rttMicros int64, ceFraction float64) {
	if rttMicros <= 0 {
		return
	}
	cc.rttMin.observe(now, rttMicros, ccRTTMinWindow)
	standing := cc.rttStanding.observe(now, rttMicros, ccStandingWindow)
	if !cc.primed {
		cc.primed, cc.slowStart, cc.cwndBytes, cc.lastSample = true, true, ccInitWindowFactor*cc.mss, now
		return // no interval yet
	}
	dt := now.Sub(cc.lastSample)
	cc.lastSample = now
	if dt <= 0 {
		return
	}

	cc.reBaseline(now, standing) // re-anchor rttMin if the propagation floor rose (path change)

	// L4S/DCTCP: track the CE-marked fraction in α (EWMA). A path that never marks keeps
	// α at exactly 0 (pure Copa); below the floor α snaps back to 0 after marks stop.
	if ceFraction < 0 {
		ceFraction = 0
	} else if ceFraction > 1 {
		ceFraction = 1
	}
	cc.alpha += ccECNAlphaGain * (ceFraction - cc.alpha)
	if cc.alpha < ccECNAlphaFloor {
		cc.alpha = 0
	}

	dq := standing - cc.rttMin.value() // queueing delay (micros)
	if dq < ccMinQueueMicros {
		dq = ccMinQueueMicros
	}
	targetRate := 1.0 / (cc.delta * float64(dq) / 1e6) // packets/sec, Copa's 1/(δ·d_q)
	cwndPkts := cc.cwndBytes / cc.mss
	currentRate := cwndPkts / (float64(standing) / 1e6) // cwnd/RTT, packets/sec

	// One sample represents at most one RTT of evolution.
	fracRTT := float64(dt) / float64(standing)
	if fracRTT > 1 {
		fracRTT = 1
	}

	if cc.slowStart {
		// Detect the building queue from the RAW RTT, not the min-filtered standing
		// (a min filter lags a rising signal). Exit once the queue is a clear fraction
		// of the propagation delay, the achieved rate reaches the target, or — for L4S —
		// the network has CE-marked (a mark is congestion: stop the exponential ramp).
		dqRaw := rttMicros - cc.rttMin.value()
		if ceFraction > 0 || currentRate >= targetRate || dqRaw > cc.rttMin.value()/4+ccSlowStartQueueMicros {
			cc.slowStart = false
		} else {
			cc.cwndBytes += cc.cwndBytes * fracRTT // ×2 per RTT
			cc.clampCwnd()
			return
		}
	}

	dir := 1.0
	if currentRate > targetRate {
		dir = -1
	}
	stepPkts := (1.0 / cc.delta) * fracRTT
	cc.cwndBytes += dir * stepPkts * cc.mss
	// L4S multiplicative decrease, proportional to α (RFC 9330/9331; DCTCP). With no marks
	// α is 0 and this is a no-op, leaving pure Copa; under marks it balances the additive
	// increase at a shallow queue. FEC is never touched (RFC 9265): marks reduce the rate.
	if cc.alpha > 0 {
		cc.cwndBytes -= cc.cwndBytes * (cc.alpha / 2) * fracRTT
	}
	cc.clampCwnd()
}

// seedRate raises a newly primed controller to an already-observed application
// rate. A live media sender is application-limited before its first feedback: it
// has already demonstrated a source+recovery offer, so restarting from a
// two-packet TCP window would create a local pacing queue and consume the very
// deadline the controller is meant to protect. This is only a starting point;
// subsequent delay/ECN samples retain full authority to reduce the window.
func (cc *congestionController) seedRate(bytesPerSec int64) {
	if !cc.primed || bytesPerSec <= 0 {
		return
	}
	rttSec := float64(cc.rttStanding.value()) / 1e6
	if rttSec <= 0 {
		return
	}
	if cwnd := float64(bytesPerSec) * rttSec; cwnd > cc.cwndBytes {
		cc.cwndBytes = cwnd
		cc.clampCwnd()
	}
}

// reBaseline raises rttMin to the standing RTT when the propagation floor has risen:
// the standing RTT staying ccReBaselineMarginMicros above rttMin for ccReBaselineMicros
// is a path change, not a queue — under the controller a real queue drains, so the
// standing RTT would have fallen back toward rttMin. (The complementary case, the floor
// FALLING, the windowed min already tracks.) This is the min-RTT re-baselining a
// delay-based controller needs to survive a failover to a longer path.
func (cc *congestionController) reBaseline(now clock.Timestamp, standing int64) {
	if standing > cc.rttMin.value()+ccReBaselineMarginMicros {
		if cc.elevatedSince == 0 {
			cc.elevatedSince = now
		} else if now.Sub(cc.elevatedSince) >= ccReBaselineMicros {
			cc.rttMin.reset(standing, now)
			cc.elevatedSince = 0
		}
	} else {
		cc.elevatedSince = 0 // the queue drained (or never formed) — not a path change
	}
}

// clampCwnd bounds cwnd to [floor, cwndCap].
func (cc *congestionController) clampCwnd() {
	if floor := ccInitWindowFactor * cc.mss; cc.cwndBytes < floor {
		cc.cwndBytes = floor
	}
	if cc.cwndCap > 0 && cc.cwndBytes > cc.cwndCap {
		cc.cwndBytes = cc.cwndCap
	}
}

// rateBudgetBytesPerSec returns the current paced send-rate budget (cwnd / standing
// RTT), clamped to the static ceiling — the CC only reduces below it. Zero before the
// first sample.
func (cc *congestionController) rateBudgetBytesPerSec() int64 {
	if !cc.primed {
		return 0
	}
	rttSec := float64(cc.rttStanding.value()) / 1e6
	if rttSec <= 0 {
		return 0
	}
	budget := cc.cwndBytes / rttSec
	if cc.maxBytesPerSec > 0 && budget > cc.maxBytesPerSec {
		budget = cc.maxBytesPerSec
	}
	return int64(budget)
}
