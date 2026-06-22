package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// bottleneck is a fluid single-link model for testing the controller in isolation:
// the link drains at capBytesPerSec; the sender fills a queue at the controller's
// rate; the measured RTT is the base propagation plus the standing queue's drain
// time. It is the minimal closed loop a delay-based controller must stabilize.
type bottleneck struct {
	capBytesPerSec float64
	baseRTTMicros  int64
	queueBytes     float64
}

func (b *bottleneck) step(rateBytesPerSec int64, dtMicros int64) int64 {
	dt := float64(dtMicros) / 1e6
	b.queueBytes += (float64(rateBytesPerSec) - b.capBytesPerSec) * dt
	if b.queueBytes < 0 {
		b.queueBytes = 0
	}
	return b.baseRTTMicros + int64(b.queueBytes/b.capBytesPerSec*1e6)
}

func (b *bottleneck) queueMillis() float64 { return b.queueBytes / b.capBytesPerSec * 1000 }

// run drives the controller around the bottleneck for the given duration, returning
// the final rate budget. rttMicros starts at the base (empty queue).
func run(cc *congestionController, b *bottleneck, steps, dtMicros int) int64 {
	now := clock.Timestamp(0)
	rtt := b.baseRTTMicros
	var rate int64
	for i := 0; i < steps; i++ {
		cc.onSample(now, rtt, 0) // no CE marks: pure delay-based Copa
		rate = cc.rateBudgetBytesPerSec()
		rtt = b.step(rate, int64(dtMicros))
		now = now.Add(int64(dtMicros))
	}
	return rate
}

// TestCCConvergesToBottleneck: the controller paces near link capacity while holding
// the standing queue bounded — the basic delay-based-CC correctness property.
func TestCCConvergesToBottleneck(t *testing.T) {
	const mss = 1316 + 29
	cc := newCongestionController(0.1, mss, 1_000_000_000)
	b := &bottleneck{capBytesPerSec: 2_500_000, baseRTTMicros: 40_000} // 20 Mbps, 40 ms RTT

	rate := run(cc, b, 6000, 5_000) // 30 s at 5 ms steps
	lo, hi := b.capBytesPerSec*0.7, b.capBytesPerSec*1.3
	if float64(rate) < lo || float64(rate) > hi {
		t.Fatalf("rate %d did not converge near capacity %.0f (band %.0f..%.0f)", rate, b.capBytesPerSec, lo, hi)
	}
	if q := b.queueMillis(); q > 60 {
		t.Fatalf("standing queue %.0f ms too large (controller not delay-bounding)", q)
	}
}

// TestCCBacksOffOnDelayRise: when the propagation RTT jumps (a path change), the
// controller must not keep overfilling — it should re-converge with a bounded queue
// rather than driving unbounded latency.
func TestCCBacksOffOnDelayRise(t *testing.T) {
	const mss = 1316 + 29
	cc := newCongestionController(0.1, mss, 1_000_000_000)
	b := &bottleneck{capBytesPerSec: 2_500_000, baseRTTMicros: 30_000}
	run(cc, b, 4000, 5_000) // settle at 30 ms

	b.baseRTTMicros = 120_000 // propagation jumps to 120 ms (e.g. failover to a longer path)
	b.queueBytes = 0
	rate := run(cc, b, 6000, 5_000)

	if float64(rate) > b.capBytesPerSec*1.3 {
		t.Fatalf("rate %d overshoots capacity %.0f after the delay rise", rate, b.capBytesPerSec)
	}
	// The min-RTT re-baseline must RECOVER utilization: without it the controller reads
	// the risen propagation as a permanent queue and starves at near-zero rate.
	if float64(rate) < b.capBytesPerSec*0.5 {
		t.Fatalf("rate %d starves after the delay rise — min-RTT re-baseline did not recover", rate)
	}
	if q := b.queueMillis(); q > 80 {
		t.Fatalf("standing queue %.0f ms too large after re-converge", q)
	}
}

// sendGenWithRTT writes one generation of media and delivers feedback rttUs after the
// generation closed, so the sender's RTT estimate (and thus the controller) sees rttUs.
func sendGenWithRTT(s *Sender, id *int, now *clock.Timestamp, rttUs int64) {
	for i := 0; i < s.cfg.GenSize; i++ {
		s.Write(*now, make([]byte, s.cfg.SymbolSize))
		*now = now.Add(100)
		*id++
	}
	for {
		if _, ok := s.PollSend(); !ok {
			break
		}
	}
	fb := (*now).Add(rttUs)
	s.FeedFeedback(fb, wire.Feedback{Flow: s.cfg.Flow, HighestSeen: uint32(*id),
		DecodedLowEdge: uint32(*id - s.cfg.GenSize)})
	*now = fb.Add(1000)
}

// TestSenderCCBacksOffAndThrottlesRepair: with CC on, a sustained RTT rise (a building
// queue) must shrink the rate budget below the static ceiling, and the resulting tight
// budget must throttle REPAIR while media keeps flowing — CC owns the budget, FEC fits
// within it.
func TestSenderCCBacksOffAndThrottlesRepair(t *testing.T) {
	const (
		mss      = testSym + symHeaderBytes
		writeGap = 200                           // µs between media writes
		mediaBps = float64(mss) * 1e6 / writeGap // offered media rate, bytes/sec
		capBps   = mediaBps * 1.2                // bottleneck just above media, below media+repair
		baseRTT  = 30_000
	)
	cfg := testConfig()
	cfg.CongestionControl = true
	cfg.MaxBitrate = 50_000_000 // 50 Mbps ceiling, far above the ~4 Mbps bottleneck
	cfg.Redundancy = 0.3        // proactive repair to sacrifice under congestion
	s := NewSender(cfg)

	// Closed-loop bottleneck: media + un-throttled repair fill a queue that drains at
	// capBps; the feedback RTT is base + queue, so the RTT responds to the sender's own
	// rate (a real, drainable queue — not a static step, which would read as a path
	// change). The controller must throttle repair until the total fits capBps.
	now := clock.Timestamp(0)
	id, queue := 0, 0.0
	var budget int64
	for round := 0; round < 400; round++ {
		start := now
		for i := 0; i < cfg.GenSize; i++ {
			s.Write(now, make([]byte, cfg.SymbolSize))
			now = now.Add(writeGap)
			id++
		}
		emitted := 0
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			emitted += len(d)
		}
		dur := float64(now.Sub(start)) / 1e6
		queue += float64(emitted) - capBps*dur
		if queue < 0 {
			queue = 0
		}
		rtt := int64(baseRTT) + int64(queue/capBps*1e6)
		fb := now.Add(rtt)
		s.FeedFeedback(fb, wire.Feedback{Flow: cfg.Flow, HighestSeen: uint32(id), DecodedLowEdge: uint32(id - cfg.GenSize)})
		budget = s.RateBudgetBitsPerSec()
		now = fb.Add(1000)
	}

	// The budget converged to ~the bottleneck (well below the static ceiling), repair
	// was throttled to fit, and not a single media symbol was dropped.
	capBits := int64(capBps * 8)
	if budget > capBits*2 || budget > cfg.MaxBitrate/2 {
		t.Fatalf("budget %d did not back off to the ~%d-bps bottleneck (ceiling %d)", budget, capBits, cfg.MaxBitrate)
	}
	st := s.Stats()
	if st.Throttled == 0 {
		t.Fatal("repair was not throttled under the congested budget")
	}
	if st.Source != uint64(id) {
		t.Fatalf("media dropped: Source=%d, wrote %d", st.Source, id)
	}
}

// TestSenderCCDisabledUsesStaticCeiling: with CC off, RateBudget is the static ceiling
// regardless of RTT — the N1 behavior is unchanged.
func TestSenderCCDisabledUsesStaticCeiling(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBitrate = 7_000_000
	s := NewSender(cfg)
	now := clock.Timestamp(0)
	id := 0
	for i := 0; i < 40; i++ {
		sendGenWithRTT(s, &id, &now, 300_000)
	}
	if got := s.RateBudgetBitsPerSec(); got != cfg.MaxBitrate {
		t.Fatalf("CC off: RateBudget %d, want the static ceiling %d", got, cfg.MaxBitrate)
	}
}
