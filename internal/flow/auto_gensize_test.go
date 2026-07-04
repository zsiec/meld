package flow

import "testing"

// TestMeasuredGenWidth pins the AutoGenSize width derived from the sender's own measurements:
// narrow while unmeasured (bootstrap), wide at a fast cadence + low RTT + generous budget, held
// narrow by the fill gate at a slow cadence, and narrow when the budget is below a reactive round.
func TestMeasuredGenWidth(t *testing.T) {
	s := NewSender(Config{SymbolSize: 1316, GenSize: 16, BufferMicros: 200_000, AutoGenSize: true})
	if w := s.measuredGenWidth(); w != 16 {
		t.Fatalf("bootstrap (no measurements): width %d, want 16", w)
	}
	// ~50 Mbps cadence (210 µs/sym), 40 ms RTT, 200 ms budget ⇒ widen to the cap.
	s.interMicros, s.rttSampled, s.rttMicros = 210, true, 40_000
	if w := s.measuredGenWidth(); w != maxAdaptiveGenWidth {
		t.Fatalf("fast cadence + low RTT: width %d, want %d", w, maxAdaptiveGenWidth)
	}
	// ~5 Mbps cadence (2106 µs/sym) ⇒ fill gate holds it at GenSize.
	s.interMicros = 2106
	if w := s.measuredGenWidth(); w != 16 {
		t.Fatalf("slow cadence: width %d, want 16 (fill gate)", w)
	}
	// Budget below a reactive round (RTT 200 ms + feedback > 200 ms budget) ⇒ narrow.
	s.interMicros, s.rttMicros = 210, 200_000
	if w := s.measuredGenWidth(); w != 16 {
		t.Fatalf("budget < reactive round: width %d, want 16", w)
	}
}

// TestAutoGenSize is the end-to-end proof that AutoGenSize is correct AND adapts with ZERO config:
// the sender measures its own RTT + write cadence, widens the generation, and STAMPS the per-gen
// width on every symbol — the receiver follows it with no matching config. A desync would corrupt
// or strand delivery, so the four invariants holding under a varying width is the alignment proof;
// the lower overhead vs a fixed GenSize-16 run confirms it actually widened.
func TestAutoGenSize(t *testing.T) {
	t.Parallel()
	const (
		budget = 200_000 // generous (budget >> RTT)
		owd    = 20_000  // 40 ms RTT
		n      = 1600
		loss   = 0.05
		burst  = 4.0
	)
	mk := func(auto bool) Config {
		return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: budget, AutoGenSize: auto}
	}
	run := func(c Config) simResult {
		// 200 µs/sym ⇒ ~10 Mbps: fast enough that the fill gate admits the full width.
		return simLink{cfg: c, owdMicros: owd, srcMicros: 200, n: n, drop: geDrop(7, loss, burst)}.run()
	}
	auto, fixed := run(mk(true)), run(mk(false))

	assertDeliveryInvariants(t, auto) // varying width, both ends via stamps — desync would show here
	assertDeliveryInvariants(t, fixed)
	t.Logf("auto deliv=%.2f%% overhead=%.0f%% | fixed16 deliv=%.2f%% overhead=%.0f%%",
		100*float64(auto.delivered)/float64(n), 100*auto.overhead(),
		100*float64(fixed.delivered)/float64(n), 100*fixed.overhead())
	// AutoGenSize must not deliver materially LESS than fixed GenSize-16 (the alignment + safe-width
	// proof); both may sit a hair under 100% on a bursty finite stream, so compare to fixed, not 99%.
	if af, ff := float64(auto.delivered)/float64(n), float64(fixed.delivered)/float64(n); af < ff-0.01 {
		t.Fatalf("AutoGenSize regressed delivery: auto=%.2f%% vs fixed16=%.2f%%", 100*af, 100*ff)
	}
	if auto.overhead() >= fixed.overhead() {
		t.Fatalf("AutoGenSize did not reduce overhead (it should widen and amortize): auto=%.1f%% vs fixed=%.1f%%",
			100*auto.overhead(), 100*fixed.overhead())
	}
}

// TestAutoGenSizeRateChange proves AutoGenSize survives a mid-stream bitrate change — the case a
// STATIC width cannot handle: the source cadence drops 10× partway through, the sender re-measures
// its fill rate and re-sizes the generation (stamping the new width), and the receiver follows it.
// A width change is exactly where a fixed-stride scheme would desync, so the four invariants holding
// ACROSS the transition is the correctness proof; delivery stays complete throughout.
func TestAutoGenSizeRateChange(t *testing.T) {
	const n = 1600
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
		BufferMicros: 300_000, AutoGenSize: true}
	res := simLink{
		cfg:          cfg,
		owdMicros:    20_000, // 40 ms RTT
		srcMicros:    200,    // phase 1: ~10 Mbps ⇒ wide generations
		srcMicros2:   2_000,  // phase 2: ~1 Mbps  ⇒ the fill gate narrows them back
		rateChangeAt: n / 2,
		n:            n,
		drop:         geDrop(7, 0.05, 4),
	}.run()
	assertDeliveryInvariants(t, res) // alignment must hold across the width change
	if frac := float64(res.delivered) / float64(n); frac < 0.98 {
		t.Fatalf("delivered %.1f%% across the rate change (< 98%%)", 100*frac)
	}
	t.Logf("rate change 10→1 Mbps mid-stream: delivered %.1f%%, overhead %.0f%%",
		100*float64(res.delivered)/float64(n), 100*res.overhead())
}
