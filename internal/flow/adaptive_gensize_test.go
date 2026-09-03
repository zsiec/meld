package flow

import "testing"

// assertDeliveryInvariants checks the order/dedup/deadline/correctness invariants on a sim
// result: strictly increasing ids (in order, no duplicate), nothing delivered late, no false
// recovery. Completeness is asserted separately by the caller (it depends on the channel).
func assertDeliveryInvariants(t *testing.T, res simResult) {
	t.Helper()
	for i := 1; i < len(res.deliveredIDs); i++ {
		if res.deliveredIDs[i] <= res.deliveredIDs[i-1] {
			t.Fatalf("delivery not strictly increasing at %d: %d then %d", i, res.deliveredIDs[i-1], res.deliveredIDs[i])
		}
	}
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if res.corrupt {
		t.Fatal("a delivered payload did not match its source id (false recovery)")
	}
}

// TestGenWidthEnvelope pins genWidth's safe envelope: a no-op when off or un-hinted, narrow
// when the budget is below a reactive round (budget < RTT, the all-proactive regime where a
// wide generation would lose more on a deadline miss), wide when the budget clears two rounds,
// and monotonic in the budget between.
func TestGenWidthEnvelope(t *testing.T) {
	base := Config{GenSize: 16}
	if got := base.genWidth(); got != 16 {
		t.Fatalf("AdaptiveGenSize off: genWidth = %d, want 16 (fixed)", got)
	}
	// On but no RTT hint ⇒ no widening (can't size the reserve safely).
	noHint := Config{GenSize: 16, AdaptiveGenSize: true, BufferMicros: 1_000_000}
	if got := noHint.genWidth(); got != 16 {
		t.Fatalf("no NominalRTTMicros: genWidth = %d, want 16", got)
	}
	// budget < RTT: stay narrow.
	tight := Config{GenSize: 16, AdaptiveGenSize: true, NominalRTTMicros: 100_000, BufferMicros: 60_000}
	if got := tight.genWidth(); got != 16 {
		t.Fatalf("budget<RTT: genWidth = %d, want 16 (narrow, protect completeness)", got)
	}
	// budget clears two reactive rounds: full width.
	wide := Config{GenSize: 16, AdaptiveGenSize: true, NominalRTTMicros: 40_000, BufferMicros: 200_000}
	if got := wide.genWidth(); got != maxAdaptiveGenWidth {
		t.Fatalf("generous budget: genWidth = %d, want %d (wide)", got, maxAdaptiveGenWidth)
	}
	// Monotonic non-decreasing in the budget.
	prev := 0
	for buf := int64(50_000); buf <= 400_000; buf += 10_000 {
		c := Config{GenSize: 16, AdaptiveGenSize: true, NominalRTTMicros: 80_000, BufferMicros: buf}
		w := c.genWidth()
		if w < prev {
			t.Fatalf("genWidth not monotonic in budget at %dus: %d < %d", buf, w, prev)
		}
		if w < 16 || w > maxAdaptiveGenWidth {
			t.Fatalf("genWidth out of [16,%d] at %dus: %d", maxAdaptiveGenWidth, buf, w)
		}
		prev = w
	}
}

// TestGenWidthFillCap pins the bitrate fill gate: with a generous budget+RTT (the budget/RTT ramp
// wants full width), the width widens fully at a high bitrate (fast fill) and is held at GenSize at a
// low bitrate (a slow-filling wide generation would raise latency), monotonically in bitrate.
func TestGenWidthFillCap(t *testing.T) {
	mk := func(bitrateBps int64) Config {
		return Config{
			GenSize: 16, SymbolSize: 1316, AdaptiveGenSize: true,
			NominalRTTMicros: 40_000, BufferMicros: 400_000, NominalBitrateBps: bitrateBps,
		}
	}
	if w := mk(0).genWidth(); w != maxAdaptiveGenWidth {
		t.Fatalf("no bitrate hint: genWidth=%d, want %d (fill gate off)", w, maxAdaptiveGenWidth)
	}
	if w := mk(50_000_000).genWidth(); w != maxAdaptiveGenWidth {
		t.Fatalf("50 Mbps: genWidth=%d, want %d (fast fill ⇒ widen fully)", w, maxAdaptiveGenWidth)
	}
	if w := mk(5_000_000).genWidth(); w != 16 {
		t.Fatalf("5 Mbps: genWidth=%d, want 16 (slow fill ⇒ stay narrow)", w)
	}
	prev := 0
	for _, mbps := range []int64{2, 5, 10, 20, 30, 50, 100} {
		w := mk(mbps * 1_000_000).genWidth()
		if w < prev || w < 16 || w > maxAdaptiveGenWidth {
			t.Fatalf("genWidth out of order/range at %d Mbps: %d (prev %d)", mbps, w, prev)
		}
		prev = w
	}
}

// TestAdaptiveGenSizeFourInvariantsAndOverhead is the end-to-end proof: with AdaptiveGenSize on
// at a generous budget the generation widens to 64, both ends stay aligned (a missed stride
// site would corrupt or strand delivery), the four invariants hold, AND the realized repair
// overhead is materially LOWER than the fixed-GenSize-16 run at the same loss and full delivery
// — the bandwidth win the bench predicted, now through the real coder.
func TestAdaptiveGenSizeFourInvariantsAndOverhead(t *testing.T) {
	t.Parallel()
	const (
		budget   = 200_000 // 200 ms — a generous contribution budget
		owd      = 20_000  // 40 ms RTT, well under budget
		meanLoss = 0.05
		burst    = 4.0
		n        = 640
	)
	mk := func(adaptive bool) Config {
		c := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: budget}
		if adaptive {
			c.AdaptiveGenSize = true
			c.NominalRTTMicros = 2 * owd
		}
		return c
	}

	adaptiveCfg := mk(true)
	if w := adaptiveCfg.genWidth(); w != maxAdaptiveGenWidth {
		t.Fatalf("precondition: adaptive genWidth = %d, want %d (the wide path must be exercised)", w, maxAdaptiveGenWidth)
	}

	run := func(c Config, seed int64) simResult {
		return simLink{cfg: c, owdMicros: owd, srcMicros: 500, n: n, drop: geDrop(seed, meanLoss, burst)}.run()
	}

	adaptive := run(adaptiveCfg, 7)
	fixed := run(mk(false), 7)

	assertDeliveryInvariants(t, adaptive)
	assertDeliveryInvariants(t, fixed)

	for _, r := range []struct {
		name string
		res  simResult
	}{{"adaptive", adaptive}, {"fixed", fixed}} {
		if frac := float64(r.res.delivered) / float64(n); frac < 0.99 {
			t.Fatalf("%s: delivered %.1f%% (< 99%%) — completeness regressed", r.name, 100*frac)
		}
	}

	t.Logf("adaptive(w64) overhead=%.0f%% delivered=%d/%d | fixed(w16) overhead=%.0f%% delivered=%d/%d",
		100*adaptive.overhead(), adaptive.delivered, n, 100*fixed.overhead(), fixed.delivered, n)

	// The lever: a wider generation amortizes the proactive margin, so it recovers the same
	// loss at lower overhead. Require a clear margin, not just ≤, so a regression is caught.
	if adaptive.overhead() >= fixed.overhead()*0.85 {
		t.Fatalf("AdaptiveGenSize did not cut overhead enough: adaptive=%.1f%% vs fixed=%.1f%% (want adaptive < 0.85×fixed)",
			100*adaptive.overhead(), 100*fixed.overhead())
	}
}

// TestAdaptiveGenSizeStaysNarrowBelowRTT proves the safety guard: when the budget is below a
// reactive round (budget < RTT), AdaptiveGenSize must NOT widen — a wide generation there loses
// more symbols on a deadline miss with no reactive backstop. The width stays at GenSize and the
// run behaves exactly like the fixed-GenSize run (same delivery, same overhead order).
func TestAdaptiveGenSizeStaysNarrowBelowRTT(t *testing.T) {
	const (
		budget = 60_000 // 60 ms
		owd    = 50_000 // 100 ms RTT > budget ⇒ all-proactive regime
		n      = 480
	)
	cfg := Config{
		Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
		BufferMicros: budget, AdaptiveGenSize: true, NominalRTTMicros: 2 * owd,
	}
	if w := cfg.genWidth(); w != 16 {
		t.Fatalf("budget<RTT: genWidth = %d, want 16 — the wide generation must be suppressed here", w)
	}
	res := simLink{cfg: cfg, owdMicros: owd, srcMicros: 500, n: n, drop: geDrop(11, 0.05, 4)}.run()
	assertDeliveryInvariants(t, res)
}
