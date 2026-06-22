package flow

import "testing"

// TestBudgetBelowRTTProfileTradeoff characterizes and pins the generation-vs-sliding tradeoff
// the bench's -lowlat sweep exposes: when the latency budget is SMALLER than the RTT, a full
// generation cannot fill, close, and have its repair arrive inside the sub-RTT deadline, so the
// generation coder collapses (bench: 50–57% at 100ms RTT / 60ms budget) — while the band-form
// sliding coder, doing continuous RTT-independent proactive repair with an adaptively narrowed
// band, recovers within the budget (bench: 96–97%). This asserts the sliding profile is the
// right tool in this regime, and serves as the guard for a future adapter that AUTO-SELECTS it
// when budget < ~RTT (today the caller must set Config.Sliding by hand — the open gap).
func TestBudgetBelowRTTProfileTradeoff(t *testing.T) {
	const (
		n         = 2000
		seed      = 7
		symSize   = 256
		lossP     = 0.10
		delayTick = 50 // 50 ms one-way ⇒ 100 ms RTT
		budgetMs  = 60 // budget < RTT
	)
	base := Config{Flow: 1, SymbolSize: symSize, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: budgetMs * 1000}

	gen := measure(NewSender(base), NewReceiver(base), base, n, seed, lossP, delayTick)
	genDeliv := float64(gen.delivered) / float64(gen.n)

	sld := base
	sld.Sliding = true
	sld.CodingWindow = 64
	sldRes := measure(NewSlidingSender(sld), NewSlidingReceiver(sld), sld, n, seed, lossP, delayTick)
	sldDeliv := float64(sldRes.delivered) / float64(sldRes.n)

	t.Logf("budget %dms < RTT %dms @ %.0f%% loss: generation coder=%.1f%% delivered, sliding coder=%.1f%% delivered",
		budgetMs, delayTick*2, lossP*100, 100*genDeliv, 100*sldDeliv)

	// Both coders deliver well here now. The generation coder used to COLLAPSE in this regime
	// (the bench's ~50–57%); the per-generation reactive sizing lifted it to ~93% — reactive
	// repair still helps a generation's later symbols, whose deadlines have not yet passed when
	// feedback arrives, even when budget < RTT. The sliding profile remains the SAFER choice in
	// this regime (continuous RTT-independent proactive repair), so it must deliver at least as
	// well as the generation coder; this guards that property and the generation coder's
	// recovered robustness. Neither may deliver past a deadline (checked by the invariant fuzz).
	if genDeliv < 0.85 {
		t.Fatalf("generation coder regressed in budget<RTT: %.1f%% (was lifted to ~93%% by per-gen reactive sizing)", 100*genDeliv)
	}
	if sldDeliv < genDeliv-0.02 {
		t.Fatalf("sliding profile should be at least as good as the generation coder where budget<RTT: %.1f%% vs %.1f%%", 100*sldDeliv, 100*genDeliv)
	}
}
