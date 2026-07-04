package flow

import "testing"

// TestSlidingResidual20pctIID is the deterministic repro of the sliding-coder
// recovery floor found over the live session: at 20% i.i.d. loss with a GENEROUS
// budget (4–6×RTT), the band (sliding) receiver plateaued ~98.5% delivery and
// MORE budget did not help — while the generation coder reached ~100% on the same
// channel. This drives the sans-I/O core directly (no sockets, no real clock) so
// the residual is reproducible and debuggable, and — crucially — uses the
// no-premature-drop oracle to split the residual into:
//
//   - premature   : the symbol ARRIVED and the ideal in-time decoder could recover
//     it, but the real receiver dropped it  → a receiver/decoder BUG.
//   - genuineLoss : the ideal decoder could NOT recover it in time from what arrived
//     → the SENDER under-provisioned repair (a sizing issue, not a
//     receiver bug).
//
// Which bucket the floor lands in tells us where to fix it.
func TestSlidingResidual20pctIID(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("slow deterministic sweep; run without -short")
	}
	base := Config{Flow: 1, SymbolSize: 64, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3}
	const n, seeds = 2000, 4
	// owd 50ms ⇒ RTT 100ms; srcMicros 500 ≈ a 20 Mbps 1.3 KB-symbol cadence; budgets
	// 400/600 ms = 4×/6×RTT — the generous regime where the live floor persisted.
	const owd, src = int64(50_000), int64(500)

	for _, budgetMs := range []int64{400, 600} {
		for _, sliding := range []bool{false, true} {
			cfg := base
			cfg.BufferMicros = budgetMs * 1000
			var deliv, prem, gen int
			for s := 0; s < seeds; s++ {
				p := oracleParams{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: sliding}
				delivered, tape, writeAt, late := oracleRun(t, p, uniformDrop(uint64(0x5000+s), 0.20))
				res := analyzeOracle(p, delivered, tape, writeAt, late)
				deliv += res.delivered
				prem += res.premature
				gen += res.genuineLoss
			}
			total := n * seeds
			coder := "generation"
			if sliding {
				coder = "sliding   "
			}
			t.Logf("%s budget=%dms (%.1f×RTT): delivered=%d/%d (%.3f%%)  premature=%d  genuineLoss=%d",
				coder, budgetMs, float64(budgetMs)/100.0, deliv, total,
				100*float64(deliv)/float64(total), prem, gen)

			if !sliding {
				// The generation coder is the control: it MUST stay perfect on this
				// channel (the band defect is sliding-specific).
				if prem != 0 || gen != 0 || deliv != total {
					t.Errorf("generation control regressed at %.1f×RTT: delivered=%d/%d premature=%d genuineLoss=%d (want 8000/8000, 0, 0)",
						float64(budgetMs)/100.0, deliv, total, prem, gen)
				}
				continue
			}
			// FIXED by the sliding cold-start (SlidingSender.codeRate sizes the proactive
			// rate for coldStartP assumed loss until the loss estimate primes ~1×RTT in).
			// Both the warmup genuineLoss (sender under-provision before feedback can
			// arrive) and the premature drops it caused are gone — strict 0 now, so this
			// guards the fix: any regression trips. Before the fix this was ≈ 71 premature
			// / 163 genuineLoss over 8000 at 97.075% delivered.
			if deliv != total || prem != 0 || gen != 0 {
				t.Errorf("sliding residual regressed at %.1f×RTT: delivered=%d/%d premature=%d genuineLoss=%d (want %d/%d, 0, 0)",
					float64(budgetMs)/100.0, deliv, total, prem, gen, total, total)
			}
		}
	}
}
