package flow

import "testing"

// TestSlidingResidual20pctIID pins conservative startup protection at 20% iid
// loss with a generous 4–6×RTT budget. It drives the sans-I/O core directly and
// uses the no-premature-drop oracle to split any residual into:
//
//   - premature   : the symbol ARRIVED and the ideal in-time decoder could recover
//     it, but the real receiver dropped it  → a receiver/decoder BUG.
//   - genuineLoss : the ideal decoder could NOT recover it in time from what arrived
//     → the SENDER under-provisioned repair (a sizing issue, not a
//     receiver bug).
//
// The split distinguishes decoder loss from insufficient emitted rank.
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
			var prematureIDs []uint32
			for s := 0; s < seeds; s++ {
				p := oracleParams{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: sliding}
				delivered, tape, writeAt, late := oracleRun(t, p, uniformDrop(uint64(0x5000+s), 0.20))
				res := analyzeOracle(p, delivered, tape, writeAt, late)
				deliv += res.delivered
				prem += res.premature
				gen += res.genuineLoss
				if len(prematureIDs) < 12 {
					prematureIDs = append(prematureIDs, res.prematureIDs...)
					if len(prematureIDs) > 12 {
						prematureIDs = prematureIDs[:12]
					}
				}
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
			// The sliding sender sizes its initial proactive rate for coldStartP until
			// feedback is credible, covering the flight that reactive repair cannot yet
			// observe.
			if deliv != total || prem != 0 || gen != 0 {
				t.Errorf("sliding residual regressed at %.1f×RTT: delivered=%d/%d premature=%d genuineLoss=%d ids=%v (want %d/%d, 0, 0)",
					float64(budgetMs)/100.0, deliv, total, prem, gen, prematureIDs, total, total)
			}
		}
	}
}
