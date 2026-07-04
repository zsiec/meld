package flow

import (
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// geDrop returns a stateful two-state Gilbert-Elliott loss predicate (good lossless, bad
// total loss) at a target marginal loss and mean burst length — the correlated channel the
// burst-aware sizer (repairForGE) exists for. Unlike uniformDrop it has MEMORY, so it must be
// driven in wire order (which simLink does). At meanBurst 1 it reduces to i.i.d.
func geDrop(seed int64, meanLoss, meanBurst float64) func(wire.Symbol) bool {
	pBG := 1 / meanBurst
	pGB := meanLoss * pBG / (1 - meanLoss)
	piB := pGB / (pGB + pBG) // steady-state bad fraction == meanLoss (held constant across bursts)
	rng := rand.New(rand.NewSource(seed))
	bad := rng.Float64() < piB
	return func(wire.Symbol) bool {
		// Emit based on the CURRENT state, then transition — so the marginal loss is exactly piB
		// regardless of mean burst length (the fixed-mean-loss sweep the bench describes).
		lost := bad
		if bad {
			if rng.Float64() < pBG {
				bad = false
			}
		} else if rng.Float64() < pGB {
			bad = true
		}
		return lost
	}
}

// TestBurstRecoveryGenerationHolds is the flow-level analog of the controller's
// TestGESizerHoldsWhereBinomialFails: at a FIXED mean loss, as the mean burst length grows
// (the regime where an i.i.d.-sized FEC silently under-protects a long run), the generation
// coder must HOLD delivery — paying for it in overhead (the burst-aware sizer ramping up), not
// in dropped frames. This mirrors the txbench -ge sweep where meld holds ~100% while overhead
// climbs 119→156→173%.
func TestBurstRecoveryGenerationHolds(t *testing.T) {
	t.Parallel()
	const (
		budget   = 200_000
		meanLoss = 0.10
	)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: budget}
	bursts := []float64{1, 2, 5, 10}
	ovhd := make([]float64, len(bursts))
	for i, b := range bursts {
		res := simLink{
			cfg:       cfg,
			owdMicros: 20_000, // 40 ms RTT, well under budget
			srcMicros: 500,
			n:         640,
			drop:      geDrop(int64(100+i), meanLoss, b),
		}.run()
		ovhd[i] = res.overhead()
		frac := float64(res.delivered) / float64(res.n)
		realized := float64(res.stats.WireLost) / float64(res.stats.WireLost+res.stats.Delivered+res.stats.Recovered)
		t.Logf("burst=%4.0f: delivered=%.1f%% overhead=%4.0f%% wireLost=%d (realized loss≈%.1f%%)",
			b, 100*frac, 100*ovhd[i], res.stats.WireLost, 100*realized)
		if res.lateDeliv {
			t.Fatalf("burst=%g: a symbol was delivered past its deadline", b)
		}
		if frac < 0.97 {
			t.Fatalf("burst=%g: generation coder degraded to %.1f%% delivery — under-protecting the burst", b, 100*frac)
		}
	}
	// The burst-aware set-point must ENGAGE: protecting longer bursts at fixed mean loss costs
	// more overhead than the i.i.d. case. If overhead were flat, the sizer is ignoring burst.
	if ovhd[len(ovhd)-1] <= ovhd[0] {
		t.Fatalf("overhead did not rise with burst length (%.0f%% at burst 1 vs %.0f%% at burst 10) — burst sizer not engaging",
			100*ovhd[0], 100*ovhd[len(ovhd)-1])
	}
}

// TestBurstRecoverySlidingDegradation characterizes the band-form sliding coder under the same
// burst sweep, where the bench shows it DEGRADES (97.8→93.8%) and its allocation explodes. It
// asserts only a loose delivery floor (the coder must not collapse), logging the curve so the
// degradation — the gap to close — is visible and tracked rather than silent.
func TestBurstRecoverySlidingDegradation(t *testing.T) {
	t.Parallel()
	const (
		budget   = 200_000
		meanLoss = 0.10
	)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: budget, Sliding: true, CodingWindow: 64}
	for i, b := range []float64{1, 2, 5, 10} {
		res := simLink{
			cfg:       cfg,
			owdMicros: 20_000,
			srcMicros: 500,
			n:         640,
			drop:      geDrop(int64(200+i), meanLoss, b),
			sliding:   true,
		}.run()
		frac := float64(res.delivered) / float64(res.n)
		t.Logf("sliding burst=%4.0f: delivered=%.1f%% overhead=%4.0f%%", b, 100*frac, 100*res.overhead())
		if res.lateDeliv {
			t.Fatalf("sliding burst=%g: delivery past deadline", b)
		}
		if frac < 0.80 {
			t.Fatalf("sliding burst=%g: collapsed to %.1f%% (below the 80%% floor)", b, 100*frac)
		}
	}
}
