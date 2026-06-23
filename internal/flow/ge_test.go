package flow

import (
	"math/rand"
	"testing"
)

// N2 burst-aware sizer oracles.

// geTailFloat is the exact float reference for geTailGreater: the same forward HMM
// erasure-count recursion in float64. The fixed-point DP must agree with it.
func geTailFloat(n int, pGB, pBG, piB float64, r int) float64 {
	g := make([]float64, r+1)
	b := make([]float64, r+1)
	g[0], b[0] = 1-piB, piB
	for step := 0; step < n; step++ {
		ng := make([]float64, r+1)
		nb := make([]float64, r+1)
		for k := 0; k <= r; k++ {
			ng[k] = (1-pGB)*g[k] + pBG*b[k]
			if k > 0 {
				nb[k] = pGB*g[k-1] + (1-pBG)*b[k-1]
			}
		}
		g, b = ng, nb
	}
	cdf := 0.0
	for k := 0; k <= r; k++ {
		cdf += g[k] + b[k]
	}
	return 1 - cdf
}

// TestGETailFixedMatchesFloat: the integer Q30 DP agrees with the exact float DP over
// a sweep of channel parameters — so the determinism-preserving fixed-point form is
// numerically faithful, not just reproducible.
func TestGETailFixedMatchesFloat(t *testing.T) {
	for _, pBG := range []float64{1.0, 0.5, 0.2, 0.1} { // mean burst 1, 2, 5, 10
		for _, pGB := range []float64{0.01, 0.05, 0.2} {
			piB := pGB / (pGB + pBG)
			for _, n := range []int{16, 40, 96} {
				for _, r := range []int{0, 2, 8, 20} {
					if r >= n {
						continue
					}
					want := geTailFloat(n, pGB, pBG, piB, r)
					gotQ := geTailGreater(n,
						int64(pGB*geScale), int64(pBG*geScale), int64(piB*geScale), r)
					got := float64(gotQ) / geScale
					if d := got - want; d > 1e-4 || d < -1e-4 {
						t.Fatalf("n=%d r=%d pGB=%g pBG=%g: fixed %.6f vs float %.6f (Δ%.2e)",
							n, r, pGB, pBG, got, want, d)
					}
				}
			}
		}
	}
}

// geErasures simulates the erasure count over n symbols of the two-parameter Gilbert
// channel (good lossless, bad total loss) used by repairForGE, starting in steady
// state — the exact channel the sizer models.
func geErasures(rng *rand.Rand, n int, pGB, pBG, piB float64) int {
	bad := rng.Float64() < piB
	e := 0
	for i := 0; i < n; i++ {
		if bad {
			e++
			if rng.Float64() < pBG {
				bad = false
			}
		} else if rng.Float64() < pGB {
			bad = true
			// the transition takes effect this symbol (erased)
			e++
		}
	}
	return e
}

// TestGESizerHoldsWhereBinomialFails is the money test: on a bursty channel at the
// SAME mean loss, the i.i.d. binomial sizer silently misses its target decode-failure
// probability by a wide margin, while the burst-aware GE sizer holds it.
func TestGESizerHoldsWhereBinomialFails(t *testing.T) {
	const (
		k         = 32
		delta     = 1e-3
		pMean     = 0.10
		meanBurst = 6.0
		trials    = 300_000
	)
	pBG := 1 / meanBurst
	pGB := pMean * pBG / (1 - pMean)
	piB := pGB / (pGB + pBG)

	rBinom := repairForTarget(k, pMean, delta, maxRepairFactor)
	rGE := repairForGE(k, int(pMean*1e6), int(meanBurst*256), delta, maxRepairFactor)
	if rGE <= rBinom {
		t.Fatalf("burst-aware sizer (%d) should provision more than the binomial sizer (%d)", rGE, rBinom)
	}

	rng := rand.New(rand.NewSource(7))
	var failBinom, failGE int
	for i := 0; i < trials; i++ {
		if geErasures(rng, k+rBinom, pGB, pBG, piB) > rBinom {
			failBinom++
		}
		if geErasures(rng, k+rGE, pGB, pBG, piB) > rGE {
			failGE++
		}
	}
	fBinom := float64(failBinom) / trials
	fGE := float64(failGE) / trials
	t.Logf("mean loss %.0f%% burst %.0f: binomial r=%d fails %.2e, GE r=%d fails %.2e (target %.0e)",
		pMean*100, meanBurst, rBinom, fBinom, rGE, fGE, delta)

	if fGE > 5*delta {
		t.Fatalf("GE sizer (r=%d) decode-failure %.2e exceeds the %.0e target", rGE, fGE, delta)
	}
	if fBinom < 10*delta {
		t.Fatalf("binomial sizer failure %.2e is not far past target — the burst regime is not exercised", fBinom)
	}
}
