package flow

import (
	"math/rand"
	"testing"
)

// symbolsForDeficit is the reactive-repair batch sizer: given a rank deficit of `deficit`
// over an erasure channel of probability p, it returns the number of fresh repair symbols
// to send so that at least `deficit` of them ARRIVE (clearing the deficit in one round)
// with probability >= 1-delta. It is the sole sizing input to Sender.reactiveRepair, yet
// it had no test — these pin the contract the reactive overhead depends on.

// TestSymbolsForDeficitDegenerate covers the boundary inputs.
func TestSymbolsForDeficitDegenerate(t *testing.T) {
	if r := symbolsForDeficit(0, 0.1, 1e-3, maxRepairFactor); r != 0 {
		t.Fatalf("zero deficit must need no repair, got %d", r)
	}
	if r := symbolsForDeficit(-3, 0.1, 1e-3, maxRepairFactor); r != 0 {
		t.Fatalf("negative deficit must need no repair, got %d", r)
	}
	// A total-loss channel saturates at the cap rather than looping forever.
	maxR := 4*maxRepairFactor + 4
	if r := symbolsForDeficit(4, 1.0, 1e-3, maxRepairFactor); r != maxR {
		t.Fatalf("total-loss channel should saturate at the cap %d, got %d", maxR, r)
	}
}

// TestSymbolsForDeficitZeroLoss guards the sizer's p=0 behavior. It used to return the CAP
// (deficit*maxRepairFactor+4) because binomTailGreater(r, q=1-p, …) divided by 1-q = 0 at q = 1
// → NaN, so the loop saturated. That over-send was LOAD-BEARING — Sender.reactiveRepair passed
// the global pEst, which reports 0 through the estimator's warmup window, so the cap masked the
// blind spot. The proper fix was twofold: (1) reactiveRepair now sizes against an accurate,
// lag-free PER-GENERATION loss estimate (from the deficit and the repair already sent), so it
// never passes p=0 with a real deficit; (2) the sizer itself is now exact at the degenerate
// probabilities. So this can assert the correct minimal value.
func TestSymbolsForDeficitZeroLoss(t *testing.T) {
	if got := symbolsForDeficit(5, 0.0, 1e-3, maxRepairFactor); got != 5 {
		t.Fatalf("lossless channel should need exactly the deficit (5), got %d", got)
	}
	if got := symbolsForDeficit(5, 1e-6, 1e-3, maxRepairFactor); got > 7 {
		t.Fatalf("near-lossless channel should need ~deficit symbols, got %d", got)
	}
}

// TestSymbolsForDeficitAchievesTarget checks the batch actually clears the deficit with
// probability >= 1-delta (the exact binomial-arrival contract), and never sends fewer than
// the deficit itself.
func TestSymbolsForDeficitAchievesTarget(t *testing.T) {
	for _, deficit := range []int{1, 2, 4, 8} {
		for _, p := range []float64{0.05, 0.1, 0.2, 0.3, 0.4} {
			for _, delta := range []float64{1e-2, 1e-3} {
				r := symbolsForDeficit(deficit, p, delta, maxRepairFactor)
				if r < deficit {
					t.Fatalf("deficit=%d p=%g: r=%d < deficit (can't clear it even losslessly)", deficit, p, r)
				}
				cap := deficit*maxRepairFactor + 4
				if r > cap {
					t.Fatalf("deficit=%d p=%g: r=%d exceeds the cap %d", deficit, p, r, cap)
				}
				if r == cap {
					continue // saturated; the target may be unmeetable under the cap
				}
				// P[arrivals >= deficit] = P[Binomial(r, 1-p) > deficit-1] must meet 1-delta.
				if got := binomTailGreater(r, 1-p, deficit-1); got < 1-delta {
					t.Fatalf("deficit=%d p=%g d=%g: r=%d clears with only %.4f < %.4f", deficit, p, delta, r, got, 1-delta)
				}
				// Minimal: r-1 would miss the target (so we never over-provision by construction).
				if r > deficit && binomTailGreater(r-1, 1-p, deficit-1) >= 1-delta {
					t.Fatalf("deficit=%d p=%g d=%g: r=%d not minimal", deficit, p, delta, r)
				}
			}
		}
	}
}

// TestSymbolsForDeficitMonotone checks the size rises with the deficit and with loss — the
// qualitative behavior the reactive loop relies on.
func TestSymbolsForDeficitMonotone(t *testing.T) {
	// Non-decreasing in deficit.
	prev := -1
	for d := 0; d <= 10; d++ {
		r := symbolsForDeficit(d, 0.2, 1e-3, maxRepairFactor)
		if r < prev {
			t.Fatalf("not monotone in deficit: d=%d gave %d < %d", d, r, prev)
		}
		prev = r
	}
	// Non-decreasing in loss, over the working range p>0 (p=0 is the over-send bug pinned
	// separately in TestSymbolsForDeficitZeroLossOverSend).
	prev = -1
	for _, p := range []float64{0.05, 0.1, 0.2, 0.3, 0.4, 0.5} {
		r := symbolsForDeficit(4, p, 1e-3, maxRepairFactor)
		if r < prev {
			t.Fatalf("not monotone in p: p=%g gave %d < %d", p, r, prev)
		}
		prev = r
	}
}

// TestSymbolsForDeficitEmpirical confirms the sizing against simulated channel draws: a
// batch of r symbols clears the deficit at least 1-delta of the time. This is the oracle
// behind the closed-form binomial — the reactive batch is dimensioned to win in one round.
func TestSymbolsForDeficitEmpirical(t *testing.T) {
	const (
		delta  = 1e-2
		trials = 200_000
	)
	rng := rand.New(rand.NewSource(42))
	for _, deficit := range []int{2, 5} {
		for _, p := range []float64{0.1, 0.25} {
			r := symbolsForDeficit(deficit, p, delta, maxRepairFactor)
			if r >= deficit*maxRepairFactor+4 {
				continue // saturated
			}
			fails := 0
			for i := 0; i < trials; i++ {
				arrived := 0
				for j := 0; j < r; j++ {
					if rng.Float64() >= p {
						arrived++
					}
				}
				if arrived < deficit {
					fails++
				}
			}
			f := float64(fails) / trials
			t.Logf("deficit=%d p=%g: r=%d empirical clear-failure %.4f (target %.0e)", deficit, p, r, f, delta)
			if f > 3*delta {
				t.Fatalf("deficit=%d p=%g: r=%d clears too rarely (failure %.4f > %.4f)", deficit, p, r, f, 3*delta)
			}
		}
	}
}
