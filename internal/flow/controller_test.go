package flow

import (
	"math"
	"testing"
)

// TestBinomTailGreater checks the tail against a hand-computed value:
// X ~ Binomial(10, 0.5), P[X > 5] = (C(10,6)+...+C(10,10))/1024 = 386/1024.
func TestBinomTailGreater(t *testing.T) {
	got := binomTailGreater(10, 0.5, 5)
	want := 386.0 / 1024.0
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("binomTailGreater(10,0.5,5) = %.12f, want %.12f", got, want)
	}
	if binomTailGreater(8, 0.3, 8) != 0 { // r >= n ⇒ tail 0
		t.Fatal("tail past n should be 0")
	}
	// Full-sum sanity: P[X>=0] over the complementary tail at r=-... use r=0 path.
	// P[X>0] = 1 - (1-p)^n.
	if g, w := binomTailGreater(5, 0.2, 0), 1-math.Pow(0.8, 5); math.Abs(g-w) > 1e-12 {
		t.Fatalf("P[X>0] = %.12f, want %.12f", g, w)
	}
}

// TestRepairForTargetAchievesTarget checks the feed-forward set-point actually
// meets the target decode-failure probability and is minimal.
func TestRepairForTargetAchievesTarget(t *testing.T) {
	for _, k := range []int{8, 16, 32} {
		for _, p := range []float64{0.05, 0.1, 0.2, 0.3, 0.4} {
			for _, delta := range []float64{1e-2, 1e-3, 1e-4} {
				r := repairForTarget(k, p, delta)
				cap := k * maxRepairFactor
				if r < 0 || r > cap {
					t.Fatalf("k=%d p=%g d=%g: r=%d out of range", k, p, delta, r)
				}
				if r == cap {
					continue // saturated; can't assert minimality at the cap
				}
				// Meets the target...
				if got := binomTailGreater(k+r, p, r); got > delta {
					t.Fatalf("k=%d p=%g d=%g: r=%d gives P_fail=%.3g > %g", k, p, delta, r, got, delta)
				}
				// ...and is minimal: r-1 would miss it.
				if r > 0 && binomTailGreater(k+r-1, p, r-1) <= delta {
					t.Fatalf("k=%d p=%g d=%g: r=%d not minimal", k, p, delta, r)
				}
				// At least the mean (k*p), since the tail above the mean is large.
				if float64(r) < float64(k)*p-1 {
					t.Fatalf("k=%d p=%g d=%g: r=%d below the mean k*p=%.1f", k, p, delta, r, float64(k)*p)
				}
			}
		}
	}
}

// TestRepairForTargetMonotone checks redundancy rises with loss and with a
// stricter target — the qualitative behavior the controller relies on.
func TestRepairForTargetMonotone(t *testing.T) {
	const k = 16
	// Increasing p ⇒ non-decreasing r.
	prev := -1
	for _, p := range []float64{0.0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5} {
		r := repairForTarget(k, p, 1e-3)
		if r < prev {
			t.Fatalf("r not monotone in p: p=%g gave %d < %d", p, r, prev)
		}
		prev = r
	}
	// Stricter target (smaller delta) ⇒ at least as much redundancy.
	if repairForTarget(k, 0.2, 1e-5) < repairForTarget(k, 0.2, 1e-2) {
		t.Fatal("smaller delta should not reduce redundancy")
	}
	// The variance margin: at 20% loss with k=16 the set-point exceeds the mean
	// (3.2) — this is the term a mean-tracking AIMD omits.
	if r := repairForTarget(16, 0.2, 1e-3); r <= 3 {
		t.Fatalf("expected a variance margin above the mean ~3.2, got r=%d", r)
	}
}
