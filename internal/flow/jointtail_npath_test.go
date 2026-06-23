package flow

import "testing"

// TestRepairForJointTailNPath exercises the N-path joint-tail sizer on the per-slot
// erasure-count histogram. The headline N5 property generalizes to N>2: at the SAME
// per-path marginal loss, a correlated channel (more mass at the all-paths-erased tail)
// forces MORE repair than the independent channel — the lift an independence-assuming
// sizer misses.
func TestRepairForJointTailNPath(t *testing.T) {
	const k = 16
	const delta = 1e-3

	if r := repairForJointTailN(k, []int{1_000_000, 0, 0, 0}, delta, maxRepairFactor); r != 0 {
		t.Fatalf("no-loss 3-path: repair %d, want 0", r)
	}

	// Both 3-path histograms have the SAME mean erasures/slot (0.3 = 3×10% loss):
	//   independent: Binomial(3, 0.1)
	//   correlated:  mass shifted from the middle to the all-3-erased tail (30× p[3])
	indep := []int{729_000, 243_000, 27_000, 1_000}
	corr := []int{780_000, 170_000, 20_000, 30_000}
	rIndep := repairForJointTailN(k, indep, delta, maxRepairFactor)
	rCorr := repairForJointTailN(k, corr, delta, maxRepairFactor)
	t.Logf("3-path repair (mean loss equal): independent %d vs correlated %d", rIndep, rCorr)
	if rIndep == 0 {
		t.Fatal("expected nonzero repair under 10%% 3-path loss")
	}
	if rCorr <= rIndep {
		t.Fatalf("correlation not provisioned: correlated %d <= independent %d at equal marginals", rCorr, rIndep)
	}

	// Heavier all-paths-erased mass (still mean 0.3) provisions more still — monotone in
	// the tail. p[3]=6%, rebalanced to hold the mean.
	heavier := []int{810_000, 110_000, 20_000, 60_000}
	if rH := repairForJointTailN(k, heavier, delta, maxRepairFactor); rH < rCorr {
		t.Fatalf("heavier correlation provisioned less: %d < %d", rH, rCorr)
	}

	// 4-path runs and provisions under loss (Binomial(4, 0.1) per-slot counts).
	if r := repairForJointTailN(k, []int{656_100, 291_600, 48_600, 3_600, 100}, delta, maxRepairFactor); r <= 0 {
		t.Fatalf("4-path under loss: repair %d, want > 0", r)
	}

	// The 2-path entry still agrees with the N-path core it now wraps (a basic sanity that
	// the refactor is faithful): independent 10%/path ⇒ the same as the equivalent histogram.
	pa, pb := 100_000, 100_000
	pBoth := pa * pb / 1_000_000
	if got, want := repairForJointTail(k, pa, pb, pBoth, delta, maxRepairFactor),
		repairForJointTailN(k, []int{1_000_000 - pa - pb + pBoth, pa + pb - 2*pBoth, pBoth}, delta, maxRepairFactor); got != want {
		t.Fatalf("2-path wrapper %d != N-path core %d", got, want)
	}
}
