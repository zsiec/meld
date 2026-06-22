package flow

import (
	"math"
	"math/rand"
	"testing"
)

func ppm(p float64) int { return int(p*1e6 + 0.5) }

// drawSlot samples one aligned 2-path slot's erasure count (0/1/2) from the joint
// distribution with marginals pa/pb and joint-erasure probability pBoth.
func drawSlot(rng *rand.Rand, pNone, pTwo float64) int {
	u := rng.Float64()
	switch {
	case u < pNone:
		return 0
	case u < pNone+pTwo:
		return 2
	default:
		return 1
	}
}

// TestJointTailHoldsWhereIIDFails is the multipath money test: as two equally-lossy
// paths become correlated (both go bad together), the i.i.d.-union sizer — which
// assumes independence — silently under-provisions and misses its decode-failure
// target, while the correlation-aware joint-tail sizer holds it.
func TestJointTailHoldsWhereIIDFails(t *testing.T) {
	const (
		k      = 32
		delta  = 1e-3
		trials = 300_000
		pa     = 0.40
		pb     = 0.40
	)
	for _, rho := range []float64{0.0, 0.5, 0.9} {
		// Pearson-correlation form of the joint-erasure probability.
		pBoth := pa*pb + rho*math.Sqrt(pa*(1-pa)*pb*(1-pb))

		rIID := repairForJointTail(k, ppm(pa), ppm(pb), ppm(pa*pb), delta) // assumes independence
		rJoint := repairForJointTail(k, ppm(pa), ppm(pb), ppm(pBoth), delta)
		if rho > 0 && rJoint <= rIID {
			t.Fatalf("rho=%.1f: joint sizer (%d) should provision more than the i.i.d. sizer (%d)", rho, rJoint, rIID)
		}

		rng := rand.New(rand.NewSource(int64(rho*100) + 1))
		var failIID, failJoint int
		for i := 0; i < trials; i++ {
			pNone := 1 - pa - pb + pBoth
			// i.i.d.-sized generation: k+rIID symbols ⇒ (k+rIID)/2 slots.
			eIID := 0
			for s := 0; s < (k+rIID)/2; s++ {
				eIID += drawSlot(rng, pNone, pBoth)
			}
			if eIID > rIID {
				failIID++
			}
			eJoint := 0
			for s := 0; s < (k+rJoint)/2; s++ {
				eJoint += drawSlot(rng, pNone, pBoth)
			}
			if eJoint > rJoint {
				failJoint++
			}
		}
		fIID := float64(failIID) / trials
		fJoint := float64(failJoint) / trials
		t.Logf("rho=%.1f pBoth=%.3f: iid r=%d fails %.2e | joint r=%d fails %.2e (target %.0e)",
			rho, pBoth, rIID, fIID, rJoint, fJoint, delta)

		if fJoint > 5*delta {
			t.Fatalf("rho=%.1f: joint sizer (r=%d) failure %.2e exceeds the %.0e target", rho, rJoint, fJoint, delta)
		}
		if rho >= 0.5 && (fIID < 2*delta || fIID < fJoint) {
			t.Fatalf("rho=%.1f: i.i.d. sizer failure %.2e does not show under-protection (joint %.2e)", rho, fIID, fJoint)
		}
	}
}

// TestJointTailReducesToIID: at zero correlation the joint-tail sizer and the
// i.i.d.-union sizer provision the same repair (the joint model degenerates to the
// product of marginals).
func TestJointTailReducesToIID(t *testing.T) {
	const k, delta = 32, 1e-3
	for _, p := range []float64{0.1, 0.25, 0.4} {
		rJoint := repairForJointTail(k, ppm(p), ppm(p), ppm(p*p), delta)
		// The independent two-path total erasures ~ Binomial(k+r, p); the binomial
		// sizer is the reference. Allow a 1-symbol slack for the slot-parity rounding.
		rBinom := repairForTarget(k, p, delta)
		if d := rJoint - rBinom; d < -2 || d > 2 {
			t.Fatalf("p=%.2f: joint(indep) r=%d vs binomial r=%d differ by %d", p, rJoint, rBinom, d)
		}
	}
}

func absI(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// slotLosses maps a sampled slot erasure count to a per-path lost vector for the
// estimator, splitting single losses evenly between the two paths.
func slotLosses(rng *rand.Rand, count int) []bool {
	switch count {
	case 2:
		return []bool{true, true}
	case 1:
		if rng.Intn(2) == 0 {
			return []bool{true, false}
		}
		return []bool{false, true}
	default:
		return []bool{false, false}
	}
}

// TestCoLossEstimatorRecoversAndCloses: the estimator recovers the per-path marginals
// and the joint co-loss of a correlated 2-path channel, and the measure→size loop
// (joint-tail fed by the estimate) holds the decode-failure target where assuming
// independence — the same marginals but pBoth = pa·pb — under-provisions.
func TestCoLossEstimatorRecoversAndCloses(t *testing.T) {
	const (
		k      = 32
		delta  = 1e-3
		pa     = 0.40
		pb     = 0.40
		rho    = 0.7
		trials = 200_000
	)
	pBoth := pa*pb + rho*math.Sqrt(pa*(1-pa)*pb*(1-pb))
	pNone := 1 - pa - pb + pBoth

	rng := rand.New(rand.NewSource(3))
	est := newCoLossEstimator(2)
	for i := 0; i < 30_000; i++ {
		est.observe(slotLosses(rng, drawSlot(rng, pNone, pBoth)))
	}
	marg, dist := est.marginals(), est.slotDist()
	epa, epb, epBoth := marg[0], marg[1], dist[2]
	if absI(epa-ppm(pa)) > 30_000 || absI(epb-ppm(pb)) > 30_000 || absI(epBoth-ppm(pBoth)) > 30_000 {
		t.Fatalf("estimator pa=%d pb=%d pBoth=%d, want ≈ %d/%d/%d", epa, epb, epBoth, ppm(pa), ppm(pb), ppm(pBoth))
	}

	rEst := repairForJointTail(k, epa, epb, epBoth, delta)              // correlation-aware
	rIndep := repairForJointTail(k, epa, epb, epa*epb/1_000_000, delta) // assumes independence
	var failEst, failIndep int
	for i := 0; i < trials; i++ {
		e := 0
		for s := 0; s < (k+rEst)/2; s++ {
			e += drawSlot(rng, pNone, pBoth)
		}
		if e > rEst {
			failEst++
		}
		e2 := 0
		for s := 0; s < (k+rIndep)/2; s++ {
			e2 += drawSlot(rng, pNone, pBoth)
		}
		if e2 > rIndep {
			failIndep++
		}
	}
	fEst := float64(failEst) / trials
	fIndep := float64(failIndep) / trials
	t.Logf("estimate pBoth=%d (true %d): joint r=%d fails %.2e | independence r=%d fails %.2e",
		epBoth, ppm(pBoth), rEst, fEst, rIndep, fIndep)
	if fEst > 5*delta {
		t.Fatalf("estimate-fed joint sizing failure %.2e exceeds the %.0e target", fEst, delta)
	}
	if fIndep < 2*delta || fIndep < fEst {
		t.Fatalf("independence sizing %.2e did not under-protect (joint %.2e)", fIndep, fEst)
	}
}

// TestPathScheduler: systematic symbols spread evenly (round-robin), and repair is
// metered toward the higher-delivery path in proportion to the quality weights.
func TestPathScheduler(t *testing.T) {
	s := newPathScheduler(2)
	// Systematic: even split.
	var sys [2]int
	for i := 0; i < 100; i++ {
		sys[s.systematicPath()]++
	}
	if sys[0] != 50 || sys[1] != 50 {
		t.Fatalf("systematic split %v, want 50/50", sys)
	}
	// Repair: path 0 delivers 90%, path 1 delivers 30% → ~3:1 repair toward path 0.
	s.setQuality([]int{900_000, 300_000})
	var rep [2]int
	for i := 0; i < 1200; i++ {
		rep[s.repairPath()]++
	}
	ratio := float64(rep[0]) / float64(rep[1])
	if ratio < 2.5 || ratio > 3.5 {
		t.Fatalf("repair split %v (ratio %.2f), want ≈ 3:1 toward the better path", rep, ratio)
	}
}
