package flow

import "testing"

// TestReorderHoldoffFrameBurst is the codec-dimension screen: a real media stream writes whole access
// units (many chunks at one instant) rather than a uniform trickle, so a generation fills in bursts.
// The reorder window is codec-blind (it works on the receive-side id sequence), but burst WRITES change
// fill timing — so confirm the over-send cut and the invariants still hold with frame-burst writes
// (burst=8) under reorder + loss, the way a real clip exercises the path.
func TestReorderHoldoffFrameBurst(t *testing.T) {
	const (
		owd    = 50_000
		budget = 400_000
		n      = 1024
	)
	mk := func(auto bool) Config {
		return Config{Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: budget, AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
			AutoReorderHoldoff: auto}
	}
	run := func(auto bool, seed int64) (proactive, delivered int) {
		res := simLink{cfg: mk(auto), owdMicros: owd, srcMicros: 526, n: n, burst: 8, // 8-chunk access units
			drop: geDrop(seed, 0.01, 2), paceBytesPerSec: 12_500_000, timingJitterMicros: 80_000, timingSeed: seed*7919 + 1}.run()
		assertDeliveryInvariants(t, res)
		return int(res.sstats.Repair), res.delivered
	}
	var offPro, onPro, offDel, onDel int
	for seed := int64(1); seed <= 4; seed++ {
		op, od := run(false, seed)
		np, nd := run(true, seed)
		offPro += op
		onPro += np
		offDel += od
		onDel += nd
	}
	t.Logf("frame-burst (8-chunk AUs) under reorder: proactive off=%d auto=%d (%.0f%%) | delivered off=%d auto=%d",
		offPro, onPro, 100*float64(onPro)/float64(offPro), offDel, onDel)
	if onPro*3 >= offPro*2 { // auto should cut the over-send by at least a third even with burst writes
		t.Fatalf("reorder window did not cut over-send under frame-burst writes: off=%d auto=%d", offPro, onPro)
	}
	if onDel < offDel-4*n/100 { // within ~1% of the 4·n total
		t.Fatalf("reorder window hurt delivery under frame-burst writes: off=%d auto=%d", offDel, onDel)
	}
}

// TestReorderHoldoffMultipathSound pins that the reorder window is SOUND on multipath: it replays each
// held id to the co-loss estimator with its own stamped pathID, so the joint-tail provisioning the
// estimator drives must still beat independence-assuming sizing on a correlated 2-path channel (the
// money-test relationship), and the four invariants must hold — with AutoReorderHoldoff on.
func TestReorderHoldoffMultipathSound(t *testing.T) {
	const (
		n     = 4000
		pa    = 0.4
		pb    = 0.4
		rho   = 0.85
		seeds = 6
	)
	pBoth := pa*pb + rho*sqrtApprox(pa*(1-pa)*pb*(1-pb))
	cfg := mpConfig(testBuf)
	cfg.AutoReorderHoldoff = true // the resequencer now active on the multipath co-loss path
	slot2 := func(pBoth float64) []int {
		a, b, both := ppm(pa), ppm(pb), ppm(pBoth)
		return []int{1_000_000 - a - b + both, a + b - 2*both, both}
	}
	joint := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = []int{ppm(pa), ppm(pb)}, slot2(pBoth) }
	indep := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = []int{ppm(pa), ppm(pb)}, slot2(pa*pb) }
	var lostJoint, lostIndep int
	for seed := int64(0); seed < seeds; seed++ {
		ch := newSlotChannel(seed+1, n, pa, pb, pBoth)
		dJ, lJ, ordJ, timeJ := runMPProactive(t, cfg, n, joint, ch)
		dI, lI, ordI, timeI := runMPProactive(t, cfg, n, indep, ch)
		if !ordJ || !timeJ || !ordI || !timeI {
			t.Fatalf("seed %d: invariant violation with the reorder window on multipath", seed)
		}
		if dJ+lJ != n || dI+lI != n {
			t.Fatalf("seed %d: accounting J=%d/%d I=%d/%d", seed, dJ+lJ, n, dI+lI, n)
		}
		lostJoint += lJ
		lostIndep += lI
	}
	t.Logf("multipath co-loss with reorder window on: joint lost %d vs independence lost %d", lostJoint, lostIndep)
	if lostIndep*100 < lostJoint*110 {
		t.Fatalf("reorder window broke multipath co-loss: joint %d not materially below independence %d", lostJoint, lostIndep)
	}
}

// sqrtApprox is a tiny Newton sqrt so this test needs no math import beyond the package's.
func sqrtApprox(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 40; i++ {
		z = (z + x/z) / 2
	}
	return z
}

// TestAutoReorderHoldoffInvariants pins that the SELF-TUNING reorder window is also a soundness no-op:
// the four invariants and completeness hold under heavy reorder + loss with AutoReorderHoldoff on.
func TestAutoReorderHoldoffInvariants(t *testing.T) {
	const (
		owd    = 50_000
		budget = 400_000
		n      = 800
	)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
		BufferMicros: budget, AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
		AutoReorderHoldoff: true}
	for seed := int64(1); seed <= 4; seed++ {
		res := simLink{cfg: cfg, owdMicros: owd, srcMicros: 526, n: n, drop: geDrop(seed, 0.02, 2),
			jitterMicros: 60_000, timingJitterMicros: 60_000, timingSeed: seed*131 + 1}.run()
		assertDeliveryInvariants(t, res)
		if frac := float64(res.delivered) / float64(n); frac < 0.99 {
			t.Fatalf("seed %d: delivered %.1f%% (< 99%%) — auto reorder window broke completeness", seed, 100*frac)
		}
	}
}

// TestAutoReorderHoldoffSelfDisables pins the property that makes it default-able: on a lossy link with
// NO reorder, the gaps come from genuine loss (they never fill), so the window must stay ~0 and add no
// proactive overhead vs off — it costs nothing where there is nothing to correct.
func TestAutoReorderHoldoffSelfDisables(t *testing.T) {
	const (
		owd    = 50_000
		budget = 400_000
		n      = 1000
	)
	mk := func(auto bool) Config {
		return Config{Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: budget, AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
			AutoReorderHoldoff: auto}
	}
	run := func(auto bool, seed int64) (proactive, delivered int) {
		// No timing jitter ⇒ no reorder; 5% loss ⇒ real gaps. The window must not engage.
		res := simLink{cfg: mk(auto), owdMicros: owd, srcMicros: 526, n: n, drop: geDrop(seed, 0.05, 2),
			paceBytesPerSec: 12_500_000}.run()
		return int(res.sstats.Repair), res.delivered
	}
	var offPro, onPro, offDel, onDel int
	for seed := int64(1); seed <= 4; seed++ {
		op, od := run(false, seed)
		np, nd := run(true, seed)
		offPro += op
		onPro += np
		offDel += od
		onDel += nd
	}
	t.Logf("no-reorder, 5%% loss: proactive off=%d auto=%d (%.0f%% of off) | delivered off=%d auto=%d",
		offPro, onPro, 100*float64(onPro)/float64(offPro), offDel, onDel)
	if onPro > offPro*11/10 { // within 10% — the window self-disabled (no onset-lag waste)
		t.Fatalf("auto reorder window did not self-disable on a no-reorder link: off=%d auto=%d", offPro, onPro)
	}
	if onDel < offDel-int(0.01*float64(4*n)) {
		t.Fatalf("auto reorder window hurt delivery on a no-reorder link: off=%d auto=%d", offDel, onDel)
	}
}

// TestReorderHoldoffInvariants pins that the loss-estimate reorder window is a SOUNDNESS no-op: it
// only gates the channel-loss ESTIMATE (which sizes proactive repair), never the decode/delivery
// path, so the four invariants must hold under heavy reorder + loss exactly as without it — no
// duplicate, in-order, nothing past deadline, completeness under recoverable loss.
func TestReorderHoldoffInvariants(t *testing.T) {
	const (
		owd    = 50_000  // RTT 100 ms
		budget = 400_000 // 4×RTT — ample, so recoverable loss completes
		n      = 800
	)
	mk := func(holdoff int64) Config {
		return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: budget, AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
			ReorderHoldoffMicros: holdoff}
	}
	for seed := int64(1); seed <= 4; seed++ {
		res := simLink{cfg: mk(80_000), owdMicros: owd, srcMicros: 526, n: n,
			drop: geDrop(seed, 0.02, 2), jitterMicros: 60_000, timingJitterMicros: 60_000, timingSeed: seed*131 + 1}.run()
		assertDeliveryInvariants(t, res)
		if frac := float64(res.delivered) / float64(n); frac < 0.99 {
			t.Fatalf("seed %d: delivered %.1f%% (< 99%%) — reorder window broke completeness", seed, 100*frac)
		}
	}
}

// TestReorderHoldoffCutsOverSend is the regression guard for the win cref confirmed: under reorder
// the receiver over-reports loss and the proactive set-point inflates; the holdoff must cut that
// over-send materially WITHOUT losing delivery. Compares proactive repair (sstats.Repair) off vs on
// at equal recoverable loss, with the paced+jittered sim (the reorder the deterministic sim hides).
func TestReorderHoldoffCutsOverSend(t *testing.T) {
	const (
		owd    = 50_000
		budget = 400_000
		n      = 1000
	)
	mk := func(holdoff int64) Config {
		return Config{Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: budget, AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
			ReorderHoldoffMicros: holdoff}
	}
	run := func(holdoff int64, seed int64) (proactive, delivered int) {
		res := simLink{cfg: mk(holdoff), owdMicros: owd, srcMicros: 526, n: n, drop: geDrop(seed, 0.01, 2),
			paceBytesPerSec: 12_500_000, timingJitterMicros: 80_000, timingSeed: seed*7919 + 1}.run()
		return int(res.sstats.Repair), res.delivered
	}
	var offPro, onPro, offDel, onDel int
	for seed := int64(1); seed <= 4; seed++ {
		op, od := run(0, seed)
		np, nd := run(60_000, seed)
		offPro += op
		onPro += np
		offDel += od
		onDel += nd
	}
	t.Logf("under reorder: proactive off=%d on=%d (%.0f%% of off) | delivered off=%d on=%d / %d",
		offPro, onPro, 100*float64(onPro)/float64(offPro), offDel, onDel, 4*n)
	if onPro*2 >= offPro {
		t.Fatalf("reorder holdoff should roughly halve the over-send or better: off=%d on=%d", offPro, onPro)
	}
	if onDel < offDel-int(0.01*float64(4*n)) {
		t.Fatalf("reorder holdoff must hold delivery: off=%d on=%d", offDel, onDel)
	}
}
