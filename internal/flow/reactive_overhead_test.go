package flow

import "testing"

// overheadAtRTT runs a fixed-loss flow at a given one-way delay and returns the realized
// repair overhead, the delivered fraction, and the reactive-repair / wire-loss counts. The
// budget is held well above the one-way delay so delivery is comparable across RTTs — only
// the controller's RTT-dependent behavior changes.
func overheadAtRTT(owdMicros int64, loss float64) simResult {
	const budget = 200_000
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: budget}
	return simLink{
		cfg:       cfg,
		owdMicros: owdMicros,
		srcMicros: 1_000,
		n:         480,
		drop:      uniformDrop(0x0BCD, loss),
	}.run()
}

// TestReactiveOverheadVsRTT pins the fix for the reactive-repair RTT overhead inversion. The
// bench showed total overhead far higher at low RTT (LAN 106–177%) than high RTT (WAN 33–91%)
// at the same loss; the REACTIVE component drove the LAN excess because the loop re-sized a
// full stale-deficit batch each HARQ round (many fit the budget at a short RTT) and reacted to
// the transient deficit of a just-closed generation whose own symbols were still in flight.
// With the convergence gate + in-flight discounting, reactive repair is now PROPORTIONAL to the
// loss it recovers (≈1:1 with wire loss) at every RTT — no longer ballooning at low RTT.
//
// (Total overhead still differs across RTT, but that residual is PROACTIVE: at high RTT the
// loss estimate lags so the proactive set-point stays near the floor and under-protects — a
// separate effect, visible as the lower high-RTT delivery — not the reactive over-send.)
func TestReactiveOverheadVsRTT(t *testing.T) {
	const loss = 0.20
	owds := []int64{10_000, 20_000, 50_000} // RTT 20, 40, 100 ms — where reactive repair is active
	for _, owd := range owds {
		res := overheadAtRTT(owd, loss)
		if res.lateDeliv {
			t.Fatalf("owd=%dms: a symbol was delivered past its deadline", owd/1000)
		}
		reactive, wl := res.sstats.ReactiveRepair, res.stats.WireLost
		t.Logf("owd=%3dms (rtt=%3dms): overhead=%4.0f%% delivered=%.1f%% reactive=%d wireLost=%d (reactive/loss=%.2f)",
			owd/1000, owd/500, 100*res.overhead(), 100*float64(res.delivered)/float64(res.n), reactive, wl, float64(reactive)/float64(wl))
		if wl == 0 {
			t.Fatal("expected wire loss to drive reactive repair")
		}
		// The fix: reactive repair tracks the loss it recovers — bounded by a small multiple of
		// wire loss at EVERY RTT (was ~7× at low RTT, the inversion).
		if reactive > 2*wl {
			t.Fatalf("reactive repair %d exceeds 2× wire loss %d at rtt=%dms — over-send inversion regressed", reactive, wl, owd/500)
		}
	}
}

// TestReactiveAmplificationBounded is the resource-safety guard the inversion must not be
// allowed to violate: however many HARQ rounds fit in the budget, the reactive repair the
// sender emits in response to loss stays bounded by a sane multiple of the wire loss it is
// actually responding to — it cannot amplify into an unbounded storm. (Distinct from the
// efficiency question above: this asserts amplification is bounded, not that it is minimal.)
func TestReactiveAmplificationBounded(t *testing.T) {
	res := overheadAtRTT(10_000, 0.20) // low RTT, heavy loss — the worst case for amplification
	t.Logf("amplification: reactive=%d wireLost=%d throttled=%d source=%d",
		res.sstats.ReactiveRepair, res.stats.WireLost, res.sstats.Throttled, res.sstats.Source)
	if res.stats.WireLost == 0 {
		t.Fatal("expected wire loss to drive reactive repair")
	}
	// Reactive repair responds to loss and must stay BOUNDED — this guards against an unbounded
	// storm, not against the known RTT-inversion (many HARQ rounds fit the budget at low RTT,
	// each sized to the per-generation loss; that elevated-but-bounded overhead is T3's gap).
	// The bound is generous because at low RTT a deficient generation is legitimately re-served
	// each round until it clears.
	const maxAmplification = 10
	if res.sstats.ReactiveRepair > maxAmplification*res.stats.WireLost {
		t.Fatalf("reactive repair %d amplified past %dx wire loss %d (storm)",
			res.sstats.ReactiveRepair, maxAmplification, res.stats.WireLost)
	}
}
