package flow

import "testing"

// TestReactiveRoundsConservative pins the reactive-availability estimate that gates the
// proactive burst-margin discount. It must be GENEROUS at low RTT (reactive can run several
// top-ups inside the budget, so proactive may lean on it) and ZERO at high RTT (a round trip
// does not fit the budget, so proactive must carry the full Gilbert-Elliott margin itself). It
// uses 2×rttMicros for the round trip because the RTT estimate under-counts (HighestSeen is
// advanced by window-covering repair) — erring toward full protection.
func TestReactiveRoundsConservative(t *testing.T) {
	s := NewSender(Config{Flow: 1, SymbolSize: 256, GenSize: 32, BufferMicros: 200_000})
	s.rttMicros = 20_000 // ~40 ms RTT
	if r := s.reactiveRounds(); r < 2 {
		t.Fatalf("low RTT should afford multiple reactive rounds (discount the burst margin), got %d", r)
	}
	s.rttMicros = 150_000 // ~300 ms RTT
	if r := s.reactiveRounds(); r != 0 {
		t.Fatalf("high RTT must afford no reactive rounds (full burst margin), got %d", r)
	}
}

// TestBurstMarginReactiveDiscount pins the win: at low RTT the now-efficient reactive tier
// absorbs the burst tail on demand, so the proactive controller need NOT blanket-provision the
// full Gilbert-Elliott margin on every generation. Overhead stays well below the heavy-handed
// blanket sizing (the bench showed ~140% at 10% loss / burst 5) while delivery holds. The GE
// channel is noisy at one rep, so this averages a few seeds.
func TestBurstMarginReactiveDiscount(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 200_000}
	const reps = 3
	for _, b := range []float64{2, 5} {
		var deliv, ovhd float64
		for seed := 0; seed < reps; seed++ {
			res := simLink{cfg: cfg, owdMicros: 20_000, srcMicros: 500, n: 640, drop: geDrop(int64(seed*97)+int64(b), 0.10, b)}.run()
			if res.lateDeliv {
				t.Fatalf("burst=%g seed=%d: delivery past deadline", b, seed)
			}
			deliv += float64(res.delivered) / float64(res.n)
			ovhd += res.overhead()
		}
		deliv /= reps
		ovhd /= reps
		t.Logf("40ms RTT burst=%g loss=10%%: deliv=%.1f%% ovhd=%.0f%% (reactive absorbs the tail; blanket GE was ~140%%)", b, 100*deliv, 100*ovhd)
		if deliv < 0.98 {
			t.Fatalf("burst=%g: delivery %.1f%% — reactive is not absorbing the burst tail the discount relies on", b, 100*deliv)
		}
		if ovhd > 0.95 {
			t.Fatalf("burst=%g: overhead %.0f%% — the burst margin is not being discounted by reactive availability", b, 100*ovhd)
		}
	}
}
