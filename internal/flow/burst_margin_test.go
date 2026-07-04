package flow

import "testing"

// TestReactiveRoundsConservative pins the reactive-availability estimate that gates the
// proactive burst-margin discount. It must be GENEROUS at low RTT (reactive can run several
// top-ups inside the budget, so proactive may lean on it) and ZERO where a single honest
// reactive cycle (reactiveCycleMicros: rtt + rtt/4 margin + the loss-onset report floor)
// does not fit the budget — the frontier regime, where proactive must carry the full
// Gilbert-Elliott margin itself. The former 2×rtt+cadence model priced reactive out of
// budgets up to ~4×RTT (the generous-budget cost gap vs ARQ); the honest cycle credits
// exactly what loss-onset feedback + one transit can deliver.
func TestReactiveRoundsConservative(t *testing.T) {
	s := NewSender(Config{Flow: 1, SymbolSize: 256, GenSize: 32, BufferMicros: 200_000})
	s.rttMicros = 20_000 // 20 ms RTT: cycle 30 ms ⇒ several rounds in 200 ms
	if r := s.reactiveRounds(); r < 2 {
		t.Fatalf("low RTT should afford multiple reactive rounds (discount the burst margin), got %d", r)
	}
	s.rttMicros = 150_000 // cycle ≈ 192.5 ms fits a 200 ms budget exactly once
	if r := s.reactiveRounds(); r != 1 {
		t.Fatalf("one honest cycle fits the budget, got %d rounds", r)
	}
	s.rttMicros = 170_000 // cycle ≈ 217.5 ms > 200 ms budget: the frontier regime
	if r := s.reactiveRounds(); r != 0 {
		t.Fatalf("a budget below one honest cycle must afford no reactive rounds (full burst margin), got %d", r)
	}
}

// TestBurstMarginReactiveDiscount pins the win: at low RTT the now-efficient reactive tier
// absorbs the burst tail on demand, so the proactive controller need NOT blanket-provision the
// full Gilbert-Elliott margin on every generation. Overhead stays well below the heavy-handed
// blanket sizing (the bench showed ~140% at 10% loss / burst 5) while delivery holds. The GE
// channel is noisy at one rep, so this averages a few seeds.
func TestBurstMarginReactiveDiscount(t *testing.T) {
	t.Parallel()
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
