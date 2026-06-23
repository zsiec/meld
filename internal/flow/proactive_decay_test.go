package flow

import "testing"

// TestProactiveDecayUnit pins the two load-bearing properties at the sizer level: on a
// reactive-capable link it sends LESS proactive repair than the full binomial set-point (but
// never below the mean-sufficient repair), and at reactiveRounds 0 (RTT ≥ budget) it is an
// EXACT no-op — the safety guarantee that it never sheds protection the reactive tier cannot
// recover.
func TestProactiveDecayUnit(t *testing.T) {
	const n = 64
	base := Config{Flow: 1, SymbolSize: 256, GenSize: 64, TargetFailure: 1e-3, BufferMicros: 200_000}

	mk := func(decay bool, rtt int64) *Sender {
		c := base
		c.ProactiveDecay = decay
		s := NewSender(c)
		s.pEst = 0.05
		s.rttMicros = rtt
		s.burstQ8 = burstQ8One // i.i.d. (no burst margin), so the variance margin is what's tested
		return s
	}

	// Reactive-capable: 40 ms RTT under a 200 ms budget ⇒ reactiveRounds ≈ 2.
	off := mk(false, 40_000).repairCountFor(n)
	on := mk(true, 40_000).repairCountFor(n)
	mean := meanRepairCount(n, 0.05)
	t.Logf("reactive-capable: proactive off=%d on=%d (mean-sufficient=%d)", off, on, mean)
	if on >= off {
		t.Fatalf("ProactiveDecay should cut proactive repair when reactive-capable: on=%d off=%d", on, off)
	}
	if on < mean {
		t.Fatalf("ProactiveDecay dropped below the mean-sufficient repair: on=%d mean=%d (would chronically under-decode)", on, mean)
	}

	// High RTT: 150 ms RTT, 200 ms budget ⇒ 2·rtt+fb > budget ⇒ reactiveRounds 0 ⇒ exact no-op.
	hiOff := mk(false, 150_000).repairCountFor(n)
	hiOn := mk(true, 150_000).repairCountFor(n)
	if hiOn != hiOff {
		t.Fatalf("ProactiveDecay must be a no-op at reactiveRounds=0 (RTT≥budget): on=%d off=%d", hiOn, hiOff)
	}
}

// TestProactiveDecayHoldsDeliveryLowerOverhead is the end-to-end proof: on a reactive-capable
// link ProactiveDecay holds full delivery and the four invariants while sending materially less
// repair than the default full-set-point sizing.
func TestProactiveDecayHoldsDeliveryLowerOverhead(t *testing.T) {
	const (
		budget = 200_000 // 200 ms
		owd    = 20_000  // 40 ms RTT ⇒ ~2 reactive rounds in budget
		n      = 640
	)
	mk := func(decay bool) Config {
		return Config{Flow: 1, SymbolSize: 256, GenSize: 32, Redundancy: 0.05, TargetFailure: 1e-3,
			BufferMicros: budget, ProactiveDecay: decay}
	}
	run := func(decay bool) simResult {
		return simLink{cfg: mk(decay), owdMicros: owd, srcMicros: 500, n: n, drop: geDrop(5, 0.05, 1)}.run()
	}
	on, off := run(true), run(false)
	assertDeliveryInvariants(t, on)
	assertDeliveryInvariants(t, off)

	for _, r := range []struct {
		name string
		res  simResult
	}{{"decay-on", on}, {"decay-off", off}} {
		if frac := float64(r.res.delivered) / float64(n); frac < 0.99 {
			t.Fatalf("%s: delivered %.1f%% (< 99%%) — completeness regressed", r.name, 100*frac)
		}
	}
	t.Logf("ProactiveDecay on overhead=%.0f%% (p99 %dms) | off overhead=%.0f%% (p99 %dms)",
		100*on.overhead(), pctlMicros(on.latencyMicros, 0.99)/1000,
		100*off.overhead(), pctlMicros(off.latencyMicros, 0.99)/1000)
	if on.overhead() >= off.overhead() {
		t.Fatalf("ProactiveDecay did not cut overhead: on=%.1f%% off=%.1f%%", 100*on.overhead(), 100*off.overhead())
	}
}
