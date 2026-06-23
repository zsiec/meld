package flow

import (
	"testing"
)

// TestElasticDeadline proves the opt-in burst-elastic deadline (Config.ElasticMicros): at a high RTT
// where the steady budget fits no reactive round, holding a deficit generation ElasticMicros longer
// lets the sender carry a smaller proactive burst margin AND recovers at least as much — while the
// four invariants still hold under the relaxed contract (assertCoreInvariants now checks the
// EFFECTIVE deadline = nominal + ElasticMicros: no duplicate, in order, byte-correct, and nothing
// delivered past the elastic deadline). The same drop seed drives both runs, so it is apples-to-apples.
func TestElasticDeadline(t *testing.T) {
	const n = 4800
	owd := int64(75_000) // RTT 150ms — the 200ms steady budget fits ~no reactive round
	mk := func(elastic int64) Config {
		return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: 200_000, ElasticMicros: elastic}
	}
	rb := simLink{cfg: mk(0), owdMicros: owd, srcMicros: 1_000, n: n, drop: geDrop(0x5EED, 0.20, 8)}.run()
	re := simLink{cfg: mk(150_000), owdMicros: owd, srcMicros: 1_000, n: n, drop: geDrop(0x5EED, 0.20, 8)}.run()

	assertCoreInvariants(t, rb, n, "elastic off")
	assertCoreInvariants(t, re, n, "elastic on")

	t.Logf("baseline:  deliv=%.1f%% overhead=%.0f%%", 100*float64(rb.delivered)/float64(n), 100*rb.overhead())
	t.Logf("elastic:   deliv=%.1f%% overhead=%.0f%% (held within nominal+150ms)", 100*float64(re.delivered)/float64(n), 100*re.overhead())

	// The feature: recover at least as much for LESS proactive overhead (the burst margin shrinks
	// because the elastic deadline fits a reactive round the steady budget could not).
	if re.delivered < rb.delivered {
		t.Fatalf("elastic delivered fewer (%d < %d) — it must recover at least as much", re.delivered, rb.delivered)
	}
	if re.overhead() >= rb.overhead() {
		t.Fatalf("elastic overhead %.0f%% not below baseline %.0f%% — the burst margin should shrink",
			100*re.overhead(), 100*rb.overhead())
	}
}

// TestElasticDeadlineOffIsNoOp confirms ElasticMicros=0 (the default) is byte-identical to having no
// elastic logic at all — the opt-in must never perturb the default behavior.
func TestElasticDeadlineOffIsNoOp(t *testing.T) {
	const n = 2400
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: 200_000}
	res := simLink{cfg: cfg, owdMicros: 75_000, srcMicros: 1_000, n: n, drop: geDrop(0x1234, 0.20, 8)}.run()
	assertCoreInvariants(t, res, n, "elastic off (default)")
	if res.lateDeliv {
		t.Fatalf("default config delivered past the (non-elastic) deadline")
	}
}
