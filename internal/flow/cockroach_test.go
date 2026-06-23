package flow

import (
	"testing"
)

// cockroachCfg builds a config pair: latency mode (bounded reactive, nominal deadline) vs cockroach
// mode (rateless reactive, deep retention). retainMicros is the cockroach retention depth.
func cockroachCfg(cockroach bool, retainMicros int64) Config {
	c := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 200_000}
	if cockroach {
		c.Mode = ModeCockroach
		c.ElasticMicros = retainMicros
	}
	return c
}

// TestCockroachRatelessRecovery proves Step 2: under SUSTAINED extreme loss, latency mode abandons a
// generation after a few rounds of bounded reactive repair (the maxRepairFactor flood guard) and
// drops it at the nominal deadline, while cockroach mode keeps emitting rateless coded repair to the
// deep retention bound and recovers essentially everything. The four invariants hold for both — and
// for cockroach, "nothing past deadline" means nothing past the RETENTION bound (the harness gates
// lateDeliv on nominal + ElasticMicros).
func TestCockroachRatelessRecovery(t *testing.T) {
	const n = 3200                  // multiple of GenSize (16) — the harness convention (no partial last gen)
	owd := int64(50_000)            // RTT 100ms — many reactive rounds fit the 3s retention
	const retain = int64(3_000_000) // 3s deep retention
	// minDeliv is the cockroach reliability floor at each loss. Up to 70% the rateless reactive
	// recovers everything where latency mode (bounded maxRepairFactor·n repair) abandons generations.
	// At 85% it pulls far ahead of latency but is bottlenecked at ~50%: the feedback reports only
	// wire.MaxFeedbackGens (8) deficient generations from the cursor, so under a deficient-gen backlog
	// deeper than 8 the reactive serves it serially and the tail overflows the retention window. That
	// frontier is a wire-format change (a wider feedback deficit window) — see ORCHESTRATION notes.
	for _, tc := range []struct {
		loss     float64
		minDeliv float64
	}{{0.50, 99.5}, {0.70, 99.5}, {0.85, 40.0}} {
		lat := simLink{cfg: cockroachCfg(false, 0), owdMicros: owd, srcMicros: 1_000, n: n, drop: uniformDrop(0xC0C0, tc.loss)}.run()
		ck := simLink{cfg: cockroachCfg(true, retain), owdMicros: owd, srcMicros: 1_000, n: n, drop: uniformDrop(0xC0C0, tc.loss)}.run()
		assertCoreInvariants(t, lat, n, "latency")
		assertCoreInvariants(t, ck, n, "cockroach")

		latD := 100 * float64(lat.delivered) / float64(n)
		ckD := 100 * float64(ck.delivered) / float64(n)
		t.Logf("loss %2.0f%%: latency=%.1f%% delivered, cockroach=%.1f%% delivered", 100*tc.loss, latD, ckD)

		if ckD < tc.minDeliv {
			t.Fatalf("loss %.0f%%: cockroach delivered only %.1f%%, expected >=%.1f%%", 100*tc.loss, ckD, tc.minDeliv)
		}
		// Cockroach never delivers less than latency mode — at every loss it recovers at least as much.
		if ckD < latD {
			t.Fatalf("loss %.0f%%: cockroach (%.1f%%) below latency (%.1f%%)", 100*tc.loss, ckD, latD)
		}
	}
}
