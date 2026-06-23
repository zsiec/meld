package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// noDrop is a clean wire (the outage window is the only loss source).
func noDrop(wire.Symbol) bool { return false }

// TestCockroachOutageSurvival proves Step 3: a TOTAL outage (both link directions down for a window)
// destroys latency mode — every generation whose deadline falls in the outage is lost — while
// cockroach mode, holding un-decoded generations to a deep retention bound and re-coding them
// rateless once the link returns, rides the outage out and delivers everything, paying latency (the
// outage duration) instead of loss. The four invariants hold for both (nothing past the retention
// bound). This is the behavior the cockroach model predicted: survive outages up to the buffer depth.
func TestCockroachOutageSurvival(t *testing.T) {
	const n = 6400                  // multiple of GenSize; ~6.4s stream at 1 chunk/ms
	owd := int64(50_000)            // RTT 100ms
	const retain = int64(2_000_000) // 2s retention — rides outages up to this depth
	// minDeliv is the cockroach reliability floor for each outage length. Latency mode loses every
	// generation whose deadline falls in the outage; cockroach holds and rateless-recodes them. A
	// 1s total outage (62 generations of backlog) still recovers 97% where latency keeps 84%.
	for _, tc := range []struct {
		outMs    int64
		minDeliv float64
	}{{200, 99.9}, {500, 98.5}, {1000, 96.0}} {
		const start = int64(3_000_000) // outage 3s into the stream
		stop := start + tc.outMs*1000
		lat := simLink{cfg: cockroachCfg(false, 0), owdMicros: owd, srcMicros: 1_000, n: n, drop: noDrop, outageStart: start, outageStop: stop}.run()
		ck := simLink{cfg: cockroachCfg(true, retain), owdMicros: owd, srcMicros: 1_000, n: n, drop: noDrop, outageStart: start, outageStop: stop}.run()
		assertCoreInvariants(t, lat, n, "latency")
		assertCoreInvariants(t, ck, n, "cockroach")

		latD := 100 * float64(lat.delivered) / float64(n)
		ckD := 100 * float64(ck.delivered) / float64(n)
		t.Logf("outage %4dms: latency=%.1f%% delivered, cockroach=%.1f%% delivered", tc.outMs, latD, ckD)

		if ckD < tc.minDeliv {
			t.Fatalf("outage %dms: cockroach delivered only %.1f%%, expected >=%.1f%% (ride the outage)", tc.outMs, ckD, tc.minDeliv)
		}
		// Once the outage exceeds the playout budget (200ms), latency mode loses it outright; cockroach
		// must beat it by a wide margin.
		if tc.outMs > 200 && ckD < latD+2 {
			t.Fatalf("outage %dms: cockroach (%.1f%%) did not clearly beat latency (%.1f%%)", tc.outMs, ckD, latD)
		}
	}
}

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
	// minDeliv is the cockroach reliability floor at each loss. The rateless reactive recovers
	// EVERYTHING — even 85% sustained loss — where latency mode (bounded maxRepairFactor·n repair)
	// collapses (it keeps ~28% at 85%). Reaching 85% needed the wider feedback deficit window
	// (wire.MaxFeedbackGens) so the deep deficient-generation backlog is served in parallel rather
	// than serially overflowing the retention window.
	for _, tc := range []struct {
		loss     float64
		minDeliv float64
	}{{0.50, 99.5}, {0.70, 99.5}, {0.85, 99.5}} {
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
