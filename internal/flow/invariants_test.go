package flow

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// The four invariants every flow/multipath sim must uphold (CLAUDE.md): (1) no duplicate
// delivered, (2) in-order output, (3) nothing past deadline, (4) completeness under
// recoverable loss. They are checked piecemeal across the suite; this is the consolidated,
// seeded property fuzz that runs them together over the cross product of loss model × loss
// rate × reorder, reproducible by seed. assertCoreInvariants checks the three SAFETY
// invariants (1–3) plus byte-correctness, which must hold under ANY loss/reorder.
func assertCoreInvariants(t *testing.T, res simResult, n int, label string) {
	t.Helper()
	// (1)+(2): strictly increasing delivery ⇒ in-order AND no duplicate.
	for i := 1; i < len(res.deliveredIDs); i++ {
		if res.deliveredIDs[i] <= res.deliveredIDs[i-1] {
			t.Fatalf("%s: delivery not strictly increasing at %d: %d then %d", label, i, res.deliveredIDs[i-1], res.deliveredIDs[i])
		}
	}
	// (3): nothing delivered past its deadline.
	if res.lateDeliv {
		t.Fatalf("%s: a symbol was delivered past its deadline", label)
	}
	// Correctness / the coded-recovery oracle: never deliver the wrong bytes for an id.
	if res.corrupt {
		t.Fatalf("%s: delivered corrupt bytes — a recovery the rank did not support", label)
	}
	// Accounting consistency: the delivered slice matches the Delivered counter, and nothing is
	// counted twice.
	if uint64(len(res.deliveredIDs)) != res.stats.Delivered {
		t.Fatalf("%s: delivered slice %d != Delivered stat %d", label, len(res.deliveredIDs), res.stats.Delivered)
	}
	if res.stats.Delivered+res.stats.Lost > uint64(n) {
		t.Fatalf("%s: accounting overshoot delivered=%d lost=%d > n=%d", label, res.stats.Delivered, res.stats.Lost, n)
	}
}

// TestInvariantsFuzz sweeps loss model × rate × reorder and asserts the safety invariants on
// every run. The budget sits above the one-way delay (plus jitter) so the channel is within
// the deliverable regime; losses above the code rate simply drop (counted), never corrupt or
// reorder delivery.
func TestInvariantsFuzz(t *testing.T) {
	t.Parallel()
	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 200_000}
	const n = 16 * 12
	losses := []float64{0.05, 0.15, 0.30, 0.45}
	jitters := []int64{0, 30_000} // 0 and 30 ms of reorder
	for li, loss := range losses {
		for ji, jit := range jitters {
			// i.i.d. channel.
			resI := simLink{cfg: cfg, owdMicros: 20_000, srcMicros: 1_000, n: n, jitterMicros: jit,
				drop: uniformDrop(uint64(li*7+ji*13+1)*0x9E3779B1, loss)}.run()
			assertCoreInvariants(t, resI, n, fmt.Sprintf("iid loss=%.0f%% jit=%dms", loss*100, jit/1000))
			// Gilbert-Elliott (burst 6) at the same mean loss.
			resG := simLink{cfg: cfg, owdMicros: 20_000, srcMicros: 1_000, n: n, jitterMicros: jit,
				drop: geDrop(int64(li*100+ji+1), loss, 6)}.run()
			assertCoreInvariants(t, resG, n, fmt.Sprintf("ge loss=%.0f%% jit=%dms", loss*100, jit/1000))
		}
	}
}

// TestInvariantCompletenessUnderRecoverableLoss is invariant (4): when the per-generation loss
// stays within what the repair can cover (and repair is delivered), recovery is COMPLETE —
// every source symbol is delivered, in order, on time. Fuzzed over loss patterns; the holes
// are bounded below the proactive repair count and repair is never dropped, so each generation
// is provably recoverable and full delivery is mandatory.
func TestInvariantCompletenessUnderRecoverableLoss(t *testing.T) {
	cfg := testConfig() // GenSize 16, repair floor 4
	const n = testGen * 8
	r := cfg.repairFloor(cfg.GenSize)
	for seed := int64(0); seed < 30; seed++ {
		// Drop EXACTLY r systematic per generation (the recoverable boundary) at seed-dependent
		// positions; never drop repair — so every generation is provably recoverable and full
		// delivery is mandatory.
		rng := rand.New(rand.NewSource(seed + 909))
		dropSet := map[uint32]bool{}
		for g := 0; g < n/cfg.GenSize; g++ {
			perm := rng.Perm(cfg.GenSize)
			for k := 0; k < r; k++ {
				dropSet[uint32(g*cfg.GenSize+perm[k])] = true
			}
		}
		drop := func(sym wire.Symbol) bool {
			return sym.Kind == wire.Systematic && dropSet[sym.SrcIndex]
		}
		res := runFlow(t, cfg, n, seed, drop)
		assertOrdered(t, res.delivered)
		if res.lateDeliv {
			t.Fatalf("seed %d: delivery past deadline", seed)
		}
		if len(res.delivered) != n {
			t.Fatalf("seed %d: recoverable loss not fully recovered: %d/%d (lost=%d recovered=%d)",
				seed, len(res.delivered), n, res.stats.Lost, res.stats.Recovered)
		}
	}
}
