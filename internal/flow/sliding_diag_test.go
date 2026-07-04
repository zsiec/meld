package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// slidingClassify re-runs one sliding config and classifies every source id as
// delivered / premature (arrived+recoverable-in-time but dropped) / genuineLoss
// (the ideal decoder couldn't recover it in time from what arrived), bucketing the
// two failure classes by decile of stream position. It mirrors analyzeOracle but
// keeps the full id positions so we can see WHERE the residual lives (warmup vs
// steady-state) — the key to which half of the coder to fix.
func slidingClassify(t *testing.T, cfg Config, n int, owd, src, jit int64, seed uint64) (deliv, prem, gen int, premDec, genDec [10]int, sstats SenderStats) {
	t.Helper()
	p := oracleParams{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: true, jitterMicros: jit}
	delivered, tape, writeAt, _ := oracleRun(t, p, uniformDrop(seed, 0.20))

	// ideal in-time recovery from the tape (same as analyzeOracle)
	recoveredAt := map[uint32]clock.Timestamp{}
	dec := code.NewDecoder(cfg.SymbolSize, 0, n)
	ord := append([]tapEntry(nil), tape...)
	// tape is already in feed order (arrival); decode in that order.
	for _, e := range ord {
		var rec []code.Recovered
		if e.sym.Kind == wire.Systematic {
			rec = dec.AddSystematic(e.sym.SrcIndex, e.sym.Payload)
		} else {
			rec = dec.AddRepair(e.sym.WindowBase, int(e.sym.N), e.sym.RepairKey, e.sym.Payload)
		}
		for _, rc := range rec {
			if _, seen := recoveredAt[rc.ID]; !seen {
				recoveredAt[rc.ID] = e.at
			}
		}
	}
	for id := uint32(0); id < uint32(n); id++ {
		bucket := int(int64(id) * 10 / int64(n))
		if delivered[id] {
			deliv++
			continue
		}
		ra, ok := recoveredAt[id]
		D := writeAt[id].Add(cfg.BufferMicros)
		if ok && !ra.After(D) {
			prem++
			premDec[bucket]++
		} else {
			gen++
			genDec[bucket]++
		}
	}
	return
}

// TestSlidingResidualDiag is a non-asserting diagnostic: it locates the sliding
// residual (warmup vs steady-state) and probes whether a higher proactive floor
// or a longer stream moves it — telling us whether genuineLoss is a warmup
// under-provision and whether premature tracks it.
func TestSlidingResidualDiag(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("diagnostic; run explicitly")
	}
	const owd, src = int64(50_000), int64(500)
	base := Config{Flow: 1, SymbolSize: 64, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 400_000}

	t.Log("decile buckets are stream position 0..9 (0 = first 10% = warmup region)")
	for _, red := range []float64{0.15, 0.30, 0.50} {
		cfg := base
		cfg.Redundancy = red
		d, pr, g, prDec, gDec, _ := slidingClassify(t, cfg, 2000, owd, src, 0, 0x5000)
		t.Logf("Redundancy=%.2f jit=0 n=2000: deliv=%d prem=%d gen=%d", red, d, pr, g)
		t.Logf("    premature by decile: %v", prDec)
		t.Logf("    genuineLoss by decile: %v", gDec)
	}
	// JITTER sweep: the live session has real-time delivery jitter the clean sim lacks.
	// If the residual reappears with jitter, the live ~2% is recovery landing too close
	// to the deadline (a margin problem), not a warmup under-provision.
	for _, jit := range []int64{0, 5_000, 15_000, 30_000} {
		d, pr, g, prDec, gDec, _ := slidingClassify(t, base, 2000, owd, src, jit, 0x5000)
		t.Logf("Redundancy=0.15 jit=%dms n=2000: deliv=%d (%.2f%%) prem=%d gen=%d  premDec=%v genDec=%v",
			jit/1000, d, 100*float64(d)/2000, pr, g, prDec, gDec)
	}
}
