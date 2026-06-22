package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestReactiveWarmupRecovery pins the per-generation reactive-sizing fix. The receiver's
// channel-loss estimator reports 0 until its first full window (~8 generations) completes, so a
// burst that hits the FIRST generation is invisible to the global pEst. Reactive repair must
// still recover it — by sizing the batch to the loss THAT generation experienced (derived from
// the deficit and the repair already sent), not the lagging global average. Loss is confined to
// the first few generations and exceeds the proactive count, so only correctly-sized reactive
// repair recovers it within the deadline. (Before the fix this leaned on a blind cap-sized
// over-send; after it, the batch is sized to the real per-generation loss.)
func TestReactiveWarmupRecovery(t *testing.T) {
	cfg := testConfig() // GenSize 16, repair floor 4, 150 ms budget
	const n = testGen * 12
	rfloor := cfg.repairFloor(cfg.GenSize)
	// Drop rfloor+2 systematic in each of the first 3 generations (the warmup window, where the
	// loss estimate is still 0), repair intact so the loss is recoverable by reactive repair.
	holes := map[uint32]bool{}
	for g := 0; g < 3; g++ {
		for k := 0; k < rfloor+2; k++ {
			holes[uint32(g*cfg.GenSize+k)] = true // a front-loaded burst within the generation
		}
	}
	drop := func(sym wire.Symbol) bool {
		return sym.Kind == wire.Systematic && holes[sym.SrcIndex]
	}
	res := runFlow(t, cfg, n, 7, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if len(res.delivered) != n {
		t.Fatalf("warmup-window loss not recovered: %d/%d (lost=%d recovered=%d reactive=%d) — reactive repair sized to the lagging pEst, not the per-generation loss",
			len(res.delivered), n, res.stats.Lost, res.stats.Recovered, res.sstats.ReactiveRepair)
	}
	if res.sstats.ReactiveRepair == 0 {
		t.Fatal("expected reactive repair to drive the warmup recovery")
	}
}
