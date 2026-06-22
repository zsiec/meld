package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestPerSymbolDeadlineTailPreserved pins the per-symbol-deadline win the bench credits with
// +8–9 points of heavy-loss long-haul delivery: "a cursor stalled on one unrecoverable symbol
// no longer drops the generation's already-received tail with it." One id in a generation is
// made unrecoverable (its systematic and ALL repair dropped); the rest of the generation
// arrives. Under a SHARED generation deadline the whole tail would expire with the stalled
// symbol; under per-symbol deadlines the cursor skips only the expired id and delivers the
// rest. A regression to a shared deadline drops Delivered from GenSize-1 to ~2 and fails here.
func TestPerSymbolDeadlineTailPreserved(t *testing.T) {
	cfg := testConfig() // GenSize = 16, 1 ms inter-write so per-symbol deadlines stagger
	const n = testGen
	const lostID = uint32(2)
	drop := func(sym wire.Symbol) bool {
		if sym.Kind == wire.Repair {
			return true // no repair ⇒ the hole is unrecoverable, the cursor must stall then skip
		}
		return sym.SrcIndex == lostID
	}
	res := runFlow(t, cfg, n, 3, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if res.stats.Lost != 1 {
		t.Fatalf("expected exactly the one unrecoverable id lost, got %d", res.stats.Lost)
	}
	if int(res.stats.Delivered) != n-1 {
		t.Fatalf("the generation tail was dropped with the stalled symbol: delivered %d, want %d", res.stats.Delivered, n-1)
	}
	for _, id := range res.delivered {
		if id == lostID {
			t.Fatalf("delivered the unrecoverable id %d", lostID)
		}
	}
}

// TestRankDeficientBoundaryContained checks the deadline-boundary behavior of a generation
// that never reaches full rank: its loss must be CONTAINED and accounted exactly — exactly its
// own holes are declared lost, the cursor advances cleanly past it, and the neighboring
// generations deliver whole. It also exercises the coded-recovery oracle at the flow level: a
// generation with fewer equations than unknowns recovers NONE of its holes (and runFlow's
// byte check would fire on any falsely-claimed recovery), so Recovered stays 0 for that gen.
func TestRankDeficientBoundaryContained(t *testing.T) {
	cfg := testConfig() // GenSize 16, repair floor 4
	const n = testGen * 3
	const badBase = uint32(testGen)                                            // the middle generation [16, 32)
	holes := map[uint32]bool{18: true, 21: true, 23: true, 25: true, 28: true} // 5 holes > 4 repair ⇒ deficient
	drop := func(sym wire.Symbol) bool {
		if sym.Kind == wire.Repair {
			return sym.WindowBase == badBase // drop ALL repair for the bad gen (incl. reactive) ⇒ stays deficient
		}
		return holes[sym.SrcIndex]
	}
	res := runFlow(t, cfg, n, 5, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if res.stats.Duplicates != 0 {
		t.Fatalf("duplicate delivery: %d", res.stats.Duplicates)
	}
	// Full accounting, and the loss is exactly the bad generation's holes — contained.
	if got := res.stats.Delivered + res.stats.Lost; got != uint64(n) {
		t.Fatalf("accounting %d (delivered=%d lost=%d) != %d", got, res.stats.Delivered, res.stats.Lost, n)
	}
	if res.stats.Lost != uint64(len(holes)) {
		t.Fatalf("expected exactly the %d holes lost (contained), got %d", len(holes), res.stats.Lost)
	}
	if int(res.stats.Delivered) != n-len(holes) {
		t.Fatalf("neighboring generations did not deliver whole: delivered %d, want %d", res.stats.Delivered, n-len(holes))
	}
	// Oracle: with 4 repair for 5 unknowns the decoder cannot prove any hole — it must claim
	// no recovery rather than guess.
	for _, id := range res.delivered {
		if holes[id] {
			t.Fatalf("delivered a hole id %d the rank cannot support (false recovery)", id)
		}
	}
}
