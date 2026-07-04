package flow

// Repair-byte attribution (scratchpad/all-regimes/PREREG-cost.md): the sliding
// profile accounts every repair emission to exactly one mechanism, so cost work
// trims what measurement indicts. The flow-level split below is DIAGNOSTIC
// (real-timing splits come from glassbench); the invariant is the load-bearing
// assertion.

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestProtectedGroupConsolidation pins the M-D' mechanism (PREREG-cost.md
// Amendments 1-2): on a WIDE band, center-tier protected chunks share ONE
// consolidated sparse repair per group (≤ protectedGroupMaxIDs ids, ≤ half a
// band of span) instead of a per-chunk singleton, while tiers above center keep
// true singletons; on a NARROW (deadline-clipped, sub-RTT) band the band rate
// cannot carry the delegated multi-loss cover, so per-chunk singletons persist
// — the 0.75×RTT frontier-guard failure Amendment 2 records.
func TestProtectedGroupConsolidation(t *testing.T) {
	run := func(budget int64) (sparse, singles int, st SenderStats) {
		cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
			Redundancy: 0, BufferMicros: budget}
		s := NewSlidingSender(cfg)
		s.interMicros = 1_000 // prime the cadence estimate so effectiveBand is deterministic from write 1
		now := clock.Timestamp(0)
		for i := 0; i < 48; i++ {
			if i%16 == 0 {
				// Parameter-set-like high tier: keeps a true singleton.
				s.WriteUnit(now, makeChunkN(uint32(i)), uepCenterTier+1)
			} else {
				// Center-tier reference frames (the frame-aware path glassbench uses).
				fd := FrameDesc{Priority: uepCenterTier, FrameID: uint32(i), Chunks: 1}
				s.WriteFrame(now, makeChunkN(uint32(i)), fd)
			}
			now = now.Add(1_000)
		}
		s.Flush(now)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				t.Fatalf("DecodeSymbol: %v", err)
			}
			switch {
			case sym.Kind == wire.SparseRepair:
				sparse++
				if len(sym.SparseIDs) > protectedGroupMaxIDs {
					t.Fatalf("group sparse repair carries %d ids, cap %d", len(sym.SparseIDs), protectedGroupMaxIDs)
				}
			case sym.Kind == wire.Repair && sym.N == 1:
				singles++
			}
		}
		return sparse, singles, s.Stats()
	}

	// Wide band with extras still active (100 ms budget at the 50 ms default RTT
	// estimate: one honest cycle plus burst detection does NOT fit, so extras are
	// on; band = 32 ≥ 2×cap): consolidation active.
	sparse, singles, st := run(100_000)
	if st.RepairSparse == 0 || sparse == 0 {
		t.Fatal("wide band: center-tier consolidation emitted no group sparse repair")
	}
	// 45 center-tier chunks share group equations (12-id cap and span cap both
	// bind at this geometry), not one per chunk.
	if int(st.RepairSparse) > 45/4 {
		t.Fatalf("wide band: sparse group repairs = %d for 45 center chunks — not consolidated", st.RepairSparse)
	}
	if st.RepairSingleton != 3 || singles != 3 {
		t.Fatalf("wide band: high-tier singletons = %d (stats %d), want exactly the 3 high-tier chunks", singles, st.RepairSingleton)
	}

	// Narrow band (60 ms budget ⇒ band 15 < 2×cap): the fallback keeps per-chunk
	// singletons for every protected chunk.
	_, singlesNarrow, stNarrow := run(60_000)
	if stNarrow.RepairSingleton != 48 || singlesNarrow != 48 {
		t.Fatalf("narrow band: singletons = %d (stats %d), want all 48 protected chunks",
			singlesNarrow, stNarrow.RepairSingleton)
	}
}

// TestRepairAttributionInvariant: across contrasting channel regimes, every
// sliding-profile repair emission lands in exactly one attribution bucket.
func TestRepairAttributionInvariant(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		owd  int64
		bud  int64
		drop func(wire.Symbol) bool
	}{
		{"clean-1x", 50_000, 100_000, func(wire.Symbol) bool { return false }},
		{"iid10-1x", 50_000, 100_000, uniformDrop(0xA11, 0.10)},
		{"iid10-1p5x", 50_000, 150_000, uniformDrop(0xA12, 0.10)},
		{"burst-2p5x", 30_000, 150_000, geDrop(0xA13, 0.10, 12)},
	}
	for _, tc := range cases {
		cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64,
			Redundancy: 0.05, TargetFailure: 1e-3, BufferMicros: tc.bud}
		res := simLink{cfg: cfg, owdMicros: tc.owd, srcMicros: 500, n: 2_000, sliding: true, drop: tc.drop}.run()
		st := res.sstats
		sum := st.RepairProactive + st.RepairProactiveCold + st.RepairSingleton + st.RepairSparse + st.RepairDeficit
		if sum != st.Repair {
			t.Errorf("%s: attribution sum %d != Repair %d (proactive=%d cold=%d singleton=%d sparse=%d deficit=%d)",
				tc.name, sum, st.Repair, st.RepairProactive, st.RepairProactiveCold, st.RepairSingleton, st.RepairSparse, st.RepairDeficit)
			continue
		}
		t.Logf("%-12s repair=%d: proactive=%d cold=%d singleton=%d sparse=%d deficit=%d (delivered %d/%d)",
			tc.name, st.Repair, st.RepairProactive, st.RepairProactiveCold, st.RepairSingleton, st.RepairSparse, st.RepairDeficit,
			res.delivered, res.n)
	}
}
