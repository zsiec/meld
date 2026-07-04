package flow

import (
	"bytes"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

func u32sEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHighPrioritySystematicGetsDeferredSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 60_000}
	s := NewSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier+1)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			t.Fatal("singleton repair was emitted adjacent to the protected source")
		}
	}

	for i := 0; i < int(cfg.singletonRepairGap()); i++ {
		s.WriteUnit(clock.Timestamp(i+1), makeChunkN(uint32(8+i)), uepCenterTier)
	}

	found := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind != wire.Repair {
			continue
		}
		found = true
		if sym.WindowBase != 0 || sym.N != 1 || sym.Priority != uepCenterTier+1 || sym.Deadline != 60_000 {
			t.Fatalf("singleton repair = base %d n %d priority %d deadline %d, want base 0 n 1 priority %d deadline 60000",
				sym.WindowBase, sym.N, sym.Priority, sym.Deadline, uepCenterTier+1)
		}
	}
	if !found {
		t.Fatal("did not find singleton repair for high-priority source")
	}
}

func TestSlidingHighPrioritySystematicGetsDeferredSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16, BufferMicros: 60_000}
	s := NewSlidingSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier+1)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Systematic && sym.SrcIndex == 0 {
			continue
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 {
			t.Fatal("sliding singleton repair was emitted adjacent to the protected source")
		}
	}

	for i := 0; i < int(cfg.singletonRepairGap()); i++ {
		s.WriteUnit(clock.Timestamp(i+1), makeChunkN(uint32(8+i)), uepCenterTier)
	}

	found := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind != wire.Repair || sym.WindowBase != 0 || sym.N != 1 {
			continue
		}
		found = true
		if sym.Priority != uepCenterTier+1 || sym.Deadline != 60_000 {
			t.Fatalf("sliding singleton priority/deadline = %d/%d, want %d/60000",
				sym.Priority, sym.Deadline, uepCenterTier+1)
		}
	}
	if !found {
		t.Fatal("did not find sliding singleton repair for high-priority source")
	}
}

func TestSlidingProtectedFeedbackGetsSparseRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 8, BufferMicros: 60_000, Redundancy: 0}
	s := NewSlidingSender(cfg)
	for i := 0; i < 20; i++ {
		fd := FrameDesc{Priority: uepCenterTier, FrameID: uint32(i), Chunks: 1}
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), fd)
	}
	drainSlidingSymbols(t, s)

	s.FeedFeedback(clock.Timestamp(100_000), wire.Feedback{
		Flow:           cfg.Flow,
		DecodedLowEdge: 2,
		HighestSeen:    20,
		Deficit:        3,
		LossRate:       32768,
		Burstiness:     burstQ8One,
	})

	// Consolidated protected GROUPS also travel as SparseRepair now, so the
	// assertion filters for the feedback-driven retry's exact id set.
	wantIDs := []uint32{2, 3, 4, 5, 6, 7, 8, 9}
	sparse := 0
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind != wire.SparseRepair || !u32sEqual(sym.SparseIDs, wantIDs) {
			continue
		}
		sparse++
		if sym.N != uint16(len(wantIDs)) || sym.Priority != uepCenterTier+1 {
			t.Fatalf("sparse repair = ids %v n %d priority %d, want %v/%d/%d",
				sym.SparseIDs, sym.N, sym.Priority, wantIDs, len(wantIDs), uepCenterTier+1)
		}
	}
	if sparse != 1 {
		t.Fatalf("feedback sparse repairs = %d, want 1", sparse)
	}
}

func TestSlidingProtectedSparseRepairGetsDelayedRetry(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 8, BufferMicros: 60_000, Redundancy: 0}
	s := NewSlidingSender(cfg)
	for i := 0; i < 20; i++ {
		fd := FrameDesc{Priority: uepCenterTier, FrameID: uint32(i), Chunks: 1}
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), fd)
	}
	drainSlidingSymbols(t, s)

	s.FeedFeedback(clock.Timestamp(100_000), wire.Feedback{
		Flow:           cfg.Flow,
		DecodedLowEdge: 2,
		HighestSeen:    20,
		Deficit:        3,
		LossRate:       32768,
		Burstiness:     burstQ8One,
	})

	// Group-consolidation sparse repairs share the wire kind; count only the
	// retry's exact id set.
	wantIDs := []uint32{2, 3, 4, 5, 6, 7, 8, 9}
	immediate := 0
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.SparseRepair && u32sEqual(sym.SparseIDs, wantIDs) {
			immediate++
		}
	}
	if immediate != 1 {
		t.Fatalf("immediate feedback sparse repairs = %d, want 1", immediate)
	}

	for i := 20; i <= 20+int(cfg.singletonRepairGap()); i++ {
		fd := FrameDesc{Priority: uepCenterTier, FrameID: uint32(i), Chunks: 1}
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), fd)
	}

	delayed := 0
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.SparseRepair && u32sEqual(sym.SparseIDs, wantIDs) {
			delayed++
		}
	}
	if delayed != 1 {
		t.Fatalf("delayed feedback sparse repairs = %d, want 1", delayed)
	}
}

func TestSlidingReceiverSparseRepairRecoversProtectedGap(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 8, BufferMicros: 60_000}
	r := NewSlidingReceiver(cfg)
	enc := code.NewEncoder(testSym)
	src := make([][]byte, 6)
	for i := range src {
		src[i] = makeChunkN(uint32(i))
		enc.Add(src[i])
	}
	now := clock.Timestamp(0)
	feedSystematic := func(id uint32) {
		r.FeedSymbol(now, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.Systematic, WindowBase: id, SrcIndex: id, N: 1,
			Deadline: int64(now.Add(cfg.BufferMicros)), Payload: src[id],
		}))
	}
	for _, id := range []uint32{0, 2, 4, 5} {
		feedSystematic(id)
	}

	ids := []uint32{1, 3}
	for key := uint16(0); key < 2; key++ {
		pay, ok := enc.RepairSparse(key, ids)
		if !ok {
			t.Fatal("RepairSparse returned !ok")
		}
		r.FeedSymbol(now, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.SparseRepair, SrcIndex: uint32(key), RepairKey: key,
			SparseIDs: ids, Deadline: int64(now.Add(cfg.BufferMicros)), Payload: pay,
		}))
	}

	delivered := map[uint32]bool{}
	for {
		_, data, ok := r.PollDeliver()
		if !ok {
			break
		}
		delivered[chunkID(data)] = true
	}
	for id := uint32(0); id < 6; id++ {
		if !delivered[id] {
			t.Fatalf("id %d not delivered after sparse repair", id)
		}
	}
}

func TestSlidingRAPAnchorClosureQueuesSpacedSparseRepair(t *testing.T) {
	const gap = 8
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64, BufferMicros: 60_000, Redundancy: 0, SingletonRepairGap: gap, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)

	for i := 0; i < gap; i++ {
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), FrameDesc{
			Priority: uepCenterTier - 1, FrameID: uint32(i), Chunks: 1, Discardable: true,
		})
	}
	s.WriteFrame(clock.Timestamp(8), makeChunkN(8), FrameDesc{Priority: uepCenterTier + 2, FrameID: 100, Chunks: 1, NonPicture: true})
	s.WriteFrame(clock.Timestamp(9), makeChunkN(9), FrameDesc{Priority: uepCenterTier + 2, FrameID: 101, Chunks: 1, NonPicture: true})
	s.WriteFrame(clock.Timestamp(10), makeChunkN(10), FrameDesc{Priority: uepCenterTier + 1, FrameID: 102, RefFrameIDs: []uint32{100, 101}, Chunks: 1, RAP: true})

	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.SparseRepair {
			t.Fatal("anchor sparse repair emitted adjacent to RAP closure")
		}
	}

	want := []uint32{8, 9, 10}
	wantSlots := map[int]bool{16: true, 18: true, 20: true, 22: true}
	gotSlots := map[int]int{}
	for i := 11; i <= 22; i++ {
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), FrameDesc{
			Priority: uepCenterTier - 1, FrameID: uint32(100 + i), Chunks: 1, Discardable: true,
		})
		for _, sym := range drainSlidingSymbols(t, s) {
			if sym.Kind != wire.SparseRepair {
				continue
			}
			if !u32sEqual(sym.SparseIDs, want) {
				t.Fatalf("anchor sparse ids = %v, want %v", sym.SparseIDs, want)
			}
			if !wantSlots[i] {
				t.Fatalf("anchor sparse repair emitted at slot %d, want one of %v", i, wantSlots)
			}
			gotSlots[i]++
		}
	}

	for slot := range wantSlots {
		if gotSlots[slot] != 1 {
			t.Fatalf("anchor sparse repairs at slot %d = %d, want 1; got slots %v", slot, gotSlots[slot], gotSlots)
		}
	}
}

func TestSlidingRAPAnchorClosureSkipsDisposablePrefix(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64, BufferMicros: 60_000, Redundancy: 0, SingletonRepairGap: 2, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)

	for i := 0; i < 4; i++ {
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), FrameDesc{
			Priority: uepCenterTier - 1, FrameID: uint32(i), Chunks: 1, Discardable: true,
		})
	}
	s.WriteFrame(clock.Timestamp(4), makeChunkN(4), FrameDesc{Priority: uepCenterTier + 2, FrameID: 10, Chunks: 1, NonPicture: true})
	s.WriteFrame(clock.Timestamp(5), makeChunkN(5), FrameDesc{Priority: uepCenterTier + 2, FrameID: 11, Chunks: 1, NonPicture: true})
	s.WriteFrame(clock.Timestamp(6), makeChunkN(6), FrameDesc{Priority: uepCenterTier + 1, FrameID: 12, RefFrameIDs: []uint32{10, 11}, Chunks: 1, RAP: true})
	drainSlidingSymbols(t, s)

	release := 6 + int(cfg.singletonRepairGap())
	for i := 7; i <= release; i++ {
		s.WriteFrame(clock.Timestamp(i), makeChunkN(uint32(i)), FrameDesc{
			Priority: uepCenterTier - 1, FrameID: uint32(100 + i), Chunks: 1, Discardable: true,
		})
	}

	want := []uint32{4, 5, 6}
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind != wire.SparseRepair {
			continue
		}
		if !u32sEqual(sym.SparseIDs, want) {
			t.Fatalf("anchor sparse ids = %v, want %v", sym.SparseIDs, want)
		}
		return
	}
	t.Fatal("did not emit anchor sparse repair")
}

// NOTE (all-regimes pass, 2026-07-02): these singleton/anchor mechanism tests run at
// BufferMicros 60 ms — BELOW the honest reactive cycle at the pre-sample 50 ms RTT
// estimate (reactiveCycleMicros = 67.5 ms) — because the dedicated per-chunk extras
// are now deliberately gated to the reactive-INCAPABLE regime: where a reactive cycle
// fits the budget, retrospective repair reaches a reference hole on demand and the
// extras would be double coverage. The dormancy of the extras in the capable regime
// is pinned by TestSingletonExtrasDormantWhenReactiveReachable (resync_test.go's
// sibling in reactive_retro_test.go).
func TestConfiguredSingletonRepairGap(t *testing.T) {
	const gap = 3
	cases := []struct {
		name string
		new  func(Config) unitWriter
		cfg  Config
	}{
		{
			name: "generation",
			new:  func(cfg Config) unitWriter { return NewSender(cfg) },
			cfg:  Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 60_000, SingletonRepairGap: gap},
		},
		{
			name: "sliding",
			new:  func(cfg Config) unitWriter { return NewSlidingSender(cfg) },
			cfg:  Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16, BufferMicros: 60_000, SingletonRepairGap: gap},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := tc.new(tc.cfg)
			s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier+1)
			if hasSingletonRepair(t, s, 0) {
				t.Fatal("singleton repair emitted adjacent to protected source")
			}
			for i := 1; i < gap; i++ {
				s.WriteUnit(clock.Timestamp(i), makeChunkN(uint32(100+i)), uepCenterTier)
				if hasSingletonRepair(t, s, 0) {
					t.Fatalf("singleton repair emitted after %d symbols, before configured gap %d", i, gap)
				}
			}
			s.WriteUnit(clock.Timestamp(gap), makeChunkN(200), uepCenterTier)
			if !hasSingletonRepair(t, s, 0) {
				t.Fatalf("singleton repair did not emit at configured gap %d", gap)
			}
		})
	}
}

func TestSlidingSingletonProtectedRepairsArePhased(t *testing.T) {
	const gap = 4
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16, BufferMicros: 60_000, SingletonRepairGap: gap, Redundancy: 0, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(0), uepCenterTier+1)
	s.WriteUnit(clock.Timestamp(1), makeChunkN(1), uepCenterTier+1)
	drainSlidingSymbols(t, s)

	for i := 2; i < gap; i++ {
		s.WriteUnit(clock.Timestamp(i), makeChunkN(uint32(100+i)), uepCenterTier)
		if got := singletonRepairBases(t, s); len(got) != 0 {
			t.Fatalf("singleton repairs before configured gap: %v", got)
		}
	}

	s.WriteUnit(clock.Timestamp(gap), makeChunkN(200), uepCenterTier)
	if got := singletonRepairBases(t, s); !u32sEqual(got, []uint32{0}) {
		t.Fatalf("singleton repairs at first release = %v, want [0]", got)
	}

	s.WriteUnit(clock.Timestamp(gap+1), makeChunkN(201), uepCenterTier)
	if got := singletonRepairBases(t, s); len(got) != 0 {
		t.Fatalf("second singleton repair emitted adjacent to first: %v", got)
	}

	s.WriteUnit(clock.Timestamp(gap+2), makeChunkN(202), uepCenterTier)
	if got := singletonRepairBases(t, s); !u32sEqual(got, []uint32{1}) {
		t.Fatalf("singleton repairs at phased release = %v, want [1]", got)
	}
}

func TestDefaultSingletonRepairGapDependsOnCoder(t *testing.T) {
	gen := Config{}
	if got := gen.singletonRepairGap(); got != defaultSingletonRepairGap {
		t.Fatalf("generation default singleton gap = %d, want %d", got, defaultSingletonRepairGap)
	}
	sliding := Config{Sliding: true}
	if got := sliding.singletonRepairGap(); got != slidingSingletonRepairGap {
		t.Fatalf("sliding default singleton gap = %d, want %d", got, slidingSingletonRepairGap)
	}
	override := Config{Sliding: true, SingletonRepairGap: 24}
	if got := override.singletonRepairGap(); got != 24 {
		t.Fatalf("configured singleton gap = %d, want 24", got)
	}
}

func TestSlidingProtectedRepairGapColdStartsForLongBurst(t *testing.T) {
	// A high RTT estimate keeps the sender in the reactive-INCAPABLE regime at this
	// generous budget, so the singleton path (whose gap policy is under test) is active.
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64, BufferMicros: 200_000, Redundancy: 0, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)
	s.rttMicros = 200_000

	s.WriteUnit(clock.Timestamp(0), makeChunkN(0), uepCenterTier+1)
	if len(s.singletons) != 1 {
		t.Fatalf("pending singleton repairs = %d, want 1", len(s.singletons))
	}
	if got := s.singletons[0].releaseAt; got != coldStartBurstGap {
		t.Fatalf("cold-start protected repair release = %d, want %d", got, coldStartBurstGap)
	}
}

func TestSlidingProtectedRepairGapUsesMeasuredBurst(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64, BufferMicros: 200_000, Redundancy: 0, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)
	s.rttMicros = 200_000 // reactive-incapable: the singleton gap policy under test stays active
	s.fbCount = coldStartFeedbacks
	s.burstQ8 = 64 * 256
	s.interMicros = 1_000

	s.WriteUnit(clock.Timestamp(0), makeChunkN(0), uepCenterTier+1)
	if len(s.singletons) != 1 {
		t.Fatalf("pending singleton repairs = %d, want 1", len(s.singletons))
	}
	if got := s.singletons[0].releaseAt; got != 64 {
		t.Fatalf("measured-burst protected repair release = %d, want 64", got)
	}
}

func TestSlidingProtectedRepairGapCapsAtDeadline(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 64, BufferMicros: 40_000, Redundancy: 0, ProtectedRepairPhasing: true}
	s := NewSlidingSender(cfg)
	s.fbCount = coldStartFeedbacks
	s.burstQ8 = 64 * 256
	s.rttMicros = 50_000
	s.interMicros = 1_000

	s.WriteUnit(clock.Timestamp(0), makeChunkN(0), uepCenterTier+1)
	if len(s.singletons) != 1 {
		t.Fatalf("pending singleton repairs = %d, want 1", len(s.singletons))
	}
	if got := s.singletons[0].releaseAt; got != 10 {
		t.Fatalf("deadline-capped protected repair release = %d, want 10", got)
	}
}

func TestHighPriorityTailFlushesSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 60_000}
	s := NewSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier+1)
	s.Flush(clock.Timestamp(1_000))

	found := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("flush did not emit pending singleton repair")
	}
}

func TestPendingSingletonKeepsPayloadAfterGenerationRetire(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, BufferMicros: 60_000}
	s := NewSender(cfg)
	src := makeChunkN(7)
	s.WriteUnit(clock.Timestamp(0), src, uepCenterTier+1)
	s.FeedFeedback(clock.Timestamp(1), wire.Feedback{Flow: cfg.Flow, DecodedLowEdge: 1})

	for i := 0; i < int(cfg.singletonRepairGap()); i++ {
		s.WriteUnit(clock.Timestamp(i+2), makeChunkN(uint32(100+i)), uepCenterTier)
	}

	enc := code.NewEncoderAt(cfg.SymbolSize, 0)
	enc.Add(src)
	_, _, want := enc.Repair(0)
	defer enc.Recycle(want)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 {
			if !bytes.Equal(sym.Payload, want) {
				t.Fatal("pending singleton repair payload changed after its generation retired")
			}
			return
		}
	}
	t.Fatal("did not find singleton repair for retired high-priority source")
}

func TestFlatSystematicGetsNoSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 200_000}
	s := NewSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			t.Fatalf("flat source emitted singleton repair: base %d n %d priority %d", sym.WindowBase, sym.N, sym.Priority)
		}
	}
}

func TestFrameReferenceGetsSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 60_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)

	for i := 0; i < int(cfg.singletonRepairGap()); i++ {
		s.WriteUnit(clock.Timestamp(i+1), makeChunkN(uint32(8+i)), uepCenterTier)
	}

	found := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("did not find singleton repair for frame-aware reference source")
	}
}

func TestFrameDisposableGetsNoSingletonRepair(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 200_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier - 1, FrameID: 1, Chunks: 1, Discardable: true}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)
	s.Flush(clock.Timestamp(1_000))

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 {
			t.Fatal("disposable frame emitted singleton repair")
		}
	}
}

func TestFrameReferenceGetsNoSingletonRepairWithHighRedundancyFloor(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, Redundancy: 0.4, BufferMicros: 200_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)
	s.Flush(clock.Timestamp(1_000))

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == 0 && sym.N == 1 && sym.Priority == uepCenterTier {
			t.Fatal("high-redundancy reference frame emitted singleton repair")
		}
	}
}

func TestReferenceGenerationReactiveRepairUsesReferenceTier(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, Redundancy: 0, BufferMicros: 200_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)
	drainSender(s)

	fb := wire.Feedback{Flow: cfg.Flow, Deficits: [wire.MaxFeedbackGens]uint8{1}}
	s.FeedFeedback(clock.Timestamp(10_000), fb) // protected one-symbol probe
	s.FeedFeedback(clock.Timestamp(30_000), fb) // stuck deficit gets repair

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			if sym.Priority != uepCenterTier+1 {
				t.Fatalf("reactive reference repair priority = %d, want %d", sym.Priority, uepCenterTier+1)
			}
			return
		}
	}
	t.Fatal("did not emit reactive repair for reference generation")
}

func TestReferenceGenerationGetsFirstObservationReactiveProbe(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, Redundancy: 0, BufferMicros: 200_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)
	drainSender(s)

	fb := wire.Feedback{Flow: cfg.Flow, Deficits: [wire.MaxFeedbackGens]uint8{1}}
	s.FeedFeedback(clock.Timestamp(10_000), fb)

	repairs := 0
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind != wire.Repair {
			continue
		}
		repairs++
		if sym.Priority != uepCenterTier+1 {
			t.Fatalf("first-observation reference repair priority = %d, want %d", sym.Priority, uepCenterTier+1)
		}
	}
	if repairs != 1 {
		t.Fatalf("first-observation reference repairs = %d, want 1", repairs)
	}
}

func TestReferenceGenerationHighRedundancyReactiveRepairUsesBaseTier(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, Redundancy: 0.4, BufferMicros: 200_000}
	s := NewSender(cfg)
	fd := FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1}
	s.WriteFrame(clock.Timestamp(0), makeChunkN(7), fd)
	drainSender(s)

	fb := wire.Feedback{Flow: cfg.Flow, Deficits: [wire.MaxFeedbackGens]uint8{1}}
	s.FeedFeedback(clock.Timestamp(10_000), fb)
	s.FeedFeedback(clock.Timestamp(30_000), fb)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			if sym.Priority != uepCenterTier {
				t.Fatalf("high-redundancy reactive reference repair priority = %d, want %d", sym.Priority, uepCenterTier)
			}
			return
		}
	}
	t.Fatal("did not emit reactive repair for high-redundancy reference generation")
}

func TestHighRedundancySizingUsesFlatTier(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, Redundancy: 0.4, BufferMicros: 200_000}

	flat := NewSender(cfg)
	flat.pEst = 0.30
	flat.genMaxPri = uepCenterTier
	flat.genMinTID = noTemporalID

	uep := NewSender(cfg)
	uep.pEst = flat.pEst
	uep.genMaxPri = uepCenterTier + 2
	uep.genMinTID = noTemporalID

	if got, want := uep.repairCountFor(16), flat.repairCountFor(16); got != want {
		t.Fatalf("high-redundancy UEP repair count = %d, want flat count %d", got, want)
	}
}

func TestFlatGenerationReactiveRepairUsesBaseTier(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, Redundancy: 0, BufferMicros: 200_000}
	s := NewSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier)
	drainSender(s)

	fb := wire.Feedback{Flow: cfg.Flow, Deficits: [wire.MaxFeedbackGens]uint8{1}}
	s.FeedFeedback(clock.Timestamp(10_000), fb)
	s.FeedFeedback(clock.Timestamp(30_000), fb)

	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			if sym.Priority != uepCenterTier {
				t.Fatalf("reactive flat repair priority = %d, want %d", sym.Priority, uepCenterTier)
			}
			return
		}
	}
	t.Fatal("did not emit reactive repair for flat generation")
}

func TestFlatGenerationWaitsForStuckDeficit(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 1, Redundancy: 0, BufferMicros: 200_000}
	s := NewSender(cfg)
	s.WriteUnit(clock.Timestamp(0), makeChunkN(7), uepCenterTier)
	drainSender(s)

	fb := wire.Feedback{Flow: cfg.Flow, Deficits: [wire.MaxFeedbackGens]uint8{1}}
	s.FeedFeedback(clock.Timestamp(10_000), fb)
	for {
		d, ok := s.PollSend()
		if !ok {
			return
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair {
			t.Fatal("flat generation emitted reactive repair on first deficit observation")
		}
	}
}

type unitWriter interface {
	WriteUnit(clock.Timestamp, []byte, uint8)
	PollSend() ([]byte, bool)
}

func hasSingletonRepair(t *testing.T, s unitWriter, base uint32) bool {
	t.Helper()
	for {
		d, ok := s.PollSend()
		if !ok {
			return false
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.WindowBase == base && sym.N == 1 {
			return true
		}
	}
}

func singletonRepairBases(t *testing.T, s interface{ PollSend() ([]byte, bool) }) []uint32 {
	t.Helper()
	var out []uint32
	for {
		d, ok := s.PollSend()
		if !ok {
			return out
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Repair && sym.N == 1 {
			out = append(out, sym.WindowBase)
		}
	}
}

func drainSender(s *Sender) {
	for {
		if _, ok := s.PollSend(); !ok {
			return
		}
	}
}
