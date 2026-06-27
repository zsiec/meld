package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

func drainSlidingSymbols(t *testing.T, s *SlidingSender) []wire.Symbol {
	t.Helper()
	var out []wire.Symbol
	for {
		d, ok := s.PollSend()
		if !ok {
			return out
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("decode symbol: %v", err)
		}
		out = append(out, sym)
	}
}

func slidingSystematics(syms []wire.Symbol) []wire.Symbol {
	out := syms[:0]
	for _, sym := range syms {
		if sym.Kind == wire.Systematic {
			out = append(out, sym)
		}
	}
	return out
}

func TestSlidingSenderCarriesFrameDescriptors(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, CodingWindow: 8, BufferMicros: testBuf, Sliding: true}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)

	key := FrameDesc{Priority: uepCenterTier + 1, FrameID: 7, Chunks: 2, RAP: true}
	s.WriteFrame(now, makeChunkN(0), key)
	s.WriteFrame(now.Add(testTick), makeChunkN(1), key)
	dep := FrameDesc{Priority: uepCenterTier, FrameID: 8, RefFrameIDs: []uint32{7}, Chunks: 1, NonPicture: true}
	s.WriteFrame(now.Add(2*testTick), makeChunkN(2), dep)

	sys := slidingSystematics(drainSlidingSymbols(t, s))
	if len(sys) != 3 {
		t.Fatalf("systematics=%d, want 3", len(sys))
	}
	for i := 0; i < 2; i++ {
		if !sys[i].HasFrameDesc || sys[i].FrameStart != 0 || sys[i].FrameLen != 2 || !sys[i].FrameRAP {
			t.Fatalf("key chunk %d descriptor = %+v", i, sys[i])
		}
	}
	if !sys[2].HasFrameDesc || sys[2].FrameStart != 2 || sys[2].FrameLen != 1 || !sys[2].FrameNonPicture {
		t.Fatalf("dependent descriptor = %+v", sys[2])
	}
	if len(sys[2].FrameRefs) != 1 || sys[2].FrameRefs[0] != 0 {
		t.Fatalf("dependent refs = %v, want [0]", sys[2].FrameRefs)
	}
}

func TestSlidingSenderRecoveryRefreshDoesNotEmitScheduledIslandByDefault(t *testing.T) {
	cfg := Config{
		Flow:                   1,
		SymbolSize:             testSym,
		CodingWindow:           16,
		BufferMicros:           testBuf,
		Sliding:                true,
		SingletonRepairGap:     1,
		ProtectedRepairPhasing: true,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)

	s.WriteFrame(now, makeChunkN(0), FrameDesc{Priority: uepCenterTier, FrameID: 1, Chunks: 1, RecoveryRefresh: true})
	s.WriteFrame(now.Add(testTick), makeChunkN(1), FrameDesc{Priority: uepCenterTier, FrameID: 2, RefFrameIDs: []uint32{1}, Chunks: 1, RecoveryRefresh: true})
	s.WriteFrame(now.Add(2*testTick), makeChunkN(2), FrameDesc{Priority: uepCenterTier, FrameID: 3, RefFrameIDs: []uint32{2}, Chunks: 1})

	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.Systematic && sym.SrcIndex < 2 && !sym.FrameRecoveryRefresh {
			t.Fatalf("refresh systematic missing descriptor tag: %+v", sym)
		}
		if sym.Kind == wire.SparseRepair && len(sym.SparseIDs) == 2 && sym.SparseIDs[0] == 0 && sym.SparseIDs[1] == 1 {
			t.Fatalf("default emitted sparse repair over refresh island: %+v", sym)
		}
	}
	if _, ok := s.protected[0]; !ok {
		t.Fatal("recovery-refresh source 0 was not retained in protected-source tracking")
	}
}

func TestSlidingReceiverFrameStatsFromDescriptors(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, CodingWindow: 8, BufferMicros: testBuf, Sliding: true}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	now := clock.Timestamp(0)

	frames := []FrameDesc{
		{Priority: uepCenterTier + 1, FrameID: 1, Chunks: 1, RAP: true},
		{Priority: uepCenterTier, FrameID: 2, RefFrameIDs: []uint32{1}, Chunks: 1},
		{Priority: uepCenterTier, FrameID: 3, RefFrameIDs: []uint32{2}, Chunks: 1},
	}
	for i, fd := range frames {
		s.WriteFrame(now, makeChunkN(uint32(i)), fd)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || sym.Kind != wire.Systematic {
				continue
			}
			r.FeedSymbol(now, d)
		}
		for {
			_, _, ok := r.PollDeliver()
			if !ok {
				break
			}
		}
		now = now.Add(testTick)
	}

	fs := r.FrameStats()
	if fs.Frames != 3 || fs.DecodableFrames != 3 || fs.Keyframes != 1 || fs.DecodableKeyframes != 1 {
		t.Fatalf("FrameStats = %+v, want all three frames and one keyframe decodable", fs)
	}
}

func TestSlidingReceiverFrameStatsExcludeNonPicture(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, CodingWindow: 8, BufferMicros: testBuf, Sliding: true}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	now := clock.Timestamp(0)

	descs := []FrameDesc{
		{Priority: uepCenterTier + 1, FrameID: 1, Chunks: 1, NonPicture: true},
		{Priority: uepCenterTier + 1, FrameID: 2, Chunks: 1, RAP: true},
	}
	for i, fd := range descs {
		s.WriteFrame(now, makeChunkN(uint32(i)), fd)
		for _, sym := range drainSlidingSymbols(t, s) {
			if sym.Kind == wire.Systematic {
				r.FeedSymbol(now, wire.EncodeSymbol(nil, sym))
			}
		}
		for {
			_, _, ok := r.PollDeliver()
			if !ok {
				break
			}
		}
		now = now.Add(testTick)
	}

	fs := r.FrameStats()
	if fs.Frames != 1 || fs.DecodableFrames != 1 || fs.Keyframes != 1 || fs.DecodableKeyframes != 1 {
		t.Fatalf("FrameStats = %+v, want only the picture RAP counted", fs)
	}
}

func TestSlidingEvictBrokenFramesAbandonsDoomedGap(t *testing.T) {
	run := func(evict bool) (map[uint32]int, ReceiverStats) {
		cfg := Config{Flow: 1, SymbolSize: testSym, CodingWindow: 8, BufferMicros: 20_000, Sliding: true, EvictBrokenFrames: evict}
		s := NewSlidingSender(cfg)
		r := NewSlidingReceiver(cfg)
		now := clock.Timestamp(0)
		deliveredAt := map[uint32]int{}
		tick := 0
		pump := func(drop map[uint32]bool) {
			for {
				d, ok := s.PollSend()
				if !ok {
					break
				}
				sym, err := wire.DecodeSymbol(d)
				if err != nil {
					continue
				}
				if sym.Kind == wire.Repair {
					continue
				}
				if drop[sym.SrcIndex] {
					continue
				}
				r.FeedSymbol(now, d)
			}
			for {
				_, d, ok := r.PollDeliver()
				if !ok {
					break
				}
				deliveredAt[chunkID(d)] = tick
			}
		}

		descs := []FrameDesc{
			{Priority: uepCenterTier + 1, FrameID: 10, Chunks: 2, RAP: true},
			{Priority: uepCenterTier + 1, FrameID: 10, Chunks: 2, RAP: true},
			{Priority: uepCenterTier, FrameID: 11, RefFrameIDs: []uint32{10}, Chunks: 2},
			{Priority: uepCenterTier, FrameID: 11, RefFrameIDs: []uint32{10}, Chunks: 2},
			{Priority: uepCenterTier + 1, FrameID: 12, Chunks: 1, RAP: true},
		}
		drop := map[uint32]bool{1: true, 2: true}
		for i, fd := range descs {
			s.WriteFrame(now, makeChunkN(uint32(i)), fd)
			pump(drop)
			now = now.Add(testTick)
			tick++
			r.Tick(now)
			pump(drop)
		}
		for tick <= 64 {
			now = now.Add(testTick)
			tick++
			r.Tick(now)
			pump(drop)
		}
		return deliveredAt, r.Stats()
	}

	off, offStats := run(false)
	on, onStats := run(true)
	if _, ok := on[3]; ok {
		t.Fatal("eviction ON delivered a chunk from a frame whose reference subtree was already dead")
	}
	if onStats.Evicted == 0 {
		t.Fatal("EvictBrokenFrames did not evict the doomed dependent gap")
	}
	if offStats.Evicted != 0 {
		t.Fatalf("eviction OFF counted evictions: %+v", offStats)
	}
	tOn, okOn := on[4]
	tOff, okOff := off[4]
	if !okOn || !okOff {
		t.Fatalf("next RAP delivery missing (on=%v off=%v)", okOn, okOff)
	}
	if tOn >= tOff {
		t.Fatalf("next RAP was not earlier with eviction: ON tick %d vs OFF tick %d", tOn, tOff)
	}
}
