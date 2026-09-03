package flow

import (
	"bytes"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

func TestSlidingRecoveryPreservesLengthAndDeadline(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 64, Sliding: true, CodingWindow: 8, BufferMicros: 1_000}
	r := NewSlidingReceiver(cfg)
	enc := code.NewEncoder(codedSymbolSize(cfg.SymbolSize))
	sources := [][]byte{
		bytes.Repeat([]byte{0x10}, 3),
		bytes.Repeat([]byte{0x20}, 17),
		bytes.Repeat([]byte{0x30}, 64),
	}
	deadlines := []clock.Timestamp{1_000, 1_100, 1_200}
	for i, src := range sources {
		enc.Add(makeCodedSource(src, cfg.SymbolSize, deadlines[i]))
	}
	feedSource := func(id uint32) {
		r.FeedSymbol(100, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.Systematic, WindowBase: id, SrcIndex: id, N: 1,
			Deadline: int64(deadlines[id]), HasSourceLength: true,
			SourceLength: uint32(len(sources[id])), Payload: sources[id],
		}))
	}
	feedSource(0)
	feedSource(2) // id 1 is recovered solely from the coded row below
	base, n, pay := enc.Repair(0)
	r.FeedSymbol(200, wire.EncodeSymbol(nil, wire.Symbol{
		Flow: cfg.Flow, Kind: wire.Repair, WindowBase: base, N: uint16(n),
		RepairKey: 0, Deadline: int64(deadlines[2]), Payload: pay,
	}))

	for id, want := range sources {
		gotID, got, ok := r.PollDeliver()
		if !ok {
			t.Fatalf("id %d not delivered", id)
		}
		if gotID != uint32(id) || !bytes.Equal(got, want) {
			t.Fatalf("delivery %d = id %d len %d, want id %d len %d", id, gotID, len(got), id, len(want))
		}
	}
}

func TestSlidingRecoveredExactDeadlineRejectsLateInteriorSymbol(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 64, Sliding: true, CodingWindow: 8, BufferMicros: 1_000}
	r := NewSlidingReceiver(cfg)
	enc := code.NewEncoder(codedSymbolSize(cfg.SymbolSize))
	deadlines := []clock.Timestamp{1_000, 1_100, 2_000}
	for i := range deadlines {
		enc.Add(makeCodedSource([]byte{byte(i)}, cfg.SymbolSize, deadlines[i]))
	}
	for _, id := range []uint32{0, 2} {
		r.FeedSymbol(100, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.Systematic, SrcIndex: id, N: 1,
			Deadline: int64(deadlines[id]), HasSourceLength: true, SourceLength: 1,
			Payload: []byte{byte(id)},
		}))
	}
	base, n, pay := enc.Repair(0)
	r.FeedSymbol(1_101, wire.EncodeSymbol(nil, wire.Symbol{
		Flow: cfg.Flow, Kind: wire.Repair, WindowBase: base, N: uint16(n),
		RepairKey: 0, Deadline: int64(deadlines[2]), Payload: pay,
	}))

	var ids []uint32
	for {
		id, _, ok := r.PollDeliver()
		if !ok {
			break
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 0 || ids[1] != 2 {
		t.Fatalf("delivered ids = %v, want [0 2]; recovered id 1 was already late", ids)
	}
}

func TestSlidingCompactRepairRecoversExactSource(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 64, Sliding: true, CodingWindow: 8, Redundancy: 0, BufferMicros: 100_000}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	sources := [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}}
	for i, src := range sources {
		s.Write(clock.Timestamp(1_000+i*1_000), src)
	}
	s.emitRepair(10_000)

	compactSeen := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatal(err)
		}
		if sym.Kind == wire.Systematic && sym.SrcIndex == 1 {
			continue
		}
		if sym.Kind == wire.Repair {
			if !sym.HasSourceLength || len(sym.Payload) >= codedSymbolSize(cfg.SymbolSize) {
				t.Fatalf("repair was not compact: sourceLength=%d payload=%d", sym.SourceLength, len(sym.Payload))
			}
			compactSeen = true
		}
		r.FeedSymbol(20_000, d)
	}
	if !compactSeen {
		t.Fatal("sender emitted no compact repair")
	}
	for id, want := range sources {
		gotID, got, ok := r.PollDeliver()
		if !ok || gotID != uint32(id) || !bytes.Equal(got, want) {
			t.Fatalf("delivery %d = id %d data %v ok=%v", id, gotID, got, ok)
		}
	}
}
