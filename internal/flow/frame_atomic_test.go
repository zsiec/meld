package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestFrameAtomicDelivery proves the perceptual delivery contract: a broken access unit is
// delivered ALL-OR-NOTHING. With FrameAtomic on, a 4-chunk frame missing one unrecoverable chunk
// delivers ZERO chunks (a clean gap the decoder conceals); with it off (the control), the prefix
// leaks as a partial picture. A fully-recovered frame still delivers whole either way.
func TestFrameAtomicDelivery(t *testing.T) {
	// Drive one RAP frame of `chunks` chunks (ids 0..chunks-1), losing the systematic for
	// `lose` (no repair ⇒ unrecoverable), and return which ids the receiver delivered.
	run := func(atomic bool, chunks, lose int) []uint32 {
		cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, Redundancy: 0, BufferMicros: 50_000, FrameAtomic: atomic}
		s := NewSender(cfg)
		r := NewReceiver(cfg)
		now := clock.Timestamp(0)
		fd := FrameDesc{Priority: 1, FrameID: 0, Chunks: uint16(chunks), RAP: true}
		for c := 0; c < chunks; c++ {
			s.WriteFrame(now, makeChunkN(uint32(c)), fd)
		}
		s.Flush(now)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			if sym, err := wire.DecodeSymbol(d); err == nil {
				if sym.Kind == wire.Systematic && lose >= 0 && sym.SrcIndex == uint32(lose) {
					continue // this chunk is lost; with no repair it is unrecoverable
				}
				r.FeedSymbol(now, d)
			}
		}
		now = now.Add(cfg.BufferMicros + 100_000) // past the frame's deadline
		r.Tick(now)
		var got []uint32
		for {
			id, _, ok := r.PollDeliver()
			if !ok {
				break
			}
			got = append(got, id)
		}
		return got
	}

	t.Run("broken frame drops whole, atomic", func(t *testing.T) {
		if got := run(true, 4, 2); len(got) != 0 {
			t.Fatalf("frame-atomic delivered %v of a broken 4-chunk frame — must drop the whole picture", got)
		}
	})
	t.Run("broken frame leaks prefix, non-atomic control", func(t *testing.T) {
		got := run(false, 4, 2)
		if len(got) == 0 {
			t.Fatal("control: a broken frame should leak its recoverable prefix (partial picture)")
		}
		t.Logf("non-atomic control delivered %v (a partial picture) from the same broken frame", got)
	})
	t.Run("whole frame still delivers, atomic", func(t *testing.T) {
		got := run(true, 4, -1) // no loss
		if len(got) != 4 {
			t.Fatalf("frame-atomic delivered %v of a fully-recovered 4-chunk frame — want all 4", got)
		}
	})
}

func TestDefaultConfigFrameAtomicIsOptIn(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Flow = 1
	cfg.SymbolSize = testSym
	cfg.GenSize = 16
	cfg.Redundancy = 0
	cfg.BufferMicros = 50_000

	if cfg.FrameAtomic {
		t.Fatal("DefaultConfig enables FrameAtomic; high-delivery frame metadata must be the default, all-or-nothing delivery is opt-in")
	}

	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	fd := FrameDesc{Priority: 1, FrameID: 0, Chunks: 4, RAP: true}
	for c := 0; c < 4; c++ {
		s.WriteFrame(now, makeChunkN(uint32(c)), fd)
	}
	s.Flush(now)
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			continue
		}
		if sym.Kind == wire.Systematic && sym.SrcIndex == 2 {
			continue // make the frame incomplete and unrecoverable
		}
		r.FeedSymbol(now, d)
	}
	now = now.Add(cfg.BufferMicros + 100_000)
	r.Tick(now)

	var got []uint32
	for {
		id, _, ok := r.PollDeliver()
		if !ok {
			break
		}
		got = append(got, id)
	}
	if len(got) == 0 {
		t.Fatal("default WriteFrame path dropped the whole broken frame; expected non-atomic byte delivery unless FrameAtomic is enabled")
	}
	if st := r.Stats(); st.Evicted != 0 {
		t.Fatalf("default WriteFrame path evicted %d frame chunks; expected no frame-atomic evictions", st.Evicted)
	}
}
