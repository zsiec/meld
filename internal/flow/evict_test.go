package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// evictResult records one run of the early-eviction harness.
type evictResult struct {
	order      []uint32       // source ids in delivery order
	deliverAt  map[uint32]int // id → tick index at delivery
	deadlineAt map[uint32]int // id → its per-symbol deadline tick
	rstats     ReceiverStats
	reactive   uint64 // sender reactive-repair symbols emitted
}

// runEvict streams a unit list through Sender(WriteFrame)/Receiver while dropping a chosen
// set of source ids and ALL repair (so the dropped ids are unrecoverable), then advances
// time to flush. evict toggles Config.EvictBrokenFrames. It returns the delivery order +
// timing so a test can compare resync latency and sender repair across the two modes.
func runEvict(t *testing.T, cfg Config, units []shape.Unit, dropID map[uint32]bool, evict bool) evictResult {
	t.Helper()
	cfg.EvictBrokenFrames = evict
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	tick := 0
	res := evictResult{deliverAt: map[uint32]int{}, deadlineAt: map[uint32]int{}}

	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				continue
			}
			if sym.Kind == wire.Repair { // all repair dropped → dropped ids are unrecoverable
				continue
			}
			if dropID[sym.SrcIndex] {
				continue
			}
			r.FeedSymbol(now, d)
		}
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			id := chunkID(d)
			res.order = append(res.order, id)
			res.deliverAt[id] = tick
		}
	}

	var nextID uint32
	for _, u := range units {
		nch := (u.Size + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		fd := FrameDesc{Priority: u.Class.Wire(), FrameID: u.ID, RefFrameIDs: u.RefersTo, Chunks: uint16(nch), RAP: u.RAP, Discardable: u.Discardable}
		for c := 0; c < nch; c++ {
			res.deadlineAt[nextID] = tick + int(cfg.BufferMicros/testTick)
			s.WriteFrame(now, makeChunkN(nextID), fd)
			nextID++
			pump()
			now = now.Add(testTick)
			tick++
			s.Tick(now)
			r.Tick(now)
			pump()
		}
		s.Flush(now)
		pump()
	}
	// Drain: advance well past the last deadline so every stalled id resolves.
	settle := int(cfg.BufferMicros/testTick) + 16*cfg.GenSize
	for k := 0; k < settle; k++ {
		now = now.Add(testTick)
		tick++
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	res.rstats = r.Stats()
	res.reactive = s.Stats().ReactiveRepair
	return res
}

// TestEarlyEvictionResyncAndBudget drives a two-GOP hierarchical-B stream where a burst
// breaks EVERY frame of the first GOP (its last chunk lost, all repair gone) while the
// second GOP arrives intact. With eviction OFF the cursor stalls on each broken GOP-1 frame
// until its own deadline, so the GOP-2 keyframe — the resync point — delivers late. With
// eviction ON the receiver abandons the whole dead GOP the moment its keyframe breaks, so
// the GOP-2 keyframe delivers far sooner; and because the cursor (the sender's stop-repair
// signal) advances past the dead generations, the sender spends less reactive repair on
// them. Either way the second GOP — the only DECODABLE pictures — is delivered whole, in
// order, and never past a symbol's deadline.
func TestEarlyEvictionResyncAndBudget(t *testing.T) {
	units := shape.GenerateGOP(2, 8) // GOP1: ids 0..8, GOP2: ids 9..17 (param+IDR+7 frames each)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 60_000, MaxBitrate: 8_000_000}

	// Map each unit to its source-id range, and find the GOP boundary (the 2nd parameter set).
	type span struct{ start, n uint32 }
	spans := make([]span, len(units))
	var nextID uint32
	gopBoundaryUnit := -1
	for i, u := range units {
		nch := uint32((u.Size + cfg.SymbolSize - 1) / cfg.SymbolSize)
		if nch < 1 {
			nch = 1
		}
		spans[i] = span{nextID, nch}
		if i > 0 && u.Class == shape.ClassParamSet && gopBoundaryUnit < 0 {
			gopBoundaryUnit = i // the second GOP starts here
		}
		nextID += nch
	}
	if gopBoundaryUnit < 1 {
		t.Fatal("expected a second GOP (a later parameter set)")
	}

	// Break every multi-chunk GOP-1 frame: drop its LAST chunk (descriptor still rides the
	// earlier chunks, so the receiver knows the frame yet finds it unrecoverable).
	dropID := map[uint32]bool{}
	for i := 0; i < gopBoundaryUnit; i++ {
		if spans[i].n >= 2 {
			dropID[spans[i].start+spans[i].n-1] = true
		}
	}
	if len(dropID) == 0 {
		t.Fatal("no GOP-1 frames to break")
	}

	off := runEvict(t, cfg, units, dropID, false)
	on := runEvict(t, cfg, units, dropID, true)

	// 1. Eviction actually fired, and only in the ON run.
	if on.rstats.Evicted == 0 {
		t.Fatal("eviction ON: no ids evicted — the dead sub-tree was not abandoned")
	}
	if off.rstats.Evicted != 0 {
		t.Fatalf("eviction OFF: %d ids evicted, want 0", off.rstats.Evicted)
	}

	// 2. Picture-completeness: GOP-2 (the only decodable pictures) is delivered whole in
	//    BOTH modes — early eviction never drops a decodable frame's chunk.
	for _, run := range []evictResult{off, on} {
		for i := gopBoundaryUnit; i < len(units); i++ {
			for c := uint32(0); c < spans[i].n; c++ {
				id := spans[i].start + c
				if _, ok := run.deliverAt[id]; !ok {
					t.Fatalf("GOP-2 id %d (unit %d) not delivered — decodable frame lost", id, units[i].ID)
				}
			}
		}
	}

	// 3. In-order, no duplicate, nothing delivered past its own deadline — both modes.
	for _, run := range []evictResult{off, on} {
		seen := map[uint32]bool{}
		var prev int64 = -1
		for _, id := range run.order {
			if seen[id] {
				t.Fatalf("duplicate delivery of id %d", id)
			}
			seen[id] = true
			if int64(id) <= prev {
				t.Fatalf("out-of-order delivery: %d after %d", id, prev)
			}
			prev = int64(id)
			if dl, ok := run.deadlineAt[id]; ok && run.deliverAt[id] > dl {
				t.Fatalf("id %d delivered at tick %d, past its deadline tick %d", id, run.deliverAt[id], dl)
			}
		}
	}

	// 4. Faster resync: the GOP-2 keyframe delivers materially sooner with eviction on.
	gop2KeyID := spans[gopBoundaryUnit+1].start // the IDR right after the 2nd parameter set
	tOn, okOn := on.deliverAt[gop2KeyID]
	tOff, okOff := off.deliverAt[gop2KeyID]
	if !okOn || !okOff {
		t.Fatalf("GOP-2 keyframe id %d not delivered (on=%v off=%v)", gop2KeyID, okOn, okOff)
	}
	t.Logf("GOP-2 keyframe delivered: eviction ON tick %d vs OFF tick %d (saved %d ticks); reactive repair ON %d vs OFF %d; evicted %d",
		tOn, tOff, tOff-tOn, on.reactive, off.reactive, on.rstats.Evicted)
	if tOn >= tOff {
		t.Fatalf("eviction did not speed resync: GOP-2 keyframe at tick %d (on) vs %d (off)", tOn, tOff)
	}

	// 5. Budget reclaimed: the cursor advancing past the dead GOP retires those generations
	//    at the sender, so it spends no more reactive repair on them than the stall case.
	if on.reactive > off.reactive {
		t.Fatalf("eviction spent MORE reactive repair (%d) than the stall case (%d)", on.reactive, off.reactive)
	}
}
