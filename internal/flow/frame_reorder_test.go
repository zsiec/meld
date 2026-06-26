package flow

import (
	"sort"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// pend is one in-flight datagram awaiting its (jittered) arrival instant. seq breaks arrival-time
// ties deterministically (send order) so the harness is reproducible across runs.
type pend struct {
	at  clock.Timestamp
	seq int
	dat []byte
}

// runFrameReorder streams a GOP through Sender(WriteFrame)/Receiver with ZERO network loss but a
// jittered one-way delay, so symbols arrive REORDERED (a later-sent id with small jitter lands
// before an earlier-sent id with large jitter). Whole access units are written as a BURST (every
// chunk at one instant), the way a real shaper emits a frame — so the chunks of a frame share one
// deadline that the uniform per-id fit must not violate. Nothing is dropped on the wire and the
// budget is ample, so a correct transport MUST deliver every real source id; any shortfall is a
// reorder-intolerance bug. Returns the receiver stats and the count of real source ids written.
func runFrameReorder(t *testing.T, cfg Config, units []shape.Unit, owdUs, jitterUs int64) (ReceiverStats, uint32, map[uint32]bool) {
	t.Helper()
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	var inflight []pend
	deliveredID := map[uint32]bool{}

	feedDue := func() {
		// Feed every datagram whose arrival instant has passed, in arrival order (the reorder),
		// breaking ties by send order so the schedule is deterministic.
		sort.Slice(inflight, func(i, j int) bool {
			if inflight[i].at != inflight[j].at {
				return inflight[i].at < inflight[j].at
			}
			return inflight[i].seq < inflight[j].seq
		})
		k := 0
		for _, p := range inflight {
			if p.at <= now {
				r.FeedSymbol(now, p.dat)
			} else {
				inflight[k] = p
				k++
			}
		}
		inflight = inflight[:k]
	}
	seq := 0
	drainSender := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			// Schedule arrival = now + owd + jitter. Jitter is a DETERMINISTIC function of the
			// datagram bytes (an FNV-1a hash), not an emit-order RNG draw, so the arrival schedule
			// depends only on which packets exist — never on the order/count the sender happens to
			// emit them in — keeping the test reproducible as reactive-repair timing shifts.
			j := int64(0)
			if jitterUs > 0 {
				h := uint64(1469598103934665603)
				for _, b := range d {
					h = (h ^ uint64(b)) * 1099511628211
				}
				j = int64(h % uint64(jitterUs))
			}
			inflight = append(inflight, pend{at: now.Add(owdUs + j), seq: seq, dat: append([]byte(nil), d...)})
			seq++
		}
	}
	drainFB := func() {
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
	}
	drainDeliver := func() {
		for {
			_, dat, ok := r.PollDeliver()
			if !ok {
				break
			}
			deliveredID[chunkID(dat)] = true
		}
	}

	const frameIntervalUs = 33_000 // ~30fps: a whole access unit is written as ONE burst, then time advances
	var nextID uint32
	for _, u := range units {
		nch := (u.Size + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		fd := FrameDesc{Priority: u.Class.Wire(), FrameID: u.ID, RefFrameIDs: u.RefersTo, Chunks: uint16(nch), RAP: u.RAP, Discardable: u.Discardable}
		// Burst: every chunk of this access unit is written at the SAME instant (real shaper
		// behavior) — so they share one deadline, which the uniform-spacing fit must not violate.
		for c := 0; c < nch; c++ {
			s.WriteFrame(now, makeChunkN(nextID), fd)
			nextID++
			drainSender()
		}
		s.Flush(now)
		drainSender()
		// Advance time one frame interval in fine steps so arrivals are processed continuously.
		for t := int64(0); t < frameIntervalUs; t += testTick {
			now = now.Add(testTick)
			s.Tick(now)
			r.Tick(now)
			feedDue()
			drainFB()
			drainDeliver()
		}
	}
	// Drain: advance well past the last arrival + deadline so every in-flight symbol lands.
	settle := int((owdUs+jitterUs+cfg.BufferMicros)/testTick) + 16*cfg.GenSize
	for k := 0; k < settle; k++ {
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		feedDue()
		drainFB()
		drainDeliver()
	}
	return r.Stats(), nextID, deliveredID
}

// TestFrameReorderNoPrematureEvict pins the frame-eviction-under-reorder fix in a pure,
// deterministic unit: a hierarchical-B GOP, written as per-access-unit bursts, arrives under a
// jittered (reordering) delay with NO network loss. There is nothing to lose, so every real
// source id must be delivered. The bug it guards against: a frame's deadline was extrapolated
// from a reorder-skewed reference id (landing in the past → premature frame drop), and a
// dependent frame was finalized undecodable before its reference resolved (→ a whole-tail doom
// cascade). Either reintroduces frame eviction at zero loss and drops Delivered below the total.
func TestFrameReorderNoPrematureEvict(t *testing.T) {
	units := shape.GenerateGOP(3, 8) // 3 GOPs, hierarchical-B, multi-chunk frames
	cfg := Config{
		Flow: 1, SymbolSize: 256, GenSize: 16,
		Redundancy: 0.1, BufferMicros: 400_000, MaxBitrate: 8_000_000,
		FrameAtomic: true, EvictBrokenFrames: true, AutoReorderHoldoff: true,
	}
	// owd 50ms, jitter up to 40ms → reorder spread tens of ids; RTT ~100ms; budget 400ms (4×RTT,
	// the stable operating point) so a genuine deadline miss never confounds the eviction assertion.
	for _, owdJit := range [][2]int64{{50_000, 20_000}, {50_000, 40_000}} {
		st, total, delivered := runFrameReorder(t, cfg, units, owdJit[0], owdJit[1])
		if st.Delivered < uint64(total) {
			var missing []uint32
			for id := uint32(0); id < total; id++ {
				if !delivered[id] {
					missing = append(missing, id)
				}
			}
			t.Errorf("owd=%dus jitter=%dus: delivered %d/%d at ZERO network loss (evicted=%d lost=%d) — reorder-intolerance bug; missing %v",
				owdJit[0], owdJit[1], st.Delivered, total, st.Evicted, st.Lost, missing)
		}
		if st.Evicted != 0 {
			t.Errorf("owd=%dus jitter=%dus: %d frame-evictions at zero loss — a frame was dropped though it was only reordered-late", owdJit[0], owdJit[1], st.Evicted)
		}
	}
}
