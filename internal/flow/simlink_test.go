package flow

import (
	"encoding/binary"
	"math/rand"
	"sort"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// simChunk builds a source chunk of size bytes carrying id in its first four bytes, so a
// delivered payload's identity is self-describing (the receiver also reports the id).
func simChunk(size int, id uint32) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b, id)
	return b
}

// simLink streams n source chunks through Sender -> a lossy link with a real one-way
// propagation delay -> Receiver, and loops feedback back over the SAME delay, on a manual
// clock. Unlike runFlow (which hands every datagram to the peer at the same instant, i.e.
// ~0 RTT), simLink models propagation so the RTT-dependent behavior of the reactive-repair
// controller, the feedback cadence, and the absolute per-symbol deadline are exercised — the
// regimes the txbench sweeps live in. Deterministic given drop.
type simLink struct {
	cfg          Config
	owdMicros    int64                  // one-way propagation each direction; rtt = 2*owd
	srcMicros    int64                  // spacing between source writes (the offered cadence)
	srcMicros2   int64                  // cadence after rateChangeAt writes (0 ⇒ constant; for the AutoGenSize rate-change test)
	rateChangeAt int                    // write index at which the cadence switches to srcMicros2
	stepMicros   int64                  // clock granularity (0 ⇒ 1 ms)
	n            int                    // number of source chunks to send
	drop         func(wire.Symbol) bool // wire loss; closed over a per-symbol coin by the caller
	sliding      bool                   // use the band-form sliding coder instead of the generation coder
	jitterMicros int64                  // max extra per-datagram delay (deterministic per symbol) — induces reorder
	burst        int                    // source chunks written at one instant per srcMicros tick (0 ⇒ 1); models a media access unit (a whole video frame) written in one go, so a generation fills over a span of wall time rather than uniformly
	// paceBytesPerSec serializes the forward path at a wire rate (0 ⇒ unlimited, the original
	// behavior): each datagram occupies the wire for len/rate, so a repair burst emitted at a
	// generation close DELAYS the media queued behind it. This is the host-pacer / bottleneck physics
	// the original sim omitted — the omission that made proactive-shifting changes look free.
	paceBytesPerSec int64
	// timingJitterMicros + timingSeed add STOCHASTIC per-datagram forward delay (0 ⇒ none). The
	// original deterministic sim resolves "did the reactive round land before the deadline?" as a fixed
	// binary; real timing resolves it as a coin flip (scheduling/GC variance). Seeding lets one config
	// be sampled over many timing draws to recover the real-timing DISTRIBUTION instead of a single
	// point — the variance the deterministic sim cannot show.
	timingJitterMicros int64
	timingSeed         int64
}

// simResult is the observed outcome of a simLink run.
type simResult struct {
	n             int // source chunks offered (== simLink.n)
	delivered     int
	deliveredIDs  []uint32
	stats         ReceiverStats
	sstats        SenderStats
	peakGens      int // max len(receiver.gens) over the run — the receiver resource-bound witness
	peakRetained  int // max len(sender.retained) — the sender resource-bound witness
	lateDeliv     bool
	corrupt       bool    // a delivered payload did not match its source id (false recovery)
	latencyMicros []int64 // per-delivered-symbol latency (now - write time), for p50/p99
	finalPEst     float64 // the sender's loss estimate at end of run (feedback-driven; for diagnostics)
	finalBurstQ8  int     // the sender's burstiness estimate at end of run (Q8; 256 == i.i.d.)
}

// pctlMicros returns the p-th percentile (0..1) of the latency samples in microseconds, or 0 if empty.
func pctlMicros(xs []int64, p float64) int64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]int64(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	i := int(p * float64(len(s)-1))
	return s[i]
}

// overhead returns the realized repair overhead (repair that actually went out, net of
// throttle) as a fraction of the source symbols emitted.
func (res simResult) overhead() float64 {
	sent := res.sstats.Repair
	if res.sstats.Throttled < sent {
		sent -= res.sstats.Throttled
	} else {
		sent = 0
	}
	if res.sstats.Source == 0 {
		return 0
	}
	return float64(sent) / float64(res.sstats.Source)
}

type inflight struct {
	at   clock.Timestamp
	data []byte
}

// run executes the sim and returns the observed outcome.
func (sl simLink) run() simResult {
	var s coreSenderT
	var r coreReceiverT
	if sl.sliding {
		s, r = NewSlidingSender(sl.cfg), NewSlidingReceiver(sl.cfg)
	} else {
		s, r = NewSender(sl.cfg), NewReceiver(sl.cfg)
	}
	step := sl.stepMicros
	if step <= 0 {
		step = 1_000
	}
	res := simResult{n: sl.n}
	srcDL := map[uint32]clock.Timestamp{}
	var s2r, r2s []inflight
	now := clock.Timestamp(0)
	nextWrite := clock.Timestamp(0)
	written := 0
	endBy := clock.Timestamp(0)
	var wireFreeAt clock.Timestamp // forward-path serialization cursor (pacer)
	var tjit *rand.Rand
	if sl.timingSeed != 0 {
		tjit = rand.New(rand.NewSource(sl.timingSeed))
	}

	deliverDue := func(q *[]inflight, to func(d []byte)) {
		keep := (*q)[:0]
		for _, p := range *q {
			if p.at.After(now) {
				keep = append(keep, p)
			} else {
				to(p.data)
			}
		}
		*q = keep
	}
	pumpSender := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || sl.drop(sym) {
				continue
			}
			extra := int64(0)
			if sl.jitterMicros > 0 {
				// Deterministic per-symbol jitter ⇒ datagrams reorder across the link without an
				// rng (keeps the run reproducible). Reorder is a first-class invariant input.
				extra = int64(coinU(0x31773, uint32(sym.Kind), sym.SrcIndex, sym.WindowBase, uint32(sym.RepairKey)) * float64(sl.jitterMicros))
			}
			if tjit != nil && sl.timingJitterMicros > 0 {
				extra += tjit.Int63n(sl.timingJitterMicros + 1) // stochastic scheduling/GC variance
			}
			// Pacer: the datagram departs when the wire frees (max of now and the serialization cursor),
			// occupies it for len/rate, and arrives owd+jitter later — so a repair burst pushes the
			// media behind it later in time (head-of-line blocking the unpaced sim could not see).
			dep := now
			if sl.paceBytesPerSec > 0 {
				if wireFreeAt.After(dep) {
					dep = wireFreeAt
				}
				wireFreeAt = dep.Add(int64(len(d)) * 1_000_000 / sl.paceBytesPerSec)
			}
			s2r = append(s2r, inflight{dep.Add(sl.owdMicros + extra), d})
		}
	}
	pumpReceiver := func() {
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			r2s = append(r2s, inflight{now.Add(sl.owdMicros), fb})
		}
		for {
			id, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			res.deliveredIDs = append(res.deliveredIDs, id)
			res.delivered++
			if len(d) >= 4 && binary.BigEndian.Uint32(d) != id {
				res.corrupt = true // delivered the wrong bytes for this id — a false recovery
			}
			if dl, ok := srcDL[id]; ok {
				// delivery latency = now - write time; write time = dl - BufferMicros.
				res.latencyMicros = append(res.latencyMicros, int64(now.Sub(dl))+sl.cfg.BufferMicros)
				if now.After(dl) {
					res.lateDeliv = true // delivered past its deadline
				}
			}
		}
	}

	const maxSteps = 5_000_000
	for steps := 0; steps < maxSteps; steps++ {
		for written < sl.n && !nextWrite.After(now) {
			b := sl.burst
			if b < 1 {
				b = 1
			}
			for k := 0; k < b && written < sl.n; k++ {
				id := uint32(written)
				srcDL[id] = now.Add(sl.cfg.BufferMicros)
				s.Write(now, simChunk(sl.cfg.SymbolSize, id))
				written++
			}
			cadence := sl.srcMicros
			if sl.srcMicros2 > 0 && written >= sl.rateChangeAt {
				cadence = sl.srcMicros2 // mid-stream bitrate change
			}
			nextWrite = nextWrite.Add(cadence)
		}
		deliverDue(&s2r, func(d []byte) { r.FeedSymbol(now, d) })
		deliverDue(&r2s, func(d []byte) {
			if f, err := wire.DecodeFeedback(d); err == nil {
				s.FeedFeedback(now, f)
			}
		})
		s.Tick(now)
		r.Tick(now)
		pumpSender()
		pumpReceiver()
		if gr, ok := r.(*Receiver); ok {
			if l := len(gr.gens); l > res.peakGens {
				res.peakGens = l
			}
		}
		if gs, ok := s.(*Sender); ok {
			if l := len(gs.retained); l > res.peakRetained {
				res.peakRetained = l
			}
		}
		if written >= sl.n {
			if endBy == 0 {
				s.Flush(now)
				endBy = now.Add(sl.cfg.BufferMicros + 6*sl.owdMicros + int64(sl.cfg.GenSize)*sl.srcMicros)
			} else if now.After(endBy) && len(s2r) == 0 && len(r2s) == 0 {
				break
			}
		}
		now = now.Add(step)
	}
	res.stats = r.Stats()
	res.sstats = s.Stats()
	if gs, ok := s.(*Sender); ok {
		res.finalPEst, res.finalBurstQ8 = gs.pEst, gs.burstQ8
	}
	return res
}
