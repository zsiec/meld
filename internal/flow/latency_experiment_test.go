package flow

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// This file is a deterministic measurement harness (not an assertion test): it
// drives the generation and band-form sliding coders across a delay line with loss
// and reports the per-symbol RECOVERY-LATENCY distribution (delivery time minus
// source time) and delivery rate. Run it to characterize the decode-delay vs
// coding-window vs propagation trade-off:
//
//	go test -run TestLatencyProfile -v ./internal/flow
//
// One source symbol is written per tick (1 ms), so source-time(id) == id ms and a
// delivered id's latency is (deliveryTick - id) ms.

type delayEv struct {
	at   int64
	data []byte
}

// measureResult is the outcome of one delay-line run.
type measureResult struct {
	delivered int
	n         int
	recovered uint64
	repair    uint64
	reactive  uint64
	source    uint64
	lost      uint64
	p50, p99  int64 // recovery latency in ms (ticks)
	maxLat    int64
}

// measure streams n symbols through s -> [loss + delay] -> r with feedback delayed
// symmetrically, on a manual clock, and records each delivered id's latency.
func measure(s coreSenderT, r coreReceiverT, cfg Config, n int, seed int64, lossPct float64, delayTicks int64) measureResult {
	rng := rand.New(rand.NewSource(seed))
	lrng := rand.New(rand.NewSource(seed ^ 0x5eed))
	const tick = int64(1_000) // 1 ms
	var symQ, fbQ []delayEv
	deliverTick := make(map[uint32]int64)
	var now int64

	pushSym := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			if lrng.Float64() < lossPct {
				continue // dropped on the forward path
			}
			cp := append([]byte(nil), d...)
			symQ = append(symQ, delayEv{at: now + delayTicks*tick, data: cp})
		}
	}
	pushFB := func() {
		for {
			d, ok := r.PollSend()
			if !ok {
				break
			}
			cp := append([]byte(nil), d...)
			fbQ = append(fbQ, delayEv{at: now + delayTicks*tick, data: cp})
		}
	}
	drainDeliv := func() {
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			id := chunkID(d)
			if _, seen := deliverTick[id]; !seen {
				deliverTick[id] = now
			}
		}
	}
	deliverDue := func() {
		keep := symQ[:0]
		for _, e := range symQ {
			if e.at <= now {
				r.FeedSymbol(clock.Timestamp(now), e.data)
			} else {
				keep = append(keep, e)
			}
		}
		symQ = keep
		keepF := fbQ[:0]
		for _, e := range fbQ {
			if e.at <= now {
				if f, err := wire.DecodeFeedback(e.data); err == nil {
					s.FeedFeedback(clock.Timestamp(now), f)
				}
			} else {
				keepF = append(keepF, e)
			}
		}
		fbQ = keepF
	}

	// Model the real host: ~20 Mbps (ratePerMs source symbols written per ms, as a
	// burst at the same instant — the bench's leaky-bucket pacer batches at Sleep
	// granularity) and a coarse receiver host tick (rxTickMs); the receiver still
	// pumps on every arrival (deliverDue → FeedSymbol), as the real recvLoop does.
	const ratePerMs = 2
	const rxTickMs = 5
	written := 0
	for ms := int64(0); written < n; ms++ {
		now = ms * tick
		deliverDue()
		for b := 0; b < ratePerMs && written < n; b++ {
			s.Write(clock.Timestamp(now), makeChunk(rng, uint32(written)))
			written++
		}
		pushSym()
		s.Tick(clock.Timestamp(now))
		if ms%rxTickMs == 0 {
			r.Tick(clock.Timestamp(now))
		}
		pushSym()
		pushFB()
		deliverDue()
		drainDeliv()
	}
	s.Flush(clock.Timestamp(now))
	pushSym()
	// Settle: drain the delay lines and run past every deadline.
	settle := cfg.BufferMicros/tick + 4*delayTicks + 8*int64(cfg.codingWindow()) + 8*int64(cfg.GenSize)
	for k := int64(0); k < settle; k++ {
		now += tick
		deliverDue()
		s.Tick(clock.Timestamp(now))
		if k%rxTickMs == 0 {
			r.Tick(clock.Timestamp(now))
		}
		pushSym()
		pushFB()
		deliverDue()
		drainDeliv()
	}

	var lats []int64
	for id, dt := range deliverTick {
		lats = append(lats, dt-(int64(id)/ratePerMs)*tick) // source-time(id) = (id/rate) ms
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	ss := s.Stats()
	rs := r.Stats()
	res := measureResult{
		delivered: len(deliverTick), n: n,
		recovered: rs.Recovered, repair: ss.Repair, reactive: ss.ReactiveRepair,
		source: ss.Source, lost: rs.Lost,
	}
	if len(lats) > 0 {
		res.p50 = lats[len(lats)*50/100] / tick
		res.p99 = lats[min(len(lats)-1, len(lats)*99/100)] / tick
		res.maxLat = lats[len(lats)-1] / tick
	}
	return res
}

// coreSenderT / coreReceiverT are the in-package method sets measure drives (the
// session.coreSender/coreReceiver shape, declared here to avoid an import cycle).
type coreSenderT interface {
	Write(now clock.Timestamp, data []byte)
	FeedFeedback(now clock.Timestamp, fb wire.Feedback)
	Tick(now clock.Timestamp)
	Flush(now clock.Timestamp)
	PollSend() ([]byte, bool)
	Stats() SenderStats
}
type coreReceiverT interface {
	FeedSymbol(now clock.Timestamp, datagram []byte)
	Tick(now clock.Timestamp)
	PollDeliver() (uint32, []byte, bool)
	PollSend() ([]byte, bool)
	Stats() ReceiverStats
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestGenBudgetBelowRTT prints the full counter breakdown for both coders at the
// tight-budget-below-RTT regime that collapses the generation coder, so the cause
// (fundamental deadline starvation vs wasteful repair) is visible, not inferred.
func TestGenBudgetBelowRTT(t *testing.T) {
	if os.Getenv("MELD_LATENCY") == "" {
		t.Skip("characterization harness; set MELD_LATENCY=1 to run")
	}
	const n, seed, symSize = 4000, 7, 1316
	type regime struct {
		name           string
		delayMs, bufMs int64
		loss           float64
	}
	regimes := []regime{
		{"tight budget<RTT (50ms/60ms/10%)", 50, 60, 0.10},
		{"WAN generous   (150ms/200ms/30%)", 150, 200, 0.30},
	}
	base := Config{Flow: 1, SymbolSize: symSize, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3}
	for _, rg := range regimes {
		t.Logf("== %s ==", rg.name)
		gc := base
		gc.BufferMicros = rg.bufMs * 1000
		gr := NewReceiver(gc)
		res := measure(NewSender(gc), gr, gc, n, seed, rg.loss, rg.delayMs)
		t.Logf("gen g32  deliv=%5.1f%% recov=%4d lost=%4d  source=%d repair=%d (reactive=%d) ovhd=%4.0f%%  lossEst=%.3f (true %.2f)",
			100*float64(res.delivered)/float64(res.n), res.recovered, res.lost,
			res.source, res.repair, res.reactive, 100*float64(res.repair)/float64(res.source), gr.LossEstimate(), rg.loss)
		sc := base
		sc.BufferMicros = rg.bufMs * 1000
		sc.Sliding = true
		sc.CodingWindow = 64
		res = measure(NewSlidingSender(sc), NewSlidingReceiver(sc), sc, n, seed, rg.loss, rg.delayMs)
		t.Logf("sld      deliv=%5.1f%% recov=%4d lost=%4d  source=%d repair=%d (reactive=%d) ovhd=%4.0f%%",
			100*float64(res.delivered)/float64(res.n), res.recovered, res.lost,
			res.source, res.repair, res.reactive, 100*float64(res.repair)/float64(res.source))
	}
}

func TestLatencyProfile(t *testing.T) {
	if os.Getenv("MELD_LATENCY") == "" {
		t.Skip("characterization harness; set MELD_LATENCY=1 to run")
	}
	const (
		n       = 2000
		seed    = 7
		symSize = 1316
	)
	type regime struct {
		name     string
		delayMs  int64
		bufferMs int64
		loss     float64
	}
	regimes := []regime{
		{"LAN  buf200", 10, 200, 0.20},
		{"LAN  buf50 ", 10, 50, 0.20},
		{"WAN  buf200", 150, 200, 0.20},
		{"WAN  buf400", 150, 400, 0.20},
	}
	bands := []int{16, 32, 48, 96}

	base := func(buf int64) Config {
		return Config{Flow: 1, SymbolSize: symSize, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: buf * 1000}
	}

	t.Logf("%-12s %-10s  %6s %6s %6s %6s %6s", "regime", "coder", "deliv%", "p50", "p99", "max", "ovhd%")
	for _, rg := range regimes {
		// Generation baseline.
		gc := base(rg.bufferMs)
		res := measure(NewSender(gc), NewReceiver(gc), gc, n, seed, rg.loss, rg.delayMs)
		t.Logf("%-12s %-10s  %5.1f%% %5dms %5dms %5dms %5.0f%%", rg.name, "gen g32",
			100*float64(res.delivered)/float64(res.n), res.p50, res.p99, res.maxLat,
			100*float64(res.repair)/float64(res.source))
		// Sliding across bands.
		for _, b := range bands {
			sc := base(rg.bufferMs)
			sc.Sliding = true
			sc.CodingWindow = b
			res := measure(NewSlidingSender(sc), NewSlidingReceiver(sc), sc, n, seed, rg.loss, rg.delayMs)
			t.Logf("%-12s %-10s  %5.1f%% %5dms %5dms %5dms %5.0f%%", rg.name, fmt.Sprintf("sld b%d", b),
				100*float64(res.delivered)/float64(res.n), res.p50, res.p99, res.maxLat,
				100*float64(res.repair)/float64(res.source))
		}
	}
}
