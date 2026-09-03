package flow

import (
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// nSlotChannel is a correlated N-path erasure channel with a shared-outage structure: in
// each aligned slot, with probability qCommon ALL paths go bad together (the correlated
// component — a shared bottleneck / weather / congestion event), and independently each
// path is also bad with probability rIndep. On a bad (path, slot) every symbol there — the
// systematic round-robined onto that path and any repair scheduled to it — is erased. A
// path's per-slot bad state is shared across the slot's symbols so the correlation the
// receiver measures is faithful. Deterministic and order-independent.
type nSlotChannel struct {
	paths int
	bad   [][]bool // bad[slot][path]
}

func newNSlotChannel(seed int64, slots, paths int, qCommon, rIndep float64) *nSlotChannel {
	rng := rand.New(rand.NewSource(seed))
	bad := make([][]bool, slots)
	for s := 0; s < slots; s++ {
		bad[s] = make([]bool, paths)
		common := rng.Float64() < qCommon
		for p := 0; p < paths; p++ {
			bad[s][p] = common || rng.Float64() < rIndep
		}
	}
	return &nSlotChannel{paths: paths, bad: bad}
}

func (c *nSlotChannel) drop(sym wire.Symbol) bool {
	var slot, path int
	if sym.Kind == wire.Systematic {
		slot = int(sym.SrcIndex) / c.paths
		path = int(sym.SrcIndex) % c.paths
	} else {
		span := int(sym.N) / c.paths
		if span < 1 {
			span = 1
		}
		slot = int(sym.WindowBase)/c.paths + int(sym.RepairKey)%span
		path = int(sym.PathID)
	}
	if slot < 0 || slot >= len(c.bad) || path < 0 || path >= c.paths {
		return false
	}
	return c.bad[slot][path]
}

// nChannelDistPpm is the channel's exact per-slot erasure-count histogram (ppm): the
// independent Binomial(N, rIndep) component scaled by (1−qCommon), plus the qCommon mass at
// count N (the shared outage erases all paths). nChannelMarginalPpm is each path's marginal.
func nChannelDistPpm(paths int, qCommon, rIndep float64) []int {
	bin := binomialPpm(paths, rIndep)
	dist := make([]int, paths+1)
	for j := range bin {
		dist[j] = int(float64(bin[j]) * (1 - qCommon))
	}
	dist[paths] += int(qCommon * 1_000_000)
	return dist
}

func nChannelMarginalPpm(qCommon, rIndep float64) int {
	return int((qCommon + (1-qCommon)*rIndep) * 1_000_000)
}

func npConfig(buf int64, paths int) Config {
	return Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: 0, BufferMicros: buf, Paths: paths}
}

// TestNPathFourInvariants runs a correlated, recoverable 3-path channel through the full
// live loop (round-robin placement across 3 paths + joint-tail sizing + co-loss feedback +
// reactive repair) and asserts the four invariants: no duplicate delivered, in-order, none
// past deadline, and complete recovery (100% delivered).
func TestNPathFourInvariants(t *testing.T) {
	const (
		n       = 384 // multiple of GenSize (16) and of 3 paths (lcm 48)
		paths   = 3
		qCommon = 0.08
		rIndep  = 0.12
	)
	for seed := int64(0); seed < 8; seed++ {
		ch := newNSlotChannel(seed+1, n, paths, qCommon, rIndep)
		res := runFlow(t, npConfig(testBuf, paths), n, seed, ch.drop)
		assertOrdered(t, res.delivered)
		if res.lateDeliv {
			t.Fatalf("seed %d: a symbol was delivered past its deadline", seed)
		}
		if got := res.stats.Delivered + res.stats.Lost; got != uint64(n) {
			t.Fatalf("seed %d: accounting %d != %d", seed, got, n)
		}
		if len(res.delivered) != n {
			t.Fatalf("seed %d: incomplete recovery %d/%d (lost=%d recovered=%d reactive=%d)",
				seed, len(res.delivered), n, res.stats.Lost, res.stats.Recovered, res.sstats.ReactiveRepair)
		}
	}
}

// runNPProactive streams n chunks through an N-path Sender/Receiver over ch with NO feedback
// loop, so PROACTIVE sizing alone determines recovery; the sender's slot histogram is
// pre-seeded by seedDist. Returns delivered/lost and whether order/deadline held.
func runNPProactive(t *testing.T, cfg Config, n int, seedDist func(*Sender), ch *nSlotChannel) (delivered, lost int, ordered, inTime bool) {
	t.Helper()
	s := NewSender(cfg)
	seedDist(s)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	rng := rand.New(rand.NewSource(7))
	srcDL := map[uint32]clock.Timestamp{}
	var last int64 = -1
	ordered, inTime = true, true
	drain := func() {
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			id := chunkID(d)
			if int64(id) <= last {
				ordered = false
			}
			last = int64(id)
			if dl, ok := srcDL[id]; ok && now.After(dl) {
				inTime = false
			}
		}
	}
	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			if sym, err := wire.DecodeSymbol(d); err == nil && !ch.drop(sym) {
				r.FeedSymbol(now, d)
			}
		}
		for { // proactive-only: discard feedback
			if _, ok := r.PollSend(); !ok {
				break
			}
		}
		drain()
	}
	for i := 0; i < n; i++ {
		id := uint32(i)
		srcDL[id] = now.Add(cfg.BufferMicros)
		s.Write(now, makeChunk(rng, id))
		pump()
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	s.Flush(now)
	pump()
	now = now.Add(cfg.BufferMicros + int64(cfg.GenSize)*testTick*4)
	r.Tick(now)
	drain()
	st := r.Stats()
	return int(st.Delivered), int(st.Lost), ordered, inTime
}

// TestNPathJointTailImprovesCorrelatedLoss verifies that on a correlated three-path channel,
// PROACTIVE joint-tail sizing fed the TRUE per-slot erasure-count histogram loses fewer
// symbols than sizing that assumes the paths are independent (same marginals, Binomial(3,
// p)) — the correlation awareness provisions for the all-paths-out tail an independence
// sizer misses.
func TestNPathJointTailImprovesCorrelatedLoss(t *testing.T) {
	const (
		n       = 3600 // multiple of GenSize and of 3 paths
		paths   = 3
		qCommon = 0.18 // a brutal shared-outage rate so independence under-provisions
		rIndep  = 0.10
		seeds   = 6
	)
	cfg := npConfig(testBuf, paths)
	marg := nChannelMarginalPpm(qCommon, rIndep)
	margs := []int{marg, marg, marg}
	jointDist := nChannelDistPpm(paths, qCommon, rIndep) // the true correlated histogram
	indepDist := binomialPpm(paths, float64(marg)/1e6)   // same marginals, assume independence
	joint := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = margs, jointDist }
	indep := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = margs, indepDist }

	var lostJointTot, lostIndepTot int
	for seed := int64(0); seed < seeds; seed++ {
		ch := newNSlotChannel(seed+1, n, paths, qCommon, rIndep)
		dJ, lJ, ordJ, timeJ := runNPProactive(t, cfg, n, joint, ch)
		dI, lI, ordI, timeI := runNPProactive(t, cfg, n, indep, ch)
		if !ordJ || !timeJ || !ordI || !timeI {
			t.Fatalf("seed %d: invariant violation (orderJ=%v timeJ=%v orderI=%v timeI=%v)", seed, ordJ, timeJ, ordI, timeI)
		}
		if dJ+lJ != n || dI+lI != n {
			t.Fatalf("seed %d: accounting J=%d/%d I=%d/%d", seed, dJ+lJ, n, dI+lI, n)
		}
		lostJointTot += lJ
		lostIndepTot += lI
	}
	t.Logf("3-path proactive losses over %d seeds: joint-tail %d vs independence-assuming %d", seeds, lostJointTot, lostIndepTot)
	if lostJointTot >= lostIndepTot {
		t.Fatalf("joint-tail sizing did not beat independence at N=3: joint lost %d >= indep lost %d", lostJointTot, lostIndepTot)
	}
}
