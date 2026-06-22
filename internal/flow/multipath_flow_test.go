package flow

import (
	"math"
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// slotChannel is a correlated 2-path erasure channel. Each aligned slot i carries a
// per-path bad state (badA[i], badB[i]) drawn jointly from (pa, pb, pBoth): on a bad
// (path, slot) EVERY symbol there is erased — the systematic the sender round-robins
// onto that path AND any repair scheduled onto it. The systematic pair (2i on path A,
// 2i+1 on path B) lets the receiver's co-loss estimator recover the cross-path
// correlation; making repair share the same per-slot state (rather than an independent
// marginal) keeps the correlation faithful, the way the isolation sizer oracle draws
// every symbol from the joint slot. Deterministic and order-independent, so a sender
// sizing a generation more or less heavily still meets the same channel.
type slotChannel struct {
	badA, badB []bool
}

func newSlotChannel(seed int64, slots int, pa, pb, pBoth float64) *slotChannel {
	pNone := 1 - pa - pb + pBoth
	pTwo := pBoth
	rng := rand.New(rand.NewSource(seed))
	ba := make([]bool, slots)
	bb := make([]bool, slots)
	for i := 0; i < slots; i++ {
		u := rng.Float64()
		switch {
		case u < pNone: // neither path bad this slot
		case u < pNone+pTwo: // both paths bad
			ba[i], bb[i] = true, true
		default: // exactly one path bad (even split — valid when pa == pb)
			if rng.Float64() < 0.5 {
				ba[i] = true
			} else {
				bb[i] = true
			}
		}
	}
	return &slotChannel{badA: ba, badB: bb}
}

// locate maps a symbol to its (slot, path-A?) cell: systematic id k → slot k/2, path
// by id parity; repair → spread across its generation's slots by key, path by PathID.
func (c *slotChannel) locate(sym wire.Symbol) (slot int, pathA bool) {
	if sym.Kind == wire.Systematic {
		return int(sym.SrcIndex / 2), sym.SrcIndex%2 == 0
	}
	span := int(sym.N) / 2
	if span < 1 {
		span = 1
	}
	return int(sym.WindowBase)/2 + int(sym.RepairKey)%span, sym.PathID == 0
}

func (c *slotChannel) drop(sym wire.Symbol) bool {
	slot, pathA := c.locate(sym)
	if slot < 0 || slot >= len(c.badA) {
		return false
	}
	if pathA {
		return c.badA[slot]
	}
	return c.badB[slot]
}

func mpConfig(buf int64) Config {
	return Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: 0, BufferMicros: buf, Paths: 2}
}

// TestMultipathFourInvariants runs a correlated, recoverable 2-path channel through
// the full live loop (placement + joint-tail sizing + co-loss feedback + reactive
// repair) and asserts the four invariants: no duplicate delivered, in-order output,
// nothing past deadline, and completeness under recoverable loss (100% delivered).
func TestMultipathFourInvariants(t *testing.T) {
	const (
		n   = 320
		pa  = 0.25
		pb  = 0.25
		rho = 0.5
	)
	pBoth := pa*pb + rho*math.Sqrt(pa*(1-pa)*pb*(1-pb))
	for seed := int64(0); seed < 8; seed++ {
		ch := newSlotChannel(seed+1, n, pa, pb, pBoth)
		res := runFlow(t, mpConfig(testBuf), n, seed, ch.drop)
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

// TestMultipathCoLossClosesLoop: over the live wire the receiver measures the
// cross-path correlation, reports it, and the sender's joint-tail inputs reflect it —
// pBoth materially exceeds the independence product pa·pb. This is the end-to-end
// proof that the co-loss signal survives the feedback codec into the sizer.
func TestMultipathCoLossClosesLoop(t *testing.T) {
	const (
		n   = 400
		pa  = 0.4
		pb  = 0.4
		rho = 0.7
	)
	pBoth := pa*pb + rho*math.Sqrt(pa*(1-pa)*pb*(1-pb))
	ch := newSlotChannel(99, n, pa, pb, pBoth)

	s := NewSender(mpConfig(testBuf))
	r := NewReceiver(mpConfig(testBuf))
	now := clock.Timestamp(0)
	rng := rand.New(rand.NewSource(5))
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
			if _, _, ok := r.PollDeliver(); !ok {
				break
			}
		}
	}
	for i := 0; i < n; i++ {
		s.Write(now, makeChunk(rng, uint32(i)))
		pump()
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	s.Flush(now)
	pump()

	marg, dist := r.coEst.marginals(), r.coEst.slotDist()
	epa, epb, epBoth := marg[0], marg[1], dist[2] // 2-path: dist[2] is "both lost"
	if epa == 0 || epb == 0 {
		t.Fatalf("receiver measured no per-path loss: pa=%d pb=%d", epa, epb)
	}
	if epBoth <= epa*epb/1_000_000 {
		t.Fatalf("receiver co-loss %d did not exceed the independence product %d", epBoth, epa*epb/1_000_000)
	}
	// The signal reached the sender's sizer inputs (parts-per-65535 quantized).
	if len(s.slotDistPpm) < 3 || len(s.pathLossPpm) < 2 {
		t.Fatalf("sender got no per-path histogram: slotDist=%v pathLoss=%v", s.slotDistPpm, s.pathLossPpm)
	}
	sBoth, prod := s.slotDistPpm[2], s.pathLossPpm[0]*s.pathLossPpm[1]/1_000_000
	if sBoth <= prod {
		t.Fatalf("sender both-lost=%d did not exceed independence product %d (pa=%d pb=%d)",
			sBoth, prod, s.pathLossPpm[0], s.pathLossPpm[1])
	}
	t.Logf("measured pa=%d pb=%d pBoth=%d (indep product %d); sender both-lost=%d",
		epa, epb, epBoth, epa*epb/1_000_000, sBoth)
}

// runMPProactive streams n chunks through a 2-path Sender/Receiver over ch with NO
// feedback loop, so PROACTIVE sizing alone determines recovery. The sender's per-path
// rate inputs are pre-seeded by seedRates (joint vs independence). Returns delivered/
// lost counts and whether order/deadline invariants held.
func runMPProactive(t *testing.T, cfg Config, n int, seedRates func(*Sender), ch *slotChannel) (delivered, lost int, ordered, inTime bool) {
	t.Helper()
	s := NewSender(cfg)
	seedRates(s)
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
		for { // proactive-only: discard feedback, never feed it back
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

// TestMultipathProactiveMoneyTest is the integrated money test: on a correlated
// 2-path channel, PROACTIVE joint-tail sizing (fed the true co-loss) loses fewer
// symbols than sizing that assumes the paths are independent (same marginals,
// pBoth = pa·pb) — every seed — shown through the real Sender emit / wire / Receiver
// decode rather than the in-isolation sizer oracle. The channel is deliberately
// brutal (40% marginal, rho=0.85, a bad slot erases everything on that path at that
// time), so neither sizer recovers ALL of it within the maxRepairFactor cap; the
// claim is the RELATIVE one — correlation awareness provisions where independence
// under-provisions, so it loses consistently less. The absolute decode-failure
// guarantee (size to delta) is the isolation oracle's job (TestJointTailHoldsWhereIIDFails).
func TestMultipathProactiveMoneyTest(t *testing.T) {
	const (
		n     = 4000 // multiple of GenSize so the final generation is full (no phantom tail ids)
		pa    = 0.4
		pb    = 0.4
		rho   = 0.85
		seeds = 6
	)
	pBoth := pa*pb + rho*math.Sqrt(pa*(1-pa)*pb*(1-pb))
	cfg := mpConfig(testBuf)

	// Seed the sender's per-slot histogram directly (bypassing feedback): the 2-path slot
	// distribution [pNone, pOne, pTwo] from the marginals + joint rate.
	slot2 := func(pBoth float64) []int {
		a, b, both := ppm(pa), ppm(pb), ppm(pBoth)
		return []int{1_000_000 - a - b + both, a + b - 2*both, both}
	}
	joint := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = []int{ppm(pa), ppm(pb)}, slot2(pBoth) }
	indep := func(s *Sender) { s.pathLossPpm, s.slotDistPpm = []int{ppm(pa), ppm(pb)}, slot2(pa*pb) }

	var lostJointTot, lostIndepTot int
	for seed := int64(0); seed < seeds; seed++ {
		ch := newSlotChannel(seed+1, n, pa, pb, pBoth)
		dJ, lJ, ordJ, timeJ := runMPProactive(t, cfg, n, joint, ch)
		dI, lI, ordI, timeI := runMPProactive(t, cfg, n, indep, ch)
		if !ordJ || !timeJ || !ordI || !timeI {
			t.Fatalf("seed %d: invariant violation (orderJ=%v timeJ=%v orderI=%v timeI=%v)", seed, ordJ, timeJ, ordI, timeI)
		}
		if dJ+lJ != n || dI+lI != n {
			t.Fatalf("seed %d: accounting J=%d/%d I=%d/%d", seed, dJ+lJ, n, dI+lI, n)
		}
		t.Logf("seed %d: joint lost %d (%.2f%%) | independence lost %d (%.2f%%)",
			seed, lJ, 100*float64(lJ)/n, lI, 100*float64(lI)/n)
		// Per seed: correlation-aware sizing is never worse than assuming independence.
		if lJ > lI {
			t.Fatalf("seed %d: joint-tail sizing lost MORE (%d) than independence (%d)", seed, lJ, lI)
		}
		lostJointTot += lJ
		lostIndepTot += lI
	}
	// Aggregate: independence under-provisions and drops materially more (≥15%), so the
	// correlation awareness earns its keep rather than being noise.
	if lostIndepTot*100 < lostJointTot*115 {
		t.Fatalf("independence (%d lost) did not under-provision materially vs joint (%d lost)", lostIndepTot, lostJointTot)
	}
}
