package flow

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

const (
	testSym  = 64
	testGen  = 16
	testRed  = 0.25    // 4 repair per generation of 16
	testBuf  = 150_000 // 150 ms — room for feedback-driven reactive repair
	testTick = 1_000   // 1 ms per source symbol
)

func testConfig() Config {
	return Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: testRed, BufferMicros: testBuf}
}

// chunk encodes id in its first 4 bytes so a delivered payload reveals its source
// id; the rest is seeded-random so correctness is meaningful.
func makeChunk(rng *rand.Rand, id uint32) []byte {
	b := make([]byte, testSym)
	binary.BigEndian.PutUint32(b, id)
	rng.Read(b[4:])
	return b
}
func chunkID(b []byte) uint32 { return binary.BigEndian.Uint32(b) }

// flowResult is the observed output of a sim run.
type flowResult struct {
	delivered []uint32 // source ids delivered, in delivery order
	sources   map[uint32][]byte
	stats     ReceiverStats
	sstats    SenderStats
	lateDeliv bool // a symbol was delivered after its deadline (invariant violation)
}

// runFlow streams n source chunks through Sender -> drop(sym) -> Receiver on a
// manual clock, looping feedback back to the sender so deficit-driven reactive
// repair is exercised. n must be a multiple of GenSize.
func runFlow(t *testing.T, cfg Config, n int, seed int64, drop func(wire.Symbol) bool) flowResult {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	res := flowResult{sources: map[uint32][]byte{}}
	genDL := map[uint32]clock.Timestamp{}
	now := clock.Timestamp(0)

	drain := func() {
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			id := chunkID(d)
			res.delivered = append(res.delivered, id)
			if dl, ok := genDL[id]; ok && now.After(dl) {
				res.lateDeliv = true
			}
			if !bytes.Equal(d, res.sources[id]) {
				t.Fatalf("delivered wrong bytes for id %d", id)
			}
		}
	}
	pumpNet := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || drop(sym) {
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
		drain()
	}

	for i := 0; i < n; i++ {
		id := uint32(i)
		// Per-symbol deadline: each chunk is due BufferMicros after its OWN write
		// (matching the sender's stamp), not the generation's first write.
		genDL[id] = now.Add(cfg.BufferMicros)
		chunk := makeChunk(rng, id)
		res.sources[id] = chunk
		s.Write(now, chunk)
		pumpNet()
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pumpNet()
	}
	s.Flush(now)
	pumpNet()
	// Settle: let feedback-driven reactive repair converge within the deadline window.
	settle := int(cfg.BufferMicros/testTick) + 4*cfg.GenSize
	for k := 0; k < settle; k++ {
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pumpNet()
	}
	// Advance past every remaining deadline so eviction releases stuck generations.
	now = now.Add(cfg.BufferMicros + int64(cfg.GenSize)*testTick*2)
	r.Tick(now)
	drain()
	res.stats = r.Stats()
	res.sstats = s.Stats()
	return res
}

// assertOrdered checks strictly increasing delivery (no duplicate, in order).
func assertOrdered(t *testing.T, ids []uint32) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("delivery not strictly increasing at %d: %d then %d", i, ids[i-1], ids[i])
		}
	}
}

func TestFlowNoLoss(t *testing.T) {
	const n = 160
	res := runFlow(t, testConfig(), n, 1, func(wire.Symbol) bool { return false })
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if len(res.delivered) != n {
		t.Fatalf("delivered %d/%d", len(res.delivered), n)
	}
	for i := 0; i < n; i++ {
		if res.delivered[i] != uint32(i) {
			t.Fatalf("position %d delivered id %d", i, res.delivered[i])
		}
	}
	// No loss ⇒ no deficit ⇒ no reactive repair.
	if res.sstats.ReactiveRepair != 0 {
		t.Fatalf("sent %d reactive repair under no loss", res.sstats.ReactiveRepair)
	}
}

// TestFlowRecoverableLoss drops exactly repairCount systematic per generation, at
// the front, and asserts full, in-order, in-time recovery from the fixed repair
// alone (no reactive repair needed).
func TestFlowRecoverableLoss(t *testing.T) {
	cfg := testConfig()
	const n = 160
	r := cfg.repairFloor(cfg.GenSize)
	drop := func(sym wire.Symbol) bool {
		if sym.Kind != wire.Systematic {
			return false
		}
		return sym.SrcIndex-sym.WindowBase < uint32(r) // first r systematic of each gen
	}
	res := runFlow(t, cfg, n, 2, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if len(res.delivered) != n {
		t.Fatalf("delivered %d/%d (lost=%d recovered=%d)", len(res.delivered), n, res.stats.Lost, res.stats.Recovered)
	}
}

// TestFlowReactiveRepair drops MORE than the fixed repair count of systematic
// symbols per generation (the variance regime fixed-ratio coding cannot cover) but
// leaves repair intact: the receiver's rank-deficit feedback must drive the sender
// to send extra repair until every generation decodes — full recovery beyond the
// proactive budget.
func TestFlowReactiveRepair(t *testing.T) {
	cfg := testConfig()
	const n = 320
	ndrop := cfg.repairFloor(cfg.GenSize) + 2
	for seed := int64(0); seed < 25; seed++ {
		rng := rand.New(rand.NewSource(seed + 200))
		dropSet := map[uint32]bool{}
		for g := 0; g < n/cfg.GenSize; g++ {
			perm := rng.Perm(cfg.GenSize)
			for k := 0; k < ndrop; k++ {
				dropSet[uint32(g*cfg.GenSize+perm[k])] = true
			}
		}
		drop := func(sym wire.Symbol) bool {
			return sym.Kind == wire.Systematic && dropSet[sym.SrcIndex]
		}
		res := runFlow(t, cfg, n, seed, drop)
		assertOrdered(t, res.delivered)
		if res.lateDeliv {
			t.Fatalf("seed %d: delivery past deadline", seed)
		}
		if len(res.delivered) != n {
			t.Fatalf("seed %d: reactive repair did not fully recover: %d/%d (lost=%d recovered=%d reactive=%d)",
				seed, len(res.delivered), n, res.stats.Lost, res.stats.Recovered, res.sstats.ReactiveRepair)
		}
		if res.sstats.ReactiveRepair == 0 {
			t.Fatalf("seed %d: full recovery but no reactive repair was sent", seed)
		}
	}
}

// TestFlowReactiveRepairEntirelyLostGeneration drops every first-flight symbol for the
// first generation, including its proactive repair. The receiver therefore has no decoder
// state at the delivery cursor even after it sees later generations. Feedback must still
// surface that structural gap so the sender can answer with coded repair.
func TestFlowReactiveRepairEntirelyLostGeneration(t *testing.T) {
	cfg := testConfig()
	const n = 64
	proactive := uint16(cfg.repairFloor(cfg.GenSize))
	drop := func(sym wire.Symbol) bool {
		if sym.WindowBase != 0 {
			return false
		}
		switch sym.Kind {
		case wire.Systematic:
			return sym.SrcIndex < uint32(cfg.GenSize)
		case wire.Repair:
			key, mds := code.BlockRepairIndex(sym.RepairKey)
			return mds && key < proactive
		default:
			return false
		}
	}
	res := runFlow(t, cfg, n, 77, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("delivery past deadline")
	}
	if len(res.delivered) != n {
		t.Fatalf("entirely-lost generation was not recovered: delivered %d/%d (lost=%d recovered=%d reactive=%d)",
			len(res.delivered), n, res.stats.Lost, res.stats.Recovered, res.sstats.ReactiveRepair)
	}
	if res.sstats.ReactiveRepair == 0 {
		t.Fatal("expected reactive repair for the structural generation gap")
	}
}

func TestReceiverFeedbackReportsInteriorStructuralGenerationGap(t *testing.T) {
	cfg := testConfig()
	cfg.Redundancy = 0
	rng := rand.New(rand.NewSource(123))
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)

	for i := 0; i < 3*cfg.GenSize; i++ {
		s.Write(now, makeChunk(rng, uint32(i)))
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				t.Fatalf("decode symbol: %v", err)
			}
			// Leave the cursor generation deficient, drop the whole next generation,
			// then admit a later generation so the missing middle one is structural
			// but not yet at the cursor.
			if sym.SrcIndex == uint32(cfg.GenSize-1) {
				continue
			}
			if sym.SrcIndex >= uint32(cfg.GenSize) && sym.SrcIndex < uint32(2*cfg.GenSize) {
				continue
			}
			r.FeedSymbol(now, d)
		}
		now = now.Add(testTick)
	}

	r.Tick(now.Add(feedbackIntervalMicros + 1))
	var fb wire.Feedback
	seen := false
	for {
		d, ok := r.PollSend()
		if !ok {
			break
		}
		got, err := wire.DecodeFeedback(d)
		if err != nil {
			t.Fatalf("decode feedback: %v", err)
		}
		fb, seen = got, true
	}
	if !seen {
		t.Fatal("receiver emitted no feedback")
	}
	if fb.Deficits[0] == 0 {
		t.Fatalf("cursor generation deficit = 0, want non-zero; feedback=%+v", fb)
	}
	if fb.Deficits[1] != structuralGapDeficit {
		t.Fatalf("interior structural deficit = %d, want %d; feedback=%+v",
			fb.Deficits[1], structuralGapDeficit, fb)
	}
}

func TestSenderStampsSendTimestamp(t *testing.T) {
	cfg := testConfig()
	cfg.GenSize = 2
	cfg.Redundancy = 1
	now := clock.Timestamp(123_456)
	s := NewSender(cfg)
	s.Write(now, make([]byte, testSym))
	d, ok := s.PollSend()
	if !ok {
		t.Fatal("no systematic emitted")
	}
	sym, err := wire.DecodeSymbol(d)
	if err != nil {
		t.Fatalf("DecodeSymbol systematic: %v", err)
	}
	if sym.SendTimestamp != int64(now) {
		t.Fatalf("systematic SendTimestamp = %d, want %d", sym.SendTimestamp, now)
	}
	s.Flush(now.Add(1))
	for {
		d, ok = s.PollSend()
		if !ok {
			t.Fatal("no repair emitted")
		}
		sym, err = wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol repair: %v", err)
		}
		if sym.Kind == wire.Repair {
			if sym.SendTimestamp != int64(now.Add(1)) {
				t.Fatalf("repair SendTimestamp = %d, want %d", sym.SendTimestamp, now.Add(1))
			}
			break
		}
	}

	sl := NewSlidingSender(Config{Flow: cfg.Flow, SymbolSize: cfg.SymbolSize, Sliding: true, CodingWindow: 8, BufferMicros: cfg.BufferMicros})
	sl.Write(now, make([]byte, testSym))
	d, ok = sl.PollSend()
	if !ok {
		t.Fatal("no sliding systematic emitted")
	}
	sym, err = wire.DecodeSymbol(d)
	if err != nil {
		t.Fatalf("DecodeSymbol sliding systematic: %v", err)
	}
	if sym.SendTimestamp != int64(now) {
		t.Fatalf("sliding systematic SendTimestamp = %d, want %d", sym.SendTimestamp, now)
	}
}

// TestFlowUnrecoverableDeadline drops systematic past the budget AND all repair, so
// the holes can never be recovered: they must be skipped at the deadline, with the
// four invariants intact and full loss accounting.
func TestFlowUnrecoverableDeadline(t *testing.T) {
	cfg := testConfig()
	const n = 160
	ndrop := cfg.repairFloor(cfg.GenSize) + 2
	rng := rand.New(rand.NewSource(99))
	dropSet := map[uint32]bool{}
	for g := 0; g < n/cfg.GenSize; g++ {
		perm := rng.Perm(cfg.GenSize)
		for k := 0; k < ndrop; k++ {
			dropSet[uint32(g*cfg.GenSize+perm[k])] = true
		}
	}
	drop := func(sym wire.Symbol) bool {
		if sym.Kind == wire.Repair {
			return true // drop ALL repair — reactive repair cannot help
		}
		return dropSet[sym.SrcIndex]
	}
	res := runFlow(t, cfg, n, 1, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if got := res.stats.Delivered + res.stats.Lost; got != uint64(n) {
		t.Fatalf("accounting %d (delivered=%d lost=%d) != %d", got, res.stats.Delivered, res.stats.Lost, n)
	}
	if res.stats.Lost == 0 {
		t.Fatal("expected unrecoverable loss but lost=0")
	}
	if uint64(len(res.delivered)) != res.stats.Delivered {
		t.Fatalf("delivered slice %d != stat %d", len(res.delivered), res.stats.Delivered)
	}
}

// TestEncoderSlide is covered in internal/code; here we only exercise flow.

// TestFlowCCInvariants: with the congestion controller enabled, a recoverable-loss
// flow over the (uncongested) sim still satisfies the four invariants — CC throttles
// only repair under a building queue, so on a path with headroom it changes nothing.
func TestFlowCCInvariants(t *testing.T) {
	cfg := testConfig()
	cfg.CongestionControl = true
	const n = 160
	r := cfg.repairFloor(cfg.GenSize)
	drop := func(sym wire.Symbol) bool {
		return sym.Kind == wire.Systematic && sym.SrcIndex-sym.WindowBase < uint32(r)
	}
	res := runFlow(t, cfg, n, 2, drop)
	assertOrdered(t, res.delivered)
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline with CC on")
	}
	if len(res.delivered) != n {
		t.Fatalf("delivered %d/%d with CC on (lost=%d recovered=%d)", len(res.delivered), n, res.stats.Lost, res.stats.Recovered)
	}
}
