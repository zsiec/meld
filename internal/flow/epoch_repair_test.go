package flow

import (
	"bytes"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

func TestEpochContinuouslyAllocatesAcrossReactiveBoundary(t *testing.T) {
	newSender := func(outage uint16, bufferMicros int64) *SlidingSender {
		cfg := DefaultConfig()
		cfg.Flow = 9
		cfg.SymbolSize = 32
		cfg.BufferMicros = bufferMicros
		cfg.MaxBitrate = 0
		s := NewSlidingSender(cfg)
		now := clock.Timestamp(0)
		for id := uint32(0); id < 8; id++ {
			s.Write(now, bytes.Repeat([]byte{byte(id + 1)}, cfg.SymbolSize))
			now = now.Add(500)
		}
		for {
			if _, ok := s.PollSend(); !ok {
				break
			}
		}
		s.FeedFeedback(now, wire.Feedback{Flow: cfg.Flow, OutageRun: outage})
		for id := uint32(8); id < 24; id++ {
			s.Write(now, bytes.Repeat([]byte{byte(id + 1)}, cfg.SymbolSize))
			now = now.Add(500)
		}
		return s
	}

	active := newSender(epochOutageMinSymbols, 60_000)
	var epochRows int
	for {
		d, ok := active.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("decode active symbol: %v", err)
		}
		if sym.Kind == wire.Systematic {
			if sym.WindowBase != 8 || sym.N != epochBlockSymbols {
				t.Fatalf("announced block = [%d,+%d), want [8,+16)", sym.WindowBase, sym.N)
			}
		}
		if _, ok := code.BlockRepairIndex(sym.RepairKey); sym.Kind == wire.Repair && ok {
			epochRows++
			if sym.WindowBase != 8 || sym.N != epochBlockSymbols {
				t.Fatalf("epoch row = [%d,+%d), want [8,+16)", sym.WindowBase, sym.N)
			}
		}
	}
	if epochRows == 0 || active.Stats().RepairEpoch != uint64(epochRows) {
		t.Fatalf("epoch rows = wire %d stats %d, want nonzero/equal", epochRows, active.Stats().RepairEpoch)
	}
	if active.Stats().EpochDemandQ8 == 0 || active.Stats().EpochShareQ8 == 0 {
		t.Fatalf("tight allocator did not expose live demand/mix: %+v", active.Stats())
	}

	reactive := newSender(epochOutageMinSymbols, 200_000)
	if reactive.Stats().RepairEpoch == 0 {
		t.Fatal("reactive-reachable flow disabled epoch repair instead of reducing its share")
	}
	if reactive.Stats().EpochShareQ8 >= active.Stats().EpochShareQ8 {
		t.Fatalf("reactive mix = %d, want below tight mix %d",
			reactive.Stats().EpochShareQ8, active.Stats().EpochShareQ8)
	}

	clean := newSender(0, 60_000)
	if got := clean.Stats().RepairEpoch; got == 0 {
		t.Fatal("cold start did not receive a bounded exploration epoch")
	}
}

func TestSlidingReceiverIsolatesAndRecoversEpochBlock(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Flow = 7
	cfg.SymbolSize = 32
	cfg.BufferMicros = 200_000
	r := NewSlidingReceiver(cfg)
	enc := code.NewEncoder(codedSymbolSize(cfg.SymbolSize))
	deadline := clock.Timestamp(cfg.BufferMicros)
	sources := make([][]byte, epochBlockSymbols)
	for id := range sources {
		sources[id] = bytes.Repeat([]byte{byte(id + 1)}, cfg.SymbolSize)
		addCodedSource(enc, sources[id], cfg.SymbolSize, deadline)
	}
	for id, source := range sources {
		if id == 3 || id == 7 {
			continue
		}
		r.FeedSymbol(0, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.Systematic, WindowBase: 0, SrcIndex: uint32(id), N: epochBlockSymbols,
			Deadline: int64(deadline), HasSourceLength: true, SourceLength: uint32(len(source)), Payload: source,
		}))
	}
	for row := uint16(0); row < 2; row++ {
		key := code.BlockRepairKey(row)
		base, n, payload := enc.Repair(key)
		r.FeedSymbol(1_000, wire.EncodeSymbol(nil, wire.Symbol{
			Flow: cfg.Flow, Kind: wire.Repair, WindowBase: base, N: uint16(n), RepairKey: key,
			Deadline: int64(deadline), Payload: payload,
		}))
	}
	for wantID, want := range sources {
		id, got, ok := r.PollDeliver()
		if !ok {
			t.Fatalf("delivery stopped at %d/%d", wantID, len(sources))
		}
		if id != uint32(wantID) || !bytes.Equal(got, want) {
			t.Fatalf("delivery %d = id %d payload %x, want id %d payload %x", wantID, id, got, wantID, want)
		}
	}
	if _, _, ok := r.PollDeliver(); ok {
		t.Fatal("unexpected extra delivery")
	}
	if got := r.Stats().Recovered; got != 2 {
		t.Fatalf("recovered = %d, want 2", got)
	}
	if len(r.epochs) != 0 {
		t.Fatalf("retired block decoders = %d, want 0", len(r.epochs))
	}
}

func TestEpochRejectsRowCostingMoreThanThreeRecentSources(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Flow = 1
	cfg.SymbolSize = 1_284
	cfg.BufferMicros = 60_000
	cfg.MaxBitrate = 10_800_000
	s := NewSlidingSender(cfg)
	s.interMicros = 500
	s.rttMicros = 60_000
	s.epochPolicy.demandQ8 = epochDemandOne

	// Ragged media: compact source datagrams are much cheaper than one algebraic
	// row, so the automatic path must retain temporal compact-copy opportunities.
	for id := 0; id < sourceWireWindowSize; id++ {
		s.Write(clock.Timestamp(id*500), []byte{byte(id)})
	}
	if s.epoch != nil || s.Stats().RepairEpoch != 0 {
		t.Fatalf("ragged source opened repair epoch: block=%+v stats=%+v", s.epoch, s.Stats())
	}
	if s.epochCostCompetitive() {
		t.Fatalf("row charge %d unexpectedly competitive with source mean %d",
			repairWireBaseBytes+codedSymbolSize(cfg.SymbolSize), s.sourceWireBytes)
	}

	// Full-width sources cross the same measured boundary without any config or
	// media label, allowing the next stable block to open.
	for id := 0; id < sourceWireWindowSize; id++ {
		s.Write(clock.Timestamp((id+sourceWireWindowSize)*500), make([]byte, cfg.SymbolSize))
	}
	if !s.epochCostCompetitive() {
		t.Fatalf("full-width row not competitive: row=%d source mean=%d",
			repairWireBaseBytes+codedSymbolSize(cfg.SymbolSize), s.sourceWireBytes)
	}
}

func TestEpochColdProbeRequiresCheaperRowThanSustainedMemory(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SymbolSize = 1_284
	s := NewSlidingSender(cfg)
	rowCharge := int64(repairWireBaseBytes + codedSymbolSize(cfg.SymbolSize))

	// Place the recent source mean strictly between the two- and three-source
	// crossovers. Cold exploration must decline it; measured channel memory may
	// spend the established sustained allowance without any application setting.
	s.sourceWireBytes = rowCharge/3 + 1
	if s.epochCostCompetitive() {
		t.Fatalf("cold probe accepted row=%d at source mean=%d", rowCharge, s.sourceWireBytes)
	}
	s.epochPolicy.demandQ8 = epochMemoryDemandQ8
	s.epochPolicy.memoryQ8 = epochDemandOne
	if !s.epochCostCompetitive() {
		t.Fatalf("memory-evidenced allocator rejected row=%d at source mean=%d", rowCharge, s.sourceWireBytes)
	}
}

func TestEpochDemandAttacksDecaysAndFreezesOnIdle(t *testing.T) {
	s := NewSlidingSender(DefaultConfig())
	s.updateEpochDemand(wire.Feedback{OutageRun: epochOutageMinSymbols}, false, true)
	if got := s.epochPolicy.demandQ8; got != epochDemandOne {
		t.Fatalf("first outage demand = %d, want %d", got, epochDemandOne)
	}
	s.updateEpochDemand(wire.Feedback{LossRate: 1, Burstiness: epochBurstThresholdQ8}, false, true)
	if got := s.epochPolicy.demandQ8; got < epochMemoryDemandQ8 || got >= epochDemandOne {
		t.Fatalf("correlated-loss demand = %d, want retained decisive memory", got)
	}
	s.updateEpochDemand(wire.Feedback{}, true, true)
	decayed := s.epochPolicy.demandQ8
	if decayed <= epochMemoryDemandQ8 || decayed >= epochDemandOne {
		t.Fatalf("clean-progress demand = %d, want bounded decay", decayed)
	}
	s.updateEpochDemand(wire.Feedback{}, true, false)
	if got := s.epochPolicy.demandQ8; got != decayed {
		t.Fatalf("idle report changed demand: %d -> %d", decayed, got)
	}
	if got := s.Stats().EpochDemandQ8; got != uint16(decayed) {
		t.Fatalf("published demand = %d, want %d", got, decayed)
	}
}

func TestEpochPromotesOnlyConfirmedCorrelation(t *testing.T) {
	s := NewSlidingSender(DefaultConfig())
	s.epochPolicy.initialized = true
	bursty := wire.Feedback{LossRate: 1, Burstiness: epochBurstThresholdQ8}
	iid := wire.Feedback{LossRate: 1, Burstiness: burstQ8One}

	s.updateEpochDemand(bursty, false, true)
	if s.epochPolicy.memoryQ8 != 0 || s.epochPolicy.demandQ8 > epochExploreDemandQ8 {
		t.Fatalf("one burst report promoted memory: demand=%d correlation=%d memory=%d",
			s.epochPolicy.demandQ8, s.epochPolicy.correlationQ8, s.epochPolicy.memoryQ8)
	}
	s.updateEpochDemand(iid, false, true)
	if s.epochPolicy.correlationQ8 != 0 || s.epochPolicy.memoryQ8 != 0 {
		t.Fatalf("intervening iid report retained unconfirmed correlation: correlation=%d memory=%d",
			s.epochPolicy.correlationQ8, s.epochPolicy.memoryQ8)
	}

	for range 3 {
		s.updateEpochDemand(bursty, false, true)
	}
	if s.epochPolicy.memoryQ8 != epochDemandOne || s.epochPolicy.demandQ8 < epochMemoryDemandQ8 {
		t.Fatalf("confirmed correlation did not promote memory: demand=%d correlation=%d memory=%d",
			s.epochPolicy.demandQ8, s.epochPolicy.correlationQ8, s.epochPolicy.memoryQ8)
	}
}

func TestEpochMemoryReleasesFasterWhenReactiveCanTakeOver(t *testing.T) {
	newSender := func(buffer int64) *SlidingSender {
		cfg := DefaultConfig()
		cfg.BufferMicros = buffer
		s := NewSlidingSender(cfg)
		s.epochPolicy.initialized = true
		s.epochPolicy.demandQ8 = epochDemandOne
		s.epochPolicy.memoryQ8 = epochDemandOne
		return s
	}
	tight := newSender(60_000)
	reactive := newSender(200_000)
	tight.updateEpochDemand(wire.Feedback{}, true, true)
	reactive.updateEpochDemand(wire.Feedback{}, true, true)
	if reactive.epochPolicy.memoryQ8 >= tight.epochPolicy.memoryQ8 || reactive.epochPolicy.demandQ8 >= tight.epochPolicy.demandQ8 {
		t.Fatalf("reactive release not faster: tight demand/memory=%d/%d reactive=%d/%d",
			tight.epochPolicy.demandQ8, tight.epochPolicy.memoryQ8,
			reactive.epochPolicy.demandQ8, reactive.epochPolicy.memoryQ8)
	}
}

func TestEpochReevaluatesMixAtSafeBlockBoundaries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SymbolSize = 1_284
	cfg.BufferMicros = 150_000
	s := NewSlidingSender(cfg)
	s.interMicros = 500
	s.rttMicros = 100_000
	s.sourceWireBytes = int64(repairWireBaseBytes + codedSymbolSize(cfg.SymbolSize))
	s.epochPolicy.demandQ8 = epochExploreDemandQ8

	first := s.epochBlockFor(0, 0, clock.Timestamp(cfg.BufferMicros))
	if first == nil || first.share <= 0 {
		t.Fatal("reactive/long-slack conditions disabled the epoch lane")
	}
	firstMix := first.share
	s.epochPolicy.demandQ8 = epochDemandOne
	for id := uint32(1); id < epochBlockSymbols; id++ {
		if got := s.epochBlockFor(clock.Timestamp(id*500), id, clock.Timestamp(cfg.BufferMicros)); got != first {
			t.Fatalf("source %d did not retain its announced block", id)
		}
		if first.share != firstMix {
			t.Fatalf("in-flight block mix changed from %.6f to %.6f", firstMix, first.share)
		}
	}
	s.epoch = nil // model the close boundary; no rows are needed for this policy test
	second := s.epochBlockFor(8_000, epochBlockSymbols, clock.Timestamp(cfg.BufferMicros+8_000))
	if second == nil {
		t.Fatal("next block did not reevaluate live demand")
	}
	if second.share <= firstMix {
		t.Fatalf("new demand did not raise next-block mix: %.6f <= %.6f", second.share, firstMix)
	}
}

// TestEpochProfileSourceTimeGate pins continuous automatic allocation against
// the same sliding sender with the epoch lane masked on the same time-indexed
// erasure realization.
// Optional packet placement therefore cannot advance the impairment oracle.
func TestEpochProfileSourceTimeGate(t *testing.T) {
	type cell struct {
		name              string
		rtt, budget       int64
		burst             float64
		minDeltaPerSource float64
	}
	const (
		n     = 6_000
		seeds = 4
	)
	for _, c := range []cell{
		{name: "iid-short-horizon", rtt: 60_000, budget: 60_000},
		{name: "iid-reactive-room", rtt: 60_000, budget: 120_000},
		{name: "ge6-short-horizon", rtt: 60_000, budget: 60_000, burst: 6, minDeltaPerSource: -0.01},
		{name: "ge24-short-horizon", rtt: 60_000, budget: 60_000, burst: 24, minDeltaPerSource: 0.04},
		{name: "ge48-short-horizon", rtt: 60_000, budget: 60_000, burst: 48, minDeltaPerSource: 0.15},
		{name: "ge48-reactive-room", rtt: 60_000, budget: 120_000, burst: 48, minDeltaPerSource: -0.01},
		{name: "ge48-wan-transition", rtt: 200_000, budget: 200_000, burst: 48, minDeltaPerSource: 0.01},
		{name: "ge48-long-slack", rtt: 400_000, budget: 400_000, burst: 48, minDeltaPerSource: -0.01},
	} {
		t.Run(c.name, func(t *testing.T) {
			var autoDelivery, offDelivery int
			var autoBytes, offBytes, rows uint64
			for seed := int64(1); seed <= seeds; seed++ {
				cfg := DefaultConfig()
				cfg.Flow = 1
				cfg.SymbolSize = 256
				cfg.BufferMicros = c.budget
				cfg.MaxBitrate = 8_000_000
				makeDrop := func() func(wire.Symbol) bool {
					if c.burst == 0 {
						return uniformDrop(uint64(seed)*7919+13, 0.10)
					}
					return geTimeDrop(seed*7919+13, 500, 0.10, c.burst)
				}
				link := simLink{
					cfg: cfg, owdMicros: c.rtt / 2, srcMicros: 500, n: n, sliding: true,
					paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: seed,
				}
				link.drop = makeDrop()
				auto := link.run()
				link.drop = makeDrop()
				offSender := NewSlidingSender(cfg)
				offSender.disableEpochRepair = true
				off := link.runCores(offSender, NewSlidingReceiver(cfg))
				for arm, result := range map[string]simResult{"auto": auto, "block-off": off} {
					if result.corrupt || result.lateDeliv {
						t.Fatalf("seed %d %s: corrupt=%v late=%v", seed, arm, result.corrupt, result.lateDeliv)
					}
					assertOrdered(t, result.deliveredIDs)
				}
				autoDelivery += auto.deliveredInTime
				offDelivery += off.deliveredInTime
				autoBytes += auto.wireBytes
				offBytes += off.wireBytes
				rows += auto.sstats.RepairEpoch
			}
			t.Logf("delivery %.2f%% -> %.2f%% (%+d); wire %.3fMB -> %.3fMB; epoch rows=%d",
				100*float64(offDelivery)/float64(n*seeds), 100*float64(autoDelivery)/float64(n*seeds),
				autoDelivery-offDelivery, float64(offBytes)/1e6, float64(autoBytes)/1e6, rows)
			if rows == 0 {
				t.Fatal("continuous allocator emitted no epoch rows")
			}
			minimum := int(c.minDeltaPerSource * n * seeds)
			if autoDelivery-offDelivery < minimum {
				t.Fatalf("delivery gain = %d, want at least %d", autoDelivery-offDelivery, minimum)
			}
			if autoBytes > offBytes+offBytes/100 {
				t.Fatalf("wire grew by more than 1%%: %d > %d", autoBytes, offBytes)
			}
		})
	}
}
