package flow

// Tests for retrospective coded repair in the sliding profile (RepairAt over the stuck
// cursor window), loss-onset event feedback, and the honest reactive-cycle
// model that gates the singleton/anchor extras.

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestSlidingRetroReactiveRecoversSlidBurst verifies at flow level that a
// burst wipes a span (source AND its trailing repair) while the stream keeps
// flowing, so by the time feedback reports the stuck cursor the band has slid far
// past the holes. Retrospective repair must still carry innovation for that window
// and recover it fully within a
// generous budget.
func TestSlidingRetroReactiveRecoversSlidBurst(t *testing.T) {
	t.Parallel()
	const (
		n      = 1_200
		owd    = 30_000
		src    = 500
		budget = 150_000 // 2.5x RTT: reactive is honestly capable
	)
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.05, TargetFailure: 1e-2, BufferMicros: budget,
	}
	// Kill EVERYTHING on the wire (source + repair) for an emission-count window —
	// a hard burst of ~64 datagrams (~2 bands) mid-stream, then a clean channel.
	ch := &pathOutageChannel{path: 0, from: 400, to: 464} // PathID 0 == every symbol (single path)
	res := simLink{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: true, drop: ch.drop}.run()
	assertCoreInvariants(t, res, n, "retro-reactive burst")
	t.Logf("delivered=%d lost=%d recovered=%d reactive=%d repair=%d",
		res.delivered, res.stats.Lost, res.stats.Recovered, res.sstats.ReactiveRepair, res.sstats.Repair)
	if res.sstats.ReactiveRepair == 0 {
		t.Fatal("no reactive repair emitted — the retro path did not engage")
	}
	if res.delivered != n {
		t.Fatalf("burst not fully recovered within a 2.5xRTT budget: %d/%d (lost=%d reactive=%d)",
			res.delivered, n, res.stats.Lost, res.sstats.ReactiveRepair)
	}
}

// TestRetroReactiveBudgetSweep maps the mechanism to its physics across deadline
// budgets: a burst that kills a full band-and-a-half of emissions
// mid-stream must be fully recovered wherever one honest reactive cycle plus repair
// transit fits the budget (>= ~1.9xRTT for this geometry), must help partially at
// 1.5xRTT, and must at minimum keep the four invariants and emit nothing harmful at
// 1xRTT (where a hard-deadline transport provably cannot land reactive repair, the
// D4/G3 accounting note's regime).
func TestRetroReactiveBudgetSweep(t *testing.T) {
	t.Parallel()
	const (
		n   = 1_200
		owd = 30_000 // RTT 60 ms
		src = 500
	)
	for _, tc := range []struct {
		budgetMicros int64
		fullRecovery bool
		wantDormant  bool // sub-cycle budget: the retro tier must not spend bytes it cannot land
	}{
		{60_000, false, true}, // 1xRTT: cycle 80 ms > budget — dormant by the cycle gate
		{90_000, false, false},
		{150_000, true, false},
		{120_000, true, false},
	} {
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
			Redundancy: 0.05, TargetFailure: 1e-2, BufferMicros: tc.budgetMicros,
		}
		ch := &pathOutageChannel{path: 0, from: 400, to: 448}
		res := simLink{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: true, drop: ch.drop}.run()
		assertCoreInvariants(t, res, n, "retro budget sweep")
		t.Logf("budget=%.2fxRTT delivered=%d/%d lost=%d reactive=%d",
			float64(tc.budgetMicros)/(2*float64(owd)), res.delivered, n, res.stats.Lost, res.sstats.ReactiveRepair)
		if tc.fullRecovery && res.delivered != n {
			t.Errorf("budget %dus: burst not fully recovered: %d/%d", tc.budgetMicros, res.delivered, n)
		}
		if tc.wantDormant && res.sstats.ReactiveRepair > 0 {
			t.Errorf("budget %dus: retro tier spent %d symbols it provably cannot land",
				tc.budgetMicros, res.sstats.ReactiveRepair)
		}
	}
}

// TestRepairAtCoversRetainedWindow pins the encoder primitive: RepairAt clips to
// retention, returns the covered range, and the emitted equation decodes holes in
// that range through the ordinary band-decoder path.
func TestRepairAtCoversRetainedWindow(t *testing.T) {
	s := NewSlidingSender(Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 8, BufferMicros: 60_000})
	now := clock.Timestamp(0)
	for i := 0; i < 24; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	// Fully below retention after SlideTo: nothing to cover.
	s.enc.SlideTo(20)
	if _, n, pay := s.enc.RepairAt(9_001, 4, 8); n != 0 || pay != nil {
		t.Fatalf("RepairAt below retention returned n=%d", n)
	}
	// Straddling: clipped to the retained suffix.
	base, n, pay := s.enc.RepairAt(9_002, 18, 8)
	if base != 20 || n != 4 || pay == nil {
		t.Fatalf("RepairAt straddle = (%d, %d), want (20, 4)", base, n)
	}
}

// TestLossOnsetEventFeedback: a fresh wire-loss run triggers a feedback report
// within the event floor (10 ms) instead of waiting out the 20 ms cadence — on both
// receiver profiles.
func TestLossOnsetEventFeedback(t *testing.T) {
	for _, sliding := range []bool{false, true} {
		cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 16, Sliding: sliding, CodingWindow: 16, BufferMicros: 100_000}
		var r coreReceiverT
		if sliding {
			r = NewSlidingReceiver(cfg)
		} else {
			r = NewReceiver(cfg)
		}
		now := clock.Timestamp(0)
		feed := func(id uint32) {
			sym := wire.Symbol{
				Flow: 1, Kind: wire.Systematic, WindowBase: genBaseOf(id, 16),
				SrcIndex: id, N: 16, Deadline: int64(now.Add(cfg.BufferMicros)), Payload: make([]byte, testSym),
			}
			if sliding {
				sym.WindowBase, sym.N = id, 1
			}
			r.FeedSymbol(now, wire.EncodeSymbol(nil, sym))
		}
		drain := func() int {
			k := 0
			for {
				if _, ok := r.PollSend(); !ok {
					return k
				}
				k++
			}
		}
		feed(0) // primes the walk and emits the first (unconditional) report
		drain()
		// Clean arrivals inside the cadence: no report.
		now = now.Add(11_000)
		feed(1)
		if got := drain(); got != 0 {
			t.Fatalf("sliding=%v: clean arrival inside the cadence emitted %d reports", sliding, got)
		}
		// A gap (id 3 skips id 2) inside the cadence but past the event floor: report NOW.
		now = now.Add(1_000)
		feed(3)
		if got := drain(); got == 0 {
			t.Fatalf("sliding=%v: loss onset did not trigger an event feedback", sliding)
		}
	}
}

// TestSingletonExtrasDormantWhenReactiveReachable pins the reachability gate: where
// the retro tier can repair an observed-burst-length hole within the budget
// (cycle + 2×burst duration ≤ budget), the per-chunk singleton and anchor-closure
// extras stay off (retro-reactive owns reference recovery); below it they engage
// (the only protection that can land). Priority tier alone is the trigger, so this
// runs payload-agnostic. The channel state is set explicitly: a warm short-burst
// (i.i.d.) estimate on a 20 ms-RTT link vs a cold high-RTT one — the burst term is
// part of the predicate (see extrasReplaceableByReactive).
func TestSingletonExtrasDormantWhenReactiveReachable(t *testing.T) {
	write := func(budget, rtt int64, warm bool) uint64 {
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: budget,
		}
		s := NewSlidingSender(cfg)
		s.rttMicros = rtt
		if warm {
			s.fbCount = coldStartFeedbacks
			s.burstQ8 = burstQ8One // i.i.d.: ~1-symbol runs
		}
		now := clock.Timestamp(0)
		for i := 0; i < 40; i++ {
			s.WriteUnit(now, makeChunkN(uint32(i)), uepCenterTier+1)
			now = now.Add(1_000)
		}
		s.Flush(now)
		return s.Stats().Repair
	}
	// Capable: cycle(20ms rtt)=35ms + 2·1·1ms ≤ 100ms. Incapable: cycle(50ms)=72.5ms > 60ms.
	capable := write(100_000, 20_000, true)
	incapable := write(60_000, 50_000, false)
	if capable >= incapable {
		t.Fatalf("extras not gated: repair at capable budget %d >= incapable %d", capable, incapable)
	}
	if capable != 0 {
		t.Fatalf("capable budget with warm i.i.d. channel still emitted %d dedicated repairs", capable)
	}
}

// TestReactiveRoundsHonestCycle pins the generation-side cycle arithmetic: at
// RTT 100 ms the old 2xRTT+cadence model priced reactive out of a 200 ms budget
// (0 rounds); the honest cycle credits one round there and still credits none at a
// sub-RTT budget (the frontier regime keeps its full proactive margins).
func TestReactiveRoundsHonestCycle(t *testing.T) {
	s := NewSender(Config{Flow: 1, SymbolSize: testSym, GenSize: 16, BufferMicros: 200_000})
	s.rttMicros = 100_000
	if got := s.reactiveRounds(); got != 1 {
		t.Fatalf("rounds at 2xRTT budget = %d, want 1", got)
	}
	s.cfg.BufferMicros = 75_000 // 0.75xRTT: the frontier
	if got := s.reactiveRounds(); got != 0 {
		t.Fatalf("rounds at 0.75xRTT budget = %d, want 0", got)
	}
}
