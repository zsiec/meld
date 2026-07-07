package flow

// Tests for the DEFAULT-ON sliding confirmed-clean floor decay (PREREG Amendment
// 7): the strict composite keying of cleanRun (any loss signal resets it), the
// structural eligibility gate (rounds >= reactiveFloorSafe), the clean-link
// overhead collapse it exists for, and the onset-burst-after-confirmed-clean
// recovery that makes it safe (the floor re-arms on first evidence and the
// reactive tier carries the burst).

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestCleanRunCompositeKeying pins the strict composite: every one of the four
// loss signals independently resets cleanRun; only an all-quiet report counts.
func TestCleanRunCompositeKeying(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0.15, BufferMicros: 200_000}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)
	for i := 0; i < 8; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)

	next := uint32(8)
	offer := func() { // the clean-run progress gate only counts intervals with offered source
		s.Write(now, makeChunkN(next))
		next++
		drainSlidingSymbols(t, s)
	}
	quiet := wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8}
	dirty := []struct {
		name string
		fb   wire.Feedback
	}{
		{"LossRate", wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8, LossRate: 1}},
		{"CongestionLoss", wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8, CongestionLoss: 1}},
		{"Deficit", wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8, Deficit: 1}},
		{"Missing", wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8, Missing: 1}},
	}
	for _, d := range dirty {
		for i := 0; i < 3; i++ {
			now = now.Add(20_000)
			offer()
			s.FeedFeedback(now, quiet)
		}
		if s.cleanRun != 3 {
			t.Fatalf("%s: cleanRun after 3 quiet reports = %d, want 3", d.name, s.cleanRun)
		}
		now = now.Add(20_000)
		offer()
		s.FeedFeedback(now, d.fb)
		if s.cleanRun != 0 {
			t.Fatalf("%s: dirty report did not reset cleanRun (= %d)", d.name, s.cleanRun)
		}
		drainSlidingSymbols(t, s) // a Deficit report may emit reactive symbols; keep the queue clean
	}
	// The counter saturates at the confirm threshold and stays there while quiet.
	for i := 0; i < cleanFloorConfirm+8; i++ {
		now = now.Add(20_000)
		offer()
		s.FeedFeedback(now, quiet)
	}
	if s.cleanRun != cleanFloorConfirm {
		t.Fatalf("cleanRun saturation = %d, want %d", s.cleanRun, cleanFloorConfirm)
	}
}

// TestFloorDecayEligibilityGate pins the structural gate: a confirmed-clean run
// decays the floor only where the budget affords reactiveFloorSafe honest
// reactive rounds, independent of any opt-in flag.
func TestFloorDecayEligibilityGate(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0.15, BufferMicros: 200_000}
	s := NewSlidingSender(cfg)
	s.cleanRun = cleanFloorConfirm
	if s.floorDecayed(reactiveFloorSafe - 1) {
		t.Fatal("floor decayed below the reactive-rounds eligibility gate")
	}
	if !s.floorDecayed(reactiveFloorSafe) {
		t.Fatal("confirmed-clean floor did not decay at the eligible rounds")
	}
	s.cleanRun = cleanFloorConfirm - 1
	if s.floorDecayed(reactiveFloorSafe) {
		t.Fatal("floor decayed before the clean run confirmed")
	}
}

// floorDecaySim runs a clean sliding stream long enough for cleanRun to confirm
// at the 20 ms feedback cadence (~1.3 s), with an optional deep source-id outage
// near the tail.
func floorDecaySim(cfg Config, n int, drop func(wire.Symbol) bool) simResult {
	if drop == nil {
		drop = func(wire.Symbol) bool { return false }
	}
	return simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: n,
		sliding: true, drop: drop}.run()
}

// TestFloorDecayCollapsesCleanOverhead is the sim form of the arc's win: on a
// confirmed-clean link at a reactive-capable budget the standing floor decays,
// collapsing repair overhead well below the configured floor while delivery
// stays complete. The ineligible-budget control keeps the full floor.
func TestFloorDecayCollapsesCleanOverhead(t *testing.T) {
	t.Parallel()
	const n = 8_000 // 4 s at the 500 us cadence: ~1.3 s to confirm, ~2.7 s decayed
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.15, BufferMicros: 200_000}
	res := floorDecaySim(cfg, n, nil)
	assertCoreInvariants(t, res, n, "clean decay")
	if res.delivered != n {
		t.Fatalf("clean link delivered %d/%d", res.delivered, n)
	}
	if ov := res.overhead(); ov > 0.08 {
		t.Fatalf("decayed clean-link overhead = %.3f, want < 0.08 (floor 0.15 should be off ~2/3 of the run)", ov)
	}

	// Control: budget below two honest reactive cycles (rtt 60 ms -> cycle 85 ms):
	// the decay is structurally ineligible and the full floor must be retained.
	tight := cfg
	tight.BufferMicros = 100_000
	resT := floorDecaySim(tight, n, nil)
	assertCoreInvariants(t, resT, n, "clean tight-budget control")
	if resT.delivered != n {
		t.Fatalf("tight-budget clean link delivered %d/%d", resT.delivered, n)
	}
	if ov := resT.overhead(); ov < 0.13 {
		t.Fatalf("tight-budget overhead = %.3f, want >= 0.13 (floor must be retained below the eligibility gate)", ov)
	}
}

// TestFloorDecayOnsetBurstRecovered is the safety half: after the floor has
// confirmed clean and decayed, a deep loss burst hits. The first contrary report
// must re-arm the floor (cleanRun reset) and the reactive tier must recover the
// burst in full within the budget — the O(p^rounds) argument the decay rests on.
func TestFloorDecayOnsetBurstRecovered(t *testing.T) {
	t.Parallel()
	const (
		n         = 8_000
		burstFrom = 7_200 // ~3.6 s in: long after cleanRun confirms at ~1.3 s
		burstTo   = 7_264 // 64 consecutive source ids (~32 ms outage)
	)
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.15, BufferMicros: 200_000}
	drop := func(sym wire.Symbol) bool {
		return sym.Kind == wire.Systematic && sym.SrcIndex >= burstFrom && sym.SrcIndex < burstTo
	}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	res := simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: n,
		sliding: true, drop: drop}.runCores(s, r)
	assertCoreInvariants(t, res, n, "onset burst after confirmed clean")
	if res.delivered != n {
		t.Fatalf("onset burst not recovered: delivered %d/%d", res.delivered, n)
	}
	if res.sstats.ReactiveRepair == 0 {
		t.Fatal("recovery did not engage the reactive tier (the decayed floor cannot have been the cover)")
	}
	// The burst's contrary reports must have re-armed the floor; the ~0.4 s of
	// stream left after it cannot re-confirm at the 20 ms cadence.
	if s.cleanRun >= cleanFloorConfirm {
		t.Fatalf("cleanRun = %d after the onset burst, want < %d (floor must re-arm on first evidence)",
			s.cleanRun, cleanFloorConfirm)
	}
	// The post-onset overhead spike is the loss estimator's honest burst response
	// (the pEst window reads a deep gap as heavy loss and re-provisions the tail);
	// it is not the decay's doing — the same spike occurs with the static floor.
	// Bound it loosely to catch an unbounded reactive flood only.
	if ov := res.overhead(); ov > 0.8 {
		t.Fatalf("overhead with one onset burst = %.3f, want < 0.8 (reactive answer must stay bounded)", ov)
	}
	t.Logf("onset after confirmed clean: delivered=%d/%d reactive=%d cleanRun=%d overhead=%.1f%%",
		res.delivered, n, res.sstats.ReactiveRepair, s.cleanRun, 100*res.overhead())
}

// TestCleanRunSettledEvidence pins the settled-evidence keying (arc 8): when the
// peer reports the settled tail, SettledLost==0 counts clean even while the
// raw-order signals read dirty (reorder ghosts), SettledLost>0 resets, and a
// peer without the tail falls back to the strict raw composite.
func TestCleanRunSettledEvidence(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0.15, BufferMicros: 200_000}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)
	for i := 0; i < 8; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)

	// Ghost-dirty raw signals with clean settled evidence: counts clean.
	next := uint32(8)
	offer := func() { // the clean-run progress gate only counts intervals with offered source
		s.Write(now, makeChunkN(next))
		next++
		drainSlidingSymbols(t, s)
	}
	ghost := wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8,
		LossRate: 300, CongestionLoss: 2, Deficit: 1, Missing: 0b10,
		HasSettled: true, SettledLost: 0}
	for i := 0; i < 5; i++ {
		now = now.Add(20_000)
		offer()
		s.FeedFeedback(now, ghost)
		drainSlidingSymbols(t, s)
	}
	if s.cleanRun != 5 {
		t.Fatalf("cleanRun with ghost-dirty raw but settled-clean = %d, want 5", s.cleanRun)
	}
	// Settled loss resets regardless of quiet raw signals.
	now = now.Add(20_000)
	offer()
	s.FeedFeedback(now, wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8,
		HasSettled: true, SettledLost: 3})
	if s.cleanRun != 0 {
		t.Fatalf("settled loss did not reset cleanRun (= %d)", s.cleanRun)
	}
	// Old peer (no tail): the raw composite governs — a dirty signal resets.
	now = now.Add(20_000)
	offer()
	s.FeedFeedback(now, wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8})
	if s.cleanRun != 1 {
		t.Fatalf("legacy quiet report = %d, want 1", s.cleanRun)
	}
	now = now.Add(20_000)
	offer()
	s.FeedFeedback(now, wire.Feedback{Flow: 1, HighestSeen: 8, DecodedLowEdge: 8, CongestionLoss: 1})
	if s.cleanRun != 0 {
		t.Fatalf("legacy dirty report did not reset cleanRun (= %d)", s.cleanRun)
	}
}

// TestSettledWalkAdjudicatesReorder pins the receiver half: pure reorder never
// counts settled loss once the adaptive window has learned the spread, while a
// real gap counts after the holdoff.
func TestSettledWalkAdjudicatesReorder(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0.15, BufferMicros: 200_000}
	r := NewSlidingReceiver(cfg)
	now := clock.Timestamp(0)
	feed := func(id uint32) {
		r.FeedSymbol(now, wire.EncodeSymbol(nil, wire.Symbol{Flow: 1, Kind: wire.Systematic,
			SrcIndex: id, Deadline: int64(now.Add(cfg.BufferMicros)),
			Payload: makeChunkN(id)}))
	}
	// Adjacent swaps at 1ms spacing: pure reorder, zero loss. The fixed holdoff
	// (budget/8, >= 10ms) far exceeds the swap spread, so nothing ever settles lost.
	id := uint32(0)
	for burst := 0; burst < 60; burst++ {
		feed(id + 1)
		feed(id)
		id += 2
		now = now.Add(1_000)
		r.Tick(now)
	}
	if r.settledLostSince != 0 {
		t.Fatalf("pure reorder counted %d settled losses", r.settledLostSince)
	}
	// A real gap: skip 3 ids, feed the ids behind it (held), then expire the
	// holdoff via the walk directly — Tick/FeedSymbol would emit a feedback report
	// that consumes the per-interval counter before the assertion.
	id += 3
	for k := 0; k < 8; k++ {
		feed(id)
		id++
		now = now.Add(1_000)
	}
	now = now.Add(settledHoldoffMicros(cfg) + 1_000)
	r.settled.drain(now)
	if r.settledLostSince != 3 {
		t.Fatalf("real gap settled %d, want 3", r.settledLostSince)
	}
}

// TestFloorDecayArmsUnderReorder is the arc-8 win in sim form: on a CLEAN link
// with deterministic reorder (the regime where the raw composite reads dirty on
// nearly every report and today's decay never arms), the settled evidence arms
// the decay and the overhead collapses below the floor — at full delivery. The
// lossy control must never arm.
func TestFloorDecayArmsUnderReorder(t *testing.T) {
	t.Parallel()
	const n = 8_000
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.15, BufferMicros: 200_000}
	s := NewSlidingSender(cfg)
	rr := NewSlidingReceiver(cfg)
	sl := simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: n, sliding: true,
		jitterMicros: 3_000, // deterministic per-datagram delay: deep reorder, zero loss
		drop:         func(wire.Symbol) bool { return false }}
	var lastTap clock.Timestamp
	sl.fbTap = func(now clock.Timestamp, fb wire.Feedback) {
		if now.Sub(lastTap) < 200_000 {
			return
		}
		lastTap = now
		t.Logf("tap t=%.2f settled=%d hasSettled=%v lr=%d cl=%d def=%d cleanRun=%d rate=%.3f",
			float64(now)/1e6, fb.SettledLost, fb.HasSettled, fb.LossRate, fb.CongestionLoss,
			fb.Deficit, s.cleanRun, s.codeRate())
	}
	res := sl.runCores(s, rr)
	assertCoreInvariants(t, res, n, "clean reordered decay")
	if res.delivered != n {
		t.Fatalf("clean reordered link delivered %d/%d", res.delivered, n)
	}
	t.Logf("clean reordered: ovh=%.3f proactive=%d reactive=%d cleanRun=%d settledSince=%d",
		res.overhead(), res.sstats.Repair-res.sstats.ReactiveRepair,
		res.sstats.ReactiveRepair, s.cleanRun, rr.settledLostSince)
	if s.cleanRun < cleanFloorConfirm {
		t.Fatalf("cleanRun = %d, want >= %d (settled evidence must arm on a clean reordered link)",
			s.cleanRun, cleanFloorConfirm)
	}
	if ov := res.overhead(); ov > 0.10 {
		t.Fatalf("overhead on clean reordered link = %.3f, want < 0.10 (settled evidence must arm the decay)", ov)
	}

	// Lossy control (same reorder): settled losses are real; the decay must not arm.
	s2 := NewSlidingSender(cfg)
	r2 := NewSlidingReceiver(cfg)
	resL := simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: n, sliding: true,
		jitterMicros: 3_000, drop: uniformDrop(0xA5A5A5, 0.02)}.runCores(s2, r2)
	assertCoreInvariants(t, resL, n, "lossy reordered control")
	if s2.cleanRun >= cleanFloorConfirm {
		t.Fatalf("cleanRun = %d on a 2%% loss link, must stay below %d", s2.cleanRun, cleanFloorConfirm)
	}
	if ov := resL.overhead(); ov < 0.13 {
		t.Fatalf("lossy reordered overhead = %.3f, want >= 0.13 (floor retained + loss-driven repair)", ov)
	}
}
