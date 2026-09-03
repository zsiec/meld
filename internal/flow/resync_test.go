package flow

// Tests for the LTR-resync mechanism (resync.go + the receiver's LTR feedback state
// + the EncoderControl plumbing): the controller's trigger/hold/expiry rules in
// isolation, the receiver's NewestDecodableLTR / BrokenAnchors reporting, and the
// end-to-end closed-loop test (the app marks FrameDesc.LTR, polls
// EncoderControl, honors Resync) recovering decodable pictures where the
// wait-for-IDR baseline collapses (ltr_resync_test.go).

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// TestResyncControllerRules pins the controller's state machine: baseline-not-event
// first report, damage-delta trigger gated on a known safe LTR, the hold-down, the
// unhonored expiry, and honored clearing.
func TestResyncControllerRules(t *testing.T) {
	const hold = 100_000
	var c resyncController
	now := clock.Timestamp(1_000_000)

	// First report is a baseline, never a trigger — even with damage counted.
	c.observe(now, 5, 0, hold)
	if c.request() != 0 {
		t.Fatal("first report must not trigger")
	}
	// New damage but no safe LTR: nothing to resync against.
	now = now.Add(20_000)
	c.observe(now, 6, 0, hold)
	if c.request() != 0 {
		t.Fatal("damage without a safe LTR must not trigger")
	}
	// Safe LTR arrives with no NEW damage: still quiet.
	now = now.Add(20_000)
	c.observe(now, 6, 41, hold)
	if c.request() != 0 {
		t.Fatal("a safe LTR alone must not trigger")
	}
	// New damage with a safe LTR: request raised, naming the newest safe.
	now = now.Add(20_000)
	c.observe(now, 7, 41, hold)
	if c.request() != 41 {
		t.Fatalf("request = %d, want 41", c.request())
	}
	// More damage inside the hold window: no re-raise after honoring (the recovery's
	// fate is not knowable yet).
	c.honored()
	now = now.Add(20_000)
	c.observe(now, 9, 41, hold)
	if c.request() != 0 {
		t.Fatal("re-raised inside the hold window")
	}
	// Damage after the hold expires: re-raised, and it names a NEWER safe LTR.
	now = now.Add(hold + 1)
	c.observe(now, 10, 77, hold)
	if c.request() != 77 {
		t.Fatalf("request = %d, want 77", c.request())
	}
	// Unhonored requests expire after one hold window (the app may not support resync).
	now = now.Add(hold + 1)
	c.observe(now, 10, 77, hold)
	if c.request() != 0 {
		t.Fatal("unhonored request did not expire")
	}
	// Counter wrap: a cumulative uint16 wrapping past zero still yields a delta.
	c2 := resyncController{}
	c2.observe(now, 0xFFFF, 5, hold)
	now = now.Add(20_000)
	c2.observe(now, 1, 5, hold) // delta 2 across the wrap
	if c2.request() != 5 {
		t.Fatal("wrapped counter delta did not trigger")
	}
	// Stale/reordered report: feedback rides UDP, so a report carrying an OLDER
	// cumulative count can arrive after a newer one. Serial-number arithmetic must
	// drop it — not read the backward step as ~65k new broken anchors — and must not
	// regress lastBroken (the next real report's delta stays honest).
	c3 := resyncController{}
	c3.observe(now, 5, 9, hold) // baseline
	now = now.Add(20_000)
	c3.observe(now, 7, 9, hold) // real damage: raised
	if c3.request() != 9 {
		t.Fatalf("request = %d, want 9", c3.request())
	}
	c3.honored()
	now = now.Add(hold + 1)
	c3.observe(now, 5, 9, hold) // the STALE report, reordered behind the 7
	if c3.request() != 0 {
		t.Fatal("stale reordered report raised a spurious resync")
	}
	if c3.lastBroken != 7 {
		t.Fatalf("lastBroken regressed to %d on a stale report, want 7", c3.lastBroken)
	}
	now = now.Add(20_000)
	c3.observe(now, 8, 9, hold) // next real report: delta 1 from 7, not 3 from 5
	if c3.request() != 9 {
		t.Fatal("real damage after a stale report did not trigger")
	}
}

// resyncLoopApp is the closed-loop application for the end-to-end test: it emits the
// synthetic GOP template via the REAL WriteFrame path, marks LTR candidates, and —
// when honoring — polls EncoderControl each frame slot and answers Resync with a
// recovery-P referencing the named LTR. The ONLY difference between the arms is
// whether EncoderControl is honored; the wire mechanism runs in both.
type resyncLoopApp struct {
	enc    *ltrEncoder
	honor  bool
	honors int
}

// emit produces the next frame's chunk writes, consulting the real EncoderControl.
func (a *resyncLoopApp) emit(ec EncoderControl) []chunkWrite {
	if a.honor && ec.Resync {
		if _, ok := a.enc.frameIDKnown(ec.ResyncRefFrameID); ok {
			a.honors++
			return a.enc.emitRecovery(ec.ResyncRefFrameID)
		}
	}
	return a.enc.emitFrame()
}

// frameIDKnown reports whether the encoder still retains fid as an LTR (here: it was
// ever emitted as a candidate — the synthetic encoder retains all).
func (e *ltrEncoder) frameIDKnown(fid uint32) (ltrUnit, bool) {
	if int(fid) < len(e.units) && e.units[fid].ltrCand {
		return e.units[fid], true
	}
	return ltrUnit{}, false
}

// emitRecovery emits a recovery-P referencing fid (an LTR the receiver confirmed),
// bypassing the encoder's internal damage model — the trigger came from the REAL
// EncoderControl loop.
func (e *ltrEncoder) emitRecovery(fid uint32) []chunkWrite {
	e.sinceResync++
	chunks := int(2 * ltrChunksP)
	out := e.addUnit(shape.Unit{
		Class: shape.ClassBase, Picture: true,
		Confidence: shape.Signaled, RefersTo: []uint32{fid},
	}, chunks, true, true)
	id := e.units[len(e.units)-1].u.ID
	e.prevAnchor = int64(id)
	e.lastResync = int64(id)
	e.recoveries++
	e.frameIdx++
	return out
}

// runResyncLoop drives the real sliding sender/receiver with the real feedback loop
// (no test-side verdict channel: damage detection and safe-LTR selection ride
// wire.Feedback into the resyncController) over a seeded channel, and scores
// decodable pictures with the dependency-closure oracle.
func runResyncLoop(t *testing.T, honor bool, drop func(wire.Symbol) bool, frames int, jitterUs int64) (picRate float64, honors int, ec0 bool, srcChunks uint64) {
	t.Helper()
	const (
		owd      = 50_000
		frameCad = 40_000
		budget   = 100_000
		paceBps  = 400_000
	)
	cfg := ltrExperimentConfig(budget)
	// The app-side template: keyint 48, damage model unused (policySchedIDR emits the
	// plain template; resync frames come only from the honored EncoderControl).
	enc := newLTREncoder(policySchedIDR, 48, 2, 0)
	app := &resyncLoopApp{enc: enc, honor: honor}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)

	var s2r, r2s []inflight
	now := clock.Timestamp(0)
	var wireFreeAt clock.Timestamp
	nextFrame := clock.Timestamp(0)
	framesEmitted := 0
	delivered := map[uint32]bool{}
	const step = int64(1_000)

	pumpSender := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || drop(sym) {
				continue
			}
			extra := int64(0)
			if jitterUs > 0 {
				extra = int64(coinU(0xC0FFEE, uint32(sym.Kind), sym.SrcIndex, sym.WindowBase, uint32(sym.RepairKey)) * float64(jitterUs))
			}
			dep := now
			if wireFreeAt.After(dep) {
				dep = wireFreeAt
			}
			wireFreeAt = dep.Add(int64(len(d)) * 1_000_000 / paceBps)
			s2r = append(s2r, inflight{dep.Add(owd + extra), d})
		}
	}
	pumpReceiver := func() {
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			r2s = append(r2s, inflight{now.Add(owd), fb})
		}
		for {
			id, _, ok := r.PollDeliver()
			if !ok {
				break
			}
			delivered[id] = true
		}
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
	tickAll := func() {
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
	}

	for framesEmitted < frames {
		for framesEmitted < frames && !nextFrame.After(now) {
			ec := s.EncoderControl()
			if framesEmitted == 0 && ec.Resync {
				ec0 = true // a resync request before any damage would be a spurious trigger
			}
			for _, cw := range app.emit(ec) {
				s.WriteFrame(now, makeChunkN(cw.id), cw.fd)
			}
			framesEmitted++
			nextFrame = nextFrame.Add(frameCad)
		}
		tickAll()
		now = now.Add(step)
	}
	s.Flush(now)
	pumpSender()
	drainUntil := now.Add(budget + 8*owd)
	for now.Before(drainUntil) {
		now = now.Add(step)
		tickAll()
	}

	// Score: a unit is delivered iff all its chunk ids were delivered; decodability by
	// the dependency-closure oracle.
	units := make([]shape.Unit, len(enc.units))
	deliveredWhole := make(map[uint32]bool, len(enc.units))
	for i, u := range enc.units {
		units[i] = u.u
		whole := true
		for c := 0; c < u.chunks; c++ {
			if !delivered[u.startID+uint32(c)] {
				whole = false
				break
			}
		}
		deliveredWhole[u.u.ID] = whole
	}
	dec := shape.Decodable(units, deliveredWhole)
	pics, decPics := 0, 0
	for _, u := range units {
		if !u.Picture {
			continue
		}
		pics++
		if dec[u.ID] {
			decPics++
		}
	}
	if pics == 0 {
		t.Fatal("no pictures emitted")
	}
	st := s.Stats()
	return float64(decPics) / float64(pics), app.honors, ec0, st.Source
}

// TestLTRResyncEndToEnd verifies the mechanism over a Gilbert-Elliott burst
// channel (10% loss, mean burst 48 — the burst-frontier cell), an application that
// honors EncoderControl.Resync keeps decisively more decodable pictures than one
// that ignores it, using ONLY the real wire loop (FrameDesc.LTR → descriptor flag →
// receiver frame graph → Feedback.NewestDecodableLTR/BrokenAnchors →
// resyncController → EncoderControl). Thresholds sit far outside the arms' measured
// spreads in the pinned cases.
func TestLTRResyncEndToEnd(t *testing.T) {
	t.Parallel()
	const frames = 900
	for seed := int64(1); seed <= 3; seed++ {
		dropBase := geDrop(seed*7919+101, 0.10, 48)
		dropHonor := geDrop(seed*7919+101, 0.10, 48)
		base, _, _, baseSrc := runResyncLoop(t, false, dropBase, frames, 3_000)
		got, honors, ec0, honorSrc := runResyncLoop(t, true, dropHonor, frames, 3_000)
		t.Logf("seed %d: honor=%.3f (resyncs=%d, src=%d) ignore=%.3f (src=%d)",
			seed, got, honors, honorSrc, base, baseSrc)
		if ec0 {
			t.Fatalf("seed %d: Resync raised before any frame was written", seed)
		}
		if honors == 0 {
			t.Fatalf("seed %d: the honoring arm never resynced — the loop is not wiring through", seed)
		}
		if got < base+0.05 {
			t.Fatalf("seed %d: honored resync %.3f did not beat ignore %.3f by ≥0.05", seed, got, base)
		}
		// Recovery frames must stay within the pinned 10% source-byte overhead bound.
		if float64(honorSrc) > float64(baseSrc)*1.10 {
			t.Fatalf("seed %d: resync source overhead too high: %d vs %d", seed, honorSrc, baseSrc)
		}
	}
}

// TestLTRResyncDormantOnCleanLink: on a lossless, jitter-free link the mechanism
// never raises Resync and never emits a recovery frame — the arms are byte-identical
// (the guard the deployable meld-auto profile requires of every default-on seam).
func TestLTRResyncDormantOnCleanLink(t *testing.T) {
	noDrop := func(wire.Symbol) bool { return false }
	base, honorsB, _, baseSrc := runResyncLoop(t, false, noDrop, 300, 0)
	got, honors, ec0, honorSrc := runResyncLoop(t, true, noDrop, 300, 0)
	if ec0 || honors != 0 || honorsB != 0 {
		t.Fatalf("resync fired on a clean link (honors=%d)", honors)
	}
	if base != 1 || got != 1 {
		t.Fatalf("clean link should decode everything: honor=%.3f ignore=%.3f", got, base)
	}
	if baseSrc != honorSrc {
		t.Fatalf("clean-link arms diverged: src %d vs %d", honorSrc, baseSrc)
	}
}

// TestReceiverReportsLTRFeedback pins the receiver half in isolation (generation
// profile, the same frame graph the sliding receiver shares): a delivered LTR frame
// advances NewestDecodableLTR; an arrived REFERENCE picture whose dependency died
// increments BrokenAnchors; an arrived disposable leaf in the same state does not.
// (A frame that vanished entirely is invisible to the graph — coding rebuilds
// payloads, not headers — so chain damage is always observed through its arrived
// dependents, exactly as here.)
func TestReceiverReportsLTRFeedback(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 4, Redundancy: 0, BufferMicros: 50_000}
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)

	// One chunk per frame: f0 = delivered LTR anchor; f1 = P referencing f0, DROPPED
	// whole (invisible to the graph); f2 = arrived disposable leaf referencing dead f1
	// (undecodable but chain-harmless); f3 = arrived P referencing dead f1 (the broken
	// anchor); f4 = trailing frame referencing the SAFE anchor f0 (the resync shape —
	// decodable, and it pushes the cursor so f3 resolves). A frame referencing dead f3
	// would count too: continuing damage keeps re-arming the controller, by design.
	writes := []struct {
		fd   FrameDesc
		drop bool
	}{
		{FrameDesc{Priority: 2, FrameID: 0, Chunks: 1, LTR: true}, false},
		{FrameDesc{Priority: 2, FrameID: 1, Chunks: 1, RefFrameIDs: []uint32{0}}, true},
		{FrameDesc{Priority: 0, FrameID: 2, Chunks: 1, RefFrameIDs: []uint32{1}, Discardable: true}, false},
		{FrameDesc{Priority: 2, FrameID: 3, Chunks: 1, RefFrameIDs: []uint32{1}}, false},
		{FrameDesc{Priority: 2, FrameID: 4, Chunks: 1, RefFrameIDs: []uint32{0}}, false},
	}
	var id uint32
	for _, w := range writes {
		s.WriteFrame(now, makeChunkN(id), w.fd)
		id++
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if sym.Kind != wire.Systematic || w.drop {
				continue // drop the marked frame AND all repair, so the hole stays a hole
			}
			r.FeedSymbol(now, d)
		}
		now = now.Add(testTick)
	}
	// Tick at fine granularity past f1's deadline: the cursor skips ONLY f1 (the
	// following frames' own deadlines are ~1 ms later each, so they deliver in time)
	// and the resolutions land — the realistic skip-one-hole shape, unlike a coarse
	// clock jump that would expire the whole tail as late.
	for end := now.Add(cfg.BufferMicros + 10_000); now.Before(end); now = now.Add(500) {
		r.Tick(now)
	}
	r.Tick(now.Add(feedbackIntervalMicros + 1))

	var fb wire.Feedback
	seen := false
	for {
		d, ok := r.PollSend()
		if !ok {
			break
		}
		if got, err := wire.DecodeFeedback(d); err == nil {
			fb, seen = got, true
		}
	}
	if !seen {
		t.Fatal("no feedback emitted")
	}
	if fb.NewestDecodableLTR != 1 { // FrameStart 0 + 1
		t.Fatalf("NewestDecodableLTR = %d, want 1", fb.NewestDecodableLTR)
	}
	// Exactly one broken anchor: the arrived P (f3) whose reference died. The arrived
	// disposable leaf (f2) must NOT count — its loss breaks no chain.
	if fb.BrokenAnchors != 1 {
		t.Fatalf("BrokenAnchors = %d, want 1 (leaf must not count)", fb.BrokenAnchors)
	}
}
