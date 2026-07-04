package flow

// LTR-resync experiment (Phase 1 oracle; pre-registered in scratchpad/ltr-resync/PREREG.md).
//
// The burst-frontier failure mode is dependency-island death (rap_anchor /
// first_base_chain attribution in the burst48 glassbench runs), not repair scarcity
// (the repair-ceiling arm did not close the gap). The candidate lever is the
// WebRTC-style long-term-reference resync: the encoder keeps an ACK-GATED LTR slot
// (the newest LTR-candidate frame the receiver has CONFIRMED decodable), and when
// feedback says the live reference chain is broken, the next frame is coded against
// that slot — resurrecting the stream one detection-lag after the burst at
// recovery-P cost, instead of waiting for the next scheduled IDR.
//
// This file scores that policy against the status quo through the REAL sliding
// transport (the meld-auto profile) over a seeded Gilbert-Elliott burst channel with
// real propagation delay, wire serialization, and delivery jitter (the simLink
// physics the "sim lies" post-mortem demands for any latency-dependent claim). The
// closed loop is modeled at the test level: unit-resolution events reach the
// synthetic encoder owd + one feedback interval after the receiver's in-order
// delivery stream resolves them — the lag the Phase-2 wire field
// (Feedback.NewestDecodableLTR) would have. Decodability is scored by the
// WP6-validated shape.Decodable closure oracle over the units actually delivered
// whole — the same arbiter model glassbench uses (proven ffprobe-exact in WP6) —
// cross-checked against an online closure walk.
//
// Three arms, identical transport, identical seeded channel; only the source
// dependency structure the closed-loop encoder emits differs:
//   - sched-idr:  damage triggers nothing; wait for the next scheduled IDR.
//   - ltr:        on damage, next frame = recovery-P referencing the ack-safe LTR
//                 (size = recoveryFactor × a normal P).
//   - force-idr:  on damage, next frame = SPS+IDR and the GOP schedule restarts
//                 (the PLI-style alternative).

import (
	"math"
	"os"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// resyncPolicy selects the closed-loop encoder's reaction to reference-chain damage.
type resyncPolicy int

const (
	policySchedIDR resyncPolicy = iota // status quo: wait for the scheduled IDR
	policyLTR                          // recovery-P against the ack-gated LTR slot
	policyForceIDR                     // on-demand SPS+IDR (PLI-style)
)

func (p resyncPolicy) String() string {
	switch p {
	case policyLTR:
		return "ltr"
	case policyForceIDR:
		return "force-idr"
	default:
		return "sched-idr"
	}
}

// Frame sizes in chunks (SymbolSize-sized coded symbols). Calibrated to the
// bbb_bframes shape: ~2 chunks per frame on average, IDR ≈ several × P ≈ several
// × leaf. ltrCandEvery marks every Nth base anchor an LTR candidate (plus every
// IDR) — a real encoder marks specific frames LTR rather than making every
// reference retainable, and the ack-gated slot only ever advances to a candidate.
const (
	ltrChunksSPS  = 1
	ltrChunksIDR  = 8
	ltrChunksP    = 3
	ltrChunksLeaf = 1
	ltrAnchorGap  = 4 // a base-P anchor every 4th frame; leaves in between
	ltrCandEvery  = 2 // every 2nd anchor (every 8th frame) is an LTR candidate
)

// ltrUnit is one emitted access unit: the shape.Unit the oracle scores plus its
// chunk placement and its role flags.
type ltrUnit struct {
	u       shape.Unit
	startID uint32 // first source chunk id
	chunks  int
	anchor  bool // a unit whose loss breaks the chain (SPS / IDR / base-P / recovery-P)
	ltrCand bool // eligible to become the ack-gated LTR slot
}

// ltrEncoder is the deterministic closed-loop synthetic encoder. It emits the GOP
// template (SPS+IDR, base-P anchors, disposable leaves) and reacts to resolution
// events (delivered-decodable / broken) that the harness delivers after the modeled
// feedback lag. Policy state:
//   - slotAcked: newest LTR-candidate unit CONFIRMED decodable — the ack-gated LTR
//     slot. Ack-driven refresh is the load-bearing rule: during a burst acks stop,
//     so the slot retains the last pre-burst candidate instead of being overwritten
//     by dead in-burst anchors.
//   - damagePending: an anchor at/after the last resync point resolved broken and
//     no resync has been emitted since (a broken disposable leaf is local damage —
//     the chain is intact — and never triggers).
type ltrEncoder struct {
	policy         resyncPolicy
	keyint         int     // frames per scheduled GOP
	recoveryFactor float64 // recovery-P size multiplier over a normal P
	// recoveryClass is the protection tier recovery-P frames are written at.
	// ClassBase = flat (a normal anchor); ClassRAP = boosted — a recovery point is
	// RAP-like in leverage (small, and everything after hangs off it), so the UEP
	// machinery can protect it harder for a few extra repair symbols.
	recoveryClass shape.PriorityClass

	// holdFrames is the resync hold-down in frame slots: after emitting a resync,
	// no further resync until this many slots pass — the sender cannot KNOW the
	// resync's own fate sooner than one detection lag, so firing faster is blind
	// re-spending (the same rule the transport's reactive tier applies via its
	// convergence gate / in-flight discounting). It also gates worth-it: damage is
	// answered only when the next scheduled IDR is farther away than a hold window
	// (otherwise the IDR arrives before or barely after the recovery could, and
	// waiting is free). 0 ⇒ undamped (fire on every damage verdict).
	holdFrames int

	frameIdx    int
	anchorCount int
	nextUnitID  uint32
	nextChunkID uint32
	spsUnit     int64
	prevAnchor  int64
	units       []ltrUnit

	slotAcked     int64
	damagePending bool
	lastResync    int64
	sinceResync   int

	recoveries int
	forcedIDRs int
}

func newLTREncoder(policy resyncPolicy, keyint int, factor float64, holdFrames int) *ltrEncoder {
	return &ltrEncoder{policy: policy, keyint: keyint, recoveryFactor: factor, holdFrames: holdFrames,
		recoveryClass: shape.ClassBase,
		spsUnit:       -1, prevAnchor: -1, slotAcked: -1, lastResync: -1, sinceResync: 1 << 30}
}

// resyncAllowed applies the hold-down and the worth-it gate to a pending damage verdict.
func (e *ltrEncoder) resyncAllowed() bool {
	if e.holdFrames <= 0 {
		return true
	}
	if e.sinceResync < e.holdFrames {
		return false // the previous resync's fate is not yet knowable
	}
	toIDR := e.keyint - e.frameIdx%e.keyint
	return toIDR > e.holdFrames // the scheduled IDR would resync sooner: wait for it
}

// onUnitResolved consumes one receiver-side resolution event (already delayed by
// the harness to model the feedback path).
func (e *ltrEncoder) onUnitResolved(unitID uint32, decodable bool) {
	un := e.units[unitID]
	if decodable {
		if un.ltrCand && int64(unitID) > e.slotAcked {
			e.slotAcked = int64(unitID)
		}
		return
	}
	if un.anchor && int64(unitID) >= e.lastResync {
		e.damagePending = true
	}
}

// chunkWrite is one WriteFrame call the harness performs.
type chunkWrite struct {
	fd FrameDesc
	id uint32
}

func (e *ltrEncoder) addUnit(u shape.Unit, chunks int, anchor, ltrCand bool) []chunkWrite {
	u.ID = e.nextUnitID
	e.nextUnitID++
	lu := ltrUnit{u: u, startID: e.nextChunkID, chunks: chunks, anchor: anchor, ltrCand: ltrCand}
	e.units = append(e.units, lu)
	fd := FrameDesc{
		Priority:    u.Class.Wire(),
		FrameID:     u.ID,
		RefFrameIDs: u.RefersTo,
		Chunks:      uint16(chunks),
		TemporalID:  u.TemporalID,
		RAP:         u.RAP,
		Discardable: u.Discardable,
		NonPicture:  !u.Picture,
		LTR:         ltrCand, // candidates ride the wire so the Phase-2 loop can name them
	}
	out := make([]chunkWrite, chunks)
	for c := 0; c < chunks; c++ {
		out[c] = chunkWrite{fd: fd, id: e.nextChunkID}
		e.nextChunkID++
	}
	return out
}

func (e *ltrEncoder) emitSPSIDR() []chunkWrite {
	sps := e.addUnit(shape.Unit{Class: shape.ClassParamSet, Confidence: shape.Signaled}, ltrChunksSPS, true, false)
	e.spsUnit = int64(e.units[len(e.units)-1].u.ID)
	idr := e.addUnit(shape.Unit{Class: shape.ClassRAP, RAP: true, Picture: true,
		Confidence: shape.Signaled, RefersTo: []uint32{uint32(e.spsUnit)}}, ltrChunksIDR, true, true)
	idrID := e.units[len(e.units)-1].u.ID
	e.prevAnchor = int64(idrID)
	e.lastResync = int64(idrID)
	e.damagePending = false // an IDR is itself a full resync
	return append(sps, idr...)
}

// emitFrame produces the next frame slot's chunks. A pending damage verdict
// replaces the slot's planned frame with the policy's resync frame. The scheduled
// IDR cadence is kept identical across the sched-idr and ltr arms (resync only
// ADDS recovery between IDRs); a forced IDR restarts the GOP schedule, the real
// PLI semantics.
func (e *ltrEncoder) emitFrame() []chunkWrite {
	e.sinceResync++
	if e.frameIdx%e.keyint == 0 {
		out := e.emitSPSIDR()
		e.frameIdx++
		return out
	}
	if e.damagePending && e.resyncAllowed() {
		switch e.policy {
		case policyLTR:
			if e.slotAcked >= 0 {
				chunks := int(math.Ceil(e.recoveryFactor * ltrChunksP))
				out := e.addUnit(shape.Unit{Class: e.recoveryClass, Picture: true,
					Confidence: shape.Signaled, RefersTo: []uint32{uint32(e.slotAcked)}}, chunks, true, true)
				id := e.units[len(e.units)-1].u.ID
				e.prevAnchor = int64(id)
				e.lastResync = int64(id)
				e.damagePending = false
				e.recoveries++
				e.sinceResync = 0
				e.frameIdx++
				return out
			}
			// No ack-safe LTR yet: emit the template; the pending verdict retries
			// at the next frame slot.
		case policyForceIDR:
			e.forcedIDRs++
			e.sinceResync = 0
			out := e.emitSPSIDR()
			e.frameIdx = 1 // the GOP restarts at this display slot
			return out
		}
	}
	var out []chunkWrite
	if e.frameIdx%ltrAnchorGap == 0 {
		e.anchorCount++
		cand := e.anchorCount%ltrCandEvery == 0
		out = e.addUnit(shape.Unit{Class: shape.ClassBase, Picture: true,
			Confidence: shape.Signaled, RefersTo: []uint32{uint32(e.prevAnchor)}}, ltrChunksP, true, cand)
		e.prevAnchor = int64(e.units[len(e.units)-1].u.ID)
	} else {
		out = e.addUnit(shape.Unit{Class: shape.ClassDisposable, Picture: true, TemporalID: 2,
			Discardable: true, Confidence: shape.Signaled,
			RefersTo: []uint32{uint32(e.prevAnchor)}}, ltrChunksLeaf, false, false)
	}
	e.frameIdx++
	return out
}

// ltrResult is one arm-run's outcome.
type ltrResult struct {
	pics, decPics int
	keys, decKeys int
	srcChunks     uint64
	repair        uint64
	recoveries    int
	forcedIDRs    int
	maxDeadRun    int
	ordered       bool
	corrupt       bool
	rstats        ReceiverStats
}

func (r ltrResult) picRate() float64 {
	if r.pics == 0 {
		return 0
	}
	return float64(r.decPics) / float64(r.pics)
}

// verdictState resolves units from the receiver's strictly-in-order delivery
// stream: a gap between consecutive delivered ids is a loss verdict for every id
// inside it (in-order delivery makes the gap authoritative). A unit resolves when
// all its chunk ids have verdicts; it is decodable iff delivered whole and its
// references (earlier units, already resolved) are decodable — the same closure
// walk shape.Decodable performs, computed online so resolution EVENTS can drive
// the encoder loop at the modeled feedback lag.
type verdictState struct {
	units      []ltrUnit
	unitOfID   []uint32 // chunk id -> unit id
	remaining  []int
	broken     []bool
	resolved   []bool
	decodable  []bool
	nextID     uint32
	nextUnit   uint32
	onResolved func(unitID uint32, decodable bool)
}

func (v *verdictState) syncUnits(units []ltrUnit) {
	for len(v.remaining) < len(units) {
		u := units[len(v.remaining)]
		for c := 0; c < u.chunks; c++ {
			v.unitOfID = append(v.unitOfID, u.u.ID)
		}
		v.remaining = append(v.remaining, u.chunks)
		v.broken = append(v.broken, false)
		v.resolved = append(v.resolved, false)
		v.decodable = append(v.decodable, false)
	}
	v.units = units
}

func (v *verdictState) verdict(id uint32, delivered bool) {
	ui := v.unitOfID[id]
	if !delivered {
		v.broken[ui] = true
	}
	v.remaining[ui]--
	// Units resolve in id order, so a reference always resolves before its dependents.
	for int(v.nextUnit) < len(v.remaining) && v.remaining[v.nextUnit] == 0 {
		ui := v.nextUnit
		v.nextUnit++
		v.resolved[ui] = true
		dec := !v.broken[ui]
		for _, ref := range v.units[ui].u.RefersTo {
			if !dec {
				break
			}
			dec = v.resolved[ref] && v.decodable[ref]
		}
		v.decodable[ui] = dec
		if v.onResolved != nil {
			v.onResolved(ui, dec)
		}
	}
}

// advanceTo issues loss verdicts for every id below hi, then a delivery verdict
// for hi itself — the receiver's in-order cursor semantics.
func (v *verdictState) advanceTo(hi uint32) {
	for v.nextID < hi {
		v.verdict(v.nextID, false)
		v.nextID++
	}
	if v.nextID == hi {
		v.verdict(hi, true)
		v.nextID = hi + 1
	}
}

type resolveEvent struct {
	at     clock.Timestamp
	unitID uint32
	dec    bool
}

// ltrLink drives one closed-loop arm through the real sliding sender/receiver over
// a lossy, delayed, serialized, jittered channel on a manual clock.
type ltrLink struct {
	cfg          Config
	policy       resyncPolicy
	keyint       int
	factor       float64
	frames       int
	frameMicros  int64
	owdMicros    int64
	drop         func(wire.Symbol) bool
	paceBytesSec int64
	jitterSeed   uint64
	jitterMicros int64
}

// run executes one arm and returns its outcome.
func (l ltrLink) run(t *testing.T) ltrResult {
	t.Helper()
	// Resync hold-down = one detection lag in frame slots: a resync's own fate
	// cannot be known sooner than deadline-expiry + transit + a feedback interval.
	hold := int((l.cfg.BufferMicros+l.owdMicros+feedbackIntervalMicros)/l.frameMicros) + 1
	enc := newLTREncoder(l.policy, l.keyint, l.factor, hold)
	s := NewSlidingSender(l.cfg)
	r := NewSlidingReceiver(l.cfg)
	res := ltrResult{ordered: true}

	var vs verdictState
	var events []resolveEvent
	now := clock.Timestamp(0)
	vs.onResolved = func(unitID uint32, dec bool) {
		// The verdict reaches the encoder after the modeled feedback path: one-way
		// propagation plus a feedback interval (the Phase-2 wire field's lag).
		events = append(events, resolveEvent{at: now.Add(l.owdMicros + feedbackIntervalMicros), unitID: unitID, dec: dec})
	}

	var s2r, r2s []inflight
	var wireFreeAt clock.Timestamp
	nextFrame := clock.Timestamp(0)
	framesEmitted := 0
	var lastDelivered int64 = -1
	const step = int64(1_000)

	deliverEvents := func() {
		keep := events[:0]
		for _, ev := range events {
			if ev.at.After(now) {
				keep = append(keep, ev)
				continue
			}
			enc.onUnitResolved(ev.unitID, ev.dec)
		}
		events = keep
	}
	pumpSender := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || l.drop(sym) {
				continue
			}
			extra := int64(0)
			if l.jitterMicros > 0 {
				extra = int64(coinU(l.jitterSeed, uint32(sym.Kind), sym.SrcIndex, sym.WindowBase, uint32(sym.RepairKey)) * float64(l.jitterMicros))
			}
			dep := now
			if l.paceBytesSec > 0 {
				if wireFreeAt.After(dep) {
					dep = wireFreeAt
				}
				wireFreeAt = dep.Add(int64(len(d)) * 1_000_000 / l.paceBytesSec)
			}
			s2r = append(s2r, inflight{dep.Add(l.owdMicros + extra), d})
		}
	}
	pumpReceiver := func() {
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			r2s = append(r2s, inflight{now.Add(l.owdMicros), fb})
		}
		for {
			id, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			if int64(id) <= lastDelivered {
				res.ordered = false
			}
			lastDelivered = int64(id)
			if chunkID(d) != id {
				res.corrupt = true
			}
			vs.advanceTo(id)
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

	for framesEmitted < l.frames {
		deliverEvents()
		for framesEmitted < l.frames && !nextFrame.After(now) {
			for _, cw := range enc.emitFrame() {
				vs.syncUnits(enc.units)
				s.WriteFrame(now, makeChunkN(cw.id), cw.fd)
			}
			framesEmitted++
			nextFrame = nextFrame.Add(l.frameMicros)
		}
		tickAll()
		now = now.Add(step)
	}
	// Tail: flush, run the deadlines out, then force the final verdicts.
	s.Flush(now)
	pumpSender()
	drainUntil := now.Add(l.cfg.BufferMicros + 8*l.owdMicros)
	for now.Before(drainUntil) {
		now = now.Add(step)
		tickAll()
	}
	now = now.Add(4 * l.cfg.BufferMicros)
	r.Tick(now)
	pumpReceiver()
	for vs.nextID < enc.nextChunkID {
		vs.verdict(vs.nextID, false)
		vs.nextID++
	}

	// Score with the WP6 oracle (the arbiter), cross-checked against the online walk.
	units := make([]shape.Unit, len(enc.units))
	deliveredWhole := make(map[uint32]bool, len(enc.units))
	for i, u := range enc.units {
		units[i] = u.u
		deliveredWhole[u.u.ID] = vs.resolved[i] && !vs.broken[i]
	}
	dec := shape.Decodable(units, deliveredWhole)
	deadRun := 0
	for i, u := range enc.units {
		if !u.u.Picture {
			continue
		}
		if dec[u.u.ID] != vs.decodable[i] {
			t.Fatalf("oracle/online decodability disagree at unit %d: oracle=%v online=%v",
				u.u.ID, dec[u.u.ID], vs.decodable[i])
		}
		res.pics++
		if dec[u.u.ID] {
			res.decPics++
			deadRun = 0
		} else {
			deadRun++
			if deadRun > res.maxDeadRun {
				res.maxDeadRun = deadRun
			}
		}
		if u.u.RAP {
			res.keys++
			if dec[u.u.ID] {
				res.decKeys++
			}
		}
	}
	st := s.Stats()
	res.srcChunks, res.repair = st.Source, st.Repair
	res.recoveries, res.forcedIDRs = enc.recoveries, enc.forcedIDRs
	res.rstats = r.Stats()
	return res
}

func ltrExperimentConfig(budgetMicros int64) Config {
	return Config{
		Flow: 1, SymbolSize: 64, GenSize: 16,
		Redundancy: 0.15, BufferMicros: budgetMicros,
		Sliding: true, ProtectedRepairPhasing: true, AutoReorderHoldoff: true,
	}
}

// TestLTRResyncExperiment is the Phase-1 pre-registered experiment (see
// scratchpad/ltr-resync/PREREG.md). It is a diagnostic sweep, env-gated like the
// other heavy experiments (MELD_LATENCY, AUTORED_SWEEP); it asserts the safety
// invariants plus clean-cell dormancy and logs the decision table. Run:
//
//	MELD_LTR_EXP=1 go test -run TestLTRResyncExperiment -v -timeout 900s ./internal/flow
func TestLTRResyncExperiment(t *testing.T) {
	if os.Getenv("MELD_LTR_EXP") == "" {
		t.Skip("experiment sweep; set MELD_LTR_EXP=1 to run")
	}
	const (
		owd      = 50_000 // 100 ms RTT
		frameCad = 40_000 // 25 fps
		frames   = 900    // 36 s
		// Wire serialization: real physics (a frame's chunk burst plus its repair
		// occupies the wire and delays what queues behind it), scaled to the real
		// deployment's headroom — the bench relay runs on loopback under a 100 Mbps
		// host ceiling with a ≤50 Mbps source, so even an IDR+repair burst
		// serializes in ~1 ms, a bubble far inside the budget. 400 KB/s here keeps
		// the worst saturated-sizer burst (~5 KB) to a ~13 ms bubble — visible,
		// never fatal. The wire-tight cell below shrinks this to 60 KB/s to
		// document the OTHER regime: a constrained last mile where a recovery
		// burst's serialization bubble itself blows the deadline.
		paceBps      = 400_000
		paceTightBps = 60_000
		jitterUs     = 3_000
		seedsMoney   = 8
	)
	type cell struct {
		name    string
		loss    float64
		burst   float64
		keyint  int
		budget  int64
		factor  float64
		seeds   int
		iidDrop bool
		pace    int64
		jitter  int64
	}
	cells := []cell{
		// Money cells (decision rules 1-3).
		{"money-k12", 0.10, 48, 12, 100_000, 2, seedsMoney, false, paceBps, jitterUs},
		{"money-k48", 0.10, 48, 48, 100_000, 2, seedsMoney, false, paceBps, jitterUs},
		// Loss / budget / factor / wire robustness.
		{"loss5-k48", 0.05, 48, 48, 100_000, 2, 4, false, paceBps, jitterUs},
		{"budget1.5-k48", 0.10, 48, 48, 150_000, 2, 4, false, paceBps, jitterUs},
		{"f1.5-k48", 0.10, 48, 48, 100_000, 1.5, 4, false, paceBps, jitterUs},
		{"f3-k48", 0.10, 48, 48, 100_000, 3, 4, false, paceBps, jitterUs},
		{"f5-k48", 0.10, 48, 48, 100_000, 5, 4, false, paceBps, jitterUs},
		{"wire-tight-k48", 0.10, 48, 48, 100_000, 2, 4, false, paceTightBps, jitterUs},
		// Guard cells (decision rule 4). guard-clean (no loss, NO jitter): nothing
		// can break, so the mechanism must be perfectly dormant and the arms
		// byte-identical. guard-clean-jitter (no loss, jitter): the sliding
		// receiver's known premature-drop residual self-inflicts a few chain breaks
		// (reorder-skewed deadline extrapolation skips ids that are merely late) —
		// the resync may fire on that REAL damage and must help, never hurt.
		// guard-iid: iid loss must not trigger destructively.
		{"guard-iid10-k48", 0.10, 1, 48, 100_000, 2, 4, true, paceBps, jitterUs},
		{"guard-clean-k48", 0, 1, 48, 100_000, 2, 2, false, paceBps, 0},
		{"guard-clean-jitter-k48", 0, 1, 48, 100_000, 2, 2, false, paceBps, jitterUs},
	}
	arms := []resyncPolicy{policySchedIDR, policyLTR, policyForceIDR}
	for _, c := range cells {
		t.Logf("=== cell %s: loss=%.0f%% burst=%.0f keyint=%d budget=%dms f=%.1f ===",
			c.name, c.loss*100, c.burst, c.keyint, c.budget/1000, c.factor)
		type agg struct {
			rates     []float64
			keyRates  []float64
			src       uint64
			rep       uint64
			rec, fidr int
			maxDead   int
		}
		sums := map[resyncPolicy]*agg{}
		for _, p := range arms {
			sums[p] = &agg{}
		}
		for seed := 0; seed < c.seeds; seed++ {
			for _, p := range arms {
				var drop func(wire.Symbol) bool
				switch {
				case c.loss == 0:
					drop = func(wire.Symbol) bool { return false }
				case c.iidDrop:
					drop = uniformDrop(uint64(seed)*0x9E3779B1+0xABCD, c.loss)
				default:
					drop = geDrop(int64(seed)*7919+101, c.loss, c.burst)
				}
				l := ltrLink{
					cfg: ltrExperimentConfig(c.budget), policy: p, keyint: c.keyint,
					factor: c.factor, frames: frames, frameMicros: frameCad,
					owdMicros: owd, drop: drop, paceBytesSec: c.pace,
					jitterSeed: uint64(seed)*13 + 7, jitterMicros: c.jitter,
				}
				r := l.run(t)
				if !r.ordered {
					t.Fatalf("%s/%s seed %d: delivery not strictly increasing", c.name, p, seed)
				}
				if r.corrupt {
					t.Fatalf("%s/%s seed %d: corrupt delivery", c.name, p, seed)
				}
				// Dormancy is asserted only on the jitter-free clean cell: with
				// jitter the transport's own premature-drop residual breaks real
				// chains, and reacting to real damage is correct (asserted
				// separately as help-not-hurt below).
				if c.loss == 0 && c.jitter == 0 && (r.recoveries != 0 || r.forcedIDRs != 0) {
					t.Fatalf("%s/%s seed %d: resync fired on a clean link (recoveries=%d forced=%d recv=%+v)",
						c.name, p, seed, r.recoveries, r.forcedIDRs, r.rstats)
				}
				a := sums[p]
				a.rates = append(a.rates, r.picRate())
				if r.keys > 0 {
					a.keyRates = append(a.keyRates, float64(r.decKeys)/float64(r.keys))
				}
				a.src += r.srcChunks
				a.rep += r.repair
				a.rec += r.recoveries
				a.fidr += r.forcedIDRs
				if r.maxDeadRun > a.maxDead {
					a.maxDead = r.maxDeadRun
				}
			}
		}
		if c.loss == 0 && c.jitter == 0 {
			if sums[policyLTR].src != sums[policySchedIDR].src {
				t.Fatalf("%s: clean-link arms not identical: ltr src=%d base src=%d",
					c.name, sums[policyLTR].src, sums[policySchedIDR].src)
			}
		}
		if c.loss == 0 && c.jitter != 0 {
			// Help-not-hurt: resync answering the transport's own reorder residual
			// must not lose decodable pictures vs doing nothing.
			for i := range sums[policySchedIDR].rates {
				if sums[policyLTR].rates[i] < sums[policySchedIDR].rates[i]-0.01 {
					t.Fatalf("%s seed %d: ltr resync HURT a clean jittered link: %.3f < %.3f",
						c.name, i, sums[policyLTR].rates[i], sums[policySchedIDR].rates[i])
				}
			}
		}
		for _, p := range arms {
			a := sums[p]
			mean, lo, hi := meanMinMax(a.rates)
			kMean, _, _ := meanMinMax(a.keyRates)
			t.Logf("  %-9s picRate mean=%.3f [%.3f..%.3f] keyRate=%.3f src=%d rep=%d resyncs=%d/%d maxDead=%d",
				p, mean, lo, hi, kMean, a.src, a.rep, a.rec, a.fidr, a.maxDead)
		}
		// Paired per-seed deltas for the money read.
		base, ltr := sums[policySchedIDR], sums[policyLTR]
		wins := 0
		var deltaSum float64
		for i := range base.rates {
			d := ltr.rates[i] - base.rates[i]
			deltaSum += d
			if d > 0 {
				wins++
			}
		}
		_, bLo, bHi := meanMinMax(base.rates)
		t.Logf("  ltr-vs-base: wins %d/%d, mean delta %+.3f (base A/A spread %.3f); src bytes %+.1f%%",
			wins, len(base.rates), deltaSum/float64(len(base.rates)), bHi-bLo,
			100*(float64(ltr.src)-float64(base.src))/float64(base.src))
	}
}

func meanMinMax(xs []float64) (mean, lo, hi float64) {
	if len(xs) == 0 {
		return 0, 0, 0
	}
	lo, hi = xs[0], xs[0]
	for _, x := range xs {
		mean += x
		if x < lo {
			lo = x
		}
		if x > hi {
			hi = x
		}
	}
	return mean / float64(len(xs)), lo, hi
}
