package flow

import (
	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// SenderStats counts what the Sender has emitted.
type SenderStats struct {
	Source         uint64 // systematic symbols emitted
	Repair         uint64 // repair symbols emitted (fixed proactive)
	ReactiveRepair uint64 // repair symbols emitted in response to a feedback deficit
	Throttled      uint64 // repair symbols dropped by the rate ceiling (N1 token bucket)
}

// retGen is a closed generation the sender retains so it can answer a feedback
// rank deficit with more repair (WP3). It holds the generation's source symbols
// (in its own encoder, whose base is the generation base), its deadline, and the
// next repair key / last reactive-send time for pacing.
type retGen struct {
	enc         *code.Encoder
	n           int
	proactive   int // proactive repair symbols emitted at close (for the per-gen loss estimate)
	deadline    clock.Timestamp
	nextKey     uint16          // next repair key (fixed repair used 0..R-1)
	closeAt     clock.Timestamp // when the generation closed (RTT estimate)
	lastRx      clock.Timestamp // last reactive-repair send (pacing floor)
	inflight    int             // reactive repair emitted but not yet reflectable in the feedback deficit
	inflightAt  clock.Timestamp // when the in-flight reactive repair was last sent
	lastDeficit int             // the deficit the previous feedback reported (to credit demonstrably-landed repair)
	pri         uint8           // the generation's protection tier (max over its units; for repair stamping)
}

// Sender is the coded transmit half of a flow. It emits systematic symbols as
// media arrives, fixed proactive repair when a generation closes, and — driven by
// receiver feedback — extra reactive repair for any generation that still has a
// rank deficit, until it decodes or its deadline passes. Deterministic; not safe
// for concurrent use.
type Sender struct {
	cfg         Config
	pool        *code.Pool            // recycles symbol payload buffers across generation encoders
	live        *code.Encoder         // the generation currently filling
	genDL       clock.Timestamp       // current generation's deadline
	inGen       int                   // source symbols in the current generation
	genMaxPri   uint8                 // highest protection tier written into the live generation (UEP; WP6)
	retained    map[uint32]*retGen    // closed generations awaiting ack/deadline, by base
	rttMicros   int64                 // EWMA RTT estimate (microseconds)
	pEst        float64               // estimated channel erasure rate (from feedback)
	burstQ8     int                   // estimated mean loss-run length, Q8 (from feedback; 256 = i.i.d.)
	cleanRun    int                   // consecutive feedback reports observing zero loss (floor-decay confidence)
	lastWrite   clock.Timestamp       // time of the last Write (for the idle flush)
	genOpenTime clock.Timestamp       // when the live generation opened (AutoGenSize fill-rate measurement)
	interMicros int64                 // EWMA of the per-symbol fill time, µs (AutoGenSize fill gate)
	curGenWidth int                   // width fixed when the live generation opened (stamped on every symbol)
	rttSampled  bool                  // a real RTT sample has arrived (AutoGenSize stays narrow until then)
	now         clock.Timestamp       // most recent entry-point time (for the rate ceiling)
	bucket      tokenBucket           // aggregate emit-rate ceiling (N1), driven by cc when present
	cc          *congestionController // delay-based budget (N3); nil ⇒ static MaxBitrate ceiling
	sched       *pathScheduler        // N-path placement (N5); nil ⇒ single path
	pathLossPpm []int                 // per-path marginal erasure rates (ppm), from feedback (N5)
	slotDistPpm []int                 // per-slot erasure-count histogram (ppm), from feedback (N5)
	// Frame descriptor (WP6): the shaper's frame id → the source id of that frame's first
	// chunk, so the wire carries frame identity + dependency in source-id space.
	frameStart   map[uint32]uint32
	curFrameID   uint32
	haveCurFrame bool
	sendQ        [][]byte
	stats        SenderStats
}

// senderFrameWindow bounds the sender's frame-start map: references are recent (within a
// GOP), so a frame id this far below the current one is pruned. maxFrameRefs caps the
// per-frame dependency count carried on the wire (a B-frame needs 2).
const (
	senderFrameWindow = 512
	maxFrameRefs      = 15
)

// pruneFrameStarts drops far-back frame-start entries so the map stays bounded.
func (s *Sender) pruneFrameStarts(cur uint32) {
	if len(s.frameStart) <= 2*senderFrameWindow {
		return
	}
	for fid := range s.frameStart {
		if fid+senderFrameWindow < cur {
			delete(s.frameStart, fid)
		}
	}
}

// NewSender constructs a Sender for cfg.
func NewSender(cfg Config) *Sender {
	pool := code.NewPool(cfg.SymbolSize)
	s := &Sender{
		cfg:       cfg,
		pool:      pool,
		live:      code.NewEncoderAt(cfg.SymbolSize, 0),
		retained:  make(map[uint32]*retGen),
		rttMicros: defaultRTTMicros,
		burstQ8:   burstQ8One,
		bucket:    newTokenBucket(cfg.maxBitrate()),
	}
	s.live.SetPool(pool)
	if cfg.CongestionControl {
		s.cc = newCongestionController(0, cfg.SymbolSize+symHeaderBytes, cfg.maxBitrate())
	}
	if cfg.multipath() {
		s.sched = newPathScheduler(cfg.paths())
	}
	return s
}

// Write hands one source media chunk (<= SymbolSize bytes, zero-padded) to the flow at
// the BASE protection tier (uepCenterTier ⇒ the configured TargetFailure). It is the
// codec-agnostic path for callers with no media descriptor — sizing identical to the
// pre-UEP behavior. A media shaper uses WriteUnit to place a unit above (parameter
// sets, RAPs) or below (disposable leaves) the base tier.
func (s *Sender) Write(now clock.Timestamp, data []byte) {
	s.WriteUnit(now, data, uint8(uepCenterTier))
}

// FrameDesc is the access-unit descriptor a media shaper hands the flow so the RECEIVER
// can compute loss propagation parse-free (WP6): the protection tier plus the dependency
// the receiver needs to know which delivered frames are decodable. It is carried on the
// systematic symbols (wire.Symbol frame-descriptor extension); the core's own sizing acts
// only on Priority.
type FrameDesc struct {
	Priority    uint8    // protection tier (as WriteUnit)
	FrameID     uint32   // the access unit this chunk belongs to (the shaper's unit id)
	RefFrameIDs []uint32 // dependency access units (a B-frame's two anchors, a P-frame's one)
	Chunks      uint16   // the access unit's total chunk count (so the receiver knows its id range)
	RAP         bool     // random-access point (keyframe)
	Discardable bool     // nothing references this unit
}

// WriteUnit hands one source media chunk to the flow at time now carrying a protection
// tier (the priority byte the media shaper assigns, internal/shape): emits it as a
// systematic symbol stamped with that tier, tracks the generation's highest tier so the
// generation's proactive repair is sized to it (unequal protection — parameter sets and
// RAPs get a tighter decode-failure target than disposable leaves, WP6), and closes the
// generation (fixed repair) when it fills. Drain datagrams with PollSend.
func (s *Sender) WriteUnit(now clock.Timestamp, data []byte, priority uint8) {
	s.writeSystematic(now, data, priority, nil)
}

// WriteFrame is WriteUnit carrying the full access-unit descriptor (FrameDesc): the chunk
// is stamped with the frame id + dependency so the receiver tracks decodability. Use it
// for every chunk of an access unit (all share the same FrameDesc); the per-symbol sizing
// still keys only on Priority.
func (s *Sender) WriteFrame(now clock.Timestamp, data []byte, fd FrameDesc) {
	s.writeSystematic(now, data, fd.Priority, &fd)
}

func (s *Sender) writeSystematic(now clock.Timestamp, data []byte, priority uint8, fd *FrameDesc) {
	s.now, s.lastWrite = now, now
	if s.inGen == 0 {
		s.genOpenTime = now             // for the AutoGenSize fill-rate measurement (span ÷ symbols at close)
		s.curGenWidth = s.genWidthNow() // fix the width for this whole generation (consistent N stamp + close)
	}
	// Per-symbol deadline: each chunk is due BufferMicros after its OWN write, not the
	// generation's first write. A shared generation deadline expires all GenSize
	// symbols at once, so a cursor stalled on one unrecoverable symbol drops every
	// later (already-received) symbol in the generation with it — the receiver's
	// dominant loss amplifier at a budget below the RTT. Staggered per-symbol deadlines
	// let the receiver skip only the expired symbol and deliver the rest. genDL tracks
	// the generation's LATEST symbol — the horizon until which its repair can still
	// help — for the repair stamp and the reactive-repair cutoff.
	dl := now.Add(s.cfg.BufferMicros)
	s.genDL = dl
	if priority > s.genMaxPri {
		s.genMaxPri = priority // the generation is protected as hard as its most-critical unit
	}
	id := s.live.Add(data)
	src, _ := s.live.Source(id)
	sym := wire.Symbol{
		Flow:       s.cfg.Flow,
		Kind:       wire.Systematic,
		WindowBase: s.live.Base(),
		SrcIndex:   id,
		N:          uint16(s.curGenWidth),
		Priority:   priority,
		Deadline:   int64(dl),
		Payload:    src,
	}
	if fd != nil {
		// Translate the shaper's frame ids into SOURCE-id space: a frame's identity on the
		// wire is the source id of its first chunk (FrameStart), which also bounds its id
		// range so the receiver attributes recovered/lost ids to frames by position. Track
		// each frame's first source id; the reference's FrameStart is looked up the same way.
		if s.frameStart == nil {
			s.frameStart = make(map[uint32]uint32)
		}
		if !s.haveCurFrame || fd.FrameID != s.curFrameID {
			s.frameStart[fd.FrameID] = id // first chunk of a new frame
			s.curFrameID, s.haveCurFrame = fd.FrameID, true
			s.pruneFrameStarts(fd.FrameID)
		}
		sym.HasFrameDesc = true
		sym.FrameStart = s.frameStart[fd.FrameID]
		sym.FrameLen = fd.Chunks
		sym.FrameRAP, sym.FrameDiscardable = fd.RAP, fd.Discardable
		sym.FrameRefs = nil
		for _, ref := range fd.RefFrameIDs {
			if rs, ok := s.frameStart[ref]; ok {
				sym.FrameRefs = append(sym.FrameRefs, rs)
				if len(sym.FrameRefs) >= maxFrameRefs {
					break
				}
			}
		}
	}
	if s.sched != nil {
		// Round-robin across paths (PLAN §3.6); one systematic per source id in order,
		// so path(id) = id mod paths — the mapping the receiver mirrors for co-loss.
		sym.PathID = uint8(s.sched.systematicPath())
	}
	s.emit(sym)
	s.stats.Source++
	s.inGen++
	if s.inGen >= s.curGenWidth {
		s.closeGen(now)
	}
}

// Flush closes a partially filled generation (end of stream), emitting its fixed
// repair and retaining it for reactive repair. No-op when no generation is open.
func (s *Sender) Flush(now clock.Timestamp) {
	s.now = now
	if s.inGen > 0 {
		s.closeGen(now)
	}
}

// closeGen emits the current generation's fixed repair, retains it for reactive
// repair, and starts a fresh generation encoder at the next base.
func (s *Sender) closeGen(now clock.Timestamp) {
	n := s.live.Len()
	if n == 0 {
		return
	}
	if (s.cfg.AutoGenSize || s.cfg.RepairWithinBudget) && s.genOpenTime != 0 {
		// Measure the ACTUAL per-symbol fill time over this whole generation (wall-clock span ÷
		// symbols) — robust to bursty writes (a frame's chunks arrive together), where per-write gaps
		// would mislead the fill gate. EWMA weight 1/4 so it tracks a bitrate change within a few gens.
		if perSym := now.Sub(s.genOpenTime) / int64(n); perSym > 0 {
			if s.interMicros == 0 {
				s.interMicros = perSym
			} else {
				s.interMicros += (perSym - s.interMicros) / 4
			}
		}
	}
	base := s.live.Base()
	pri := s.genMaxPri
	var key uint16
	for r := s.repairCountFor(n); int(key) < r; key++ {
		s.emitRepair(s.live, key, n, pri, false)
	}
	s.retained[base] = &retGen{enc: s.live, n: n, proactive: int(key), deadline: s.genDL, nextKey: key, closeAt: now, pri: pri}
	s.live = code.NewEncoderAt(s.cfg.SymbolSize, base+uint32(n))
	s.live.SetPool(s.pool)
	s.inGen = 0
	s.genMaxPri = 0 // reset for the next generation
}

// FeedFeedback absorbs a receiver feedback report: it retires generations the
// receiver has decoded, refreshes the RTT estimate, and — if the blocking
// generation still has a rank deficit — sends more repair for it.
func (s *Sender) FeedFeedback(now clock.Timestamp, fb wire.Feedback) {
	if fb.Flow != s.cfg.Flow {
		return
	}
	s.now = now
	s.updateRTT(now, fb)
	if s.cc != nil {
		// The congestion controller owns the budget; the bucket enforces it (repair
		// throttled first, media preserved). Surfaced via RateBudgetBitsPerSec.
		if budget := s.cc.rateBudgetBytesPerSec(); budget > 0 {
			s.bucket.setRate(budget)
		}
	}
	s.pEst = float64(fb.LossRate) / 65535 // feed-forward channel erasure estimate
	// Floor-decay confidence: count consecutive feedbacks that POSITIVELY report a clean link, and
	// snap to zero on the first loss observation. Keyed on the report (fb.LossRate), never on the
	// mere absence of a signal — a black hole or warmup delivers no feedback at all, so cleanRun
	// stays 0 and the full onset floor is retained (the distinction the earlier pEst-keyed attempt
	// missed). The decay also requires a reactive backstop, applied in effectiveFloor.
	if fb.LossRate == 0 {
		if s.cleanRun < cleanFloorConfirm {
			s.cleanRun++
		}
	} else {
		s.cleanRun = 0
	}
	if s.burstQ8 = int(fb.Burstiness); s.burstQ8 < burstQ8One {
		s.burstQ8 = burstQ8One // mean loss-run length for the burst-aware sizer (N2)
	}
	if s.sched != nil && len(fb.PathLoss) > 0 && len(fb.SlotDist) == len(fb.PathLoss)+1 {
		// Per-path marginals weight the scheduler (toward the better deliverer); the per-slot
		// erasure-count histogram drives the joint-tail sizer (proactive repair).
		s.pathLossPpm = make([]int, len(fb.PathLoss))
		delivered := make([]int, len(fb.PathLoss))
		for i, m := range fb.PathLoss {
			s.pathLossPpm[i] = p65535ToPPM(m)
			delivered[i] = 1_000_000 - s.pathLossPpm[i]
		}
		s.slotDistPpm = make([]int, len(fb.SlotDist))
		for j, d := range fb.SlotDist {
			s.slotDistPpm[j] = p65535ToPPM(d)
		}
		s.sched.setQuality(delivered)
	}
	// Retire generations the receiver has fully decoded/delivered past.
	for base, g := range s.retained {
		if base+uint32(g.n) <= fb.DecodedLowEdge {
			g.enc.Release() // recycle the generation's source buffers
			delete(s.retained, base)
		}
	}
	// Reactive repair for EVERY deficient generation the feedback names, in parallel — so a
	// backlog recovers concurrently instead of one-at-a-time behind the delivery cursor (the
	// coded analog of an ARQ NACK covering all gaps). The deficits are positional against the
	// ACTUAL generation boundaries from the cursor (base += width), not a fixed stride, so the
	// two ends need not share a constant generation width — the sender walks its own retained
	// boundaries, which the receiver mirrors from the width stamped on every symbol.
	base, ok := s.genBaseContaining(fb.DecodedLowEdge)
	for i := 0; ok && i < len(fb.Deficits); i++ {
		g := s.retained[base]
		if g == nil {
			break // structural gap (an entirely-lost generation); matches the receiver's walk
		}
		if fb.Deficits[i] > 0 {
			s.reactiveRepair(now, g, int(fb.Deficits[i]))
		}
		base += uint32(g.n)
	}
}

// genBaseContaining returns the base of the retained generation whose id range covers id, and
// true — or false if no retained generation does (e.g. id is in the still-live generation). It
// replaces fixed-stride genBaseOf so the generation width need not be a shared constant: the
// sender walks its own (possibly varying) generation boundaries.
func (s *Sender) genBaseContaining(id uint32) (uint32, bool) {
	for base, g := range s.retained {
		if base <= id && id < base+uint32(g.n) {
			return base, true
		}
	}
	return 0, false
}

// genWidthNow returns the width to OPEN the next generation with — fixed for that whole
// generation and stamped on every one of its symbols, which the receiver follows (so the two ends
// never need a shared width). With AutoGenSize it is derived from the sender's own measurements;
// otherwise it is the static Config width (a fixed GenSize, or AdaptiveGenSize's hint-derived width).
func (s *Sender) genWidthNow() int {
	if s.cfg.AutoGenSize {
		return s.measuredGenWidth()
	}
	return s.cfg.genWidth()
}

// measuredGenWidth is the AutoGenSize width: the same budget/RTT ramp and fill-time cap as
// Config.genWidth, but driven by the sender's MEASURED RTT (rttMicros) and write cadence
// (interMicros) instead of the static NominalRTTMicros/NominalBitrateBps hints — so an operator
// sets nothing and the width tracks the path and the encoder, re-sizing if either drifts (a
// mid-stream bitrate change moves interMicros, the next generation re-sizes). It bootstraps to
// GenSize until BOTH a write-cadence sample and a real RTT sample exist, so the stream is born
// narrow-and-safe and only widens once it has measured the conditions that make widening safe.
func (s *Sender) measuredGenWidth() int {
	base := s.cfg.GenSize
	if base < 1 {
		base = 1
	}
	if base >= maxAdaptiveGenWidth || s.interMicros <= 0 || !s.rttSampled {
		return base // bootstrap / not yet measured ⇒ stay narrow
	}
	round := s.rttMicros + feedbackIntervalMicros
	headroom := s.cfg.BufferMicros - round
	if headroom <= 0 {
		return base // budget below a reactive round ⇒ a wide generation would lose more on a deadline miss
	}
	frac := float64(headroom) / float64(round)
	if frac > 1 {
		frac = 1
	}
	ceiling := maxAdaptiveGenWidth
	// Fill-time gate from the MEASURED cadence: keep generation fill (width × interMicros) within
	// adaptiveMaxFillMicros, so a slow-filling wide generation never adds head-of-line latency.
	if fillCap := int(adaptiveMaxFillMicros / s.interMicros); fillCap < ceiling {
		ceiling = fillCap
	}
	if ceiling <= base {
		return base
	}
	w := base + int(frac*float64(ceiling-base)+0.5)
	if w < base {
		w = base
	}
	if w > ceiling {
		w = ceiling
	}
	return w
}

// reactiveRepair tops up the repair for one deficient generation, persisting until the
// generation decodes (the deficit drops out of feedback) or its deadline passes. It sends only
// the SHORTFALL — the deficit minus the repair already in flight — so it does not re-send a
// full batch each round. All deficient generations are serviced in parallel.
func (s *Sender) reactiveRepair(now clock.Timestamp, g *retGen, deficit int) {
	if now.After(g.deadline) {
		return // too late to matter
	}
	// Cap total reactive repair per generation: once a generation has been served ~maxRepairFactor·n
	// and is STILL deficient, the channel is erasing faster than repair can fix within the budget, so
	// stop flooding it (its remaining holes are skipped at the deadline). Bounds the per-generation
	// repair keyspace and the work a persistently-unrecoverable generation can demand.
	if int(g.nextKey)-g.proactive >= maxRepairFactor*g.n {
		return
	}
	// Expire in-flight reactive repair presumed LOST: a batch sent longer ago than one reflection
	// latency (a round trip to arrive plus a feedback interval to be reported) that has not shown
	// up as a deficit drop is gone — drop it so a stuck generation is re-served.
	if g.inflightAt != 0 && now.Sub(g.inflightAt) >= s.rttMicros+feedbackIntervalMicros {
		g.inflight = 0
	}
	prev := g.lastDeficit
	g.lastDeficit = deficit
	// Convergence gate (the key to not over-sending, and RTT-estimate-free): a deficit that is
	// still DROPPING — or seen for the first time — is converging on its own and must NOT be
	// reacted to. The first feedback after a generation closes counts its still-in-flight
	// systematic symbols as losses (a wildly inflated deficit that shrinks as they land), and a
	// prior reactive batch landing also shows up as a drop; sizing to either floods the link —
	// the low-RTT overhead inversion. Only a STUCK deficit (no improvement since the last
	// feedback) is a genuine residual needing new repair. A drop IS repair / late systematic
	// landing, so credit it against the in-flight tally and wait one more feedback.
	if prev == 0 || deficit < prev {
		if landed := prev - deficit; landed > 0 {
			if g.inflight -= landed; g.inflight < 0 {
				g.inflight = 0
			}
		}
		return
	}
	// Per-generation loss estimate (lag-free): of the n + proactive symbols sent, being
	// `deficit` short of rank n means ≈ (proactive + deficit) were lost; over GF(256)
	// (negligible linear dependence) that fraction is an accurate erasure rate, and unlike the
	// global EWMA it has no warmup lag and captures a burst that hit only this generation.
	p := s.pEst
	if sent := g.n + g.proactive; sent > 0 {
		if pGen := float64(g.proactive+deficit) / float64(sent); pGen > p {
			p = pGen
		}
	}
	// Discount the repair already in flight by its expected arrivals (HARQ incremental
	// redundancy): top up only the residual the in-flight batch will not cover.
	effective := deficit - int(float64(g.inflight)*(1-p))
	if effective <= 0 {
		return // the in-flight batch should clear the deficit; wait for it to reflect
	}
	if g.lastRx != 0 && now.Sub(g.lastRx) < minReactiveIntervalMicros {
		return // pacing floor: do not emit on every feedback packet in a burst
	}
	g.lastRx = now
	// Size the reactive top-up to the generation's PROTECTION TIER, exactly as the proactive
	// set-point does (repairCountFor): a keyframe/parameter-set generation gets a tighter
	// decode-failure target (more repair per unit of deficit), a disposable leaf a looser one.
	// Without this the reactive tier was flat across tiers, diluting unequal protection — the
	// budget that should climb the dependency spine was spread evenly on every deficit.
	delta := targetFailureForPriority(g.pri, s.cfg.targetFailure())
	extra := symbolsForDeficit(effective, p, delta, maxRepairFactor)
	for i := 0; i < extra; i++ {
		s.emitRepair(g.enc, g.nextKey, g.n, g.pri, true)
		g.nextKey++
	}
	g.inflight += extra
	g.inflightAt = now
}

// repairCountFor returns the proactive repair count for a generation of n source
// symbols: the feed-forward set-point sized to the target decode-failure
// probability at the current estimated erasure rate, floored at the configured
// baseline redundancy (which covers the lag before the estimate catches a sudden
// loss onset).
func (s *Sender) repairCountFor(n int) int {
	// Unequal protection (WP6): the generation's decode-failure target is the configured
	// baseline tightened/loosened by its protection tier — parameter sets and RAPs get an
	// exponentially smaller δ (more repair), disposable leaves a larger one (less), so a
	// fixed budget is steered up the dependency spine.
	delta := targetFailureForPriority(s.genMaxPri, s.cfg.targetFailure())
	var r int
	if s.sched != nil {
		// Multipath: size the TOTAL repair against the JOINT erasure tail across all paths
		// (the generation is spread over them and decoded from the union, N5). The
		// per-slot erasure-count histogram embeds the cross-path correlation, so a
		// correlated channel provisions more than an i.i.d.-union sizer; at zero
		// correlation it reduces to the binomial.
		r = repairForJointTailN(n, s.slotDistribution(), delta, maxRepairFactor)
	} else {
		r = repairForTarget(n, s.pEst, delta, maxRepairFactor)
	}
	// Burst-aware set-point: size for the Gilbert-Elliott tail when the channel is
	// bursty, taking the larger so an i.i.d. channel is never under the base sizer and a
	// bursty one gets the concentration margin the memoryless tail misses (N2). At mean
	// burst 1 the GE tail ≈ the binomial, so this is a no-op. In multipath the GE term
	// keys on the worse path's marginal — per-path burst is orthogonal to the cross-path
	// correlation the joint-tail captures, so we provision for whichever tail is heavier.
	//
	// The burst MARGIN (the GE term above the i.i.d. set-point) provisions EVERY generation for
	// the worst-case concentration of a loss run. But when reactive repair can run several rounds
	// inside the deadline (RTT small relative to the budget), it cleans up those burst outliers
	// on-demand — cheaply, and only on the generations a burst actually hit — so proactive need
	// carry only a fraction of the margin. Carry the FULL margin when reactive cannot help (RTT
	// ≥ budget); shrink it toward the i.i.d. set-point as more reactive rounds fit. This is the
	// proactive analog of "common case eager, tail lazy": it cuts the bursty-LAN overhead that
	// blanket GE sizing spends on every generation while reactive sits idle.
	ge := repairForGE(n, s.burstMarginalPPM(), s.burstQ8, delta, maxRepairFactor)
	rounds := s.reactiveRounds()
	// The two margins above the mean expected loss are offloaded to the reactive tier
	// independently, because they are safe to offload under DIFFERENT conditions:
	//
	//   • BURST margin (GE term above the i.i.d. set-point): carried at a 1/(rounds+1) fraction
	//     whenever reactive can land — the long-standing default. Reactive cleans up the rare
	//     generations a burst actually hits; the proactive layer need not pay the worst-case
	//     concentration on EVERY generation.
	//   • VARIANCE margin (i.i.d. set-point above the mean): offloaded only with ProactiveDecay,
	//     and only on a MEMORYLESS channel (burst guard: roundsEff → 0 as the mean run length grows,
	//     because a concentrated run needs this margin proactively — reactive cannot recover it in
	//     time). Single-path only (multipath keeps the joint-tail set-point).
	//
	// Keeping them separate is what the cref bench forced: folding both into one discount made the
	// burst guard drop the burst-margin discount too, so a bursty channel paid MORE overhead than
	// the default — strictly worse. They must be discounted on their own clocks.
	burstMargin := ge - r // ge is max(binomial, GE); r is the i.i.d./joint set-point
	if burstMargin < 0 {
		burstMargin = 0
	}
	r += burstMargin / (rounds + 1)
	if s.cfg.ProactiveDecay && s.sched == nil {
		mean := meanRepairCount(n, s.pEst)
		roundsEff := rounds * burstQ8One / s.burstQ8 // 0 on a bursty channel ⇒ no variance shed
		varMargin := r - burstMargin/(rounds+1) - mean
		if varMargin > 0 && roundsEff > 0 {
			// Replace the full variance margin with its reactive-scaled fraction; the (already
			// discounted) burst margin and the mean are carried unchanged.
			r = mean + varMargin/(roundsEff+1) + burstMargin/(rounds+1)
		}
	}
	if floor := s.effectiveFloor(n); r < floor {
		r = floor
	}
	// Fix A (RepairWithinBudget, RFC 9265): never provision repair the rate budget cannot
	// afford on top of the media — total offered (media + repair) must stay within the
	// budget, or the host pacer queues the overage as delay on MEDIA and the tight deadline
	// evicts it (the budget-below-RTT collapse). Cap to the budget's repair headroom; this
	// sheds protection gracefully (graceful under-protection) rather than overflowing.
	if s.cfg.RepairWithinBudget {
		if lim := s.maxRepairWithinBudget(n); r > lim {
			r = lim
		}
	}
	return r
}

// maxRepairWithinBudget returns the largest proactive repair count for a generation of n
// source symbols that keeps the total emitted rate (media + repair) within the rate budget:
// repairBps = rateBps − mediaBps, repair-per-source = repairBps/mediaBps, scaled by n. mediaBps
// comes from the measured per-symbol cadence (interMicros). With no cadence/budget signal yet
// it imposes no extra cap (maxRepairFactor still bounds r); when the budget barely covers the
// media it sheds ALL proactive repair (media is never dropped — it takes the non-droppable
// path) — the graceful-degradation floor.
func (s *Sender) maxRepairWithinBudget(n int) int {
	rateBps := s.bucket.bytesPerSec * 8
	if rateBps <= 0 || s.interMicros <= 0 {
		return n * maxRepairFactor
	}
	mediaBps := int64(s.cfg.SymbolSize) * 8 * 1_000_000 / s.interMicros
	if mediaBps <= 0 {
		return n * maxRepairFactor
	}
	repairBps := rateBps - mediaBps
	if repairBps <= 0 {
		return 0
	}
	lim := int(int64(n) * repairBps / mediaBps)
	if lim < 0 {
		return 0
	}
	return lim
}

// effectiveFloor returns the proactive repair floor, decayed to zero ONLY when the link is both
// confirmed clean (cleanRun feedbacks in a row reporting no loss) and able to recover an onset
// reactively (reactiveRounds >= reactiveFloorSafe gives an onset generation >= 2 reactive top-up
// opportunities inside its deadline). On a confirmed-clean, reactive-capable link the static floor
// recovers nothing — it is pure overhead — and any onset is caught by the reactive tier with margin
// (its under-recovery probability is O(p^rounds), far below the floor's own decode-failure target);
// everywhere else (warmup, any loss, high RTT, a black hole that yields no feedback) the full floor
// is retained, so this can only remove waste, never protection that was load-bearing.
func (s *Sender) effectiveFloor(n int) int {
	if s.cleanRun >= cleanFloorConfirm && s.reactiveRounds() >= reactiveFloorSafe {
		return 0
	}
	return s.cfg.repairFloor(n)
}

// reactiveRounds estimates how many reactive-repair top-ups the reactive tier can land inside
// the deadline budget at the current RTT — each costs one round trip plus a feedback interval
// to observe. It is 0 when that cycle does not fit the budget (high RTT), so the proactive
// layer must carry the full burst margin itself; it grows as the RTT shrinks relative to the
// budget, letting reactive repair absorb the burst tail instead.
func (s *Sender) reactiveRounds() int {
	// One reactive cycle is a full round trip (the batch out, its effect back) plus a feedback
	// interval to observe. Use 2×rttMicros for the round trip: the RTT estimate (updateRTT)
	// tends to UNDER-count because HighestSeen is advanced by window-covering repair, so it
	// tracks closer to one-way than round-trip. Doubling keeps reactiveRounds CONSERVATIVE — it
	// must never over-credit reactive availability, or the burst margin is discounted on a link
	// where reactive cannot actually land in time (under-protection at high RTT).
	cycle := 2*s.rttMicros + feedbackIntervalMicros
	budget := s.cfg.BufferMicros
	if cycle <= 0 || budget <= 0 {
		return 0
	}
	return int(budget / cycle)
}

// burstMarginalPPM is the marginal erasure rate (ppm) the GE burst term sizes against:
// the aggregate channel estimate on a single path, or the worst per-path marginal in
// multipath (the path whose burst tail is heaviest).
func (s *Sender) burstMarginalPPM() int {
	if s.sched == nil {
		return int(s.pEst * 1e6)
	}
	worst := 0
	for _, m := range s.pathLossPpm {
		if m > worst {
			worst = m
		}
	}
	if worst == 0 {
		worst = int(s.pEst * 1e6) // bootstrap before the first per-path feedback
	}
	return worst
}

// slotDistribution returns the per-slot erasure-count histogram (ppm, len paths+1) the
// joint-tail sizer convolves: the receiver-reported measurement once it arrives, else the
// independence prior Binomial(N, pEst) — assuming uncorrelated paths is the right bootstrap
// before any per-path feedback.
func (s *Sender) slotDistribution() []int {
	if len(s.slotDistPpm) >= 2 {
		return s.slotDistPpm
	}
	return binomialPpm(s.cfg.paths(), s.pEst)
}

// binomialPpm returns the Binomial(n, p) erasure-count histogram in ppm (len n+1): the
// distribution of how many of n INDEPENDENT paths, each lost with probability p, erase
// their symbol in one aligned slot. Computed by convolving the single-path [1−p, p]
// distribution n times — integer, deterministic, no float pow.
func binomialPpm(n int, p float64) []int {
	pPpm := int(p * 1_000_000)
	if pPpm < 0 {
		pPpm = 0
	} else if pPpm > 1_000_000 {
		pPpm = 1_000_000
	}
	dist := []int{1_000_000} // 0 paths so far: all mass at count 0
	for k := 0; k < n; k++ {
		nd := make([]int, len(dist)+1)
		for j, m := range dist {
			nd[j] += m * (1_000_000 - pPpm) / 1_000_000 // this path delivered
			nd[j+1] += m * pPpm / 1_000_000             // this path erased
		}
		dist = nd
	}
	return dist
}

// updateRTT refreshes the EWMA RTT estimate from how long ago the generation the
// receiver's HighestSeen has reached was sent (a feedback-only estimate, no echo).
func (s *Sender) updateRTT(now clock.Timestamp, fb wire.Feedback) {
	if fb.HighestSeen == 0 {
		return
	}
	base, ok := s.genBaseContaining(fb.HighestSeen - 1)
	if !ok {
		return // the seen generation is still live (not yet closed) — no sample
	}
	g := s.retained[base]
	if g == nil {
		return
	}
	sample := now.Sub(g.closeAt)
	if sample <= 0 {
		return
	}
	s.rttSampled = true                                  // a real RTT sample exists (AutoGenSize may now widen)
	s.rttMicros = s.rttMicros - s.rttMicros/8 + sample/8 // EWMA, weight 1/8
	if s.cc != nil {
		// Raw RTT sample drives the delay-based budget (N3); the receiver-reported
		// CE-marked fraction adds the L4S/DCTCP response on top (RFC 9330).
		s.cc.onSample(now, sample, float64(fb.EcnCE)/65535)
	}
}

// Tick advances sender time: it closes a stale partial generation (so a
// tail/idle generation is protected before its deadline) and retires retained
// generations past their deadline.
func (s *Sender) Tick(now clock.Timestamp) {
	s.now = now
	if s.inGen > 0 && now.Sub(s.lastWrite) >= flushIdleMicros {
		s.closeGen(now)
	}
	for base, g := range s.retained {
		if now.After(g.deadline) {
			g.enc.Release() // recycle the generation's source buffers
			delete(s.retained, base)
		}
	}
}

// PollSend returns the next datagram to transmit and true, or nil/false when
// drained.
func (s *Sender) PollSend() ([]byte, bool) {
	if len(s.sendQ) == 0 {
		return nil, false
	}
	d := s.sendQ[0]
	s.sendQ = s.sendQ[1:]
	return d, true
}

// Stats returns a snapshot of what has been emitted.
func (s *Sender) Stats() SenderStats { return s.stats }

// RateBudgetBitsPerSec returns the current send-rate budget the host should pace the
// media source within (the congestion controller's output, or the static ceiling when
// CC is off) — the AdaptRate signal. The sizer already keeps repair inside this via
// the token bucket; the source rate is the host's to limit.
func (s *Sender) RateBudgetBitsPerSec() int64 {
	if s.cc != nil {
		if b := s.cc.rateBudgetBytesPerSec(); b > 0 {
			return b * 8
		}
	}
	return s.cfg.maxBitrate()
}

// emitRepair builds one repair symbol from enc with the given key and counts it. The
// repair carries its generation's protection tier so the wire reflects what it protects.
func (s *Sender) emitRepair(enc *code.Encoder, key uint16, n int, pri uint8, reactive bool) {
	base, nn, pay := enc.Repair(key)
	sym := wire.Symbol{
		Flow:       s.cfg.Flow,
		Kind:       wire.Repair,
		WindowBase: base,
		SrcIndex:   uint32(key),
		N:          uint16(nn),
		RepairKey:  key,
		Priority:   pri,
		Deadline:   int64(s.deadlineOf(base, n)),
		Payload:    pay,
	}
	if s.sched != nil {
		// Repair is metered toward the better-delivering path (weighted round-robin),
		// so fewer redundancy bytes are spent only to be dropped on the worse path.
		sym.PathID = uint8(s.sched.repairPath())
	}
	s.emit(sym)
	enc.Recycle(pay) // emit copied the payload onto the wire (EncodeSymbol), so reuse the buffer
	s.stats.Repair++
	if reactive {
		s.stats.ReactiveRepair++
	}
}

// deadlineOf returns the deadline to stamp on a repair symbol for the generation
// at base: the live generation uses genDL; a retained generation uses its own.
func (s *Sender) deadlineOf(base uint32, n int) clock.Timestamp {
	if g := s.retained[base]; g != nil {
		return g.deadline
	}
	return s.genDL
}

func (s *Sender) emit(sym wire.Symbol) {
	d := wire.EncodeSymbol(nil, sym)
	// Media (systematic) is never dropped; repair is throttled to hold the aggregate
	// emit rate under the ceiling, bounding a reactive-amplification storm (N1). Under
	// pressure the throttle sheds repair by protection tier — disposable first, parameter
	// sets / RAPs last (WP6 unequal protection) — so a tight budget preserves the
	// dependency spine instead of dropping whatever happens to be in flight.
	if sym.Kind == wire.Repair {
		if !s.bucket.allowRepair(s.now, len(d), sym.Priority) {
			s.stats.Throttled++
			return
		}
	} else {
		s.bucket.allow(s.now, len(d), false)
	}
	s.sendQ = append(s.sendQ, d)
}
