package flow

import (
	"sort"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// This file is the band-form sliding-window coder (the low-latency profile,
// Config.Sliding). It mirrors the generation Sender/Receiver method set so the host
// drives it identically, but codes over one elastic window with a bounded band
// (code.RepairWindow / code.BandDecoder): repair is continuous and fungible across
// the band, symbols are delivered the instant they decode (no per-generation close),
// and decode is O(band²) per symbol. The same redundancy controller drives it —
// feed-forward repairForTarget at the loss estimate + reactive symbolsForDeficit on
// the window degree-of-freedom deficit.

// SlidingSender is the band-form transmit half. Same methods as the generation
// Sender; selected by Config.Sliding.
type SlidingSender struct {
	cfg         Config
	b           int   // configured MAX band (decode-cost cap); the effective span adapts below it
	interMicros int64 // EWMA of the inter-write interval (source cadence)
	enc         *code.Encoder
	repairKey   uint16
	credit      float64
	pEst        float64
	burstQ8     int // estimated mean loss-run length, Q8 (from feedback; 256 = i.i.d.)
	fbCount     int // feedback reports received (for the cold-start proactive floor)
	rttMicros   int64
	// Windowed-min RTT filter state (updateRTT): rttMicros is the min over two
	// rolling half-windows of samples, NOT an EWMA. The EWMA it replaces ratcheted
	// under self-induced queueing (samples include the pacer queue and only ever
	// pushed it up under load): a measured 60 ms path reported 2.9 s mid-flood,
	// which deadline-clipped the band 64→8, saturated the set-point at the cap, and
	// sustained the very queue inflating it — a self-locking flood (the holdoff-
	// excursion root cause). The min filter reports the path's PROPAGATION-scale
	// RTT — what band sizing and the reactive-cycle math actually need — and is
	// immune to standing queues by construction; a genuine route/RTT change is
	// adopted within one full window. Queue-INCLUSIVE delay is a different signal
	// for a different consumer (a delay-cliff detector); if the sliding profile
	// grows one it must be tracked separately, never folded back in here.
	rttMinCur   int64
	rttMinPrev  int64
	rttSample   int64 // latest instantaneous RTT sample (pre-min-fold): sample − rttMicros = queue delay
	rttWinStart clock.Timestamp
	// Repair flood breaker: AIMD cap on the PROACTIVE code rate, driven by wire-
	// overrun evidence in feedback — the receiver's source-arrival rate falling far
	// below the offered rate in a way loss cannot explain. Without it a saturated
	// sizer floods the pacer queue faster than the wire drains, and no estimator
	// fix alone can recover (the queue outlives every estimation window: the
	// holdoff-excursion post-mortem). Protective only: media and deficit-driven
	// retro repair are never throttled. floodCap is the current rate ceiling
	// (>= maxRepairFactor ⇒ inactive); floodClear counts consecutive clean reports.
	floodCap      float64
	floodClear    int
	lastFBAt      clock.Timestamp
	lastFBHighest uint32
	lastFBSource  uint64 // stats.Source at the previous report (the exact offer)
	lastFBRepair  uint64 // stats.Repair at the previous report (the offered repair mix)
	arriveRatioQ8 int64  // EWMA of observed/offered source rate, Q8 (256 = keeping up)
	// Headroom-aware sizing (Amendment 9): a CONTINUOUS affordable-rate ceiling on
	// the proactive set-point, measured from the passed-through fraction f =
	// arrival/offer ÷ (1−reported loss). The breaker above is the post-hoc AIMD
	// backstop; sizing to an unaffordable δ target and letting it clamp after the
	// damage is the measured breaker/set-point LIMIT CYCLE (boom → 0.25-cap slam →
	// 300-400 ms under-protection → drain → boom; the arc-8 isolation). This cap
	// enters the sizer instead: tighten to the measured affordable rate on
	// saturation evidence, probe upward additively only when arrivals track offer
	// AND the RTT min-filter shows no standing queue. >= maxRepairFactor ⇒ inactive.
	headroomCap  float64
	lastWrite    clock.Timestamp
	lastRepair   clock.Timestamp
	lastReactive clock.Timestamp
	reactiveBase uint32
	reactiveSent int
	// wireLossBudget gates retrospective repair on EVIDENCE of wire loss: the
	// receiver's CongestionLoss field counts pre-recovery loss from the forward-gap
	// walk — arrivals-based, so symbols merely in flight can never inflate it (the
	// failure mode of deficit-only gating; a same-cursor-twice "stuck" gate was
	// built first and never fired at real timing, because per-symbol deadline skips
	// make the cursor CREEP between reports). Each report's count adds to the
	// budget; each retro round spends the deficit it addressed. Warmup and clean
	// links report zero, so the retro tier stays silent there by construction.
	wireLossBudget int
	// unitSentAt dedups NACK-bitmap unit repairs: id → last unit emission, so an
	// in-flight unit is not re-sent on every 10-20 ms report (see answerMissing).
	unitSentAt    map[uint32]clock.Timestamp
	deadlines     map[uint32]clock.Timestamp
	singletons    []pendingSingletonRepair
	protGroups    map[uint8]*protectedGroup // consolidated center-tier protection (one sparse repair per group)
	protectedSlot map[uint8][]uint32
	protected     map[uint32]clock.Timestamp
	sparseBase    uint32
	sparseSent    int
	sparsePend    []pendingSparseRepair
	frameStart    map[uint32]uint32
	frameInfo     map[uint32]*senderFrameInfo
	frameOrder    []uint32
	anchorSent    bool
	curFrameID    uint32
	haveCurFrame  bool
	ltrFidByStart map[uint32]uint32 // FrameStart+1 → frame id of FrameDesc.LTR candidates (resync translation)
	resync        resyncController
	cleanRun      int // consecutive feedbacks positively reporting zero loss (floor-decay confidence, as in the generation sender)
	sendQ         [][]byte
	stats         SenderStats
	cadence       recoveryCadenceController

	// codeRate memo: the proactive set-point depends only on (effectiveBand, pEst,
	// burstQ8, and — with SlidingReactiveShift — the reactive-rounds credit and the
	// floor-decay verdict); recomputing the GE-tail search per source symbol made the
	// controller O(symbols) instead of O(feedback) and detonated CPU/alloc under
	// bursts. Cache it and recompute only when an input actually moves.
	crBand     int
	crPEst     float64
	crBurstQ8  int
	crRounds   int
	crFloorOff bool
	crRelief   bool
	crRate     float64
	crValid    bool
}

// RateBudgetBitsPerSec returns the send-rate budget the host pacer should release within.
// The sliding coder has no delay-based controller, so this is the static MaxBitrate
// ceiling (the same value the generation coder falls back to when CC is off).
func (s *SlidingSender) RateBudgetBitsPerSec() int64 { return s.cfg.maxBitrate() }

func NewSlidingSender(cfg Config) *SlidingSender {
	return &SlidingSender{
		cfg:           cfg,
		b:             cfg.codingWindow(),
		enc:           code.NewEncoder(cfg.SymbolSize),
		rttMicros:     defaultRTTMicros,
		floodCap:      maxRepairFactor, // breaker inactive until wire-overrun evidence
		headroomCap:   maxRepairFactor, // affordable-rate ceiling inactive until saturation evidence
		arriveRatioQ8: 256,
		burstQ8:       burstQ8One,
		deadlines:     make(map[uint32]clock.Timestamp),
	}
}

// Band-sizing (deadline-aware). A symbol's whole coding span must arrive before its
// deadline: span_time (effectiveBand × inter-write interval) + one-way propagation
// (~rtt/2) + a guard must fit the buffer, or the trailing edge of the span lands at
// the deadline and the deadline-skip beats recovery (a fixed-wide band is why the
// sliding profile stalled at WAN RTT). So the effective coded span shrinks from the
// configured max as the budget tightens or the RTT grows — small at WAN, full at LAN
// — which the measurement harness confirms is the per-regime sweet spot. The larger
// the span the lower the overhead for a given protection (variance margin shrinks
// with block length), so it rides as wide as the deadline allows.
const (
	bandGuardMicros = 20_000 // slack beyond rtt/2 (≈ one feedback interval)
	minBand         = 8      // floor: a too-narrow band loses coding efficiency

	// Cold start: the proactive rate is sized for coldStartP assumed loss until the
	// loss estimate has primed and fed back, for at most coldStartFeedbacks reports.
	// It lifts the instant the measured loss reaches coldStartP (a lossy link takes
	// over via pEst), so the assumption only loads a clean/low-loss link, and only
	// during the first ~RTT of writes — exactly the ones the band coder cannot
	// reactively rescue (their repair window ages out before any feedback arrives).
	// 0.20 sizes the trailing window to the steady-state set-point of a ~20% first
	// mile so the WHOLE estimate-ramp is provisioned, not just the very first band:
	// over the real session the loss estimate primes a band or two slower than the
	// tight deterministic feedback loop, and a 0.15 floor (≈0.37 rate) left the
	// post-cold ramp under the ~0.50 the channel needed (a 1-2% warmup residual).
	// Lossy links above 20% provision further via pEst; the cost is a bounded burst
	// of warmup repair on a clean link.
	coldStartP                = 0.15
	coldStartFeedbacks        = 6
	coldStartBurstGap         = 48
	targetedRepairGuardMicros = 5_000

	// Sparse protected repair answers a stuck feedback cursor with equations over
	// only protected/reference source ids in that old neighborhood. The total sent
	// for a cursor window is capped at the number of protected ids in the sparse set.
	sparseProtectedMaxIDs = 16

	// RAP-anchor closure repair protects the minimal source island needed to start
	// decoding after a random-access point: the RAP frame plus its descriptor-resolved
	// reference frames, usually parameter sets. It is intentionally limited to the
	// first sliding band: this is the cold-start/first-P-chain fragility observed in
	// glassbench, while normal sliding repair handles later steady-state holes.
	sparseAnchorMaxIDs  = 6
	sparseAnchorExtra   = 1
	sparseAnchorStripes = 1
)

const (
	protectedLaneBase uint8 = iota
	protectedLaneRAP
)

type pendingSparseRepair struct {
	ids       []uint32
	releaseAt uint32
	reactive  bool // feedback-driven retry (counts toward ReactiveRepair) vs scheduled group protection
}

type senderFrameInfo struct {
	start           uint32
	length          uint16
	refs            []uint32
	rap             bool
	recoveryRefresh bool
	discardable     bool
	anchorSent      bool
}

// effectiveBand returns the coded span to use now: the configured max band, reduced
// to what fits the deadline budget after one-way propagation and a guard.
func (s *SlidingSender) effectiveBand() int {
	b := s.b
	if s.interMicros <= 0 || s.cfg.BufferMicros <= 0 {
		return b
	}
	budget := s.cfg.BufferMicros - s.rttMicros/2 - bandGuardMicros
	if budget < s.interMicros {
		budget = s.interMicros // at least one symbol of span
	}
	if fit := int(budget / s.interMicros); fit < b {
		b = fit
	}
	if b < minBand {
		b = minBand
	}
	return b
}

func (s *SlidingSender) repairWindowBase(band int) (uint32, int) {
	n := s.enc.Len()
	base := s.enc.Base()
	if band > 0 && n > band {
		base += uint32(n - band)
		n = band
	}
	return base, n
}

// Write emits one source symbol systematic, appends it to the window, and emits
// credit-paced proactive repair over the trailing band.
func (s *SlidingSender) Write(now clock.Timestamp, data []byte) {
	s.write(now, data, uepCenterTier, nil)
}

func (s *SlidingSender) write(now clock.Timestamp, data []byte, priority uint8, fd *FrameDesc) {
	if s.lastWrite != 0 {
		if gap := now.Sub(s.lastWrite); gap > 0 {
			if s.interMicros == 0 {
				s.interMicros = gap
			} else {
				s.interMicros += (gap - s.interMicros) / 8
			}
		}
	}
	s.lastWrite = now
	id := s.enc.Add(data)
	dl := now.Add(s.cfg.BufferMicros)
	s.deadlines[id] = dl
	src, _ := s.enc.Source(id)
	sym := wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Systematic, WindowBase: id, SrcIndex: id, N: 1, Priority: priority, Deadline: int64(dl), SendTimestamp: int64(now), Payload: src}
	var frame *senderFrameInfo
	if fd != nil {
		if s.frameStart == nil {
			s.frameStart = make(map[uint32]uint32)
		}
		if s.frameInfo == nil {
			s.frameInfo = make(map[uint32]*senderFrameInfo)
		}
		if !s.haveCurFrame || fd.FrameID != s.curFrameID {
			s.frameStart[fd.FrameID] = id
			s.curFrameID, s.haveCurFrame = fd.FrameID, true
			if fd.LTR {
				if s.ltrFidByStart == nil {
					s.ltrFidByStart = make(map[uint32]uint32)
				}
				s.ltrFidByStart[id+1] = fd.FrameID // +1: the wire's FrameStart+1 encoding (0 = none)
			}
			s.pruneFrameStarts(fd.FrameID)
		}
		noteResyncHonored(&s.resync, s.ltrFidByStart, fd)
		sym.HasFrameDesc = true
		sym.FrameStart = s.frameStart[fd.FrameID]
		sym.FrameLen = fd.Chunks
		sym.FrameRAP, sym.FrameDiscardable = fd.RAP, fd.Discardable
		sym.FrameRecoveryRefresh = fd.RecoveryRefresh
		sym.FrameNonPicture = fd.NonPicture
		sym.FrameLTR = fd.LTR
		for _, ref := range fd.RefFrameIDs {
			if rs, ok := s.frameStart[ref]; ok {
				sym.FrameRefs = append(sym.FrameRefs, rs)
				if len(sym.FrameRefs) >= maxFrameRefs {
					break
				}
			}
		}
		frame = s.noteSenderFrame(sym.FrameStart, fd, sym.FrameRefs)
	}
	s.emit(sym)
	s.stats.Source++
	if fd != nil && fd.RecoveryRefresh {
		s.noteProtectedSource(id, dl)
	}
	if s.shouldSingletonProtect(priority, fd) {
		lane := protectedLaneFor(priority, fd)
		s.noteProtectedSource(id, dl)
		if band := s.effectiveBand(); priority == uepCenterTier && band >= 2*protectedGroupMaxIDs {
			// Center-tier references consolidate into a per-lane protected GROUP,
			// released as ONE sparse repair — attribution measured per-chunk
			// singletons at 60% of ALL repair on the high-rate 1×RTT cell
			// (PREREG-cost.md Amendment 1). One equation repairs any single loss
			// among the group; multi-loss neighborhoods lean on the BAND rate,
			// so consolidation requires a band wide enough to actually carry
			// that cover (2× the group cap): at a deadline-clipped narrow band
			// (the sub-RTT frontier, band ≈ 5) the per-chunk singletons are the
			// only real multi-loss protection and stay (the 0.75×RTT guard
			// measured 6-69 lost chunks per run without this condition —
			// PREREG-cost.md Amendment 2).
			s.appendProtectedGroup(id, lane, band)
		} else {
			// Tiers above center (parameter sets, RAP-lane anchors) keep the true
			// per-chunk singleton: rare, and their per-chunk guarantee is cheap.
			releaseAt := id + s.protectedRepairGap()
			if s.cfg.ProtectedRepairPhasing {
				releaseAt = s.singletonReleaseAt(id, lane)
			}
			s.queueSingletonRepair(id, src, priority, dl, releaseAt, lane)
		}
	}
	if frame != nil {
		s.maybeQueueAnchorClosure(frame, id)
	}
	s.flushSingletonRepairs(now, id, false)
	s.flushProtectedGroups(id, false)
	s.flushSparseRepairs(now, id, false)
	for s.credit += s.codeRate(); s.credit >= 1; s.credit-- {
		s.emitRepair(now)
	}
}

// WriteUnit satisfies the media-aware write contract. The sliding profile still codes
// one continuous fungible band, but protected units get delayed targeted repair so a
// burst that wipes the source is less likely to wipe the only repair for that unit too.
func (s *SlidingSender) WriteUnit(now clock.Timestamp, data []byte, priority uint8) {
	s.write(now, data, priority, nil)
}

// WriteFrame satisfies the media-aware write contract; dependency hints steer the
// targeted repair used by the sliding UEP benchmark arm.
func (s *SlidingSender) WriteFrame(now clock.Timestamp, data []byte, fd FrameDesc) {
	s.write(now, data, fd.Priority, &fd)
}

func (s *SlidingSender) pruneFrameStarts(cur uint32) {
	if len(s.frameStart) <= 2*senderFrameWindow {
		return
	}
	for fid := range s.frameStart {
		if fid+senderFrameWindow < cur {
			delete(s.frameStart, fid)
		}
	}
	for start, fid := range s.ltrFidByStart {
		if fid+senderFrameWindow < cur {
			delete(s.ltrFidByStart, start)
		}
	}
}

func (s *SlidingSender) pruneSenderFrames(before uint32) {
	if len(s.frameOrder) == 0 {
		return
	}
	keep := s.frameOrder[:0]
	for _, st := range s.frameOrder {
		fi := s.frameInfo[st]
		if fi == nil || st < before {
			delete(s.frameInfo, st)
			continue
		}
		keep = append(keep, st)
	}
	s.frameOrder = keep
}

func (s *SlidingSender) noteSenderFrame(start uint32, fd *FrameDesc, refs []uint32) *senderFrameInfo {
	if fi := s.frameInfo[start]; fi != nil {
		return fi
	}
	cp := append([]uint32(nil), refs...)
	fi := &senderFrameInfo{
		start:           start,
		length:          fd.Chunks,
		refs:            cp,
		rap:             fd.RAP,
		recoveryRefresh: fd.RecoveryRefresh,
		discardable:     fd.Discardable,
	}
	s.frameInfo[start] = fi
	i := sort.Search(len(s.frameOrder), func(i int) bool { return s.frameOrder[i] >= start })
	s.frameOrder = append(s.frameOrder, 0)
	copy(s.frameOrder[i+1:], s.frameOrder[i:])
	s.frameOrder[i] = start
	return fi
}

func (s *SlidingSender) maybeQueueAnchorClosure(fi *senderFrameInfo, highest uint32) {
	if s.anchorSent || fi.anchorSent || !fi.rap || fi.discardable {
		return
	}
	if s.cfg.MaxBitrate > 0 || s.cfg.Redundancy > referenceBoostMaxRedundancy {
		return
	}
	if s.extrasReplaceableByReactive() {
		return // retro-reactive reaches a broken anchor closure on demand
	}
	gap := s.protectedRepairGap()
	if fi.start < gap {
		return
	}
	if fi.start >= uint32(s.effectiveBand()) {
		return
	}
	end := fi.start + 1
	if fi.length > 0 {
		end = fi.start + uint32(fi.length)
	}
	if highest+1 < end {
		return
	}
	ids := s.frameClosureIDs(fi.start, sparseAnchorMaxIDs)
	if len(ids) <= 1 {
		return
	}
	s.anchorSent = true
	fi.anchorSent = true
	s.dropSingletonRepairs(ids)
	first := highest + gap + gap/2
	if s.cfg.ProtectedRepairPhasing {
		first = highest + gap
	}
	if first <= highest {
		first = highest + 1
	}
	for stripe := 0; stripe < sparseAnchorStripes; stripe++ {
		releaseAt := first + uint32(stripe)*gap
		for i := 0; i < len(ids)+sparseAnchorExtra; i++ {
			if s.cfg.ProtectedRepairPhasing {
				phase := protectedRepairPhaseSpacing(gap)
				target := releaseAt + uint32(i)*phase
				s.queueSparseRepair(ids, s.scheduleProtectedRelease(highest, target, protectedLaneRAP), false)
			} else {
				s.queueSparseRepair(ids, releaseAt, false)
			}
		}
	}
}

func (s *SlidingSender) frameClosureIDs(start uint32, maxIDs int) []uint32 {
	if maxIDs <= 0 || s.frameInfo[start] == nil {
		return nil
	}
	seen := make(map[uint32]bool)
	ids := make([]uint32, 0, maxIDs)
	var visit func(uint32) bool
	visit = func(st uint32) bool {
		if seen[st] {
			return true
		}
		seen[st] = true
		fi := s.frameInfo[st]
		if fi == nil {
			if _, ok := s.enc.Source(st); !ok || len(ids) >= maxIDs {
				return false
			}
			ids = append(ids, st)
			return true
		}
		for _, ref := range fi.refs {
			if !visit(ref) {
				return false
			}
		}
		n := uint32(fi.length)
		if n == 0 {
			n = 1
		}
		for id := fi.start; id < fi.start+n; id++ {
			if _, ok := s.enc.Source(id); !ok || len(ids) >= maxIDs {
				return false
			}
			ids = append(ids, id)
		}
		return true
	}
	if !visit(start) {
		return nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:0]
	var prev uint32
	for i, id := range ids {
		if i > 0 && id == prev {
			continue
		}
		out = append(out, id)
		prev = id
	}
	if len(out) == 0 || uint64(out[len(out)-1])-uint64(out[0])+1 > uint64(s.effectiveBand()) {
		return nil
	}
	return out
}

// Flush protects the tail by emitting enough band-repair to cover its estimated loss.
func (s *SlidingSender) Flush(now clock.Timestamp) {
	n := s.enc.Len()
	if eb := s.effectiveBand(); n > eb {
		n = eb
	}
	if n == 0 {
		return
	}
	r := repairForTarget(n, s.pEst, s.cfg.targetFailure(), maxRepairFactor)
	if floor := s.cfg.repairFloor(n); r < floor {
		r = floor
	}
	for ; r > 0; r-- {
		s.emitRepair(now)
	}
	s.flushSingletonRepairs(now, 0, true)
	s.flushProtectedGroups(0, true)
	s.flushSparseRepairs(now, 0, true)
}

// protectedGroupMaxIDs caps how many center-tier references one consolidated
// sparse repair covers. One equation repairs any ONE loss among the group; the
// cap (fixed in PREREG-cost.md before results) bounds the multi-loss exposure a
// group shares, and the span cap below keeps the equation inside the receiver's
// band. 12 ids ≈ one twelfth of the former per-chunk singleton mass.
const protectedGroupMaxIDs = 12

// protectedGroup accumulates center-tier protected chunk ids for consolidated
// sparse release (per lane, so RAP-adjacent and base references phase apart).
type protectedGroup struct {
	ids       []uint32
	releaseAt uint32
}

// appendProtectedGroup adds a center-tier protected chunk to its lane's open
// group. A group that hits the id cap, or whose span would exceed half the
// effective band (the sparse-repair reach), is SEALED — queued into the shared
// sparse-release schedule at the phased releaseAt derived from its FIRST id, the
// same source-separation the per-chunk singleton machinery used — and a fresh
// group opens with the incoming id. Burst decorrelation is therefore preserved:
// the equation never travels adjacent to the chunks it protects.
func (s *SlidingSender) appendProtectedGroup(id uint32, lane uint8, band int) {
	if s.protGroups == nil {
		s.protGroups = make(map[uint8]*protectedGroup, 2)
	}
	g := s.protGroups[lane]
	if g != nil && len(g.ids) > 0 {
		spanCap := uint32(band / 2)
		if spanCap < 2 {
			spanCap = 2
		}
		if len(g.ids) >= protectedGroupMaxIDs || id-g.ids[0] >= spanCap {
			s.sealProtectedGroup(lane)
			g = nil
		}
	}
	if g == nil {
		releaseAt := id + s.protectedRepairGap()
		if s.cfg.ProtectedRepairPhasing {
			releaseAt = s.scheduleProtectedRelease(id, releaseAt, lane)
		}
		g = &protectedGroup{releaseAt: releaseAt, ids: make([]uint32, 0, protectedGroupMaxIDs)}
		s.protGroups[lane] = g
	}
	g.ids = append(g.ids, id)
}

// flushProtectedGroups seals any open group whose scheduled release point the
// write frontier has passed (or every group, on force); the shared sparse
// schedule then emits it — this write for an already-due releaseAt.
func (s *SlidingSender) flushProtectedGroups(highest uint32, force bool) {
	for lane, g := range s.protGroups {
		if g == nil || len(g.ids) == 0 {
			continue
		}
		if force || highest >= g.releaseAt {
			s.sealProtectedGroup(lane)
		}
	}
}

// sealProtectedGroup moves the lane's open group into the sparse-release queue
// (scheduled, non-reactive) and closes it.
func (s *SlidingSender) sealProtectedGroup(lane uint8) {
	g := s.protGroups[lane]
	if g == nil || len(g.ids) == 0 {
		return
	}
	s.queueSparseRepair(g.ids, g.releaseAt, false)
	delete(s.protGroups, lane)
}

// pruneProtectedGroups drops open-group members the receiver has already
// delivered (below the feedback low edge); an emptied group is discarded.
func (s *SlidingSender) pruneProtectedGroups(before uint32) {
	for lane, g := range s.protGroups {
		if g == nil {
			continue
		}
		ids := g.ids[:0]
		for _, id := range g.ids {
			if id >= before {
				ids = append(ids, id)
			}
		}
		if g.ids = ids; len(g.ids) == 0 {
			delete(s.protGroups, lane)
		}
	}
}

func (s *SlidingSender) queueSingletonRepair(id uint32, src []byte, priority uint8, deadline clock.Timestamp, releaseAt uint32, lane uint8) {
	cp := make([]byte, len(src))
	copy(cp, src)
	s.singletons = append(s.singletons, pendingSingletonRepair{
		id:        id,
		src:       cp,
		priority:  priority,
		deadline:  deadline,
		releaseAt: releaseAt,
		lane:      lane,
	})
}

func (s *SlidingSender) singletonReleaseAt(id uint32, lane uint8) uint32 {
	gap := s.protectedRepairGap()
	return s.scheduleProtectedRelease(id, id+gap, lane)
}

func (s *SlidingSender) protectedRepairGap() uint32 {
	if s.cfg.SingletonRepairGap > 0 {
		return uint32(s.cfg.SingletonRepairGap)
	}
	gap := s.cfg.singletonRepairGap()
	if !s.cfg.ProtectedRepairPhasing {
		return gap
	}
	if s.fbCount < coldStartFeedbacks && gap < coldStartBurstGap {
		gap = coldStartBurstGap
	}
	if burst := uint32((s.burstQ8 + 255) / 256); burst > gap {
		gap = burst
	}
	if cap := s.protectedRepairDeadlineSlots(); cap > 0 && gap > cap {
		gap = cap
	}
	if gap == 0 {
		return 1
	}
	return gap
}

func (s *SlidingSender) protectedRepairDeadlineSlots() uint32 {
	if s.interMicros <= 0 || s.cfg.BufferMicros <= 0 {
		return 0
	}
	budget := s.cfg.BufferMicros - s.rttMicros/2 - targetedRepairGuardMicros
	if budget < s.interMicros {
		return 1
	}
	return uint32(budget / s.interMicros)
}

func protectedLaneFor(priority uint8, fd *FrameDesc) uint8 {
	lane := protectedLaneBase
	if priority > uepCenterTier || (fd != nil && fd.RAP) {
		lane = protectedLaneRAP
	}
	return lane
}

func (s *SlidingSender) scheduleProtectedRelease(sourceID, target uint32, lane uint8) uint32 {
	gap := s.protectedRepairGap()
	window := protectedRepairPhaseWindow(gap)
	minGap, maxGap := s.protectedRepairGapBounds(gap, window)
	minAt := sourceID + minGap
	maxAt := sourceID + maxGap
	if target < minAt {
		target = minAt
	}
	if target > maxAt {
		target = maxAt
	}
	if s.protectedSlot == nil {
		s.protectedSlot = make(map[uint8][]uint32, 2)
	}
	slots := s.pruneProtectedSlots(lane, target, maxGap+protectedRepairPhaseSpacing(gap)+window)
	releaseAt, ok := nearestSpacedProtectedSlot(target, minAt, maxAt, slots, protectedRepairPhaseSpacing(gap))
	if !ok {
		releaseAt = target
	}
	s.protectedSlot[lane] = append(slots, releaseAt)
	return releaseAt
}

func protectedRepairPhaseSpacing(gap uint32) uint32 {
	switch {
	case gap < 4:
		return 1
	case gap < 12:
		return 2
	default:
		return 3
	}
}

func protectedRepairPhaseWindow(gap uint32) uint32 {
	switch {
	case gap < 4:
		return 1
	case gap < 8:
		return 2
	case gap < 12:
		return 4
	default:
		return 6
	}
}

func (s *SlidingSender) protectedRepairGapBounds(gap, window uint32) (uint32, uint32) {
	minGap := uint32(1)
	if gap > window {
		minGap = gap - window
	}
	maxGap := gap + window
	if deadlineSlots := s.protectedRepairDeadlineSlots(); deadlineSlots > 0 && maxGap > deadlineSlots {
		maxGap = deadlineSlots
	}
	if maxGap < minGap {
		minGap = maxGap
	}
	return minGap, maxGap
}

func (s *SlidingSender) pruneProtectedSlots(lane uint8, center, horizon uint32) []uint32 {
	slots := s.protectedSlot[lane]
	if len(slots) == 0 {
		return slots
	}
	min := uint32(0)
	if center > horizon {
		min = center - horizon
	}
	keep := slots[:0]
	for _, slot := range slots {
		if slot >= min {
			keep = append(keep, slot)
		}
	}
	s.protectedSlot[lane] = keep
	return keep
}

func nearestSpacedProtectedSlot(target, minAt, maxAt uint32, existing []uint32, spacing uint32) (uint32, bool) {
	var best uint32
	bestDist := uint32(0)
	bestOffset := uint32(0)
	haveBest := false
	consider := func(slot uint32) (uint32, bool) {
		if slot < minAt || slot > maxAt {
			return 0, false
		}
		dist := minProtectedSlotDistance(slot, existing)
		if dist >= spacing {
			return slot, true
		}
		offset := protectedSlotDistance(slot, target)
		if !haveBest || dist > bestDist || (dist == bestDist && (offset < bestOffset || (offset == bestOffset && slot < best))) {
			best, bestDist, bestOffset, haveBest = slot, dist, offset, true
		}
		return 0, false
	}
	if slot, ok := consider(target); ok {
		return slot, true
	}
	for delta := uint32(1); delta <= protectedSlotDistance(maxAt, minAt); delta++ {
		if target >= delta {
			if slot, ok := consider(target - delta); ok {
				return slot, true
			}
		}
		if maxAt-target >= delta {
			if slot, ok := consider(target + delta); ok {
				return slot, true
			}
		}
	}
	return best, haveBest
}

func minProtectedSlotDistance(slot uint32, existing []uint32) uint32 {
	if len(existing) == 0 {
		return ^uint32(0)
	}
	min := ^uint32(0)
	for _, other := range existing {
		if d := protectedSlotDistance(slot, other); d < min {
			min = d
		}
	}
	return min
}

func protectedSlotDistance(a, b uint32) uint32 {
	if a > b {
		return a - b
	}
	return b - a
}

func (s *SlidingSender) flushSingletonRepairs(now clock.Timestamp, highest uint32, force bool) {
	keep := s.singletons[:0]
	for _, p := range s.singletons {
		if force || highest >= p.releaseAt {
			s.emitSingletonRepair(now, p)
			continue
		}
		keep = append(keep, p)
	}
	s.singletons = keep
}

func (s *SlidingSender) dropSingletonRepairs(ids []uint32) {
	if len(ids) == 0 || len(s.singletons) == 0 {
		return
	}
	drop := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		drop[id] = true
	}
	keep := s.singletons[:0]
	for _, p := range s.singletons {
		if drop[p.id] {
			s.releaseProtectedSlot(p.lane, p.releaseAt)
			continue
		}
		keep = append(keep, p)
	}
	s.singletons = keep
}

func (s *SlidingSender) releaseProtectedSlot(lane uint8, releaseAt uint32) {
	if len(s.protectedSlot) == 0 {
		return
	}
	slots := s.protectedSlot[lane]
	for i, slot := range slots {
		if slot != releaseAt {
			continue
		}
		copy(slots[i:], slots[i+1:])
		slots = slots[:len(slots)-1]
		s.protectedSlot[lane] = slots
		return
	}
}

func (s *SlidingSender) emitSingletonRepair(now clock.Timestamp, p pendingSingletonRepair) {
	enc := code.NewEncoderAt(s.cfg.SymbolSize, p.id)
	enc.Add(p.src)
	_, n, pay := enc.Repair(0)
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: p.id, SrcIndex: 0, N: uint16(n), RepairKey: 0, Priority: p.priority, Deadline: int64(p.deadline), SendTimestamp: int64(now), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairSingleton++
}

// reactiveReachable reports whether one honest reactive cycle fits the deadline
// budget — the gate for the retrospective repair tier itself.
func (s *SlidingSender) reactiveReachable() bool {
	return s.cfg.BufferMicros > 0 && reactiveCycleMicros(s.rttMicros) <= s.cfg.BufferMicros
}

// extrasReplaceableByReactive is the shared capability predicate (extrasReplaceable)
// with the sliding profile's cold-start stand-in: a band-length burst.
func (s *SlidingSender) extrasReplaceableByReactive() bool {
	return extrasReplaceable(s.cfg.BufferMicros, s.rttMicros, s.interMicros,
		int64(s.effectiveBand()), s.fbCount, s.burstQ8)
}

func (s *SlidingSender) shouldSingletonProtect(priority uint8, fd *FrameDesc) bool {
	if s.cfg.MaxBitrate > 0 || s.cfg.Redundancy > referenceBoostMaxRedundancy {
		return false
	}
	if fd != nil && fd.RecoveryRefresh {
		return false
	}
	protectable := priority > uepCenterTier ||
		(fd != nil && priority == uepCenterTier && !fd.Discardable && !fd.RAP)
	// The capability predicate runs last: most chunks fail the cheap tier gates above,
	// and this is the per-write hot path.
	return protectable && !s.extrasReplaceableByReactive()
}

func (s *SlidingSender) noteProtectedSource(id uint32, deadline clock.Timestamp) {
	if s.protected == nil {
		s.protected = make(map[uint32]clock.Timestamp)
	}
	s.protected[id] = deadline
}

func (s *SlidingSender) pruneProtected(before uint32) {
	for id := range s.protected {
		if id < before {
			delete(s.protected, id)
		}
	}
}

func (s *SlidingSender) protectedIDsIn(base uint32, width int) []uint32 {
	if width <= 0 || len(s.protected) == 0 {
		return nil
	}
	end := uint64(base) + uint64(width)
	ids := make([]uint32, 0, len(s.protected))
	for id := range s.protected {
		if id >= base && uint64(id) < end {
			if _, ok := s.enc.Source(id); ok {
				ids = append(ids, id)
			}
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > sparseProtectedMaxIDs {
		ids = ids[:sparseProtectedMaxIDs]
	}
	return ids
}

func (s *SlidingSender) queueSparseRepair(ids []uint32, releaseAt uint32, reactive bool) {
	if len(ids) == 0 {
		return
	}
	s.sparsePend = append(s.sparsePend, pendingSparseRepair{
		ids:       append([]uint32(nil), ids...),
		releaseAt: releaseAt,
		reactive:  reactive,
	})
}

func (s *SlidingSender) flushSparseRepairs(now clock.Timestamp, highest uint32, force bool) {
	if len(s.sparsePend) == 0 {
		return
	}
	keep := s.sparsePend[:0]
	for _, p := range s.sparsePend {
		if force || highest >= p.releaseAt {
			s.emitSparseRepair(now, p.ids, p.reactive)
			continue
		}
		keep = append(keep, p)
	}
	s.sparsePend = keep
}

func (s *SlidingSender) pruneSparseRepairs(before uint32) {
	if len(s.sparsePend) == 0 {
		return
	}
	keep := s.sparsePend[:0]
	for _, p := range s.sparsePend {
		ids := p.ids[:0]
		for _, id := range p.ids {
			if id < before {
				continue
			}
			if _, ok := s.enc.Source(id); ok {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			continue
		}
		p.ids = ids
		keep = append(keep, p)
	}
	s.sparsePend = keep
}

func (s *SlidingSender) latestSourceID() (uint32, bool) {
	if s.enc.Len() == 0 {
		return 0, false
	}
	return s.enc.Base() + uint32(s.enc.Len()-1), true
}

// FeedFeedback refreshes the loss/RTT estimates, prunes the window to the decoded
// low-edge, and sends reactive band-repair on a degree-of-freedom deficit.
func (s *SlidingSender) FeedFeedback(now clock.Timestamp, fb wire.Feedback) {
	if fb.Flow != s.cfg.Flow {
		return
	}
	s.fbCount++
	// Captured before updateFloodBreaker's defer refreshes lastFBSource: whether any
	// source symbols were offered in the interval this report covers (the clean-run
	// progress gate below).
	offeredSince := s.stats.Source != s.lastFBSource
	s.updateRTT(now, fb)
	s.updateFloodBreaker(now, fb)
	s.pEst = float64(fb.LossRate) / 65535
	// Floor-decay confidence (see floorDecayed): count consecutive feedbacks that
	// POSITIVELY report a clean link; snap to zero on any contrary evidence.
	// Keyed on the report, never on signal absence — a black hole or warmup delivers
	// no feedback at all, so cleanRun stays 0 and the full floor is retained.
	//
	// A clean report is the STRICT composite: the persistent rate estimate AND the
	// per-interval honest counter AND both decoder-state snapshots quiet. The rate
	// term is the anchor — the per-regime signal survey (2026-07-04) measured the
	// snapshot signals alone stringing 64+ quiet reports between bursts at 1% and
	// even 10% GE loss (false-arm), while LossRate's EWMA/hold memory never goes
	// quiet under real loss (longest zero-run 8-23 of the 64 required). The extra
	// terms guard the rate term's own blind spot: LossRate is quantized to uint16,
	// so sub-1.5e-5 loss reads 0 there while CongestionLoss>0 still reports each
	// event, and Deficit/Missing surface damage the instant a report is assembled.
	// On a clean link at natural timing all four are simultaneously quiet for 100%
	// of reports (survey), so the strict conjunction costs nothing where it should
	// arm. Under heavy reorder the walks fire constantly and the raw composite
	// never arms — for that regime the peer's SETTLED evidence takes over below.
	clean := fb.LossRate == 0 && fb.CongestionLoss == 0 && fb.Deficit == 0 && fb.Missing == 0
	if fb.HasSettled {
		// A settled-walk peer adjudicates reorder for us: SettledLost counts only
		// ids proven absent past the reorder holdoff, so a clean-but-reordered link
		// reads clean (the raw walks read dirty on nearly every report there) and
		// any REAL wire loss — recovered or not — reads dirty within holdoff + one
		// cadence. That bound is the re-arm latency trade: the raw composite
		// re-armed on the same report as the loss evidence; settled evidence lags
		// it by up to the holdoff (~10-30 ms), well inside what rounds >=
		// reactiveFloorSafe already guarantees recoverable. The instantaneous
		// Deficit/Missing snapshots are deliberately excluded here: on a
		// clean+reorder link they are exactly the in-flight transients the settled
		// walk exists to adjudicate. Like the raw composite, a total forward
		// outage freezes every signal and reads clean — benign for the floor (a
		// decayed floor changes nothing while the wire drops everything; the
		// backlog settles dirty the moment arrivals resume).
		clean = fb.SettledLost == 0
	}
	switch {
	case !clean:
		s.cleanRun = 0
	case offeredSince && s.cleanRun < cleanFloorConfirm:
		// Only reports covering an interval where source was actually OFFERED build
		// confidence — an idle stream's "nothing offered, nothing lost" reports are
		// vacuously clean and must not arm the decay toward an unprotected resume
		// (the raw composite blocked idle re-arming by accident: LossRate's EWMA
		// memory; settled evidence reads honestly quiet there). Idle freezes the
		// counter; dirty evidence resets it regardless of offering.
		s.cleanRun++
	}
	if s.burstQ8 = int(fb.Burstiness); s.burstQ8 < burstQ8One {
		s.burstQ8 = burstQ8One // mean loss-run length for the burst-aware sizer (N2)
	}
	s.cadence.observeFeedback(fb)
	s.resync.observe(now, fb.BrokenAnchors, fb.NewestDecodableLTR, resyncHoldMicros(s.cfg.BufferMicros, s.rttMicros))
	if fb.DecodedLowEdge > s.enc.Base() {
		for id := s.enc.Base(); id < fb.DecodedLowEdge; id++ {
			delete(s.deadlines, id)
		}
		s.enc.SlideTo(fb.DecodedLowEdge)
		s.pruneSenderFrames(fb.DecodedLowEdge)
		s.pruneProtected(fb.DecodedLowEdge)
		s.pruneProtectedGroups(fb.DecodedLowEdge)
		s.pruneSparseRepairs(fb.DecodedLowEdge)
		s.reactiveBase, s.reactiveSent = 0, 0
		s.sparseBase, s.sparseSent = 0, 0
	}
	if cl := int(fb.CongestionLoss); cl > 0 {
		// Arm the retro-reactive tier with the reported pre-recovery wire loss,
		// capped so a long outage cannot bank an unbounded repair debt. ARQ-mode
		// raises the cap: the retro tier is the only burst recovery there.
		capX := wireLossBudgetCap
		if s.deltaReliefOn() {
			capX = wireLossBudgetCapARQ
		}
		if s.wireLossBudget += cl; s.wireLossBudget > capX*s.effectiveBand() {
			s.wireLossBudget = capX * s.effectiveBand()
		}
	}
	if fb.Deficit > 0 {
		s.reactiveSparseProtected(now, fb, int(fb.Deficit))
	}
	answered := s.answerMissing(now, fb)
	// The coded retro tier covers only the deficit BEYOND the unit-answered bits
	// (its windows still span the same neighborhood, so residual overlap is fine
	// and both spend from the shared wireLossBudget evidence).
	if residual := int(fb.Deficit) - answered; residual > 0 {
		s.reactive(now, fb.DecodedLowEdge, residual)
	}
}

// answerMissing answers the receiver's stuck-neighborhood NACK bitmap
// (wire.Feedback.Missing) with UNIT repairs — base=id, n=1, the retained source
// itself — and reports how many it sent. A unit vector closes at the decoder the
// moment it arrives: it is the only response type exempt from the coupled-span
// closure delay the burst autopsy measured (rank arrives at need-rate; VALUES wait
// on full-rank closure of pepper-coupled spans, 195-680 ms on the ge48 glass
// cell). Gates: the reactive-capability cycle (units past the deadline are waste),
// the shared wire-loss evidence budget, a per-id dedup within one honest cycle
// (a unit in flight is not re-sent on every report), retention clipping, and the
// provably-dead deadline arithmetic. An old peer never sets Missing → dormant.
//
// OPT-IN (SlidingReactiveShift), by measurement (2026-07-04): on the ge48@2.5×RTT
// glass hole the units fire (156-410/run, all forwarded) and DO collapse hole
// release latency exactly as predicted — 195-680 ms → 33/72/92 ms q25/50/75 in
// exact-stream replay, hole on-time recovery up, never-recovered down — but the
// ARBITERED delivery total is a WASH (16.4 vs 15.7% median, 16 paired seeds),
// because the cell's score is governed by the DIRECT chunks queued behind each
// stall, whose ~50-120 ms survival boundary the collapsed stalls only graze.
// Mechanism validated, cell bar not met → the answering stays in the
// experimental reactive bundle until a regime where release latency is the
// margin (real paths, tighter budgets) earns it the default.
func (s *SlidingSender) answerMissing(now clock.Timestamp, fb wire.Feedback) int {
	if !s.cfg.SlidingReactiveShift {
		return 0
	}
	if fb.Missing == 0 || !s.reactiveReachable() || s.wireLossBudget <= 0 {
		return 0
	}
	if s.unitSentAt == nil {
		s.unitSentAt = make(map[uint32]clock.Timestamp)
	}
	for id := range s.unitSentAt {
		if id < fb.DecodedLowEdge {
			delete(s.unitSentAt, id) // delivered or dead history; keep the map bounded
		}
	}
	cycle := reactiveCycleMicros(s.rttMicros)
	handled := 0 // sent now + still in flight: everything the coded residual need not cover
	for k := 0; k < 64 && s.wireLossBudget > 0; k++ {
		if fb.Missing&(1<<k) == 0 {
			continue
		}
		id := fb.DecodedLowEdge + uint32(k)
		if at, ok := s.unitSentAt[id]; ok && now.Sub(at) < cycle {
			handled++ // a unit for this id is still in flight
			continue
		}
		if !s.emitUnitRepair(now, id) {
			continue // outside retention, or provably past its deadline
		}
		s.unitSentAt[id] = now
		s.wireLossBudget--
		handled++
	}
	return handled
}

// emitUnitRepair retransmits one retained source id as a unit repair — the exact
// singleton wire shape (Kind=Repair, WindowBase=id, N=1, RepairKey=0) with the
// payload built by a one-symbol encoder, so it carries the key-0 COEFFICIENT-
// multiplied bytes the decoder expects (a raw-source payload decodes to garbage:
// GenCoeffs(0,1)[0] != 1).
func (s *SlidingSender) emitUnitRepair(now clock.Timestamp, id uint32) bool {
	src, ok := s.enc.Source(id)
	if !ok {
		return false // slid out of retention
	}
	dl, ok := s.deadlines[id]
	if !ok {
		dl = now.Add(s.cfg.BufferMicros)
	}
	if s.cfg.OutageAware && now.Add(s.rttMicros/2).After(dl) {
		s.stats.DeadReactiveSkips++
		return false // provably cannot arrive in time
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	enc := code.NewEncoderAt(s.cfg.SymbolSize, id)
	enc.Add(cp)
	_, n, pay := enc.Repair(0)
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: id, SrcIndex: 0, N: uint16(n),
		RepairKey: 0, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	s.stats.ReactiveRepair++
	s.stats.RepairDeficit++
	return true
}

// Tick idle-flushes a band-repair when no source has arrived for a while.
func (s *SlidingSender) Tick(now clock.Timestamp) {
	if s.enc.Len() > 0 && now.Sub(s.lastWrite) >= flushIdleMicros && now.Sub(s.lastRepair) >= flushIdleMicros {
		s.emitRepair(now)
	}
}

// PollSend returns the next datagram to transmit, or nil/false.
func (s *SlidingSender) PollSend() ([]byte, bool) {
	if len(s.sendQ) == 0 {
		return nil, false
	}
	d := s.sendQ[0]
	s.sendQ = s.sendQ[1:]
	return d, true
}

// Stats returns a snapshot of what has been emitted.
func (s *SlidingSender) Stats() SenderStats {
	st := s.stats
	st.RecoveryCadenceFrames = s.cadence.encoderControl().RecoveryCadenceFrames
	return st
}

// EncoderControl returns the current advisory source-control request for an attached encoder.
func (s *SlidingSender) EncoderControl() EncoderControl {
	return withResync(s.cadence.encoderControl(), &s.resync, s.ltrFidByStart)
}

func (s *SlidingSender) codeRate() float64 {
	b := s.effectiveBand()
	// Cold start: until the loss estimate has had time to prime and feed back (the
	// first ~RTT of writes, before which feedback CANNOT have arrived), size the
	// proactive rate for a conservative assumed loss instead of the bare Redundancy
	// floor. The band coder cannot reactively rescue these early ids — its repair
	// covers the trailing window, and by the time the first feedback lands they have
	// aged more than a band behind the frontier — so they must be provisioned at
	// send time or they are lost (the warmup residual the oracle pins). This costs a
	// bounded one-time burst of extra repair at stream start; on a clean link it
	// lifts after a couple feedback rounds (pEst stays low, the normal set-point
	// resumes). The generation coder gets this robustness from reactive per-
	// generation re-repair, which the trailing-band sliding profile cannot replicate.
	p := s.pEst
	cold := s.fbCount < coldStartFeedbacks
	if cold && p < coldStartP {
		p = coldStartP
	}
	// The set-point depends only on (effectiveBand, p, burstQ8, the reactive-rounds
	// credit, the floor-decay verdict) — delta
	// is constant — so return the memo when none has moved. These change only on
	// feedback / slow RTT drift, so the expensive GE-tail search runs once per change
	// instead of once per source symbol (the per-symbol cadence was the sliding
	// profile's CPU/alloc pathology). The cached value is byte-identical to
	// recomputing it. The cold-start window bypasses the memo (it is brief and p is
	// being floored).
	rounds := reactiveRoundsFrom(s.cfg.BufferMicros, s.rttMicros)
	floorOff := s.floorDecayed(rounds)
	relief := s.deltaReliefOn()
	if !cold && s.crValid && b == s.crBand && p == s.crPEst && s.burstQ8 == s.crBurstQ8 &&
		rounds == s.crRounds && floorOff == s.crFloorOff && relief == s.crRelief {
		if cap := s.proactiveCap(); s.crRate > cap {
			return cap // the headroom/breaker caps vary per feedback, outside the memo key
		}
		return s.crRate
	}
	delta := s.cfg.targetFailure()
	if relief {
		// Budget-conditional δ relief (SlidingReactiveShift): a real-timing glassbench cell
		// (ge48 10%, 2.5×RTT ≈ 1.76 honest cycles) measured the δ=1e-3 GE set-point
		// FLOODING — 182% overhead and a 10% delivery median, against 151% and 66%
		// at δ=1e-2 — because just below the outage-censoring horizon the burst term
		// sizes for a tail the wire cannot afford, and the flood's queueing costs
		// more than the protection buys. Where the budget affords ≥1.5 honest
		// reactive cycles the retro tier carries the tail instead, so δ loosens one
		// decade toward the validated 1e-2 (never past it, and never gated at the
		// frontier: at 1.06 cycles the same knob measured a ~1pp delivery COST, so
		// the boundary sits between — see deltaReliefOn). Proactive sizing only;
		// reactive deficit sizing keeps the base δ.
		if delta *= deltaReliefFactor; delta > deltaReliefCap {
			delta = deltaReliefCap
		}
	}
	r := repairForTarget(b, p, delta, maxRepairFactor)
	if ge := repairForGE(b, int(p*1e6), s.burstQ8, delta, maxRepairFactor); ge > r {
		// Burst-aware set-point (N2): size for the GE tail on a bursty channel.
		// NOTE (two-regime control): a saturation fallback — hold the recoverable
		// set-point when the GE tail cannot reach δ at any affordable count — was
		// built and REJECTED by the glassbench burst48 gate: at the bench's tiny
		// per-window horizon (deadline-clipped band ≈ 8, ~57 pkt/s) it cut repair
		// 49% but cost ~35 ffprobe frames — the "unreachable-δ" blanket overhead is
		// in fact the only edge protection when nearly every burst spans the window.
		// Numbers in docs/decisions/2026-07-02-outage-composure.md; do not re-add
		// without a horizon-deep bench cell.
		//
		// The SlidingReactiveShift margin discount below is a DIFFERENT mechanism from that
		// rejected fallback: it shrinks the margin only in proportion to measured
		// reactive capability (rounds = budget over the honest cycle), so at the tiny
		// horizons that killed the fallback rounds is 0 and the full GE margin is
		// carried unchanged — the guard is structural, not estimated.
		switch {
		case relief:
			// ARQ-mode (SlidingReactiveShift at >= 1.5 honest cycles): the burst
			// tail belongs ENTIRELY to the retro-reactive tier — proactive carries
			// only the i.i.d. set-point at the relieved δ. The real-timing
			// head-to-head motivated this: at ge48/2.5×RTT the GE-sized flood
			// delivered a 9% median while ARQ transports calmly retransmitted the
			// actual 10% loss for ~92%; the coded analog of that behavior is a lean
			// proactive layer plus deficit-driven retrospective repair with raised
			// caps (see reactive/retroMaxWindowsARQ). The flood breaker backstops
			// the transition regimes.
		case s.cfg.SlidingReactiveShift && rounds >= reactiveFloorSafe:
			// Two-margin offload port (generation repairCountFor): the BURST margin
			// shrinks by 1/(rounds+1); the VARIANCE margin sheds only on a
			// memoryless channel (roundsEff → 0 as the mean run length grows).
			// Engages only at rounds >= reactiveFloorSafe — a single-round credit
			// measured a -51 pp per-seed collapse in a mid-budget GE cell.
			burstMargin := ge - r
			r += burstMargin / (rounds + 1)
			if roundsEff := rounds * burstQ8One / s.burstQ8; roundsEff > 0 {
				mean := meanRepairCount(b, p)
				if varMargin := r - burstMargin/(rounds+1) - mean; varMargin > 0 {
					r = mean + varMargin/(roundsEff+1) + burstMargin/(rounds+1)
				}
			}
		default:
			r = ge
		}
	} else if s.cfg.SlidingReactiveShift && rounds >= reactiveFloorSafe {
		if roundsEff := rounds * burstQ8One / s.burstQ8; roundsEff > 0 {
			mean := meanRepairCount(b, p)
			if varMargin := r - mean; varMargin > 0 {
				r = mean + varMargin/(roundsEff+1)
			}
		}
	}
	rate := float64(r) / float64(b)
	if floorOff {
		// Confirmed clean zeroes the WHOLE proactive set-point, not just the static
		// floor: 64 consecutive settled-clean reports prove the wire lost nothing
		// for >1.2s, so any pEst/burstQ8 above zero here is by construction a
		// reorder ghost of the raw-order sizing walks (a real loss would have
		// settled dirty within the holdoff and reset cleanRun) — sizing proactive
		// repair from a known-false estimate is pure waste. Measured (arming sim,
		// clean link + 3ms reorder): ghost pEst held the rate ABOVE the floor, so
		// removing only the floor removed nothing. Exit is the same evidence path
		// as the floor re-arm: first settled loss resets cleanRun and full sizing
		// resumes instantly at the raw walks' (ghost-inflated, aggressive) view.
		rate = 0
	} else if rate < s.cfg.Redundancy {
		rate = s.cfg.Redundancy
	}
	if !cold {
		s.crBand, s.crPEst, s.crBurstQ8, s.crRounds, s.crFloorOff, s.crRelief, s.crRate, s.crValid =
			b, p, s.burstQ8, rounds, floorOff, relief, rate, true
	}
	if cap := s.proactiveCap(); rate > cap {
		rate = cap // headroom cap (continuous) / flood breaker (backstop): see updateHeadroom
	}
	return rate
}

// deltaReliefFactor/deltaReliefCap: the δ relief loosens the PROACTIVE target one
// decade, capped at the real-timing-validated 1e-2 (a base δ already at/past the
// cap gets no relief). deltaReliefMinCyclesX2 is the qualifying budget in HALF
// honest cycles: 3 (=1.5 cycles) sits between the validated win at 1.76 cycles and
// the measured mild loss at 1.06 cycles.
const (
	deltaReliefFactor      = 10
	deltaReliefCap         = 1e-2
	deltaReliefMinCyclesX2 = 3
)

// deltaReliefOn reports whether the budget-conditional δ relief qualifies: gated on
// SlidingReactiveShift (it is a reactive-offload lever), a base δ below the cap, and a
// budget of at least 1.5 honest reactive cycles at the current RTT estimate.
func (s *SlidingSender) deltaReliefOn() bool {
	if !s.cfg.SlidingReactiveShift || s.cfg.targetFailure() >= deltaReliefCap {
		return false
	}
	cycle := reactiveCycleMicros(s.rttMicros)
	return cycle > 0 && 2*s.cfg.BufferMicros >= deltaReliefMinCyclesX2*cycle
}

// floorDecayed is the sliding port of the generation sender's effectiveFloor rule:
// drop the static Redundancy floor only when the link is CONFIRMED clean
// (cleanFloorConfirm consecutive feedbacks positively reporting the strict clean
// composite — never mere signal absence: warmup and black holes keep the full
// floor) AND the retro-reactive tier can run at least reactiveFloorSafe rounds
// inside the budget, so a loss onset is caught reactively with margin. On a
// confirmed-clean, reactive-capable link the floor recovers nothing — it is pure
// overhead the ARQ competitors do not pay.
//
// DEFAULT-ON (2026-07-04), previously gated on SlidingReactiveShift: the glass
// clean cell (rtt 60, 3×RTT budget) measured 15.7% standing overhead at 100%
// delivery against 3.3% with the decay engaged, identical delivery — the floor
// premium was meld's largest real cost vs ARQ transports on clean links. The
// eligibility gate is structural: at 2.5×RTT and below, rounds < reactiveFloorSafe
// and the full floor is retained (measured: the decay cannot and does not engage
// there). The composite detector never armed across 1%-loss, 10% GE-burst, and
// heavy-reorder trace surveys; see cleanRun's keying in FeedFeedback.
func (s *SlidingSender) floorDecayed(rounds int) bool {
	return s.cleanRun >= cleanFloorConfirm && rounds >= reactiveFloorSafe
}

// reactive answers a feedback deficit with coded repair over the STUCK window at
// the delivery cursor (fb.DecodedLowEdge) — retrospective repair. The former
// trailing-band form could not, structurally, fix a burst at any budget: by the
// time feedback reports the holes (cadence + RTT), new source has slid the band
// hundreds of symbols past them, so trailing repair carried no innovation for the
// stuck window and the deadline eviction was the only exit (the measured low-rate
// burst48 deficit vs SRT). The encoder retains everything the receiver has not
// acknowledged (SlideTo(DecodedLowEdge)), so RepairAt can cover exactly the window
// the cursor is stuck on; the band decoder folds already-delivered columns via its
// recent map and rejects what it cannot use.
// retroMaxWindows bounds one retro round's span: up to this many band-strided
// repair windows from the stuck cursor. The whole reported deficit must be
// addressed in ONE round when possible — the per-symbol deadline wavefront moves
// at the source rate, so a hole deferred to a second round is usually a hole lost.
const retroMaxWindows = 4

// retroMaxWindowsARQ is the retro span cap in ARQ-mode (SlidingReactiveShift at a
// >= 1.5-cycle budget): with the proactive burst margin shed entirely, the retro
// tier is the ONLY burst recovery, so it must be allowed to cover a whole deep
// burst in one round. wireLossBudgetCapARQ raises the evidence-budget cap in step.
const (
	retroMaxWindowsARQ   = 12
	wireLossBudgetCap    = 8
	wireLossBudgetCapARQ = 24
)

func (s *SlidingSender) reactive(now clock.Timestamp, cursor uint32, deficit int) {
	if s.enc.Len() == 0 {
		return
	}
	band := s.effectiveBand()
	if deficit <= 0 || s.wireLossBudget <= 0 {
		return // no deficit, or no wire-loss evidence backing it (in-flight transit)
	}
	if !s.reactiveReachable() {
		// Sub-cycle budget (the frontier regime): a retro repair emitted NOW arrives
		// after the stuck window's deadline wavefront by construction — pure waste
		// (measured: ~120 dead symbols per burst at a 1xRTT budget). Proactive margins
		// and the singleton/anchor extras own this regime; they stay on exactly here.
		return
	}
	// Debounce successive rounds by half the honest cycle: long enough that a new
	// round is sized against a deficit the previous round has visibly shrunk, short
	// enough that a residual round can still fit the deadline wavefront.
	// NOTE (one-round retro, REFUTED 2026-07-04): replacing this debounce with
	// in-flight innovation accounting (answer every report's deficit delta
	// immediately) was built and measured a WASH on the ge48@2.5×RTT glass hole
	// (median 14.6→14.4%, 16 paired seeds, overhead unchanged) — the stall is not
	// sender response latency; on unbounded loopback there is no queue delaying the
	// response either. The hole's binder is receiver/stream-structural (which
	// equations complete the hole's rank, and when) — measure rank-growth-vs-time
	// in the trace-replay harness before the next attempt.
	interval := reactiveCycleMicros(s.rttMicros) / 2
	if interval < minReactiveIntervalMicros {
		interval = minReactiveIntervalMicros
	} else if interval > maxReactiveIntervalMicros {
		interval = maxReactiveIntervalMicros
	}
	if s.lastReactive != 0 && now.Sub(s.lastReactive) < interval {
		return
	}
	// The repair span: band-strided windows anchored at the stuck cursor (clipped to
	// retention), covering the WHOLE reported deficit up to the caps.
	base := cursor
	if encBase := s.enc.Base(); base < encBase {
		base = encBase
	}
	if s.reactiveBase != base {
		s.reactiveBase, s.reactiveSent = base, 0
	}
	maxWin := retroMaxWindows
	if s.deltaReliefOn() {
		maxWin = retroMaxWindowsARQ // ARQ-mode: the retro tier owns the burst tail
	}
	span := deficit
	if cap := maxWin * band; span > cap {
		span = cap
	}
	limit := maxRepairFactor*span - s.reactiveSent
	if limit <= 0 {
		return // this stuck window has had its fill; the deadline decides the rest
	}
	// Size against the loss the stuck span actually experienced, not the global EWMA
	// (which reports 0 through warmup and lags a burst): being `deficit` short of a
	// span protected at the current code rate implies ≈ (span·rate + deficit) of the
	// span·(1+rate) sent were lost — a lag-free estimate.
	p := s.pEst
	if sp := float64(span); sp > 0 {
		proactive := sp * s.codeRate()
		if pGen := (proactive + float64(deficit)) / (sp + proactive); pGen > p {
			p = pGen
		}
	}
	// NOTE (narrow-window retro, REFUTED 2026-07-04): marching the retro windows at
	// width 16 instead of the band — the closure-law candidate after the burst
	// autopsy showed values emerge only at full-rank closure of coupled spans —
	// measured a WASH on the ge48@2.5×RTT glass hole (median 14.2 vs 15.7%, 16
	// paired seeds, guards/overhead unchanged), as did the one-round debounce
	// removal before it. The retro tier is ~9% of repair volume at that cell and is
	// not the release vector; the hole's binder sits in the proactive stream's
	// closure dynamics + in-order amplification. Next candidate with a straight
	// causal line: feedback carrying a which-ids bitmap for the stuck neighborhood
	// so the sender can answer with unit repairs (true retransmissions — base=id,
	// n=1 — which close instantly); needs a wire-tail extension and its own arc.
	sent, remaining := 0, deficit
	for w := 0; w < maxWin && remaining > 0 && sent < limit; w++ {
		wBase := base + uint32(w*band)
		wDef := remaining
		if wDef > band {
			wDef = band
		}
		batch := symbolsForDeficit(wDef, p, s.cfg.targetFailure(), maxRepairFactor)
		if batch > limit-sent {
			batch = limit - sent
		}
		emitted := 0
		for i := batch; i > 0; i-- {
			if !s.emitRepairAt(now, wBase, band) {
				break // past retention: nothing left to cover
			}
			emitted++
		}
		if emitted == 0 {
			break
		}
		sent += emitted
		remaining -= wDef
	}
	if sent == 0 {
		return // limit-blocked or past retention: do NOT burn the debounce slot
	}
	s.lastReactive = now
	s.reactiveSent += sent
	if s.wireLossBudget -= deficit - remaining; s.wireLossBudget < 0 {
		s.wireLossBudget = 0 // addressed holes are spent; fresh wire loss re-arms
	}
}

// emitRepairAt emits one retrospective repair over the retained window [at, at+n),
// stamped with the deadline of the window's LAST symbol (the horizon until which
// this repair can still matter). Reports whether a repair was actually emitted.
func (s *SlidingSender) emitRepairAt(now clock.Timestamp, at uint32, n int) bool {
	key := s.repairKey
	base, nn, pay := s.enc.RepairAt(key, at, n)
	if nn == 0 {
		return false
	}
	s.repairKey++
	dl, ok := s.deadlines[base+uint32(nn)-1]
	if !ok {
		dl = now.Add(s.cfg.BufferMicros)
	}
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(nn), RepairKey: key, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	s.stats.ReactiveRepair++
	s.stats.RepairDeficit++
	return true
}

func (s *SlidingSender) reactiveSparseProtected(now clock.Timestamp, fb wire.Feedback, deficit int) {
	if deficit <= 0 || s.enc.Len() == 0 || len(s.protected) == 0 {
		return
	}
	if fb.LossRate == 0 {
		return
	}
	// NOTE (two-regime control): a provably-dead gate here — skip when now + rtt/2
	// is past the newest protected deadline — was built and REVERTED: the sliding
	// rttMicros estimate inflates under queueing, and the glassbench high-rate iid
	// 1×RTT guard showed the gate starving UEP reference repair near deadlines
	// (466.8±26.5 ff vs 480±0 with it removed) for a negligible byte saving. The
	// generation reactive keeps its gate (validated harmless by the flow sweep):
	// its per-generation deadline is the window's LATEST symbol, a conservative
	// bound, and its reactive volume is the cost that matters.
	band := s.effectiveBand()
	trailingBase, _ := s.repairWindowBase(band)
	base := fb.DecodedLowEdge
	if encBase := s.enc.Base(); base < encBase {
		base = encBase
	}
	if base >= trailingBase {
		return
	}
	if base >= uint32(band) {
		return // default policy only protects the first reference neighborhood
	}
	winEnd := uint64(s.enc.Base()) + uint64(s.enc.Len())
	if uint64(base) >= winEnd {
		return
	}
	width := band
	if available := int(winEnd - uint64(base)); width > available {
		width = available
	}
	ids := s.protectedIDsIn(base, width)
	if len(ids) == 0 {
		return
	}
	if s.sparseBase != base {
		s.sparseBase, s.sparseSent = base, 0
	}
	remaining := len(ids) - s.sparseSent
	if remaining <= 0 {
		return
	}
	if s.emitSparseRepair(now, ids, true) {
		s.sparseSent++
		if s.sparseSent == 1 && len(ids) > 1 {
			if hi, ok := s.latestSourceID(); ok {
				target := hi + s.protectedRepairGap()
				if s.cfg.ProtectedRepairPhasing {
					target = s.scheduleProtectedRelease(hi, target, protectedLaneBase)
				}
				s.queueSparseRepair(ids, target, true)
				s.sparseSent++
			}
		}
	}
}

// rttMinHalfWindowMicros is the min filter's half-window: two halves roll so a
// valid minimum always spans 1.5-3 s of samples. Long enough that a transient
// self-flood (which drains once the band stops clipping) cannot poison the
// estimate; short enough that a genuine route change is adopted within ~3 s.
const rttMinHalfWindowMicros = 1_500_000

// Flood-breaker constants: the trigger threshold is scaled by (1−pEst) so honest
// heavy loss (arrivals below offer BECAUSE the wire dropped them) never trips it —
// only the wire-overrun signature (arrivals far below what loss explains) does.
const (
	floodTriggerQ8    = 218  // 0.85 in Q8: trigger when arrivals < 0.85×(1−pEst)×offered
	floodTriggerMinQ8 = 128  // …but never above-trigger below half the offer (deep-loss floor)
	floodCapMin       = 0.25 // the cap never starves protection entirely
	floodClearReports = 3    // consecutive clean reports before the cap relaxes
)

// updateFloodBreaker folds one feedback report into the wire-overrun breaker: the
// receiver's source-arrival rate (ΔHighestSeen/Δt) against the offered source rate
// (1/interMicros), EWMA'd in Q8. Sustained arrivals far below what the reported
// loss explains mean the sender itself is overrunning the wire — the pacer queue
// grows without bound, every latency estimate poisons, and delivery collapses (the
// excursion post-mortem). The response is AIMD on the PROACTIVE rate cap only.
func (s *SlidingSender) updateFloodBreaker(now clock.Timestamp, fb wire.Feedback) {
	defer func() {
		s.lastFBAt, s.lastFBHighest, s.lastFBSource, s.lastFBRepair =
			now, fb.HighestSeen, s.stats.Source, s.stats.Repair
	}()
	if s.lastFBAt == 0 || fb.HighestSeen <= s.lastFBHighest {
		return
	}
	offered := s.stats.Source - s.lastFBSource // exact source symbols offered since the last report
	if offered == 0 {
		return
	}
	arrived := int64(fb.HighestSeen-s.lastFBHighest) * 256 / int64(offered) // Q8 ratio vs offer
	if arrived > 512 {
		arrived = 512 // in-flight skew can transiently exceed the window's offer; clamp
	}
	s.arriveRatioQ8 += (arrived - s.arriveRatioQ8) / 4
	s.updateHeadroom(fb, offered, arrived, now.Sub(s.lastFBAt))
	thresh := int64(float64(floodTriggerQ8) * (1 - s.pEst))
	if thresh < floodTriggerMinQ8 {
		thresh = floodTriggerMinQ8
	}
	if s.arriveRatioQ8 < thresh {
		s.floodClear = 0
		if cap := s.floodCap * 0.7; cap >= floodCapMin {
			s.floodCap = cap
		} else {
			s.floodCap = floodCapMin
		}
		return
	}
	if s.floodCap >= maxRepairFactor {
		return // inactive
	}
	if s.floodClear++; s.floodClear >= floodClearReports {
		if s.floodCap *= 1.25; s.floodCap > maxRepairFactor {
			s.floodCap = maxRepairFactor
		}
	}
}

// Headroom-cap constants (Amendment 9). The saturation threshold sits below the
// honest-loss noise floor measured by the three-regime survey (f reads 1.0 under
// 10% GE loss with rare onset dips to ~0.93, and 0.53-0.80 under real overrun),
// so honest loss never tightens the cap; the clear threshold plus the standing-
// queue gate govern the time-based upward probe — small-signal hunting around
// capacity instead of the AIMD's 0.25↔3.0 relaxation oscillation.
const (
	headroomSatF        = 0.90 // saturation evidence: passed-through fraction below this tightens
	headroomClearF      = 0.97 // arrivals track offer above this: eligible to probe upward
	headroomProbePerSec = 0.50 // additive probe rate (time-based: 10 ms event feedbacks must not multiply it)
	headroomSafety      = 0.90 // discount on the measured affordable rate
)

// updateHeadroom folds one report into the affordable-rate ceiling: with the wire
// passing fraction f of the offered (1+r) mix, the affordable proactive rate is
// f·(1+r)−1 — offered load equal to what the wire demonstrably serves. Tighten to
// that (discounted) on saturation evidence; probe upward additively (per unit
// TIME, not per report) only when arrivals track offer AND the RTT min-filter
// window shows no standing queue (at capacity the queue stands even while
// arrivals track offer, and probing then is what re-enters the boom).
//
// f is the INSTANTANEOUS same-interval ratio, deliberately not the breaker's
// EWMA: during the post-tighten queue DRAIN the EWMA keeps reading low while the
// offered mix has already been cut, and recombining the two ratchets the
// "affordable" estimate below zero (measured: the cap slammed to the 0.25 floor
// within two reports of every tighten). The instantaneous ratio reads ≥1 during
// a drain (arrivals = wire service against the reduced offer) — no false
// tighten — and the standing-queue gate keeps the drain from reading as
// probe-eligible.
//
// Tightening requires BOTH kinds of evidence — f low AND a standing queue in
// the RTT min-filter — because a low f alone is ambiguous: GE-burst frontier
// stalls depress the same-interval arrival ratio with no congestion at all, and
// on an UNSATURATED wire that misread strips protection that was load-bearing
// (glass, first cut: ge12@0.75x 97.9→90.6 and ge48@1.5x 98.4→91.0 median, both
// cells where the baseline DELIVERS at 245-256% overhead — the wire affords
// it). Real overrun always stands a queue (sim booms: rtt 61→72-93ms; the hole
// cell's flood self-queues per the excursion post-mortem), so requiring the
// delay signature restores those guards while keeping every true-saturation
// tighten. The tighten queue bar (min + 1/8) sits below the probe's quiet bar
// (min + 1/4): asymmetric hysteresis — tighten on modest queue evidence, probe
// only when the queue is clearly gone. The cap never drops below the breaker's
// protection floor nor below the reported mean-loss replacement rate plus
// margin — a cap under the mean guarantees decode failure, the onset
// false-constraint hazard the survey's transient f≈0.93 dips under honest loss
// demand guarding against.
func (s *SlidingSender) updateHeadroom(fb wire.Feedback, offered uint64, arrivedQ8, dtMicros int64) {
	if !s.cfg.HeadroomAwareSizing {
		return // opt-in (Config.HeadroomAwareSizing): the cap stays inactive
	}
	repOffered := float64(s.stats.Repair-s.lastFBRepair) / float64(offered)
	denom := 1 - float64(fb.LossRate)/65535
	if denom < 0.5 {
		denom = 0.5 // deep-loss floor, as the breaker's trigger: attribution saturates
	}
	f := (float64(arrivedQ8) / 256) / denom
	if f > 1 {
		f = 1
	}
	// The queue witness is the INSTANTANEOUS sample, not the min-filter: the
	// min-window (1.5 s halves) is built to exclude queueing, so it cannot see a
	// sub-window boom (gating on it un-broke the sim limit cycle). The 1/6 bar
	// clears glass natural release wobble (~±13%).
	switch {
	case f < headroomSatF && s.rttSample > s.rttMicros+s.rttMicros/6:
		if cap := headroomSafety * (f*(1+repOffered) - 1); cap < s.headroomCap {
			s.headroomCap = cap
			s.stats.HeadroomTightens++
		}
	case f >= headroomClearF && s.rttMinCur <= s.rttMicros+s.rttMicros/4:
		s.headroomCap += headroomProbePerSec * float64(dtMicros) / 1e6
	}
	if s.headroomCap < floodCapMin {
		s.headroomCap = floodCapMin
	}
	if pFloor := 1.3 * float64(fb.LossRate) / 65535; s.headroomCap < pFloor {
		s.headroomCap = pFloor
	}
	if s.headroomCap > maxRepairFactor {
		s.headroomCap = maxRepairFactor // inactive
	}
}

// proactiveCap is the effective ceiling on the proactive set-point: the continuous
// headroom cap (sizer-integrated, Amendment 9) backstopped by the AIMD flood
// breaker (which should now rarely bind — it fires only when the continuous cap's
// estimate lagged real overrun).
func (s *SlidingSender) proactiveCap() float64 {
	if s.headroomCap < s.floodCap {
		return s.headroomCap
	}
	return s.floodCap
}

func (s *SlidingSender) updateRTT(now clock.Timestamp, fb wire.Feedback) {
	if fb.HighestSeen == 0 {
		return
	}
	dl, ok := s.deadlines[fb.HighestSeen-1]
	if !ok {
		return
	}
	sample := now.Sub(dl.Add(-s.cfg.BufferMicros))
	if sample <= 0 {
		return
	}
	s.rttSample = sample // per-report instantaneous RTT: the queue-delay witness (updateHeadroom)
	if s.rttWinStart == 0 {
		s.rttWinStart, s.rttMinCur, s.rttMinPrev = now, sample, sample
	} else if now.Sub(s.rttWinStart) > rttMinHalfWindowMicros {
		s.rttMinPrev, s.rttMinCur, s.rttWinStart = s.rttMinCur, sample, now
	} else if sample < s.rttMinCur {
		s.rttMinCur = sample
	}
	if s.rttMicros = s.rttMinCur; s.rttMinPrev < s.rttMicros {
		s.rttMicros = s.rttMinPrev
	}
}

// emitRepair emits one proactive trailing-band repair (deficit-answering reactive
// repair goes through emitRepairAt, which does its own attribution).
func (s *SlidingSender) emitRepair(now clock.Timestamp) {
	if s.enc.Len() == 0 {
		return
	}
	key := s.repairKey
	s.repairKey++
	base, n, pay := s.enc.RepairWindow(key, s.effectiveBand())
	dl := s.deadlines[base+uint32(n)-1]
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(n), RepairKey: key, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	if s.fbCount < coldStartFeedbacks {
		s.stats.RepairProactiveCold++
	} else {
		s.stats.RepairProactive++
	}
}

func (s *SlidingSender) emitSparseRepair(now clock.Timestamp, ids []uint32, reactive bool) bool {
	if len(ids) == 0 {
		return false
	}
	key := s.repairKey
	pay, ok := s.enc.RepairSparse(key, ids)
	if !ok {
		return false
	}
	s.repairKey++
	dl, ok := s.deadlines[ids[len(ids)-1]]
	if !ok {
		dl = now.Add(s.cfg.BufferMicros)
	}
	copiedIDs := append([]uint32(nil), ids...)
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.SparseRepair, SrcIndex: uint32(key), N: uint16(len(copiedIDs)), RepairKey: key, SparseIDs: copiedIDs, Priority: uepCenterTier + 1, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairSparse++
	if reactive {
		s.stats.ReactiveRepair++
	}
	return true
}

func (s *SlidingSender) emit(sym wire.Symbol) {
	s.sendQ = append(s.sendQ, wire.EncodeSymbol(nil, sym))
}

// SlidingReceiver is the band-form receive half. Same methods as the generation
// Receiver; selected by Config.Sliding.
type SlidingReceiver struct {
	cfg        Config
	dec        *code.BandDecoder
	directRecv map[uint32]bool
	lateDrops  uint64
	lastFB     clock.Timestamp
	fedOnce    bool
	fbDue      bool // loss-onset event: emit feedback now (rate-limited), don't wait the cadence
	deliverQ   []deliveredSym
	sendQ      [][]byte
	stats      ReceiverStats

	// Reorder window for the loss estimate (lossrun.go); see the generation receiver.
	// fastExpect is the UNDELAYED walk's cursor (fastLossWalk): honest counters and
	// loss-onset events fire on raw arrival order; estimators settle behind the window.
	reorder        reorderWindow
	fastHaveExpect bool
	fastExpect     uint32

	// Settled-loss walk (wire.Feedback.SettledLost): a second, ALWAYS-ON adaptive
	// reorder window whose resequenced gap walk counts only losses that survived the
	// holdoff — the reorder-tolerant clean/dirty evidence the sender's floor decay
	// keys on. It feeds NOTHING else: the sizing estimators keep the raw-order walks
	// above (a settled sizing walk reads truer burst lengths, and the honest GE
	// set-point then exceeds paced wire headroom — the measured breaker/set-point
	// limit cycle that refuted the holdoff port for sizing, twice).
	settled          reorderWindow
	settledStarted   bool
	settledNext      uint32
	settledLostSince int // settled-lost ids since the last feedback report

	// Per-symbol deadline. Each directly-received id carries its own stamped deadline (write
	// time + budget); the receiver delivers/evicts by THAT, so an access unit written as one
	// burst — a whole video frame at one instant, sharing one deadline — is not gated by the
	// uniform-spacing fit it violates (the clean-link premature-drop cliff). Pruned as the
	// cursor advances past each id.
	symDL map[uint32]clock.Timestamp
	// Deadline extrapolation (lossrun.go), used ONLY for never-directly-received
	// (recovered/missing) ids: fit deadline(id) = refDL + (id-refID)*intervalUs.
	deadlineFit

	// Channel-erasure-rate estimator.
	lossStarted bool
	lossBase    uint32
	lossHighest uint32
	lossRecv    int
	pEWMA       float64
	pHold       float64

	// Forward-gap walk for the mean loss-run length (N2 burst-aware sizing) and the
	// pre-recovery wire-loss count (N1 — the honest congestion signal, never
	// decremented on decode; it also arms the retro-reactive tier). Shares the
	// generation receiver's loss-run machinery (lossrun.go).
	haveExpect bool
	expectNext uint32
	lossRunObserver

	frames      map[uint32]*frameInfo
	frameStarts []uint32
	curStart    uint32
	haveCur     bool
	idDelivered map[uint32]bool
	fstats      FrameStats

	// LTR resync feedback state (lossrun.go).
	ltrResyncState
}

func NewSlidingReceiver(cfg Config) *SlidingReceiver {
	r := &SlidingReceiver{
		cfg:             cfg,
		dec:             code.NewBandDecoder(cfg.SymbolSize, cfg.codingWindow(), slidingMaxWin),
		directRecv:      make(map[uint32]bool),
		symDL:           make(map[uint32]clock.Timestamp),
		deadlineFit:     deadlineFit{intervalUs: 1},
		lossRunObserver: lossRunObserver{meanBurstQ8: burstQ8One},
	}
	// REFUTED for default-on (permutation sweep, 2026-07-03): wiring the reorder
	// window to AutoReorderHoldoff on the SLIDING profile destabilized every paced
	// lossy cell (proactive repair ~3x to the ceiling, p99 x3-5, delivery -6 to
	// -12 pp vs the un-windowed walk, same seeds) through a mid-run estimator
	// excursion the diagnostics did not fully isolate (final pEst LOWER yet the
	// sizer flooding; suspicion: the censored loss window freezing against
	// queueing-delayed settles). The generation profile keeps its cref-validated
	// holdoff. Until the sliding interaction is explained and re-validated, the
	// window engages here only on an EXPLICIT ReorderHoldoffMicros (experimental);
	// the lateSink credit (a late-settled arrival still counts RECEIVED for the
	// rate window) guards the known queueing-runaway mode when it is enabled.
	r.reorder = reorderWindow{cfgHoldoff: cfg.ReorderHoldoffMicros, auto: false,
		budget:   cfg.BufferMicros,
		sink:     func(id uint32, _ uint8) { r.observeLoss(id) },
		lateSink: func(id uint32, _ uint8) { r.observeLossWindow(id) }}
	// The settled-loss walk is always on, with a FIXED, conservatively generous
	// holdoff (an explicit ReorderHoldoffMicros overrides it). Fixed, not the
	// adaptive AutoReorderHoldoff dynamics: the adaptive window decays 1/8 on every
	// filled gap, so under steady reorder it keeps shrinking back below the spread
	// and periodically settles a merely-late id lost — one such false settle per
	// 1.3 s is enough to keep resetting a 64-consecutive-clean detector forever
	// (measured: the arming sim never armed on a clean reordered link). Estimator
	// tolerance forgives occasional miscounts; a consecutive-run detector does not.
	// Over-holding is the fail-safe direction here — it only delays loss evidence,
	// bounding floor re-arm at holdoff + one report cadence, which the decay's
	// rounds >= reactiveFloorSafe eligibility already covers with margin. Its gap
	// walk counts a loss only after the holdoff proves the id absent. No lateSink:
	// this walk feeds no rate window, so the queueing-runaway credit does not apply.
	r.settled = reorderWindow{cfgHoldoff: settledHoldoffMicros(cfg),
		budget: cfg.BufferMicros,
		sink: func(id uint32, _ uint8) {
			if r.settledStarted && id > r.settledNext {
				r.settledLostSince += int(id - r.settledNext)
			}
			r.settledStarted, r.settledNext = true, id+1
		}}
	return r
}

// settledHoldoffMicros is the settled-loss walk's fixed reorder holdoff: an
// explicit ReorderHoldoffMicros wins; otherwise an eighth of the deadline budget,
// clamped to [10ms, 30ms] — generous against plausible reorder spreads (the glass
// jitter cells measure <= 5ms; real-path reorder is typically single-digit ms)
// while adding at most one-to-two report cadences of floor re-arm latency.
func settledHoldoffMicros(cfg Config) int64 {
	if cfg.ReorderHoldoffMicros > 0 {
		return cfg.ReorderHoldoffMicros
	}
	h := cfg.BufferMicros / 8
	if h < 10_000 {
		h = 10_000
	}
	if h > 30_000 {
		h = 30_000
	}
	return h
}

// FeedSymbolECN absorbs one inbound symbol with its IP ECN codepoint. The sliding profile
// has no delay/ECN-reactive congestion controller (it paces to the static MaxBitrate), so the
// codepoint is accepted for a uniform host interface and ignored; FeedSymbol is the live path.
func (r *SlidingReceiver) FeedSymbolECN(now clock.Timestamp, datagram []byte, _ ECN) {
	r.FeedSymbol(now, datagram)
}

// FeedSymbol decodes and absorbs one inbound symbol datagram, delivering ready
// symbols in order.
func (r *SlidingReceiver) FeedSymbol(now clock.Timestamp, datagram []byte) {
	sym, err := wire.DecodeSymbol(datagram)
	if err != nil || sym.Flow != r.cfg.Flow {
		return
	}
	if len(sym.Payload) != r.cfg.SymbolSize {
		// A coded symbol is exactly SymbolSize on the wire; a different length means the peer's
		// SymbolSize disagrees with ours (it is configured per-end, not negotiated). Reject it
		// rather than truncate/zero-pad it into the band and corrupt recovery (the genBaseOf class).
		r.stats.Rejected++
		return
	}
	dl := clampDeadline(now, clock.Timestamp(sym.Deadline), r.cfg.BufferMicros)
	switch sym.Kind {
	case wire.Systematic:
		id := sym.SrcIndex
		if !r.admit(id) {
			r.stats.Rejected++
			return
		}
		// Count the network arrival for the erasure estimate independent of the
		// deadline/cursor — a symbol received late was not dropped by the channel, so
		// counting it as loss would inflate the redundancy controller (see the
		// generation receiver). Each id's systematic arrives once. The reorder window
		// (lossrun.go) resequences the estimate's input, so reordered-late arrivals
		// are counted received rather than as fictitious loss — without it, real
		// timing jitter kept LossRate nonzero on CLEAN links, permanently blocking
		// the floor decay and inflating pEst-driven repair (the pathology the
		// generation receiver's holdoff was built for; the sliding profile simply
		// never got the port).
		if r.reorder.enabled() {
			// Split walk: the honest counters (WireLost/CongestionLoss) and the
			// loss-onset event fire IMMEDIATELY on the raw arrival order — they arm
			// the retro-reactive tier and detection must not wait out the holdoff
			// (delaying them measurably collapsed lossy delivery). The sizing
			// estimators settle behind the reorder window via the resequenced sink.
			r.fastLossWalk(id)
			r.reorder.feed(now, id, 0)
		} else {
			r.observeLoss(id)
		}
		if sym.HasFrameDesc {
			r.noteFrame(sym)
		}
		// The settled walk observes WIRE arrivals, so it must be fed BEFORE the
		// duplicate/cursor gate: a reorder-ghost repair can RECOVER an in-flight id,
		// advancing the cursor past it, and the real systematic then lands here as a
		// "duplicate" — gating the walk on novelty made it settle that id lost on a
		// zero-loss link (measured: 1-3 false settled losses per report, exactly the
		// ghost-reactive rate, holding cleanRun at zero forever). A true wire dup is
		// harmless to the walk: its second copy takes the id<next late path, which
		// counts nothing under the fixed holdoff.
		r.settled.feed(now, id, 0)
		if id < r.dec.Cursor() || r.directRecv[id] {
			r.stats.Duplicates++
		} else {
			r.directRecv[id] = true
			r.symDL[id] = dl // gate this id by its own true deadline, not the uniform-spacing fit
		}
		r.updateRef(id, dl)
		r.observeSlack(now, dl)
		r.dec.AddSystematic(id, sym.Payload)
	case wire.Repair:
		n := int(sym.N)
		if n <= 0 || uint64(sym.WindowBase)+uint64(n) > 1<<32 {
			return // non-positive width, or a window that wraps the id space (forged)
		}
		hi := sym.WindowBase + uint32(n) - 1
		if !r.admit(hi) {
			r.stats.Rejected++
			return
		}
		r.updateRef(hi, dl)
		r.dec.AddRepair(sym.WindowBase, n, sym.RepairKey, sym.Payload)
	case wire.SparseRepair:
		if len(sym.SparseIDs) == 0 {
			return
		}
		hi := sym.SparseIDs[len(sym.SparseIDs)-1]
		if !r.admit(hi) {
			r.stats.Rejected++
			return
		}
		r.updateRef(hi, dl)
		r.dec.AddSparseRepair(sym.SparseIDs, sym.RepairKey, sym.Payload)
	}
	r.pump(now)
	r.maybeFeedback(now)
}

// admit bounds the frontier jump a single datagram can force on the band decoder. The
// decoder's grow() advances the frontier one id at a time (delivering or skipping each), so a
// forged id far beyond the window would stall it for O(id-cursor) iterations — a one-packet
// receiver hang. This mirrors the generation receiver's admit() horizon: an id within the
// delivery window is cheap; one more than a full window past the frontier can never be
// delivered in-window, so it is refused. coverID is the highest source id the symbol touches.
func (r *SlidingReceiver) admit(coverID uint32) bool {
	if coverID < r.dec.Cursor() {
		return true // duplicate of an already delivered/skipped id — no frontier growth
	}
	if h := r.dec.Highest(); coverID >= h && coverID-h > uint32(slidingMaxWin) {
		return false
	}
	return true
}

// Tick advances time, enforcing deadline skips and periodic feedback.
func (r *SlidingReceiver) Tick(now clock.Timestamp) {
	if r.reorder.enabled() {
		r.reorder.drain(now) // settle losses whose holdoff expired without a new arrival
	}
	if r.settled.enabled() {
		r.settled.drain(now) // the settled-loss walk expires holdoffs on the clock too
	}
	r.pump(now)
	r.maybeFeedback(now)
}

// PollDeliver returns the next in-order delivered source symbol (id + chunk), or
// 0/nil/false. The id is the host's AEAD nonce input (epoch‖src_index).
func (r *SlidingReceiver) PollDeliver() (uint32, []byte, bool) {
	if len(r.deliverQ) == 0 {
		return 0, nil, false
	}
	d := r.deliverQ[0]
	r.deliverQ = r.deliverQ[1:]
	return d.id, d.data, true
}

// PollSend returns the next feedback datagram, or nil/false.
func (r *SlidingReceiver) PollSend() ([]byte, bool) {
	if len(r.sendQ) == 0 {
		return nil, false
	}
	d := r.sendQ[0]
	r.sendQ = r.sendQ[1:]
	return d, true
}

// Stats returns delivery outcomes.
func (r *SlidingReceiver) Stats() ReceiverStats {
	s := r.stats
	s.Lost = r.dec.Lost() + r.lateDrops
	return s
}

// FrameStats satisfies the media-aware receiver contract; the sliding profile does not
// parse the codec; it derives decodability from WriteFrame descriptors on systematic symbols.
func (r *SlidingReceiver) FrameStats() FrameStats {
	if r.haveCur {
		r.resolveFrame(r.curStart)
	}
	return r.fstats
}

func (r *SlidingReceiver) pump(now clock.Timestamp) {
	for {
		r.drainDeliver(now)
		c := r.dec.Cursor()
		if c >= r.dec.Highest() {
			return
		}
		// c is a gap: never directly received (a directly-received systematic decodes on
		// arrival and is drained above), not yet recovered. Skip it once its own deadline has
		// passed, OR — the monotonic backstop ported from the generation receiver — once an id
		// AT OR ABOVE the cursor is itself overdue (refID >= c && now > refDL): deadlines are
		// non-decreasing in id, so the highest stamped id's deadline bounds c's, and a gap whose
		// fit is unprimed or too tight is still not evicted before a provably-overdue neighbor.
		if r.cfg.EvictBrokenFrames && r.frameDoomed(c) {
			if !r.dec.Evict() {
				return
			}
			r.evictAt(c)
			continue
		}
		gd, gdKnown := r.deadlineOf(c)
		overdue := (gdKnown && now.After(gd)) || (r.haveRef && r.refID >= c && now.After(r.refDL))
		if !overdue {
			return // wait: c is not yet past due and nothing above it is either
		}
		if !r.dec.Skip() {
			return
		}
		r.attributeFrame(c, true)
		delete(r.directRecv, c)
		delete(r.symDL, c)
	}
}

func (r *SlidingReceiver) drainDeliver(now clock.Timestamp) {
	for {
		rec, ok := r.dec.Deliver()
		if !ok {
			return
		}
		// Late-drop a DIRECTLY-RECEIVED symbol whose OWN stamped deadline (symDL) passed
		// while it waited behind an earlier gap. A RECOVERED id has no stamp — only the
		// extrapolated fit — and the old policy delivered it unconditionally on the
		// claim that pump bounds recovered-id lateness; the config fuzzer FALSIFIED
		// that bound (post-outage retro recovery delivered whole stale windows up to
		// ~3× the budget late — dead-on-arrival data pushed at the app). A recovered
		// id is now dropped when it is PROVABLY long-expired, by either witness:
		// (a) the monotone stamp bound — deadlines never decrease in id, so an id at
		// or below refID whose refDL has passed is expired by an EXACT stamp — or
		// (b) the fit says it expired more than a generous grace ago (the grace
		// absorbs fit error so the premature-drop guarantee stands; budget/8 within
		// [10 ms, 25 ms]).
		if r.cfg.EvictBrokenFrames && r.frameDoomed(rec.ID) {
			r.evictAt(rec.ID)
			continue
		}
		if dl, ok := r.symDL[rec.ID]; ok && now.After(dl) {
			r.lateDrops++
			r.attributeFrame(rec.ID, true)
			delete(r.directRecv, rec.ID)
			delete(r.symDL, rec.ID)
			continue
		}
		if _, direct := r.symDL[rec.ID]; !direct && !r.directRecv[rec.ID] {
			expired := r.haveRef && rec.ID <= r.refID && now.After(r.refDL)
			if !expired {
				if dl, ok := r.deadline(rec.ID); ok && now.Sub(dl) > r.lateRecoveryGraceMicros() {
					expired = true
				}
			}
			if expired {
				r.lateDrops++
				r.attributeFrame(rec.ID, true)
				delete(r.directRecv, rec.ID)
				continue
			}
		}
		r.attributeFrame(rec.ID, false)
		r.deliverQ = append(r.deliverQ, deliveredSym{rec.ID, append([]byte(nil), rec.Data...)})
		r.stats.Delivered++
		if !r.directRecv[rec.ID] {
			r.stats.Recovered++
		}
		delete(r.directRecv, rec.ID)
		delete(r.symDL, rec.ID)
	}
}

// deadlineOf returns id's delivery deadline: the symbol's own stamp when it was directly
// received, else the uniform-spacing extrapolation (the only estimate available for an id
// that was recovered or never arrived). The bool is false when neither is known yet.
func (r *SlidingReceiver) deadlineOf(id uint32) (clock.Timestamp, bool) {
	if dl, ok := r.symDL[id]; ok {
		return dl, true
	}
	return r.deadline(id)
}

func (r *SlidingReceiver) maybeFeedback(now clock.Timestamp) {
	if r.fedOnce {
		since := now.Sub(r.lastFB)
		if since < feedbackIntervalMicros && !(r.fbDue && since >= eventFeedbackMinMicros) {
			return // not yet due: neither the cadence nor a rate-limited loss-onset event
		}
	}
	r.fedOnce = true
	r.fbDue = false
	r.lastFB = now
	def := r.dec.Deficit()
	if def > 0xFFFF {
		def = 0xFFFF
	}
	settledLost := r.settledLostSince
	if settledLost > 0xFFFF {
		settledLost = 0xFFFF
	}
	frames, decFrames, keys, decKeys := feedbackFrameStats(r.fstats)
	r.sendQ = append(r.sendQ, wire.EncodeFeedback(nil, wire.Feedback{
		Flow:               r.cfg.Flow,
		DecodedLowEdge:     r.dec.Cursor(),
		HighestSeen:        r.dec.Highest(),
		Deficit:            uint16(def),
		LossRate:           uint16(r.lossEstimate() * 65535),
		Burstiness:         uint16(r.meanBurstQ8),
		CongestionLoss:     uint16(r.clSinceFB), // pre-recovery loss this interval (N1)
		Frames:             frames,
		DecodableFrames:    decFrames,
		Keyframes:          keys,
		DecodableKeyframes: decKeys,
		NewestDecodableLTR: r.newestDecLTR,
		BrokenAnchors:      r.brokenAnchors,
		// The stuck-neighborhood NACK bitmap: which of the 64 ids above the cursor
		// are missing (covered-but-unproducible). The sender answers set bits with
		// unit repairs — the closure-law-exempt response (see wire.Feedback.Missing).
		Missing: r.dec.MissingIn(r.dec.Cursor()),
		// Settled-loss evidence for the sender's floor decay (reorder-tolerant;
		// see wire.Feedback.SettledLost and the settled walk's construction).
		SettledLost: uint16(settledLost),
	}))
	r.clSinceFB = 0        // per-interval; consumers integrate the reported deltas
	r.settledLostSince = 0 // per-interval, like clSinceFB
	// No open-deficit latch — loss onset only (see the generation receiver's doc).
}

// lateRecoveryGraceMicros is the fit-error allowance before a recovered id is
// dropped as expired: generous against the slope fit's measured error (a few ms)
// so the no-premature-drop guarantee stands, tight enough to bound the stale-
// delivery leak to the same order as the generation profile's residual.
func (r *SlidingReceiver) lateRecoveryGraceMicros() int64 {
	g := r.cfg.BufferMicros / 8
	if g < 10_000 {
		g = 10_000
	}
	if g > 25_000 {
		g = 25_000
	}
	return g
}

func (r *SlidingReceiver) deadline(id uint32) (clock.Timestamp, bool) {
	if !r.haveRef || r.refSamples == 0 {
		return 0, false
	}
	return r.refDL.Add((int64(id) - int64(r.refID)) * r.intervalUs), true
}

func (r *SlidingReceiver) evictAt(id uint32) {
	r.attributeFrame(id, true)
	delete(r.directRecv, id)
	delete(r.symDL, id)
	r.stats.Evicted++
}

func (r *SlidingReceiver) noteFrame(sym wire.Symbol) {
	if r.frames == nil {
		r.frames = make(map[uint32]*frameInfo)
	}
	if r.frames[sym.FrameStart] != nil {
		return
	}
	r.frames[sym.FrameStart] = &frameInfo{
		refs: sym.FrameRefs, length: sym.FrameLen, rap: sym.FrameRAP,
		nonPic: sym.FrameNonPicture, disc: sym.FrameDiscardable, ltr: sym.FrameLTR,
	}
	i := sort.Search(len(r.frameStarts), func(i int) bool { return r.frameStarts[i] >= sym.FrameStart })
	r.frameStarts = append(r.frameStarts, 0)
	copy(r.frameStarts[i+1:], r.frameStarts[i:])
	r.frameStarts[i] = sym.FrameStart
}

func (r *SlidingReceiver) attributeFrame(id uint32, lost bool) {
	if r.frames == nil {
		return
	}
	r.recordDelivery(id, !lost)
	i := sort.Search(len(r.frameStarts), func(i int) bool { return r.frameStarts[i] > id })
	if i == 0 {
		return
	}
	f := r.frameStarts[i-1]
	fi := r.frames[f]
	if fi == nil {
		return
	}
	if fi.length > 0 && id >= f+uint32(fi.length) {
		if r.haveCur && r.curStart == f {
			r.resolveFrame(f)
			r.haveCur = false
		}
		return
	}
	if !r.haveCur || f != r.curStart {
		if r.haveCur {
			r.resolveFrame(r.curStart)
		}
		r.curStart, r.haveCur = f, true
	}
	if lost {
		fi.broken = true
	}
	if fi.length > 0 && id+1 >= f+uint32(fi.length) {
		r.resolveFrame(f)
		r.haveCur = false
	}
}

func (r *SlidingReceiver) resolveFrame(start uint32) {
	fi := r.frames[start]
	if fi == nil || fi.resolved {
		return
	}
	fi.resolved = true
	dec := !fi.broken
	for _, refStart := range fi.refs {
		if !dec {
			break
		}
		if ref := r.frames[refStart]; ref != nil {
			if !ref.resolved {
				r.resolveFrame(refStart)
			}
			dec = ref.resolved && ref.decodable
		} else {
			dec = r.idDelivered[refStart]
		}
	}
	fi.decodable = dec
	if !fi.nonPic {
		r.fstats.Frames++
		if dec {
			r.fstats.DecodableFrames++
		}
		if fi.rap {
			r.fstats.Keyframes++
			if dec {
				r.fstats.DecodableKeyframes++
			}
		}
	}
	r.noteResolvedLTR(start, fi, dec)
	if len(r.frames) > frameMapCap {
		r.pruneFrames(start)
	}
}

func (r *SlidingReceiver) frameDoomed(id uint32) bool {
	if r.frames == nil {
		return false
	}
	i := sort.Search(len(r.frameStarts), func(i int) bool { return r.frameStarts[i] > id })
	if i == 0 {
		return false
	}
	f := r.frameStarts[i-1]
	fi := r.frames[f]
	if fi == nil || (fi.length > 0 && id >= f+uint32(fi.length)) {
		return false
	}
	if fi.broken {
		return true
	}
	for _, ref := range fi.refs {
		if rf := r.frames[ref]; rf != nil {
			if rf.resolved && !rf.decodable {
				return true
			}
		} else if d, ok := r.idDelivered[ref]; ok && !d {
			return true
		}
	}
	return false
}

func (r *SlidingReceiver) recordDelivery(id uint32, ok bool) {
	if r.idDelivered == nil {
		r.idDelivered = make(map[uint32]bool)
	}
	r.idDelivered[id] = ok
	if len(r.idDelivered) > 2048 {
		for k := range r.idDelivered {
			if k+1024 < id {
				delete(r.idDelivered, k)
			}
		}
	}
}

func (r *SlidingReceiver) pruneFrames(cur uint32) {
	kept := r.frameStarts[:0]
	for _, st := range r.frameStarts {
		if fi := r.frames[st]; fi != nil && fi.resolved && st+frameRefWindow < cur {
			delete(r.frames, st)
			continue
		}
		kept = append(kept, st)
	}
	r.frameStarts = kept
}

// fastLossWalk is the UNDELAYED forward-gap walk run on raw arrival order when the
// reorder window is active: it fires the loss-onset event and the honest counters
// the moment a gap appears (detection latency — CongestionLoss arms the retro
// tier), accepting that reorder briefly overcounts them (saturating integrators;
// consumers difference deltas). The sizing estimators are fed only by the
// resequenced walk (observeLoss), where reorder has been settled out.
func (r *SlidingReceiver) fastLossWalk(id uint32) {
	if !r.fastHaveExpect {
		r.fastHaveExpect, r.fastExpect = true, id+1
		return
	}
	if id < r.fastExpect {
		return
	}
	if run := id - r.fastExpect; run > 0 {
		r.fbDue = true // loss onset: report now (rate-limited), the gap-triggered-NACK reflex
		r.countRun(run, &r.stats)
	}
	r.fastExpect = id + 1
}

func (r *SlidingReceiver) observeLoss(id uint32) {
	// Forward-gap walk: a first-arrival id past the expected one is a loss run of that
	// length, folded into the shared loss-run machinery (lossrun.go). With the reorder
	// window active this walk sees the RESEQUENCED order and feeds only the sizing
	// estimators; the honest counters and the loss-onset event already fired on the
	// raw walk (fastLossWalk).
	if !r.haveExpect {
		r.haveExpect, r.expectNext = true, id+1
	} else if id >= r.expectNext {
		if run := id - r.expectNext; run > 0 {
			if r.reorder.enabled() {
				r.observeRunEstimates(run, r.intervalUs, r.cfg.OutageAware, &r.stats)
			} else {
				r.fbDue = true // loss onset: report now (rate-limited), the gap-triggered-NACK reflex
				r.observeRun(run, r.intervalUs, r.cfg.OutageAware, &r.stats)
			}
		}
		r.expectNext = id + 1
	}
	r.observeLossWindow(id)
}

// observeLossWindow credits one received id to the loss-rate window (the pEst
// estimate). Split from the gap walk so the reorder window's lateSink can credit
// an arrival that settled lost in the walk — it still arrived, and dropping it
// from the rate fed the queueing runaway (see reorderWindow.lateSink).
func (r *SlidingReceiver) observeLossWindow(id uint32) {
	if !r.lossStarted {
		r.lossStarted, r.lossBase, r.lossHighest, r.lossRecv = true, id, id, 1
		return
	}
	if id < r.lossBase {
		return
	}
	if id > r.lossHighest {
		r.lossHighest = id
	}
	r.lossRecv++
	// Loss-estimate window: a fixed, band-INDEPENDENT span. It was tied to the band
	// (max(64, codingWindow)), but a wide band (e.g. 256) then delays the FIRST loss
	// estimate by that many ids — the sender sits at the bare Redundancy floor through
	// the whole warmup before pEst can rise, the deciles-0/1 residual the live trace
	// pinned (pEst=0 until ~write 512 at band 256). A 64-id sample already resolves the
	// channel loss rate to ~1.6%, so widening it buys nothing but priming latency.
	// The span is CENSORED like the generation receiver's: outage ids are excluded so
	// the window neither closes on outage time nor reports outage loss as channel rate.
	win := lossWindowMin
	span := int(r.lossHighest-r.lossBase) + 1 - r.lossExcl
	if span < win {
		return
	}
	loss := 1 - float64(r.lossRecv)/float64(span)
	if loss < 0 {
		loss = 0
	}
	r.pEWMA += (loss - r.pEWMA) / float64(int(1)<<lossEWMAShift)
	if hold := r.pHold * lossHoldDecay; loss > hold {
		r.pHold = loss
	} else {
		r.pHold = hold
	}
	r.lossBase, r.lossRecv, r.lossExcl = r.lossHighest+1, 0, 0
}

func (r *SlidingReceiver) lossEstimate() float64 {
	p := r.pEWMA
	if r.pHold > p {
		p = r.pHold
	}
	if p < 0 {
		return 0
	}
	if p > 0.95 {
		return 0.95
	}
	return p
}
