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
	cfg                 Config
	b                   int   // configured MAX band (decode-cost cap); the effective span adapts below it
	interMicros         int64 // median block cadence, resistant to stalls and catch-up bursts
	interSamples        [9]int64
	interSampleCount    int
	interSamplePos      int
	interBlockStart     clock.Timestamp
	interBlockWrites    int
	interBackfill       int64
	sourceWireBytes     int64            // recent mean encoded systematic size, for headroom sizing
	sourceWireWindow    sourceWireWindow // bounded exact-size observation window
	sourcePayloadBytes  int64            // recent mean encoder-controlled bytes per systematic
	sourcePayloadWindow sourceWireWindow
	bitrateAdvice       bitrateAdvisor
	enc                 *code.Encoder
	proactiveSeq        uint16
	reactiveSeq         uint16
	sparseSeq           uint16
	credit              float64
	pEst                float64
	burstQ8             int // estimated mean loss-run length, Q8 (from feedback; 256 = i.i.d.)
	fbCount             int // feedback reports received (for the cold-start proactive floor)
	rttMicros           int64
	// The two-window minimum tracks propagation-scale RTT for band sizing and
	// reactive-cycle math. rttSample retains the queue-inclusive observation for
	// capacity control; the two signals must remain distinct.
	rttMinCur   int64
	rttMinPrev  int64
	rttSample   int64 // latest instantaneous RTT sample (pre-min-fold): sample − rttMicros = queue delay
	rttWinStart clock.Timestamp
	// floodCap bounds proactive repair when arrival rate shows wire overrun. It
	// never throttles source traffic or deficit-driven recovery.
	floodCap      float64
	floodClear    int
	lastFBAt      clock.Timestamp
	lastFBHighest uint32
	lastFBSource  uint64 // stats.Source at the previous report (the exact offer)
	lastFBRepair  uint64 // stats.Repair at the previous report (the offered repair mix)
	arriveRatioQ8 int64  // EWMA of observed/offered source rate, Q8 (256 = keeping up)
	// headroomCap is the continuously estimated affordable proactive rate. It
	// tightens on saturation evidence and probes upward only after the queue clears.
	headroomCap  float64
	lastWrite    clock.Timestamp
	lastRepair   clock.Timestamp
	lastReactive clock.Timestamp
	reactiveBase uint32
	reactiveSent int
	// wireLossBudget admits retrospective repair only against receiver-observed
	// wire loss. Each round spends the deficit it addresses.
	wireLossBudget int
	// unitSentAt dedups NACK-bitmap unit repairs: id → last unit emission, so an
	// in-flight unit is not re-sent on every 10-20 ms report (see answerMissing).
	unitSentAt        map[uint32]clock.Timestamp
	missingSince      map[uint32]clock.Timestamp // persistence gate for missing-driven exact repair
	exactOffloadUntil clock.Timestamp            // deep unresolved band keeps targeted closure in ownership
	deadlines         map[uint32]clock.Timestamp
	singletons        []pendingSingletonRepair
	burstDuplicates   []pendingBurstDuplicate
	burstDupCredit    float64
	// Outage composure deliberately excludes unrecoverable interiors from burstQ8.
	// The separate OutageRun signal keeps their fade geometry available to the
	// delayed-equation selector without poisoning FEC-rate sizing.
	outageDiversityUntil   clock.Timestamp
	outageRunSyms          uint32
	outageReportCount      uint32
	outageDiversityCredit  float64
	outageRepairs          []pendingOutageRepair
	disableOutageDiversity bool // white-box A/B control; never exposed as configuration
	epoch                  *outboundEpoch
	epochPolicy            epochPolicy
	epochSlidingCredit     float64
	disableEpochRepair     bool                      // white-box A/B control; never exposed as configuration
	disableExactRepair     bool                      // white-box A/B control; never exposed as configuration
	disableExactCrossover  bool                      // white-box A/B control; never exposed as configuration
	protGroups             map[uint8]*protectedGroup // consolidated center-tier protection (one sparse repair per group)
	protectedSlot          map[uint8][]uint32
	protected              map[uint32]clock.Timestamp
	sparseBase             uint32
	sparseSent             int
	sparsePend             []pendingSparseRepair
	frameStart             map[uint32]uint32
	frameInfo              map[uint32]*senderFrameInfo
	frameOrder             []uint32
	anchorSent             bool
	curFrameID             uint32
	haveCurFrame           bool
	ltrFidByStart          map[uint32]uint32 // FrameStart+1 → frame id of FrameDesc.LTR candidates (resync translation)
	resync                 resyncController
	cleanRun               int // consecutive feedbacks positively reporting zero loss (floor-decay confidence, as in the generation sender)
	bucket                 tokenBucket
	cc                     *congestionController // delay/ECN controller; nil keeps the static ceiling
	repairTokens           int64                 // source-progress-earned recovery allowance; wall-clock stalls cannot refill it
	repairBurst            int64
	sendQ                  [][]byte
	stats                  SenderStats
	cadence                recoveryCadenceController

	// codeRate is memoized by every policy input so the GE-tail search runs only
	// when feedback or effective geometry changes.
	crBand     int
	crPEst     float64
	crBurstQ8  int
	crRounds   int
	crFloorOff bool
	crRelief   bool
	crRate     float64
	crValid    bool
}

// RateBudgetBitsPerSec returns the total send-rate budget the host pacer should
// release within: the delay/ECN controller's output when enabled, otherwise the
// static ceiling.
func (s *SlidingSender) RateBudgetBitsPerSec() int64 {
	if s.cc != nil {
		if b := s.cc.rateBudgetBytesPerSec(); b > 0 {
			return b * 8
		}
	}
	return s.cfg.maxBitrate()
}

func NewSlidingSender(cfg Config) *SlidingSender {
	bucket := newTokenBucket(cfg.maxBitrate())
	bytesPerSec := cfg.maxBitrate() / 8
	repairBurst := bytesPerSec / 5
	if repairBurst < 1<<16 {
		repairBurst = 1 << 16
	}
	repairTokens := repairBurst
	if cfg.MaxBitrate > 0 {
		// An explicit capacity is a hard contract. Do not begin with the generic
		// bucket's 200 ms reservoir: that would spend future recovery headroom
		// before the source cadence is measured and create a startup queue.
		bucket.limitStartupCredit(5_000)
		repairTokens = bytesPerSec / 200
		if repairTokens < 1<<12 {
			repairTokens = 1 << 12
		}
	}
	s := &SlidingSender{
		cfg:           cfg,
		b:             cfg.codingWindow(),
		enc:           code.NewEncoder(codedSymbolSize(cfg.SymbolSize)),
		rttMicros:     defaultRTTMicros,
		floodCap:      maxRepairFactor, // breaker inactive until wire-overrun evidence
		headroomCap:   maxRepairFactor, // affordable-rate ceiling inactive until saturation evidence
		arriveRatioQ8: 256,
		burstQ8:       burstQ8One,
		bucket:        bucket,
		repairTokens:  repairTokens,
		repairBurst:   repairBurst,
		deadlines:     make(map[uint32]clock.Timestamp),
	}
	if cfg.CongestionControl {
		s.cc = newCongestionController(0, cfg.SymbolSize+symHeaderBytes, cfg.maxBitrate())
	}
	return s
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
	// The floor sizes the trailing window conservatively through the estimate ramp;
	// loss above the assumption provisions further through pEst. The cost is a
	// bounded burst of warmup repair on a clean link.
	coldStartP                = 0.15
	coldStartFeedbacks        = 6
	coldStartBurstGap         = 48
	targetedRepairGuardMicros = 5_000
	// A mean run of two packets is enough evidence that overlapping, immediately
	// emitted sliding equations share loss fate. The receiver's Q8 EWMA must cross
	// this threshold before the block lane activates; i.i.d. loss remains near one.
	burstBandThresholdQ8 = 2 * burstQ8One

	// Sparse protected repair answers a stuck feedback cursor with equations over
	// only protected/reference source ids in that old neighborhood. The total sent
	// for a cursor window is capped at the number of protected ids in the sparse set.
	sparseProtectedMaxIDs = 16

	// RAP-anchor closure repair protects the minimal source island needed to start
	// decoding after a random-access point: the RAP frame plus its descriptor-resolved
	// reference frames, usually parameter sets. It is intentionally limited to the
	// first sliding band; ordinary sliding repair handles later steady-state holes.
	sparseAnchorMaxIDs  = 6
	sparseAnchorExtra   = 1
	sparseAnchorStripes = 1
)

const (
	protectedLaneBase uint8 = iota
	protectedLaneRAP
)

// Sliding repair keys carry a two-bit lane namespace. Optional recovery can add
// equations without consuming the proactive lane's coefficient sequence, which
// keeps the base code stable as latency-dependent actuators turn on and off.
const (
	repairKeySeqMask      uint16 = 0x3fff
	repairKeyLaneReactive uint16 = 0x4000
	repairKeyLaneSparse   uint16 = 0x8000
)

func nextSlidingRepairKey(seq *uint16, lane uint16) uint16 {
	key := lane | (*seq & repairKeySeqMask)
	*seq = (*seq + 1) & repairKeySeqMask
	return key
}

type pendingSparseRepair struct {
	ids       []uint32
	releaseAt uint32
	reactive  bool // feedback-driven retry (counts toward ReactiveRepair) vs scheduled group protection
}

type pendingBurstDuplicate struct {
	id        uint32
	releaseAt clock.Timestamp
	priority  uint8
}

type pendingOutageRepair struct {
	base      uint32
	releaseAt clock.Timestamp
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

// effectiveBand returns the configured max band reduced to what fits the deadline
// budget after one-way propagation and a guard. Optional recovery lanes have
// independent coefficient sequences and a base-capacity reserve, so they cannot
// displace this proactive lane while the effective band remains unchanged.
func (s *SlidingSender) effectiveBand() int {
	b := s.b
	if s.interMicros <= 0 || s.cfg.BufferMicros <= 0 {
		return b
	}
	guard := int64(bandGuardMicros)
	if !s.reactiveReachable() && s.burstQ8 >= burstBandThresholdQ8 {
		// Proactive post-burst equations do not spend a feedback cadence. Once
		// measured burst memory proves the trailing band needs to reach farther
		// back, retain only the admission/clock-error guard used by targeted
		// repair. Exact per-source deadlines still reject a late equation.
		guard = targetedRepairGuardMicros
	}
	budget := s.cfg.BufferMicros - s.rttMicros/2 - guard
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
	s.observeSourceWrite(now)
	s.lastWrite = now
	dl := now.Add(s.cfg.BufferMicros)
	id := addCodedSource(s.enc, data, s.cfg.SymbolSize, dl)
	s.deadlines[id] = dl
	block := s.epochBlockFor(now, id, dl)
	src, _ := s.enc.Source(id)
	sourceLen := len(data)
	if sourceLen > s.cfg.SymbolSize {
		sourceLen = s.cfg.SymbolSize
	}
	sym := wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Systematic, WindowBase: id, SrcIndex: id, N: 1, Priority: priority, Deadline: int64(dl), SendTimestamp: int64(now), HasSourceLength: true, SourceLength: uint32(sourceLen), Payload: src[:sourceLen]}
	s.sourcePayloadBytes = s.sourcePayloadWindow.observe(sourceLen)
	if block != nil {
		sym.WindowBase = block.base
		sym.N = uint16(block.n)
	}
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
			// released as one sparse repair. One equation repairs any single loss
			// among the group; multi-loss neighborhoods lean on the band rate,
			// so consolidation requires a band wide enough to actually carry
			// that cover (2× the group cap): at a deadline-clipped narrow band
			// the per-chunk singletons are the only real multi-loss protection.
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
	if block == nil {
		s.maybeQueueBurstDuplicate(now, id, priority)
	}
	s.flushSingletonRepairs(now, id, false)
	s.flushProtectedGroups(id, false)
	s.flushSparseRepairs(now, id, false)
	s.flushOutageRepairs(now)
	burstWaiting := s.flushBurstDuplicates(now)
	if !burstWaiting {
		for s.credit += s.codeRate(); s.credit >= 1; s.credit-- {
			if block != nil {
				s.allocateEpochCredit(now, block)
				continue
			}
			if s.maybeDelayOutageRepair(now, id) {
				continue
			}
			s.emitRepair(now)
		}
	}
	if block != nil && block.used == block.n {
		s.closeEpochBlock(now)
	}
}

const sourceCadenceBlockWrites = 8

// observeSourceWrite samples cadence over small source blocks and takes the
// median of the last nine blocks. A protocol stall normally produces one slow
// block followed by one catch-up block; neither outlier controls the median or
// manufactures recovery headroom. A genuine source-rate change replaces the
// window within roughly 64 writes without an operator hint.
func (s *SlidingSender) observeSourceWrite(now clock.Timestamp) {
	if s.interBlockWrites == 0 {
		s.interBlockStart, s.interBlockWrites = now, 1
		return
	}
	s.interBlockWrites++
	if s.interBlockWrites < sourceCadenceBlockWrites {
		return
	}
	span := now.Sub(s.interBlockStart)
	s.interBlockStart, s.interBlockWrites = now, 1
	if span <= 0 {
		return
	}
	gap := span / (sourceCadenceBlockWrites - 1)
	first := s.interMicros == 0
	s.interSamples[s.interSamplePos] = gap
	s.interSamplePos = (s.interSamplePos + 1) % len(s.interSamples)
	if s.interSampleCount < len(s.interSamples) {
		s.interSampleCount++
	}
	s.interMicros = medianCadence(&s.interSamples, s.interSampleCount)
	if first {
		// The current Write earns one interval below; backfill the earlier
		// intervals in the first measured block exactly once.
		s.interBackfill = sourceCadenceBlockWrites - 2
	}
}

// maybeQueueBurstDuplicate schedules a delayed compact repetition when feedback
// proves a correlated fade and no reactive cycle can fit the deadline. The delay
// is one measured mean burst: for a memoryless bad state, that separates the two
// copies enough to give the retransmission an independent-ish chance while still
// leaving exact deadline admission in control. The credit rate is derived from
// measured source bytes and the aggregate ceiling; source traffic remains first.
func (s *SlidingSender) maybeQueueBurstDuplicate(now clock.Timestamp, id uint32, priority uint8) {
	if s.reactiveReachable() || s.fbCount < coldStartFeedbacks ||
		s.burstQ8 < burstBandThresholdQ8 || s.interMicros <= 0 {
		return
	}
	gap := uint32((s.burstQ8 + 255) / 256)
	if gap == 0 || int64(gap)*s.interMicros+s.rttMicros/2+targetedRepairGuardMicros > s.cfg.BufferMicros {
		return
	}
	rate := s.compactRepairHeadroomRate()
	if rate <= 0 {
		return
	}
	if rate > 1 {
		rate = 1
	}
	s.burstDupCredit += rate
	if s.burstDupCredit < 1 {
		return
	}
	s.burstDupCredit--
	s.burstDuplicates = append(s.burstDuplicates, pendingBurstDuplicate{
		id: id, releaseAt: now.Add(int64(gap) * s.interMicros), priority: priority,
	})
}

func (s *SlidingSender) compactRepairHeadroomRate() float64 {
	if !s.cfg.RepairWithinBudget {
		return 1
	}
	if s.interMicros <= 0 || s.sourceWireBytes <= 0 {
		return 0
	}
	// Use the live aggregate budget. Once delay/ECN control has reduced the
	// sender rate, pricing compact copies against the static ceiling would admit
	// a copy storm precisely while the path is congested.
	capacity := s.bucket.bytesPerSec * s.interMicros / 1_000_000
	spare := capacity - s.sourceWireBytes
	if spare <= 0 {
		return 0
	}
	return float64(spare) / float64(s.sourceWireBytes)
}

func (s *SlidingSender) flushBurstDuplicates(now clock.Timestamp) bool {
	keep := s.burstDuplicates[:0]
	duePending := false
	for _, p := range s.burstDuplicates {
		if p.releaseAt.After(now) {
			keep = append(keep, p)
			continue
		}
		if s.emitCompactUnit(now, p.id, p.priority, false) {
			continue
		}
		dl, exists := s.deadlines[p.id]
		if _, retained := s.enc.Source(p.id); exists && retained && !now.Add(s.rttMicros/2).After(dl) {
			keep = append(keep, p)
			duePending = true
		}
	}
	s.burstDuplicates = keep
	return duePending
}

// maybeDelayOutageRepair moves a bounded share of already-earned proactive
// equations across the measured fade span. It never raises the code rate: true
// means the caller must consume this repair credit without emitting an immediate
// equation. The ordinary burst-copy path remains unchanged and takes precedence
// once recoverable burst evidence has reached its own estimator.
func (s *SlidingSender) maybeDelayOutageRepair(now clock.Timestamp, id uint32) bool {
	if s.disableOutageDiversity || s.reactiveReachable() ||
		!now.Before(s.outageDiversityUntil) || s.outageRunSyms == 0 ||
		s.burstQ8 >= burstBandThresholdQ8 || s.interMicros <= 0 {
		return false
	}
	if len(s.outageRepairs) >= s.b {
		return false
	}
	gap := s.outageRunSyms
	deadlineSlots := (s.cfg.BufferMicros - s.rttMicros/2 - targetedRepairGuardMicros) / s.interMicros
	if deadlineSlots <= 0 {
		return false
	}
	maxGap := uint32(deadlineSlots)
	if retained := s.effectiveBand() - 1; retained <= 0 {
		return false
	} else if maxGap > uint32(retained) {
		maxGap = uint32(retained)
	}
	if gap > maxGap {
		gap = maxGap
	}
	if gap == 0 {
		return false
	}
	share := outageDiversityShare
	horizon := s.rttMicros
	if s.cfg.BufferMicros > 0 && (horizon <= 0 || s.cfg.BufferMicros < horizon) {
		horizon = s.cfg.BufferMicros
	}
	if s.outageReportCount == 1 && int64(s.outageRunSyms)*s.interMicros < horizon/outageDiversityRTTShare {
		// An isolated tail can cross the outage classifier after transient slack
		// compression without proving a long-memory path. Preserve some temporal
		// exploration on the first report, but move half the normal share; repeated
		// outage evidence restores the established share.
		share = outageDiversityShortShare
	}
	s.outageDiversityCredit += share
	if s.outageDiversityCredit < 1 {
		return false
	}
	s.outageDiversityCredit--
	s.outageRepairs = append(s.outageRepairs, pendingOutageRepair{
		base: id, releaseAt: now.Add(int64(gap) * s.interMicros),
	})
	return true
}

func (s *SlidingSender) flushOutageRepairs(now clock.Timestamp) {
	keep := s.outageRepairs[:0]
	for _, p := range s.outageRepairs {
		if p.releaseAt.After(now) {
			keep = append(keep, p)
			continue
		}
		if s.emitOutageRepair(now, p.base) {
			continue
		}
		dl, exists := s.deadlines[p.base]
		if _, retained := s.enc.Source(p.base); exists && retained && !now.Add(s.rttMicros/2).After(dl) {
			keep = append(keep, p)
		}
	}
	s.outageRepairs = keep
}

func (s *SlidingSender) emitOutageRepair(now clock.Timestamp, base uint32) bool {
	if base < s.enc.Base() {
		return false
	}
	off := int(base - s.enc.Base())
	if off < 0 || off >= s.enc.Len() {
		return false
	}
	n := s.enc.Len() - off
	if band := s.effectiveBand(); n > band {
		n = band
	}
	dl, ok := s.deadlines[base]
	if !ok || !s.repairAdmissible(now, uepCenterTier, dl, 0, false) {
		return false
	}
	key := nextSlidingRepairKey(&s.proactiveSeq, 0)
	actualBase, nn, pay := s.enc.RepairAt(key, base, n)
	if actualBase != base || nn == 0 {
		return false
	}
	if !s.emit(wire.Symbol{
		Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: actualBase, SrcIndex: uint32(key), N: uint16(nn),
		RepairKey: key, Priority: uepCenterTier, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay,
	}) {
		return false
	}
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairProactive++
	s.stats.RepairOutageDiversity++
	return true
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
	s.flushOutageRepairs(now)
	// An incomplete announced block has no algebraically valid fixed-width repair.
	// Retire the hint and let the existing tail flush protect its live sources with
	// ordinary sliding equations.
	s.epoch = nil
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
// cap bounds the multi-loss exposure a group shares, and the span cap below keeps
// the equation inside the receiver's band.
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
	if !s.repairAdmissible(now, p.priority, p.deadline, 0, false) {
		return
	}
	enc := code.NewEncoderAt(codedSymbolSize(s.cfg.SymbolSize), p.id)
	enc.Add(p.src)
	_, n, pay := enc.Repair(0)
	if !s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: p.id, SrcIndex: 0, N: uint16(n), RepairKey: 0, Priority: p.priority, Deadline: int64(p.deadline), SendTimestamp: int64(now), Payload: pay}) {
		return
	}
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairSingleton++
}

// reactiveReachable reports whether one honest reactive cycle fits the deadline
// budget — the gate for the retrospective repair tier itself.
func (s *SlidingSender) reactiveReachable() bool {
	return s.cfg.BufferMicros > 0 && reactiveCycleMicros(s.rttMicros) <= s.cfg.BufferMicros
}

func (s *SlidingSender) reactiveRounds() int {
	return reactiveRoundsFrom(s.cfg.BufferMicros, s.rttMicros)
}

// extrasReplaceableByReactive is the shared eligibility predicate (extrasReplaceable)
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
	// The eligibility predicate runs last: most chunks fail the cheap tier gates above,
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
	s.noteOutageRun(now, fb)
	if fb.HighestSeen > 0 {
		// Receiver sessions can exchange periodic feedback before media starts.
		// Those empty reports contain no channel evidence and must not disable
		// cold-start protection before the first source symbol is observed.
		s.fbCount++
	}
	// Captured before updateFloodBreaker's defer refreshes lastFBSource: whether any
	// source symbols were offered in the interval this report covers (the clean-run
	// progress gate below).
	offeredSince := s.stats.Source != s.lastFBSource
	s.updateRTT(now, fb)
	if s.cc != nil {
		if budget := s.cc.rateBudgetBytesPerSec(); budget > 0 {
			s.bucket.setRate(budget)
		}
	}
	s.updateFloodBreaker(now, fb)
	s.noteExactOffload(now, fb)
	s.pEst = float64(fb.LossRate) / 65535
	// Floor-decay confidence (see floorDecayed): count consecutive feedbacks that
	// POSITIVELY report a clean link; snap to zero on any contrary evidence.
	// Keyed on the report, never on signal absence — a black hole or warmup delivers
	// no feedback at all, so cleanRun stays 0 and the full floor is retained.
	//
	// SettledLost adjudicates reorder. LossRate, Deficit, and Missing may describe
	// in-flight reordering, so they are deliberately absent from this policy signal.
	clean := fb.SettledLost == 0
	switch {
	case !clean:
		s.cleanRun = 0
	case offeredSince && s.cleanRun < cleanFloorConfirm:
		// Only reports covering an interval where source was actually OFFERED build
		// confidence. Idle intervals freeze the counter; dirty evidence always resets it.
		s.cleanRun++
	}
	if s.burstQ8 = int(fb.Burstiness); s.burstQ8 < burstQ8One {
		s.burstQ8 = burstQ8One // mean loss-run length for the burst-aware sizer
	}
	s.updateEpochDemand(fb, clean, offeredSince)
	s.cadence.observeFeedback(fb)
	s.updateBitrateAdvice()
	s.resync.observe(now, fb.BrokenAnchors, fb.NewestDecodableLTR, resyncHoldMicros(s.cfg.BufferMicros, s.rttMicros))
	slideTo := fb.DecodedLowEdge
	if latest, ok := s.latestSourceID(); ok {
		// Keep one complete frontier band even when the peer has acknowledged
		// farther ahead. Feedback timing depends on the peer's playout budget;
		// allowing that timing to truncate the trailing window changes otherwise
		// identical proactive equations when latency changes. The retained band is
		// bounded and already required for late/reordered feedback.
		// Once the newest retained symbol can no longer be repaired before its
		// deadline, release the whole acknowledged tail. Keeping an expired band
		// makes idle Tick calls attempt repair forever after a stream ends.
		if dl, exists := s.deadlines[latest]; exists && !now.Add(s.rttMicros/2).After(dl) {
			keep := uint32(s.effectiveBand())
			if latest+1 > keep {
				frontierBase := latest + 1 - keep
				if slideTo > frontierBase {
					slideTo = frontierBase
				}
			} else {
				slideTo = s.enc.Base()
			}
		}
	}
	if slideTo > s.enc.Base() {
		for id := s.enc.Base(); id < slideTo; id++ {
			delete(s.deadlines, id)
		}
		s.enc.SlideTo(slideTo)
		s.pruneSenderFrames(slideTo)
		s.pruneProtected(slideTo)
		s.pruneProtectedGroups(slideTo)
		s.pruneSparseRepairs(slideTo)
		s.reactiveBase, s.reactiveSent = 0, 0
		s.sparseBase, s.sparseSent = 0, 0
	}
	if cl := int(fb.CongestionLoss); cl > 0 {
		// Arm the retro-reactive tier with the reported pre-recovery wire loss,
		// capped so a long outage cannot bank an unbounded repair debt. Exact-closure mode
		// raises the cap: the retro tier is the only burst recovery there.
		capX := wireLossBudgetCap
		if s.deltaReliefOn() {
			capX = wireLossBudgetCapExact
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

// Tick idle-flushes a band-repair when no source has arrived for a while.
func (s *SlidingSender) Tick(now clock.Timestamp) {
	s.flushOutageRepairs(now)
	s.flushBurstDuplicates(now)
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
	if s.sourceWireBytes > 0 {
		st.SourceWireBytesMean = uint64(s.sourceWireBytes)
	}
	return st
}

// EncoderControl returns the current advisory source-control request for an attached encoder.
func (s *SlidingSender) EncoderControl() EncoderControl {
	ec := s.cadence.encoderControl()
	ec.TargetBitrateBps = s.bitrateAdvice.control()
	return withResync(ec, &s.resync, s.ltrFidByStart)
}

func (s *SlidingSender) codeRate() float64 {
	b := s.effectiveBand()
	// Cold start: until the loss estimate primes, provision for a conservative
	// assumed loss. Feedback-driven repair cannot protect the initial flight before
	// the first reports return.
	p := s.pEst
	cold := s.fbCount < coldStartFeedbacks
	rounds := s.reactiveRounds()
	if cold && p < coldStartP {
		p = coldStartP
	}
	// Reuse the set-point while its policy inputs are unchanged. Cold start bypasses
	// the memo because the temporary loss floor is feedback-count dependent.
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
		// When the deadline admits enough reactive cycles, targeted recovery carries
		// the burst tail and proactive sizing can use the bounded relaxed target.
		if delta *= deltaReliefFactor; delta > deltaReliefCap {
			delta = deltaReliefCap
		}
	}
	r := repairForTarget(b, p, delta, maxRepairFactor)
	if relief && s.exactOffloadOn() {
		// With an explicitly enabled exact-closure lane and enough complete
		// feedback cycles, proactive repair owns only the measured mean loss.
		// Variance and burst-tail misses are observed rather than predicted and
		// answered by the targeted lane before their deadlines.
		r = meanRepairCount(b, p)
	}
	if ge := repairForGE(b, int(p*1e6), s.burstQ8, delta, maxRepairFactor); ge > r {
		// Size for the GE tail. Reactive margin discounts are proportional to the
		// number of complete recovery cycles, so frontier regimes retain full margin.
		switch {
		case relief:
			// Exact-closure mode (SlidingReactiveShift at >= 1.5 honest cycles):
			// the retro-reactive tier owns the burst tail, so proactive carries only
			// the i.i.d. set-point at the relieved δ. This avoids a repair burst that
			// competes with source traffic when targeted closure can arrive in time.
			// Raised retrospective caps cover the residual; the flood breaker
			// backstops transition regimes.
		case s.cfg.SlidingReactiveShift && rounds >= reactiveFloorSafe:
			// Two-margin offload port (generation repairCountFor): the BURST margin
			// shrinks by 1/(rounds+1); the VARIANCE margin sheds only on a
			// memoryless channel (roundsEff → 0 as the mean run length grows).
			// Engages only once the deadline admits the required safety rounds.
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
	if cap := s.sourceHeadroomRate(); rate > cap {
		rate = cap
	}
	return rate
}

// sourceHeadroomRate is the proactive repair-per-source ceiling implied by the
// declared aggregate rate and the sender's measured source cadence. It is the
// sliding equivalent of generation mode's maxRepairWithinBudget: source wire
// bytes are priced first, and only the remaining capacity is available to repair.
func (s *SlidingSender) sourceHeadroomRate() float64 {
	if !s.cfg.RepairWithinBudget || s.interMicros <= 0 || s.sourceWireBytes <= 0 {
		return maxRepairFactor
	}
	budgetBytesPerSec := s.bucket.bytesPerSec
	sourceBytesPerSec := s.sourceWireBytes * 1_000_000 / s.interMicros
	if sourceBytesPerSec <= 0 {
		return maxRepairFactor
	}
	if sourceBytesPerSec >= budgetBytesPerSec {
		return 0
	}
	repairBytes := int64(repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize))
	rate := float64(budgetBytesPerSec-sourceBytesPerSec) * float64(s.interMicros) /
		(float64(repairBytes) * 1_000_000)
	if rate > maxRepairFactor {
		return maxRepairFactor
	}
	return rate
}

// slidingObservedRate estimates the application-limited source+recovery offer
// available before the first congestion-control sample. The source term uses
// measured variable-size systematic datagrams; the recovery term uses the
// actual repair/source ratio generated so far and the full algebraic row charge
// used by admission. It seeds CC startup only—later samples may reduce it.
func (s *SlidingSender) slidingObservedRate() int64 {
	if s.interMicros <= 0 || s.sourceWireBytes <= 0 {
		return 0
	}
	sourceRate := s.sourceWireBytes * 1_000_000 / s.interMicros
	if s.stats.Source == 0 || s.stats.Repair == 0 {
		return sourceRate
	}
	repairBytes := int64(repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize))
	repairRate := repairBytes * int64(s.stats.Repair) * 1_000_000 /
		(int64(s.stats.Source) * s.interMicros)
	return sourceRate + repairRate
}

// Delta relief loosens only proactive sizing and only when at least 1.5 complete
// feedback cycles fit before the deadline.
const (
	deltaReliefFactor      = 10
	deltaReliefCap         = 1e-2
	deltaReliefMinCyclesX2 = 3
)

// deltaReliefOn reports whether the budget-conditional δ relief qualifies. The
// explicit benchmark override can force the reactive-offload bundle; automatic operation
// selects it when exactOffloadOn proves two affordable targeted rounds, or when
// measured repair headroom is already scarce. A base δ already at the relief cap
// is never loosened.
func (s *SlidingSender) deltaReliefOn() bool {
	if s.cfg.targetFailure() >= deltaReliefCap {
		return false
	}
	cycle := reactiveCycleMicros(s.rttMicros)
	if cycle <= 0 || 2*s.cfg.BufferMicros < deltaReliefMinCyclesX2*cycle {
		return false
	}
	return s.exactOffloadOn() || s.sourceHeadroomRate() <= bulkExactHeadroomMax
}

// exactOffloadOn reports when targeted repair can own the variance and burst
// residual instead of forcing the proactive lane to predict both. Automatic
// ownership requires a recently observed deep unresolved band, two complete
// feedback/repair cycles, and units materially cheaper than a full equation.
// The explicit research switch continues to force the policy for controlled A/Bs.
func (s *SlidingSender) exactOffloadOn() bool {
	if s.disableExactRepair {
		return false
	}
	if s.cfg.SlidingReactiveShift {
		return true
	}
	if s.reactiveRounds() < reactiveFloorSafe {
		return false
	}
	policyNow := s.lastWrite
	if s.lastFBAt.After(policyNow) {
		policyNow = s.lastFBAt
	}
	if s.exactOffloadUntil == 0 || !policyNow.Before(s.exactOffloadUntil) {
		return false
	}
	if s.sourceWireBytes <= 0 {
		return false
	}
	equationBytes := int64(repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize))
	return equationBytes > 2*s.sourceWireBytes
}

// floorDecayed drops the static floor only after positive clean evidence and when
// the deadline admits enough reactive rounds to recover a new loss onset.
func (s *SlidingSender) floorDecayed(rounds int) bool {
	return s.cleanRun >= cleanFloorConfirm && rounds >= reactiveFloorSafe
}

// reactive answers a feedback deficit over the delivery cursor's retained window.
// The decoder folds already-delivered columns and rejects unusable equations.
// retroMaxWindows bounds one retro round's span: up to this many band-strided
// repair windows from the stuck cursor. The whole reported deficit must be
// addressed in ONE round when possible — the per-symbol deadline wavefront moves
// at the source rate, so a hole deferred to a second round is usually a hole lost.
const retroMaxWindows = 4

// retroMaxWindowsExact is the retro span cap in exact-closure mode (SlidingReactiveShift at a
// >= 1.5-cycle budget): with the proactive burst margin shed entirely, the retro
// tier is the ONLY burst recovery, so it must be allowed to cover a whole deep
// burst in one round. wireLossBudgetCapExact raises the evidence-budget cap in step.
const (
	retroMaxWindowsExact   = 12
	wireLossBudgetCap      = 8
	wireLossBudgetCapExact = 24
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
		// A sub-cycle repair cannot reach the stuck window before its deadline.
		return
	}
	// Debounce successive rounds by half the honest cycle: long enough that a new
	// round is sized against a deficit the previous round has visibly shrunk, while
	// leaving time for a residual round.
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
		maxWin = retroMaxWindowsExact // exact-closure mode: the retro tier owns the burst tail
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
	// Band-strided windows preserve closure geometry; exact bitmap repair handles
	// named residuals separately in targeted_repair.go.
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
	// Clip and admit the repair before doing the O(window*symbol-size) coding.
	base := at
	if encBase := s.enc.Base(); base < encBase {
		n -= int(encBase - base)
		base = encBase
	}
	off := int(base - s.enc.Base())
	if off < 0 || off >= s.enc.Len() || n <= 0 {
		return false
	}
	if available := s.enc.Len() - off; n > available {
		n = available
	}
	dl, ok := s.deadlines[base+uint32(n)-1]
	if !ok {
		dl = now.Add(s.cfg.BufferMicros)
	}
	if !s.repairAdmissible(now, uepCenterTier, dl, 0, true) {
		return false
	}
	key := nextSlidingRepairKey(&s.reactiveSeq, repairKeyLaneReactive)
	base, nn, pay := s.enc.RepairAt(key, base, n)
	if nn == 0 {
		return false
	}
	if !s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(nn), RepairKey: key, Priority: uepCenterTier, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay}) {
		return false
	}
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
	// This path intentionally does not use the generation profile's newest-deadline
	// gate: a sliding RTT sample may include queueing and is not a safe proof that
	// every protected symbol is dead.
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

// emitRepair emits one proactive trailing-band repair (deficit-answering reactive
// repair goes through emitRepairAt, which does its own attribution).
func (s *SlidingSender) emitRepair(now clock.Timestamp) {
	if s.enc.Len() == 0 {
		return
	}
	base, n := s.repairWindowBase(s.effectiveBand())
	dl := s.deadlines[base+uint32(n)-1]
	if !s.repairAdmissible(now, uepCenterTier, dl, 0, false) {
		return
	}
	key := nextSlidingRepairKey(&s.proactiveSeq, 0)
	base, n, pay := s.enc.RepairWindow(key, s.effectiveBand())
	if !s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(n), RepairKey: key, Priority: uepCenterTier, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay}) {
		return
	}
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
	key := nextSlidingRepairKey(&s.sparseSeq, repairKeyLaneSparse)
	dl, ok := s.deadlines[ids[len(ids)-1]]
	if !ok {
		dl = now.Add(s.cfg.BufferMicros)
	}
	if !s.repairAdmissible(now, uepCenterTier+1, dl, len(ids)*4, reactive) {
		return false
	}
	pay, ok := s.enc.RepairSparse(key, ids)
	if !ok {
		return false
	}
	copiedIDs := append([]uint32(nil), ids...)
	if !s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.SparseRepair, SrcIndex: uint32(key), N: uint16(len(copiedIDs)), RepairKey: key, SparseIDs: copiedIDs, Priority: uepCenterTier + 1, Deadline: int64(dl), SendTimestamp: int64(now), Payload: pay}) {
		return false
	}
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairSparse++
	if reactive {
		s.stats.ReactiveRepair++
	}
	return true
}

const repairWireBaseBytes = 30 + 8 // base symbol header + send-timestamp extension

// repairAdmissible rejects expired or unaffordable recovery before the encoder
// spends O(window*symbol-size) work constructing it. sparseTailBytes accounts for
// the explicit four-byte source ids on SparseRepair.
func (s *SlidingSender) repairAdmissible(now clock.Timestamp, priority uint8, deadline clock.Timestamp, sparseTailBytes int, optional bool) bool {
	n := repairWireBaseBytes + sparseTailBytes + codedSymbolSize(s.cfg.SymbolSize)
	return s.repairAdmissibleWire(now, priority, deadline, n, optional)
}

func (s *SlidingSender) repairAdmissibleWire(now clock.Timestamp, priority uint8, deadline clock.Timestamp, n int, optional bool) bool {
	if deadline != 0 && now.Add(s.rttMicros/2).After(deadline) {
		s.stats.DeadlineRepairSkips++
		return false
	}
	reserve := 0
	if optional {
		reserve = repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize)
	}
	if s.repairTokens < int64(n+reserve) {
		s.stats.Throttled++
		return false
	}
	if !s.bucket.canRepairReserved(now, n, priority, reserve) {
		s.stats.Throttled++
		return false
	}
	return true
}

func (s *SlidingSender) emit(sym wire.Symbol) bool {
	d, repairCharge := encodeSymbol(sym, s.cfg.SymbolSize)
	now := clock.Timestamp(sym.SendTimestamp)
	if sym.Kind == wire.Systematic {
		// Source is non-droppable and charges the shared budget first. It may
		// drive the bucket negative when the application exceeds the ceiling;
		// every repair path then stays closed until source has caught up.
		s.bucket.allow(now, len(d), false)
		s.sourceWireBytes = s.sourceWireWindow.observe(len(d))
		s.earnRepairCredit(len(d))
	} else {
		// Deadline admission is common to proactive, sparse, singleton, and
		// feedback-driven repair. Idle ticks and repeated feedback must not spend
		// newly refilled capacity on a window whose repair cannot traverse the
		// measured one-way path before its last useful deadline.
		if sym.Deadline != 0 && now.Add(s.rttMicros/2).After(clock.Timestamp(sym.Deadline)) {
			s.stats.DeadlineRepairSkips++
			return false
		}
		if s.repairTokens < int64(repairCharge) {
			s.stats.Throttled++
			return false
		}
		if !s.bucket.allowRepair(now, repairCharge, sym.Priority) {
			s.stats.Throttled++
			return false
		}
		s.repairTokens -= int64(repairCharge)
	}
	if repairCharge > len(d) {
		s.stats.RepairCompacted++
		s.stats.RepairBytesSaved += uint64(repairCharge - len(d))
	}
	s.sendQ = append(s.sendQ, d)
	return true
}

// earnRepairCredit converts source progress into recovery allowance. This ledger
// is separate from the wall-clock aggregate bucket: it prevents feedback/coding
// stalls from manufacturing more repair budget and creating a self-financing
// overload loop. The measured cadence lets a genuinely slower source earn more
// spare capacity after its rolling window settles.
func (s *SlidingSender) earnRepairCredit(sourceBytes int) {
	if s.interMicros <= 0 {
		return
	}
	intervals := int64(1) + s.interBackfill
	s.interBackfill = 0
	capacityBytes := s.bucket.bytesPerSec * s.interMicros / 1_000_000 * intervals
	debit := int64(sourceBytes)
	if intervals > 1 {
		debit = s.sourceWireBytes * intervals
	}
	spare := capacityBytes - debit
	if spare <= 0 {
		return
	}
	s.repairTokens += spare
	if s.repairTokens > s.repairBurst {
		s.repairTokens = s.repairBurst
	}
}

// SlidingReceiver is the band-form receive half. Same methods as the generation
// Receiver; selected by Config.Sliding.
