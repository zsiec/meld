package flow

import (
	"sort"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// SlidingReceiver is the receive half of the continuous recovery profile.
// It owns ordered delivery and coordinates independent equation geometries
// through one source sequence space.
type SlidingReceiver struct {
	cfg           Config
	dec           *code.BandDecoder
	epochs        map[uint32]*inboundEpoch
	directRecv    map[uint32]bool
	lateDrops     uint64
	lastFB        clock.Timestamp
	fedOnce       bool
	fbDue         bool // loss-onset event: emit feedback now (rate-limited), don't wait the cadence
	deliverQ      []deliveredSym
	sendQ         [][]byte
	stats         ReceiverStats
	repairScratch []byte // reusable expansion buffer for compact equations
	ecnSeen       uint32 // admitted symbols in the current feedback interval
	ecnCE         uint32 // admitted symbols carrying Congestion Experienced
	// White-box control for proving the multi-neighborhood exact feedback gain.
	// Production always leaves continuation/range closure enabled.
	disableExtendedClosure bool

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

	// Forward-gap walk for the mean loss-run length used by burst-aware sizing and the
	// pre-recovery wire-loss count (the honest congestion signal, never
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

// inboundEpoch isolates stable epoch equations from the moving band. Its
// decoder may determine values in any order; recovered values enter the band
// decoder as exact sources, preserving one ordered delivery cursor.
type inboundEpoch struct {
	n   int
	dec *code.Decoder
}

const maxInboundEpochs = 64

func NewSlidingReceiver(cfg Config) *SlidingReceiver {
	r := &SlidingReceiver{
		cfg:             cfg,
		dec:             code.NewBandDecoder(codedSymbolSize(cfg.SymbolSize), cfg.codingWindow(), slidingMaxWin),
		directRecv:      make(map[uint32]bool),
		symDL:           make(map[uint32]clock.Timestamp),
		deadlineFit:     deadlineFit{intervalUs: 1},
		lossRunObserver: lossRunObserver{meanBurstQ8: burstQ8One},
	}
	// Sliding estimation uses reorder holdoff only when explicitly configured.
	// A late-settled arrival still counts as received in the rate window.
	r.reorder = reorderWindow{
		cfgHoldoff: cfg.ReorderHoldoffMicros, auto: false,
		budget:   cfg.BufferMicros,
		sink:     func(id uint32, _ uint8) { r.observeLoss(id) },
		lateSink: func(id uint32, _ uint8) { r.observeLossWindow(id) },
	}
	// The settled-loss walk uses a fixed holdoff because consecutive-clean policy
	// needs stable loss classification. It feeds policy evidence, not rate sizing.
	r.settled = reorderWindow{
		cfgHoldoff: settledHoldoffMicros(cfg),
		auto:       false,
		budget:     cfg.BufferMicros,
		sink: func(id uint32, _ uint8) {
			if r.settledStarted && id > r.settledNext {
				r.settledLostSince += int(id - r.settledNext)
			}
			r.settledStarted, r.settledNext = true, id+1
		},
	}
	return r
}

// settledHoldoffMicros is the settled-loss walk's fixed reorder holdoff: an
// explicit ReorderHoldoffMicros wins; otherwise an eighth of the deadline budget,
// clamped to [10ms, 30ms].
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

// FeedSymbol decodes and absorbs one inbound symbol datagram, delivering ready
// symbols in order.
func (r *SlidingReceiver) FeedSymbol(now clock.Timestamp, datagram []byte) {
	r.FeedSymbolECN(now, datagram, NotECT)
}

// FeedSymbolECN is FeedSymbol carrying the datagram's IP ECN codepoint. Only
// admitted symbols contribute to the reported fraction, so malformed traffic
// cannot forge congestion input.
func (r *SlidingReceiver) FeedSymbolECN(now clock.Timestamp, datagram []byte, ecn ECN) {
	sym, err := wire.DecodeSymbol(datagram)
	if err != nil || sym.Flow != r.cfg.Flow {
		return
	}
	var repairPayload []byte
	if sym.Kind == wire.Systematic || sym.Kind == wire.UnitRepair {
		if _, ok := systematicSourceLength(sym, r.cfg.SymbolSize); !ok {
			r.stats.Rejected++
			return
		}
	} else {
		var ok bool
		repairPayload, ok = expandRepairPayloadInto(sym, r.cfg.SymbolSize, r.repairScratch)
		if !ok {
			// A malformed compact prefix or a full-width mismatch cannot enter GF
			// arithmetic without changing the equation.
			r.stats.Rejected++
			return
		}
		if sym.HasSourceLength {
			r.repairScratch = repairPayload
		}
	}
	dl := clampDeadline(now, clock.Timestamp(sym.Deadline), r.cfg.BufferMicros)
	switch sym.Kind {
	case wire.Systematic:
		id := sym.SrcIndex
		if !r.admit(id) {
			r.stats.Rejected++
			return
		}
		r.observeECN(ecn)
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
		// advancing the cursor past it before the source arrives. A true duplicate is
		// harmless because its second copy takes the already-settled path.
		r.settled.feed(now, id, 0)
		if id < r.dec.Cursor() || r.directRecv[id] {
			r.stats.Duplicates++
		} else {
			r.directRecv[id] = true
			r.symDL[id] = dl // gate this id by its own true deadline, not the uniform-spacing fit
		}
		r.updateRef(id, dl)
		r.observeSlack(now, dl)
		sourceLen, _ := systematicSourceLength(sym, r.cfg.SymbolSize)
		coded := makeCodedSource(sym.Payload[:sourceLen], r.cfg.SymbolSize, dl)
		r.dec.AddSystematic(id, coded)
		if b := r.inboundEpochFor(sym.WindowBase, int(sym.N), id); b != nil {
			r.absorbEpochRecovered(b.dec.AddSystematic(id, coded))
		}
	case wire.UnitRepair:
		id := sym.SrcIndex
		if !r.admit(id) {
			r.stats.Rejected++
			return
		}
		r.observeECN(ecn)
		if id < r.dec.Cursor() {
			r.stats.Duplicates++
		}
		r.symDL[id] = dl
		r.updateRef(id, dl)
		r.observeSlack(now, dl)
		sourceLen, _ := systematicSourceLength(sym, r.cfg.SymbolSize)
		coded := makeCodedSource(sym.Payload[:sourceLen], r.cfg.SymbolSize, dl)
		r.dec.AddSystematic(id, coded)
		r.feedExactToEpochs(id, coded)
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
		r.observeECN(ecn)
		r.updateRef(hi, dl)
		if _, epochRow := code.BlockRepairIndex(sym.RepairKey); epochRow && n == epochBlockSymbols {
			// The stable block lane owns epoch rows exclusively. Exposing their
			// coefficients to the moving RREF recreates the coupled-span geometry
			// this lane exists to avoid.
			r.dec.Cover(hi)
			if b := r.inboundEpochFor(sym.WindowBase, n, hi); b != nil {
				recovered := b.dec.AddRepair(sym.WindowBase, n, sym.RepairKey, repairPayload)
				r.absorbEpochRecovered(recovered)
			}
		} else {
			r.dec.AddRepair(sym.WindowBase, n, sym.RepairKey, repairPayload)
		}
	case wire.SparseRepair:
		if len(sym.SparseIDs) == 0 {
			return
		}
		hi := sym.SparseIDs[len(sym.SparseIDs)-1]
		if !r.admit(hi) {
			r.stats.Rejected++
			return
		}
		r.observeECN(ecn)
		r.updateRef(hi, dl)
		r.dec.AddSparseRepair(sym.SparseIDs, sym.RepairKey, repairPayload)
	}
	r.pump(now)
	r.pruneEpochs()
	r.maybeFeedback(now)
}

func (r *SlidingReceiver) observeECN(ecn ECN) {
	r.ecnSeen++
	if ecn == CE {
		r.ecnCE++
	}
}

// inboundEpochFor returns the bounded decoder named by base and width. Source
// hints and epoch equations share this path, so either can establish the epoch.
func (r *SlidingReceiver) inboundEpochFor(base uint32, n int, coverID uint32) *inboundEpoch {
	if n != epochBlockSymbols || coverID < base || uint64(base)+uint64(n) > 1<<32 ||
		coverID >= base+uint32(n) {
		return nil
	}
	if b := r.epochs[base]; b != nil {
		return b
	}
	if base+uint32(n) <= r.dec.Cursor() {
		return nil
	}
	r.pruneEpochs()
	if len(r.epochs) >= maxInboundEpochs {
		return nil
	}
	if r.epochs == nil {
		r.epochs = make(map[uint32]*inboundEpoch)
	}
	b := &inboundEpoch{n: n, dec: code.NewDecoder(codedSymbolSize(r.cfg.SymbolSize), base, n)}
	r.epochs[base] = b
	return b
}

func (r *SlidingReceiver) absorbEpochRecovered(recovered []code.Recovered) {
	for _, rec := range recovered {
		r.dec.AddSystematic(rec.ID, rec.Data)
	}
}

// feedExactToEpochs folds a value recovered by another lane into every live
// block that covers it. Isolation is about equation geometry, not withholding
// known columns: unit repair or ordinary RLNC can reduce an epoch system just as a
// directly received systematic can.
func (r *SlidingReceiver) feedExactToEpochs(id uint32, coded []byte) {
	for base, b := range r.epochs {
		if id < base || id >= base+uint32(b.n) {
			continue
		}
		r.absorbEpochRecovered(b.dec.AddSystematic(id, coded))
	}
}

func (r *SlidingReceiver) pruneEpochs() {
	cur := r.dec.Cursor()
	for base, b := range r.epochs {
		if base+uint32(b.n) <= cur {
			delete(r.epochs, base)
		}
	}
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
	r.pruneEpochs()
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
		r.feedExactToEpochs(rec.ID, rec.Data)
		payload, _, exactDeadline, valid := parseCodedSource(rec.Data, r.cfg.SymbolSize)
		if !valid {
			r.stats.Rejected++
			r.lateDrops++
			r.attributeFrame(rec.ID, true)
			delete(r.directRecv, rec.ID)
			delete(r.symDL, rec.ID)
			continue
		}
		exactDeadline = clampDeadline(now, exactDeadline, r.cfg.BufferMicros)
		// Enforce the same hard deadline for direct and recovered symbols. Direct
		// source carries its exact stamp in symDL; a lost/recovered id uses the
		// deadline fit maintained from neighboring exact stamps. There is no recovery
		// grace: the advertised deadline is a hard bound.
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
			if now.After(exactDeadline) {
				r.lateDrops++
				r.attributeFrame(rec.ID, true)
				delete(r.directRecv, rec.ID)
				continue
			}
		}
		r.attributeFrame(rec.ID, false)
		r.deliverQ = append(r.deliverQ, deliveredSym{rec.ID, append([]byte(nil), payload...)})
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
		if since < feedbackIntervalMicros && (!r.fbDue || since < eventFeedbackMinMicros) {
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
	var ceFrac uint16
	if r.ecnSeen > 0 {
		ceFrac = uint16(uint64(r.ecnCE) * 65535 / uint64(r.ecnSeen))
	}
	fb := wire.Feedback{
		Flow:               r.cfg.Flow,
		DecodedLowEdge:     r.dec.Cursor(),
		HighestSeen:        r.dec.Highest(),
		Deficit:            uint16(def),
		EcnCE:              ceFrac,
		LossRate:           uint16(r.lossEstimate() * 65535),
		Burstiness:         uint16(r.meanBurstQ8),
		CongestionLoss:     uint16(r.clSinceFB), // pre-recovery loss this interval
		Frames:             frames,
		DecodableFrames:    decFrames,
		Keyframes:          keys,
		DecodableKeyframes: decKeys,
		NewestDecodableLTR: r.newestDecLTR,
		BrokenAnchors:      r.brokenAnchors,
		// The stuck-neighborhood closure basis: free columns among the 64 ids above
		// the cursor. Each exact answer removes one independent degree of freedom;
		// requesting arbitrary unresolved pivots can waste a repair and fail to close.
		Missing: r.dec.ClosureIn(r.dec.Cursor()),
		// Settled-loss evidence for the sender's floor decay (reorder-tolerant;
		// see wire.Feedback.SettledLost and the settled walk's construction).
		SettledLost: uint16(settledLost),
		OutageRun:   r.outageRunSinceFB,
	}
	// Generation mode uses Deficits as 32 one-byte generation deficits. Sliding
	// mode reuses the same fixed bytes as four continuation words for its closure
	// basis, extending the first Missing word without growing feedback.
	if !r.disableExtendedClosure {
		var closureMasks []uint64
		for word := 1; word*closureWordBits < slidingMaxWin; word++ {
			base := uint64(r.dec.Cursor()) + uint64(word*closureWordBits)
			if base > uint64(^uint32(0)) || base >= uint64(r.dec.Highest()) {
				break
			}
			closureMasks = append(closureMasks, r.dec.ClosureIn(uint32(base)))
		}
		setClosureExtensions(&fb, closureMasks)
	}
	r.sendQ = append(r.sendQ, wire.EncodeFeedback(nil, fb))
	r.clSinceFB = 0 // per-interval; consumers integrate the reported deltas
	r.ecnSeen, r.ecnCE = 0, 0
	r.settledLostSince = 0 // per-interval, like clSinceFB
	r.outageRunSinceFB = 0 // per-interval time-diversity evidence
	// No open-deficit latch — loss onset only (see the generation receiver's doc).
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
