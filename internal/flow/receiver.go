package flow

import (
	"sort"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// ReceiverStats counts delivery outcomes.
type ReceiverStats struct {
	Delivered  uint64 // source symbols delivered in order
	Lost       uint64 // source symbols skipped (unrecoverable before deadline)
	Recovered  uint64 // source symbols surfaced by a repair symbol (not received directly)
	Duplicates uint64 // symbols arriving for an id already delivered/ready
	// WireLost is the cumulative PRE-recovery source wire-loss — symbols the network
	// dropped, counted before the decoder and NEVER decremented on a successful
	// decode. The honest congestion signal (RFC 9265, N1): coding cannot hide it.
	WireLost uint64
	// Rejected counts inbound symbols refused by the resource-safety admission cap
	// (declared window too wide, or too many live generations) — the DoS bound (N1).
	Rejected uint64
	// Evicted counts source ids dropped by media-aware early eviction
	// (Config.EvictBrokenFrames): ids of a frame already known undecodable (its own loss
	// or a dead reference sub-tree), dropped before their deadline so the next decodable
	// frame delivers sooner. Distinct from Lost (a deadline/unrecoverable byte-loss):
	// these were sacrificed deliberately because their picture could never decode.
	Evicted uint64
}

// FrameStats reports media-frame decodability the receiver computes parse-free from the
// per-symbol frame descriptors (WP6): how many access units / keyframes were decodable
// (delivered with their whole dependency closure intact) versus total. It turns Meld's
// byte-level delivery into a picture-level QoE signal without the receiver parsing the
// codec. Approximate where a frame had NO directly-received symbol (coding rebuilds
// payloads, not headers): such a fully-recovered frame's structure is unknown, so it is
// not counted, and a lost id at a frame boundary may be attributed to the adjacent frame.
type FrameStats struct {
	Frames             uint64 // displayed coded pictures resolved (excludes non-picture metadata/parameter units)
	DecodableFrames    uint64 // of those, decodable (delivered + dependency closure intact)
	Keyframes          uint64 // resolved random-access points
	DecodableKeyframes uint64
}

// frameInfo is the receiver's per-access-unit propagation state.
type frameInfo struct {
	refs      []uint32 // dependency frame starts (all must be decodable)
	length    uint16   // chunk count: the frame's ids are [start, start+length)
	rap       bool
	nonPic    bool
	broken    bool // a source id of this frame was lost (not recovered)
	resolved  bool
	decodable bool
}

// frameRefWindow bounds how far back a frame's reference may be found before it is
// treated as lost (refs are recent — within a GOP); frameMapCap bounds the live frame
// map so the receiver's propagation state stays O(1) regardless of stream length.
const (
	frameRefWindow = 256
	frameMapCap    = 4096
)

// deliveredSym is one in-order delivered source symbol: its source id and payload. The
// id lets the host derive the per-symbol AEAD nonce (epoch‖src_index) for a recovered
// symbol, which carries no wire header of its own — the core reports the id it already
// knows, staying crypto-blind.
type deliveredSym struct {
	id   uint32
	data []byte
}

// genState is one generation's decoder at the receiver.
type genState struct {
	dec *code.Decoder
	n   int
}

// Receiver is the coded receive half of a flow. The host feeds inbound datagrams
// via FeedSymbol, drives time via Tick, and drains delivered media via
// PollDeliver and feedback datagrams via PollSend. It delivers source symbols in
// strict order, recovering erasures from repair symbols and skipping any
// generation whose deadline passes. Deterministic; not safe for concurrent use.
type Receiver struct {
	cfg         Config
	pool        *code.Pool // recycles symbol payload buffers across generation decoders
	gens        map[uint32]*genState
	ready       map[uint32][]byte          // recovered/received payloads not yet delivered
	symDL       map[uint32]clock.Timestamp // exact stamped deadline (write time + budget) of each directly-received id at/above the cursor; pruned as the cursor advances. A received symbol is delivered/evicted by its OWN deadline, so a generation written as one burst (a whole access unit at one instant) is not gated by the uniform-spacing deadline fit it violates.
	cursor      uint32                     // next source id to deliver
	highestSeen uint32                     // one past the highest source id any symbol has covered
	lastFB      clock.Timestamp
	fedOnce     bool
	deliverQ    []deliveredSym
	sendQ       [][]byte
	stats       ReceiverStats

	// A structural gap is a generation at the delivery cursor for which no symbol has
	// arrived, so there is no decoder state to report a rank deficit from. Hold the verdict
	// briefly so a whole generation that is only reordered-late is not mistaken for loss.
	structGapActive bool
	structGapCursor uint32
	structGapAt     clock.Timestamp

	// ECN / L4S congestion signal (N3): over each feedback interval, the CE-marked
	// fraction of ADMITTED symbols, echoed to the sender's congestion controller so it
	// reacts to the marks an L4S AQM sets before a standing queue forms. ecnSeen counts
	// admitted symbols, ecnCE the CE-marked subset; reported as parts-per-65535 in
	// Feedback.EcnCE. Only admitted symbols count, so a forged-symbol flood cannot fake
	// congestion. Zero (no marks) reports 0 — a pre-ECN / bleached path is unaffected.
	ecnSeen uint32
	ecnCE   uint32

	// Per-symbol deadline extrapolation. Symbols carry their own deadline (write time
	// + budget); the receiver fits deadline(id) = refDL + (id-refID)*intervalUs from
	// the stamps so a RECOVERED symbol (which never arrived to carry one) gets the same
	// staggered deadline as its neighbors, and the cursor evicts per symbol, not per
	// generation — a generation's already-received tail is no longer dropped when the
	// cursor stalls on one unrecoverable symbol at the shared deadline.
	haveRef    bool
	refID      uint32
	refDL      clock.Timestamp
	intervalUs int64
	refSamples int

	// Channel-erasure-rate estimator (reported to the sender's redundancy
	// controller): the fraction of the dense systematic source-id sequence not
	// directly received, over a sliding window, smoothed by an EWMA and a slow
	// max-hold (the reported value is the larger, so a burst is covered).
	lossStarted      bool
	lossBootstrapped bool   // first (short) loss window reported — subsequent ones use the full width
	lossBase         uint32 // source id at the current loss window's start
	lossHighest      uint32 // highest systematic id seen in the window
	lossRecv         int    // systematic symbols directly received in the window
	pEWMA            float64
	pHold            float64

	// Forward-gap walk over the dense source-id sequence (N1 honest loss + N2 burst
	// structure): a first-arrival id past the expected one means [expectNext, id)
	// were dropped on the wire — one loss RUN of that length. Assumes near-in-order
	// arrival; reorder over-counts loss, which is conservative for sizing. Feeds the
	// pre-recovery loss count and the mean burst length, independent of decode.
	haveExpect  bool
	expectNext  uint32 // next source id expected in order
	clSinceFB   uint32 // pre-recovery loss since the last feedback (→ CongestionLoss)
	meanBurstQ8 uint32 // smoothed mean loss-run length, Q8 (256 == i.i.d.; → Burstiness)

	// Reorder window for the loss estimate (ReorderHoldoffMicros): a small resequencer in front of
	// observeLoss that settles a source id received-or-lost only after lower ids have had a holdoff to
	// arrive, so a reordered-late id is counted RECEIVED (not a fictitious loss that over-sizes repair).
	// reseqSeen holds arrived ids in (reseqNext, reseqHigh]; reseqGapAt is when reseqNext first became a
	// gap (a higher id arrived while it was missing). Single-path only.
	reseqStarted  bool
	reseqNext     uint32
	reseqHigh     uint32
	reseqGapAt    clock.Timestamp
	reseqSeen     map[uint32]uint8 // arrived-out-of-order ids → their stamped pathID (for multipath co-loss)
	reorderHoldUs int64            // AutoReorderHoldoff: tracked reorder spread (max-hold of fill delays, grown by late arrivals, decayed)

	// Multipath co-loss estimation (N5). When the sender spreads across N paths, systematic
	// id k rides path k mod N, so each block of N consecutive ids forms one aligned slot. The
	// forward-gap walk's in-order arrived/lost decisions are grouped into slots and folded
	// into coEst, whose per-path marginals (→ PathLoss) and per-slot erasure-count histogram
	// (→ SlotDist) are reported so the sender's joint-tail sizer sees the cross-path
	// correlation an i.i.d.-union sizer misses. Disabled (nothing reported) on a single path.
	mpEnabled  bool
	paths      int
	coEst      *coLossEstimator
	mpSlot     uint32 // the slot index (id / paths) currently accumulating
	mpLost     []bool // per-path lost flags for mpSlot (len paths)
	mpHave     int    // path-positions filled in mpSlot
	mpMismatch int    // arrived stamps disagreeing with the id-mod-paths model (path-layout cross-check)

	// Frame-level loss propagation (WP6). Systematic symbols carry an access-unit
	// descriptor (FrameStart = the frame's first source id + the dominant reference's
	// FrameStart + a RAP flag). The receiver learns the frame boundaries from
	// directly-received systematics (frameStarts, kept sorted) and attributes EVERY id —
	// delivered, recovered, or lost — to a frame by POSITION (the largest start ≤ id), so a
	// later frame's losses cannot pollute an earlier one. A frame is decodable iff none of
	// its ids was lost AND its reference is decodable; the result folds into FrameStats. nil
	// until the first descriptor arrives (zero cost for non-media flows).
	frames      map[uint32]*frameInfo // by FrameStart
	frameStarts []uint32              // distinct FrameStart values, ascending
	curStart    uint32                // the frame the delivery cursor is currently inside
	haveCur     bool
	idDelivered map[uint32]bool // recent id → delivered?, for resolving a reference frame
	fstats      FrameStats      // that had no descriptor (fully recovered, no header)
}

// NewReceiver constructs a Receiver for cfg.
func NewReceiver(cfg Config) *Receiver {
	r := &Receiver{
		cfg:         cfg,
		pool:        code.NewPool(cfg.SymbolSize),
		gens:        make(map[uint32]*genState),
		ready:       make(map[uint32][]byte),
		symDL:       make(map[uint32]clock.Timestamp),
		intervalUs:  1,
		meanBurstQ8: burstQ8One, // start at the i.i.d. baseline (mean run length 1)
		mpEnabled:   cfg.multipath(),
		paths:       cfg.paths(),
	}
	if r.mpEnabled {
		r.coEst = newCoLossEstimator(r.paths)
		r.mpLost = make([]bool, r.paths)
	}
	return r
}

// FeedSymbol decodes and absorbs one inbound symbol datagram at time now,
// delivering any source symbols that become ready in order. Malformed or stale
// datagrams are ignored. Equivalent to FeedSymbolECN with a not-ECN-capable codepoint.
func (r *Receiver) FeedSymbol(now clock.Timestamp, datagram []byte) {
	r.FeedSymbolECN(now, datagram, NotECT)
}

// FeedSymbolECN is FeedSymbol carrying the datagram's ECN codepoint (read by the host from
// the IP header). A CE mark on an admitted symbol is counted toward the CE-marked fraction
// reported to the sender's congestion controller (N3 / L4S); the codepoint does not affect
// decoding.
func (r *Receiver) FeedSymbolECN(now clock.Timestamp, datagram []byte, ecn ECN) {
	sym, err := wire.DecodeSymbol(datagram)
	if err != nil || sym.Flow != r.cfg.Flow {
		return
	}
	n := int(sym.N)
	if n <= 0 {
		n = r.cfg.GenSize
	}
	base := sym.WindowBase
	// Resource-safety admission cap (N1): refuse a symbol whose declared window is
	// wider than ls_max_size, whose window sits beyond the bounded look-ahead horizon,
	// or that would push the live-decoder count past its cap — so a forged symbol
	// cannot allocate unbounded decoder state or explode the delivery range. Checked
	// before any allocation or estimator update so forged input cannot pollute either.
	if !r.admit(sym, n, base) {
		r.stats.Rejected++
		return
	}
	// Count the ECN signal on admitted symbols only (a rejected forgery cannot fake a mark).
	r.ecnSeen++
	if ecn == CE {
		r.ecnCE++
	}
	// The channel-erasure estimate counts a systematic's arrival as a NETWORK delivery
	// of its id, independent of whether it is in time to be delivered. A symbol
	// received LATE — its generation's deadline already skipped the cursor past it —
	// was NOT dropped by the channel; counting it as loss conflates deadline-loss with
	// channel-loss and inflates the redundancy controller (at a budget below the RTT a
	// generation's tail routinely lands past its shared deadline, which otherwise reads
	// as a fictitious ~35% channel loss and triples proactive repair). So observe it
	// before the cursor/deadline gates. Each id's systematic arrives once, so this
	// counts each network delivery once.
	if sym.Kind == wire.Systematic {
		if r.reorderEnabled() {
			r.reseqFeed(now, sym.SrcIndex, sym.PathID)
		} else {
			r.observeLoss(sym.SrcIndex, sym.PathID)
		}
		if sym.HasFrameDesc {
			r.noteFrame(sym)
		}
	}
	if base+uint32(n) <= r.cursor {
		if sym.Kind == wire.Systematic {
			r.stats.Duplicates++
		}
		return // every id in this generation is already delivered/skipped
	}
	// Clamp the peer-stamped deadline to a sane window around now before it drives any
	// deadline state — a forged Deadline would otherwise overflow the extrapolation math
	// (symDeadline) and poison the monotonic refDL backstop. A no-op for honest stamps.
	dl := clampDeadline(now, clock.Timestamp(sym.Deadline), r.cfg.BufferMicros)
	g := r.gen(base, n)
	switch sym.Kind {
	case wire.Systematic:
		r.updateRef(sym.SrcIndex, dl)
		if sym.SrcIndex >= r.cursor {
			r.symDL[sym.SrcIndex] = dl // gate this id by its own true deadline, not the fit
		}
		if sym.SrcIndex < r.cursor || r.hasReady(sym.SrcIndex) {
			r.stats.Duplicates++
		}
		r.absorb(g.dec.AddSystematic(sym.SrcIndex, sym.Payload), false)
	case wire.Repair:
		r.updateRef(base+uint32(n)-1, dl)
		r.absorb(g.dec.AddRepair(base, n, sym.RepairKey, sym.Payload), true)
	}
	if end := base + uint32(n); end > r.highestSeen {
		r.highestSeen = end
	}
	r.pump(now)
	r.reap()
	r.maybeFeedback(now)
}

// Tick advances receiver time, enforcing deadline eviction and emitting periodic
// feedback even when no symbol arrives.
func (r *Receiver) Tick(now clock.Timestamp) {
	if r.reorderEnabled() {
		r.reseqDrain(now) // settle losses whose holdoff expired without a new arrival
	}
	r.pump(now)
	r.maybeFeedback(now)
}

// PollDeliver returns the next in-order delivered source symbol — its source id and
// media chunk — and true, or 0/nil/false when none is pending. The id is the AEAD nonce
// input the host needs to open an encrypted symbol (it equals the delivery cursor; the
// core reports it rather than the host re-deriving it, which losses would desync).
func (r *Receiver) PollDeliver() (uint32, []byte, bool) {
	if len(r.deliverQ) == 0 {
		return 0, nil, false
	}
	d := r.deliverQ[0]
	r.deliverQ = r.deliverQ[1:]
	return d.id, d.data, true
}

// PollSend returns the next feedback datagram to transmit and true, or nil/false.
func (r *Receiver) PollSend() ([]byte, bool) {
	if len(r.sendQ) == 0 {
		return nil, false
	}
	d := r.sendQ[0]
	r.sendQ = r.sendQ[1:]
	return d, true
}

// Stats returns a snapshot of delivery outcomes.
func (r *Receiver) Stats() ReceiverStats { return r.stats }

// LossEstimate exposes the current reported channel-erasure estimate (for tests).
func (r *Receiver) LossEstimate() float64 { return r.lossEstimate() }

// MeanBurstQ8 exposes the current smoothed mean loss-run length in Q8 (for tests).
func (r *Receiver) MeanBurstQ8() uint32 { return r.meanBurstQ8 }

// admit reports whether an inbound symbol is within the resource-safety bounds: a
// window no wider than ls_max_size, a systematic id inside its declared window, a
// window within the bounded look-ahead horizon, and a new generation only while
// under the live-decoder cap. A duplicate (entirely below the cursor) allocates
// nothing and is always admitted. Honest symbols (N ≤ GenSize, window near the
// cursor) always pass; the caps bite only on forged input.
func (r *Receiver) admit(sym wire.Symbol, n int, base uint32) bool {
	if len(sym.Payload) != r.cfg.SymbolSize {
		// Every coded symbol (systematic or repair) is exactly SymbolSize on the wire; a different
		// length means the peer's SymbolSize disagrees with ours — and SymbolSize is configured
		// independently on each end, not negotiated. Reject it rather than zero-pad/truncate it
		// into the GF math, which would silently corrupt the recovered bytes (the genBaseOf class).
		return false
	}
	if n < 1 || n > r.cfg.maxGenSymbols() {
		return false
	}
	if sym.Kind == wire.Systematic && (sym.SrcIndex < base || sym.SrcIndex >= base+uint32(n)) {
		return false // a systematic id must lie within its declared window
	}
	if base+uint32(n) <= r.cursor {
		return true // duplicate of delivered/skipped ids — no allocation
	}
	horizon := int64(r.cfg.maxRetainedGens()) * int64(r.cfg.GenSize)
	if int64(base)-int64(r.cursor) > horizon {
		return false // window beyond the bounded look-ahead
	}
	if r.gens[base] == nil && len(r.gens) >= r.cfg.maxRetainedGens() {
		return false // new generation past the live-decoder cap
	}
	return true
}

func (r *Receiver) gen(base uint32, n int) *genState {
	g := r.gens[base]
	if g == nil {
		dec := code.NewDecoder(r.cfg.SymbolSize, base, n)
		dec.SetPool(r.pool)
		g = &genState{dec: dec, n: n}
		r.gens[base] = g
	}
	return g
}

// absorb stores newly recovered/received source payloads as ready for delivery.
// repaired marks symbols surfaced by a repair symbol (for the Recovered stat).
func (r *Receiver) absorb(rec []code.Recovered, repaired bool) {
	for _, x := range rec {
		if x.ID < r.cursor {
			continue
		}
		if _, ok := r.ready[x.ID]; ok {
			continue
		}
		r.ready[x.ID] = append([]byte(nil), x.Data...)
		if repaired {
			r.stats.Recovered++
		}
	}
}

// pump delivers ready source symbols in strict order and skips any whose
// per-symbol deadline (or the global deadline backstop) has passed. It never
// delivers a symbol after its own deadline and never delivers out of order.
func (r *Receiver) pump(now clock.Timestamp) {
	for r.cursor < r.highestSeen {
		id := r.cursor
		// Frame-atomic delivery: when the cursor sits at a known access unit's first id, deliver
		// or drop the WHOLE frame as a unit so the app never sees a partial (corrupt) picture.
		if r.cfg.FrameAtomic {
			if start, length, ok := r.frameStartAt(id); ok {
				if !r.pumpFrame(now, start, length) {
					return // the frame is still recoverable and in time — wait for it
				}
				continue
			}
		}
		// Media-aware early eviction: an id whose frame can never decode (its own loss, or
		// a dead reference sub-tree) is dropped now rather than waiting out its deadline, so
		// the next decodable frame delivers sooner and the cursor — the sender's
		// stop-repair signal — advances past the dead GOP. Never fires for a decodable
		// frame (frameDoomed only flags confirmed-dead state), preserving picture-completeness.
		if r.cfg.EvictBrokenFrames && r.frameDoomed(id) {
			r.evictAt(id, r.hasReady(id))
			continue
		}
		gd, gdKnown := r.deadlineOf(id)
		refDL := r.refDL
		payload, ready := r.ready[id]
		switch {
		case ready && (!gdKnown || !now.After(gd)):
			r.attributeFrame(id, false)
			r.deliverQ = append(r.deliverQ, deliveredSym{id, payload})
			delete(r.ready, id)
			delete(r.symDL, id)
			r.cursor++
			r.stats.Delivered++
		case gdKnown && now.After(gd):
			r.dropAt(id, ready)
		case r.haveRef && r.refID >= id && now.After(refDL):
			// Backstop for an id with no per-id deadline (never arrived, fit unprimed): drop
			// only once a symbol for an id AT OR ABOVE the cursor is itself overdue. Deadlines
			// are non-decreasing in id, so refDL (the highest id seen) ≥ this id's deadline —
			// the id is provably past due, not merely behind a stale earlier-generation anchor
			// (the clean-link cliff: a gap between bursts must wait, never evict on time).
			r.dropAt(id, ready)
		default:
			return // waiting on an in-time, not-yet-ready symbol
		}
	}
}

func (r *Receiver) dropAt(id uint32, ready bool) {
	r.attributeFrame(id, true)
	if ready {
		delete(r.ready, id)
	}
	delete(r.symDL, id)
	r.cursor++
	r.stats.Lost++
}

// evictAt drops a source id whose frame is already known undecodable — like dropAt but
// counted as a deliberate media-aware eviction (not a deadline loss), and reached before
// the id's deadline. The id is attributed as not delivered, so the frame it belongs to is
// (re)confirmed broken and its own dependents cascade.
func (r *Receiver) evictAt(id uint32, ready bool) {
	r.attributeFrame(id, true)
	if ready {
		delete(r.ready, id)
	}
	delete(r.symDL, id)
	r.cursor++
	r.stats.Evicted++
}

// frameStartAt reports whether id is the FIRST id of a known access unit (one with a
// directly-received descriptor), returning its chunk count — the gate for frame-atomic delivery.
func (r *Receiver) frameStartAt(id uint32) (uint32, uint16, bool) {
	if r.frames == nil {
		return 0, 0, false
	}
	if fi := r.frames[id]; fi != nil && fi.length > 0 {
		return id, fi.length, true
	}
	return 0, 0, false
}

// frameDeadline returns the access unit's display deadline taken from an ARRIVED chunk's EXACT
// stamp (every chunk of a burst-written frame shares one deadline) and whether any chunk has
// supplied one. It deliberately never EXTRAPOLATES: the uniform per-id fit is invalid for bursty
// streams, so a not-yet-arrived chunk gets the frame's real deadline from a sibling, not a
// reorder-skewed estimate. Returns false only if no chunk of the frame has arrived (then the
// caller falls back to the refDL backstop).
func (r *Receiver) frameDeadline(start, end uint32) (clock.Timestamp, bool) {
	var dl clock.Timestamp
	found := false
	for id := start; id < end; id++ {
		if s, ok := r.symDL[id]; ok && (!found || s < dl) {
			dl, found = s, true
		}
	}
	return dl, found
}

// pumpFrame resolves a whole access unit [start, start+length) atomically: it delivers every
// chunk together once they are ALL recoverable in time, or drops every chunk together (a clean
// gap, never a fragment) once the frame is doomed or its deadline passes incomplete. Returns
// false to wait — the frame is still recoverable and in time — and true once it is resolved.
func (r *Receiver) pumpFrame(now clock.Timestamp, start uint32, length uint16) bool {
	end := start + uint32(length)
	if r.frameDoomed(start) { // own id lost, or a dead reference sub-tree
		r.dropFrame(start, end)
		return true
	}
	// The frame's display deadline is taken from an ARRIVED chunk's EXACT stamp: every chunk of an
	// access unit is written as one burst (one instant), so they share one deadline. Never extrapolate
	// it — the uniform per-id fit (deadlineOf's fallback) is invalid for burst-written frames, where a
	// reorder-skewed refID would place a not-yet-arrived chunk's deadline in the past and drop a frame
	// whose chunks are merely reordered-late. At least one chunk always carries the descriptor that put
	// us here, so a stamp is present; if somehow none is, dKnown is false and only the refDL backstop
	// (the provably-past gate) can mark the frame overdue.
	d, dKnown := r.frameDeadline(start, end)
	overdue := (dKnown && now.After(d)) || (r.haveRef && r.refID >= start && now.After(r.refDL))
	complete := true
	for id := start; id < end; id++ {
		if _, ok := r.ready[id]; !ok {
			complete = false
			break
		}
	}
	switch {
	case complete && !overdue:
		for id := start; id < end; id++ {
			r.attributeFrame(id, false)
			r.deliverQ = append(r.deliverQ, deliveredSym{id, r.ready[id]})
			delete(r.ready, id)
			delete(r.symDL, id)
		}
		r.cursor = end
		r.stats.Delivered += uint64(length)
		return true
	case overdue:
		r.dropFrame(start, end) // incomplete (or recovered too late) at the deadline — drop clean
		return true
	default:
		return false // still incomplete but in time — wait
	}
}

// dropFrame discards a whole access unit as a clean gap: its already-recovered chunks are evicted
// (a deliberate media-aware drop, not delivered as a fragment) and its missing chunks are losses.
// None is delivered, so the frame is undecodable and its dependents cascade (attributeFrame).
func (r *Receiver) dropFrame(start, end uint32) {
	for id := start; id < end; id++ {
		ready := r.hasReady(id)
		r.attributeFrame(id, true)
		delete(r.ready, id)
		delete(r.symDL, id)
		if ready {
			r.stats.Evicted++ // a recoverable chunk dropped to keep the picture atomic
		} else {
			r.stats.Lost++ // a genuinely missing chunk
		}
	}
	r.cursor = end
}

// noteFrame records an access unit's descriptor from a directly-received systematic,
// inserting its FrameStart into the sorted boundary list (lazily allocating).
func (r *Receiver) noteFrame(sym wire.Symbol) {
	if r.frames == nil {
		r.frames = make(map[uint32]*frameInfo)
	}
	if r.frames[sym.FrameStart] != nil {
		return
	}
	r.frames[sym.FrameStart] = &frameInfo{
		refs: sym.FrameRefs, length: sym.FrameLen, rap: sym.FrameRAP,
		nonPic: sym.FrameNonPicture,
	}
	i := sort.Search(len(r.frameStarts), func(i int) bool { return r.frameStarts[i] >= sym.FrameStart })
	r.frameStarts = append(r.frameStarts, 0)
	copy(r.frameStarts[i+1:], r.frameStarts[i:])
	r.frameStarts[i] = sym.FrameStart
}

// attributeFrame tallies one delivered/lost source id into the frame whose range contains
// it — the largest known FrameStart ≤ id. When the cursor crosses into a new frame the one
// it leaves is resolved; a lost id breaks the frame it belongs to (by position, so a later
// frame's loss never pollutes an earlier frame). Ids before any known frame are skipped.
func (r *Receiver) attributeFrame(id uint32, lost bool) {
	if r.frames == nil {
		return
	}
	r.recordDelivery(id, !lost)
	i := sort.Search(len(r.frameStarts), func(i int) bool { return r.frameStarts[i] > id })
	if i == 0 {
		return // id precedes every known frame start
	}
	f := r.frameStarts[i-1]
	fi := r.frames[f]
	if fi == nil {
		return
	}
	// The id must lie within the frame's exact range [f, f+length); an id past the end is
	// a gap — a generation-fill phantom or an unknown (fully-recovered) frame's id — and is
	// NOT attributed, so it cannot falsely break this frame. The frame the cursor leaves is
	// resolved either way.
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
}

// resolveFrame finalizes a frame's decodability (delivered AND its reference decodable)
// and folds it into FrameStats, then prunes far-back resolved frames to stay bounded.
func (r *Receiver) resolveFrame(start uint32) {
	fi := r.frames[start]
	if fi == nil || fi.resolved {
		return
	}
	fi.resolved = true
	dec := !fi.broken
	// A frame is decodable only if EVERY dependency is — a B-frame needs both its
	// bracketing anchors. A known reference uses its resolved decodability; a reference
	// with no directly-received systematic (fully recovered, so no header — or lost) falls
	// back to whether its first id was delivered (exact for 1-chunk parameter sets).
	for _, refStart := range fi.refs {
		if !dec {
			break
		}
		if ref := r.frames[refStart]; ref != nil {
			// A dependency must be finalized before its dependent. References are EARLIER frames
			// the cursor has already passed, so resolving one now is safe and gives its TRUE
			// decodability. Without this, reorder/eviction can finalize a dependent before its
			// reference resolves, read the reference as not-yet-resolved (which is NOT the same as
			// undecodable), and wrongly doom the dependent and everything that transitively
			// references it — a whole-tail cascade from one out-of-order resolution.
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
	if len(r.frames) > frameMapCap {
		r.pruneFrames(start)
	}
}

// frameDoomed reports whether the frame containing source id can never decode, so its ids
// may be evicted early. It is CONSERVATIVE — true only on confirmed-dead state, never on a
// frame still in flight — so a decodable frame is never evicted:
//   - the frame is broken (one of its own ids was already lost), or
//   - a reference frame is resolved and not decodable (its whole sub-tree is dead;
//     decodability is transitive, so checking direct references suffices), or
//   - a reference with no descriptor (fully recovered or lost) had its anchor id
//     definitively NOT delivered.
//
// Returns false when id's frame is unknown (no directly-received systematic) or id lies past
// the frame's range (a generation-fill phantom) — those fall back to the deadline path.
func (r *Receiver) frameDoomed(id uint32) bool {
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

// recordDelivery notes whether a source id was delivered, in a window bounded to recent
// ids — enough to resolve a descriptor-less reference frame (references are recent).
func (r *Receiver) recordDelivery(id uint32, ok bool) {
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

// pruneFrames drops resolved frames far below the current one (references are recent),
// rebuilding the sorted boundary list.
func (r *Receiver) pruneFrames(cur uint32) {
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

// FrameStats returns the receiver's parse-free frame-decodability snapshot, resolving the
// in-progress frame the cursor currently occupies (accurate at end-of-stream / a lull).
func (r *Receiver) FrameStats() FrameStats {
	if r.haveCur {
		r.resolveFrame(r.curStart)
	}
	return r.fstats
}

// reap frees decoders for generations entirely below the delivery cursor.
func (r *Receiver) reap() {
	for base, g := range r.gens {
		if base+uint32(g.n) <= r.cursor {
			g.dec.Release() // recycle the generation's payload buffers for the next decoder
			delete(r.gens, base)
		}
	}
}

const structuralGapDeficit = 0xFF

func (r *Receiver) structuralGapReady(now clock.Timestamp) bool {
	if !r.structGapActive || r.structGapCursor != r.cursor {
		r.structGapActive = true
		r.structGapCursor = r.cursor
		r.structGapAt = now
		return false
	}
	return now.Sub(r.structGapAt) >= r.structuralGapHoldoff()
}

func (r *Receiver) clearStructuralGap() {
	r.structGapActive = false
}

func (r *Receiver) structuralGapHoldoff() int64 {
	h := r.reorderMarginUs()
	if floor := r.cfg.BufferMicros / 4; floor > h {
		h = floor
	}
	if h <= 0 {
		return feedbackIntervalMicros
	}
	if cap := r.cfg.BufferMicros / 2; cap > 0 && h > cap {
		h = cap
	}
	return h
}

func (r *Receiver) maybeFeedback(now clock.Timestamp) {
	if r.fedOnce && now.Sub(r.lastFB) < feedbackIntervalMicros {
		return
	}
	r.fedOnce = true
	r.lastFB = now
	// Report the rank deficit of each generation from the cursor, walking the ACTUAL
	// generation boundaries (base += width) the sender stamped on every symbol — not a fixed
	// stride — so the two ends need not be configured with the same generation width. The
	// sender's reactive walk mirrors this exactly.
	var defs [wire.MaxFeedbackGens]uint8
	if base, ok := r.genBaseContaining(r.cursor); ok {
		r.clearStructuralGap()
		for i := range defs {
			g := r.gens[base]
			if g == nil {
				if base < r.highestSeen {
					defs[i] = structuralGapDeficit
				}
				break // structural gap (an entirely-lost generation); sender clamps to its width
			}
			if d := g.dec.Deficit(g.n); d > 0 {
				if d > 255 {
					d = 255
				}
				defs[i] = uint8(d)
			}
			base += uint32(g.n)
		}
	} else if r.cursor < r.highestSeen {
		if r.structuralGapReady(now) {
			// The cursor is blocked on a generation for which no symbol arrived, so the receiver
			// has no decoder state and cannot compute a rank deficit. Report a saturated deficit
			// for the cursor generation; the sender walks its retained boundaries and clamps this
			// to that generation's width.
			defs[0] = structuralGapDeficit
		}
	} else {
		r.clearStructuralGap()
	}
	var ceFrac uint16
	if r.ecnSeen > 0 {
		ceFrac = uint16(uint64(r.ecnCE) * 65535 / uint64(r.ecnSeen)) // CE-marked fraction (N3 / L4S)
	}
	frames, decFrames, keys, decKeys := feedbackFrameStats(r.fstats)
	fb := wire.Feedback{
		Flow:               r.cfg.Flow,
		DecodedLowEdge:     r.cursor,
		HighestSeen:        r.highestSeen,
		Deficit:            uint16(defs[0]),
		EcnCE:              ceFrac,
		LossRate:           uint16(r.lossEstimate() * 65535),
		Deficits:           defs,
		CongestionLoss:     uint16(r.clSinceFB), // pre-recovery loss this interval (N1)
		Burstiness:         uint16(r.meanBurstQ8),
		Frames:             frames,
		DecodableFrames:    decFrames,
		Keyframes:          keys,
		DecodableKeyframes: decKeys,
	}
	r.ecnSeen, r.ecnCE = 0, 0 // per-interval; the CC integrates the reported fraction
	if r.mpEnabled && r.coEst.primed {
		// Per-path marginals (→ scheduler weighting) + the per-slot erasure-count histogram
		// (→ joint-tail sizer), ppm → parts per 65535 (matching LossRate), so the sender sees
		// the cross-path correlation an i.i.d.-union sizer misses (N5).
		marg, dist := r.coEst.marginals(), r.coEst.slotDist()
		fb.PathLoss = make([]uint16, len(marg))
		for i, m := range marg {
			fb.PathLoss[i] = ppmToP65535(m)
		}
		fb.SlotDist = make([]uint16, len(dist))
		for j, d := range dist {
			fb.SlotDist[j] = ppmToP65535(d)
		}
	}
	r.sendQ = append(r.sendQ, wire.EncodeFeedback(nil, fb))
	r.clSinceFB = 0 // per-interval; the CC loop integrates the reported deltas
}

// ppmToP65535 converts a rate in parts per million to parts per 65535 (the wire
// loss-field unit), clamped to the uint16 range.
func ppmToP65535(ppm int) uint16 {
	if ppm <= 0 {
		return 0
	}
	v := int64(ppm) * 65535 / 1_000_000
	if v > 65535 {
		v = 65535
	}
	return uint16(v)
}

// maxReorderDepth bounds the resequencer's in-flight set: if this many ids pile up above the
// still-missing low edge, the low edge is declared lost regardless of holdoff (a conservative
// fallback that bounds memory and forces progress on a pathological reorder/loss burst).
const maxReorderDepth = 1024

// reorderMarginUs is how long a NOT-yet-arrived id whose deadline is only EXTRAPOLATED (no received
// stamp) is given before the deadline path evicts it — the measured reorder spread, so a reordered-late
// id is not dropped on a deadline computed (under reorder) from a skewed refID before it can arrive. A
// genuinely lost id is still evicted one margin later; an id with its OWN received stamp is unaffected.
func (r *Receiver) reorderMarginUs() int64 {
	m := r.reorderHoldUs
	if r.cfg.ReorderHoldoffMicros > m {
		m = r.cfg.ReorderHoldoffMicros
	}
	return m
}

// reorderEnabled reports whether the loss-estimate reorder window is active. Works on single-path and
// multipath: each held id is replayed to observeLoss in order with its OWN stamped pathID, so the
// multipath co-loss estimator sees the same arrived/lost facts it would without the window — only
// with reordered-late ids correctly counted received rather than as fictitious per-path loss.
func (r *Receiver) reorderEnabled() bool {
	return r.cfg.ReorderHoldoffMicros > 0 || r.cfg.AutoReorderHoldoff
}

// holdoffMicros is the effective reorder window: the fixed config value, or — under
// AutoReorderHoldoff — the measured reorder spread plus a quarter margin, capped at half the deadline
// budget (holding longer than the budget would let the symbol miss its deadline regardless).
func (r *Receiver) holdoffMicros() int64 {
	if r.cfg.ReorderHoldoffMicros > 0 {
		return r.cfg.ReorderHoldoffMicros
	}
	h := r.reorderHoldUs + r.reorderHoldUs/4
	if cap := r.cfg.BufferMicros / 2; cap > 0 && h > cap {
		h = cap
	}
	return h
}

// reseqFeed routes a first-arrival systematic id through the reorder window before the loss
// estimators. It records the arrival, then drains every id whose verdict has now settled — RECEIVED
// (it arrived) or LOST (still missing past the holdoff) — feeding observeLoss the received ids in
// strict increasing order, so the gaps observeLoss infers between them are genuine losses, not reorder.
func (r *Receiver) reseqFeed(now clock.Timestamp, id uint32, pathID uint8) {
	if !r.reseqStarted {
		r.reseqStarted, r.reseqNext, r.reseqHigh = true, id, id
		r.reseqSeen = make(map[uint32]uint8)
		r.observeLoss(id, pathID) // the first id is in order by definition
		r.reseqNext = id + 1
		return
	}
	if id < r.reseqNext {
		// Arrived after it was already declared lost ⇒ the window was too short for this reorder. Grow
		// it (a kickstart from zero plus a quarter), capped, so the next such reorder is held long enough.
		if r.cfg.AutoReorderHoldoff && r.cfg.ReorderHoldoffMicros == 0 {
			r.reorderHoldUs += r.reorderHoldUs/4 + 2_000
			if cap := r.cfg.BufferMicros / 2; cap > 0 && r.reorderHoldUs > cap {
				r.reorderHoldUs = cap
			}
		}
		return // already settled — ignore for the loss estimate
	}
	r.reseqSeen[id] = pathID // remember the stamped path so the in-order replay carries the right one
	if id > r.reseqHigh {
		r.reseqHigh = id
	}
	r.reseqDrain(now)
}

// reseqDrain settles ids from reseqNext upward: an arrived id is fed to observeLoss in order; a
// still-missing id with a higher id present is held until the holdoff expires (or the in-flight set
// would exceed the depth cap), then skipped — the next received id's gap counts it lost. A contiguous
// lost run shares one gap-open time, so it drains in one holdoff. Called on arrival and on Tick (the
// holdoff can expire with no new arrival). Each received id is replayed with its OWN stamped pathID.
func (r *Receiver) reseqDrain(now clock.Timestamp) {
	if !r.reseqStarted {
		return
	}
	holdoff := r.holdoffMicros()
	for {
		if pid, ok := r.reseqSeen[r.reseqNext]; ok {
			if r.cfg.AutoReorderHoldoff && r.reseqGapAt != 0 { // a held gap that just FILLED — a reorder sample
				if s := now.Sub(r.reseqGapAt); s > r.reorderHoldUs {
					r.reorderHoldUs = s // max-hold up to cover the observed spread
				} else {
					r.reorderHoldUs -= r.reorderHoldUs / 8 // decay toward the recent spread
				}
			}
			delete(r.reseqSeen, r.reseqNext)
			r.observeLoss(r.reseqNext, pid)
			r.reseqNext++
			r.reseqGapAt = 0
			continue
		}
		if r.reseqHigh > r.reseqNext { // reseqNext is a gap — a higher id has arrived
			if r.reseqGapAt == 0 {
				r.reseqGapAt = now
			}
			if now.Sub(r.reseqGapAt) >= holdoff || r.reseqHigh-r.reseqNext > maxReorderDepth {
				if r.cfg.AutoReorderHoldoff {
					r.reorderHoldUs -= r.reorderHoldUs / 16 // a confirmed loss is not reorder — let the window relax
				}
				r.reseqNext++ // lost; the next received id's gap will count it (keep reseqGapAt for the run)
				continue
			}
		}
		return
	}
}

// observeLoss updates the channel-erasure-rate estimate from a first-arrival
// systematic source id: over a window of source ids the fraction NOT directly
// received is the channel loss (coding recovery is excluded — this measures what
// the network dropped, the signal the sender's controller sizes redundancy from).
// pathID is the symbol's host-stamped path, used to validate multipath co-loss alignment.
func (r *Receiver) observeLoss(id uint32, pathID uint8) {
	r.walkGap(id, pathID) // pre-recovery loss count + burst run-lengths (N1/N2), before the windowed rate
	if !r.lossStarted {
		r.lossStarted, r.lossBase, r.lossHighest, r.lossRecv = true, id, id, 1
		return
	}
	if id < r.lossBase {
		return // belongs to an already-closed window (late / reorder)
	}
	if id > r.lossHighest {
		r.lossHighest = id
	}
	r.lossRecv++
	win := lossWindowMin
	if w := 8 * r.cfg.GenSize; w > win {
		win = w
	}
	// Bootstrap fast: report the FIRST estimate after a single generation rather than waiting the
	// full variance-reduction window. Until then the sender's feed-forward proactive rate sits at
	// the floor and under-protects (most visible at high RTT, where the feedback delay already
	// postpones the estimate); a noisy-but-early estimate, kept conservative by the max-hold, is
	// far better than zero. Subsequent windows use the full width for low steady-state variance.
	if !r.lossBootstrapped && lossWindowMin < win {
		win = lossWindowMin
	}
	span := int(r.lossHighest-r.lossBase) + 1
	if span < win {
		return
	}
	r.lossBootstrapped = true
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
	r.lossBase, r.lossRecv = r.lossHighest+1, 0
}

// walkGap advances the forward-gap walk for a first-arrival source id: a jump past
// the expected id means [expectNext, id) were dropped on the wire — one loss run of
// that length. It accumulates the pre-recovery loss count (N1, never decremented on
// decode) and smooths the mean loss-run length in Q8 (N2). Late/reorder arrivals
// (id < expectNext) are already accounted and ignored.
func (r *Receiver) walkGap(id uint32, pathID uint8) {
	r.mpCheckPath(id, pathID) // the arrived stamp validates the loss-attribution model (genBaseOf class)
	if !r.haveExpect {
		r.haveExpect, r.expectNext = true, id+1
		r.mpFact(id, true)
		return
	}
	if id < r.expectNext {
		return
	}
	if run := id - r.expectNext; run > 0 {
		r.stats.WireLost += uint64(run)
		if cl := uint64(r.clSinceFB) + uint64(run); cl > 0xFFFF {
			r.clSinceFB = 0xFFFF // saturate; the CC loop integrates deltas
		} else {
			r.clSinceFB = uint32(cl)
		}
		// EWMA the run length toward the new sample in Q8, in SIGNED arithmetic (the
		// delta is negative when a short run follows a longer one — unsigned would
		// underflow and explode). Cap the per-run sample so one long outage can't
		// dominate the burst estimate (WireLost above still counts the full run).
		s := int64(run)
		if s > burstSampleCap {
			s = burstSampleCap
		}
		mb := int64(r.meanBurstQ8) + ((s<<8)-int64(r.meanBurstQ8))>>burstEWMAShift
		if mb < burstQ8One {
			mb = burstQ8One
		}
		r.meanBurstQ8 = uint32(mb)
	}
	r.mpReplayLost(r.expectNext, id) // [expectNext, id) were dropped on the wire
	r.mpFact(id, true)               // id itself arrived
	r.expectNext = id + 1
}

// coLossMaxReplay caps the per-arrival lost-fact replay so one large forward jump
// cannot loop unboundedly; the dropped middle of a long outage is all-paths-lost
// (maximal correlation) anyway, so under-sampling it errs toward MORE provisioning.
const coLossMaxReplay = 256

// mpMaxPathMismatch is how many arrived stamps may disagree with the id-mod-paths placement
// model before co-loss estimation gives up. A real path-count/layout mismatch disagrees on
// (nearly) every id, so it trips within the first handful — long before the estimator primes
// (coLossWindow slots) — while a few isolated disagreements (a corrupted or spoofed PathID:
// it rides the cleartext header, outside the AEAD AAD) are tolerated rather than permanently
// killing co-loss for the flow.
const mpMaxPathMismatch = 4

// mpCheckPath validates the co-loss estimator's deterministic placement model against the
// sender's PathID stamp on an arrived systematic. The estimator must attribute LOSSES — which
// carry no stamp, the symbol never arrived — to a path, so it reconstructs the sender's
// round-robin as id mod paths and the slot as id / paths. That reconstruction is correct only
// while both ends agree on the path count and the sender places id k on path k mod paths;
// Config.Paths is configured independently on each end (not negotiated), so a mismatch would
// silently misalign every slot and feed the sender's joint-tail sizer garbage per-path stats —
// worse than no multipath awareness. The PathID stamp is the ground truth the wire already
// carries (ST 2022-7 style), so cross-check it and, once disagreements pass mpMaxPathMismatch,
// disable co-loss reporting and fall back to the i.i.d.-union sizer. The union decoder still
// delivers (it is path-agnostic), so this costs only the correlation refinement, not data.
func (r *Receiver) mpCheckPath(id uint32, pathID uint8) {
	if !r.mpEnabled {
		return
	}
	if uint32(pathID) == id%uint32(r.paths) {
		return
	}
	r.mpMismatch++
	if r.mpMismatch >= mpMaxPathMismatch {
		r.mpEnabled = false // the two ends disagree on the path layout; stop reporting misaligned stats
	}
}

// mpFact folds one in-order per-id wire fact (arrived or lost) into the aligned-slot
// co-loss estimator: id rides path id mod paths and slot id / paths (the sender's
// round-robin). It accumulates a slot's per-path losses until all `paths` positions are
// seen, then folds the slot in. A slot-index discontinuity (a capped replay jump, or
// reorder) drops the partial slot rather than mis-aligning. No-op single-path.
func (r *Receiver) mpFact(id uint32, arrived bool) {
	if !r.mpEnabled {
		return
	}
	slot := id / uint32(r.paths)
	if r.mpHave > 0 && slot != r.mpSlot {
		r.mpHave = 0 // discontinuity: discard the incomplete slot
	}
	if r.mpHave == 0 {
		r.mpSlot = slot
		for i := range r.mpLost {
			r.mpLost[i] = false
		}
	}
	if !arrived {
		r.mpLost[id%uint32(r.paths)] = true
	}
	r.mpHave++
	if r.mpHave == r.paths {
		r.coEst.observe(r.mpLost)
		r.mpHave = 0
	}
}

// mpReplayLost emits a wire-loss fact for each id in [lo, hi), bounded by
// coLossMaxReplay so a large gap stays O(1) per arrival.
func (r *Receiver) mpReplayLost(lo, hi uint32) {
	if !r.mpEnabled || hi <= lo {
		return
	}
	if hi-lo > coLossMaxReplay {
		lo = hi - coLossMaxReplay
		if rem := lo % uint32(r.paths); rem != 0 {
			lo += uint32(r.paths) - rem // align to a slot boundary across the jump
		}
		r.mpHave = 0 // the slot straddling the jump is incomplete
	}
	for j := lo; j < hi; j++ {
		r.mpFact(j, false)
	}
}

// lossEstimate returns the conservative (max of EWMA and max-hold) erasure-rate
// estimate to report, clamped to [0, 0.95].
func (r *Receiver) lossEstimate() float64 {
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

// genBaseContaining returns the base of the received generation whose id range covers id, and
// true — or false if no received generation does (an entirely-lost generation, or id past the
// frontier). It replaces fixed-stride genBaseOf so the generation width need not be a shared
// constant: the receiver follows the per-generation width the sender stamps on every symbol.
func (r *Receiver) genBaseContaining(id uint32) (uint32, bool) {
	var best uint32
	found := false
	for base, g := range r.gens {
		if base <= id && id < base+uint32(g.n) {
			if !found || base < best {
				best, found = base, true
			}
		}
	}
	return best, found
}

// updateRef refines the per-symbol deadline fit from one stamped (id, deadline):
// it anchors on the highest id seen and tracks the inter-symbol interval by EWMA, so
// deadline(id) can be extrapolated for any id, including one only recovered (never
// directly received). Stamps for ids at or below the anchor are ignored (stale).
func (r *Receiver) updateRef(id uint32, dl clock.Timestamp) {
	if !r.haveRef {
		r.haveRef, r.refID, r.refDL = true, id, dl
		return
	}
	if id > r.refID {
		span := int64(id - r.refID)
		if sample := dl.Sub(r.refDL) / span; sample > 0 {
			r.intervalUs = r.intervalUs - r.intervalUs/4 + sample/4
			if r.intervalUs > maxIntervalMicros {
				r.intervalUs = maxIntervalMicros // bound the extrapolation multiplier (forged stamps)
			}
			r.refSamples++
		}
		r.refID, r.refDL = id, dl
	}
}

// deadlineOf returns id's delivery deadline and whether it is known. A directly-received
// symbol carries its own exact stamp (write time + budget), which is authoritative — used
// so a generation written as one burst (a whole access unit) is gated by each id's true
// deadline, never by the uniform-spacing fit it violates. An id that never arrived
// (recovered, or still missing) falls back to the extrapolated fit.
func (r *Receiver) deadlineOf(id uint32) (clock.Timestamp, bool) {
	if dl, ok := r.symDL[id]; ok {
		return dl, true
	}
	return r.symDeadline(id)
}

// symDeadline returns id's extrapolated per-symbol deadline, or false until the fit
// has a sample (the cursor then waits rather than evicting blind).
func (r *Receiver) symDeadline(id uint32) (clock.Timestamp, bool) {
	if !r.haveRef || r.refSamples == 0 {
		return 0, false
	}
	return r.refDL.Add((int64(id) - int64(r.refID)) * r.intervalUs), true
}

func (r *Receiver) hasReady(id uint32) bool {
	_, ok := r.ready[id]
	return ok
}
