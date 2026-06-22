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
	Frames             uint64 // access units resolved (excludes parameter-set-only units the shaper didn't mark)
	DecodableFrames    uint64 // of those, decodable (delivered + dependency closure intact)
	Keyframes          uint64 // resolved random-access points
	DecodableKeyframes uint64
}

// frameInfo is the receiver's per-access-unit propagation state.
type frameInfo struct {
	refs      []uint32 // dependency frame starts (all must be decodable)
	length    uint16   // chunk count: the frame's ids are [start, start+length)
	rap       bool
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

	// Multipath co-loss estimation (N5). When the sender spreads across N paths, systematic
	// id k rides path k mod N, so each block of N consecutive ids forms one aligned slot. The
	// forward-gap walk's in-order arrived/lost decisions are grouped into slots and folded
	// into coEst, whose per-path marginals (→ PathLoss) and per-slot erasure-count histogram
	// (→ SlotDist) are reported so the sender's joint-tail sizer sees the cross-path
	// correlation an i.i.d.-union sizer misses. Disabled (nothing reported) on a single path.
	mpEnabled bool
	paths     int
	coEst     *coLossEstimator
	mpSlot    uint32 // the slot index (id / paths) currently accumulating
	mpLost    []bool // per-path lost flags for mpSlot (len paths)
	mpHave    int    // path-positions filled in mpSlot

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
		r.observeLoss(sym.SrcIndex)
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
		g = &genState{dec: code.NewDecoder(r.cfg.SymbolSize, base, n), n: n}
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
		case r.haveRef && r.refID >= id && now.After(r.refDL):
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

// noteFrame records an access unit's descriptor from a directly-received systematic,
// inserting its FrameStart into the sorted boundary list (lazily allocating).
func (r *Receiver) noteFrame(sym wire.Symbol) {
	if r.frames == nil {
		r.frames = make(map[uint32]*frameInfo)
	}
	if r.frames[sym.FrameStart] != nil {
		return
	}
	r.frames[sym.FrameStart] = &frameInfo{refs: sym.FrameRefs, length: sym.FrameLen, rap: sym.FrameRAP}
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
			dec = ref.resolved && ref.decodable
		} else {
			dec = r.idDelivered[refStart]
		}
	}
	fi.decodable = dec
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
			delete(r.gens, base)
		}
	}
}

func (r *Receiver) maybeFeedback(now clock.Timestamp) {
	if r.fedOnce && now.Sub(r.lastFB) < feedbackIntervalMicros {
		return
	}
	r.fedOnce = true
	r.lastFB = now
	// Report the rank deficit of each of the next MaxFeedbackGens generations from
	// the cursor, so the sender repairs all of them in parallel.
	cursorGen := r.genBaseOf(r.cursor)
	var defs [wire.MaxFeedbackGens]uint8
	for i := range defs {
		if g := r.gens[cursorGen+uint32(i*r.cfg.GenSize)]; g != nil {
			if d := g.dec.Deficit(g.n); d > 0 {
				if d > 255 {
					d = 255
				}
				defs[i] = uint8(d)
			}
		}
	}
	var ceFrac uint16
	if r.ecnSeen > 0 {
		ceFrac = uint16(uint64(r.ecnCE) * 65535 / uint64(r.ecnSeen)) // CE-marked fraction (N3 / L4S)
	}
	fb := wire.Feedback{
		Flow:           r.cfg.Flow,
		DecodedLowEdge: r.cursor,
		HighestSeen:    r.highestSeen,
		Deficit:        uint16(defs[0]),
		EcnCE:          ceFrac,
		LossRate:       uint16(r.lossEstimate() * 65535),
		Deficits:       defs,
		CongestionLoss: uint16(r.clSinceFB), // pre-recovery loss this interval (N1)
		Burstiness:     uint16(r.meanBurstQ8),
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

// observeLoss updates the channel-erasure-rate estimate from a first-arrival
// systematic source id: over a window of source ids the fraction NOT directly
// received is the channel loss (coding recovery is excluded — this measures what
// the network dropped, the signal the sender's controller sizes redundancy from).
func (r *Receiver) observeLoss(id uint32) {
	r.walkGap(id) // pre-recovery loss count + burst run-lengths (N1/N2), before the windowed rate
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
func (r *Receiver) walkGap(id uint32) {
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

// genBaseOf returns the generation base for a source id. Generation bases are
// aligned to GenSize for the steady stream (the sender only closes a partial
// generation at end of stream), so this is exact arithmetic.
func (r *Receiver) genBaseOf(id uint32) uint32 { return genBaseOf(id, r.cfg.GenSize) }

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
