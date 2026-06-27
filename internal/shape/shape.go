// Package shape is Meld's media-aware layer ABOVE the narrow waist. A per-codec
// shaper maps each access unit (AU) to a generic, codec-blind descriptor — the
// importance + dependency the sans-I/O core acts on for unequal protection (UEP) and
// deadline eviction. The core never sees an OBU / NAL / codestream; it acts only on
// the descriptor's priority tier (carried as wire.Symbol.Priority) and deadline. New
// codec behavior is a new shaper filling the same descriptor, never a codec branch in
// the core. Mirrors the AV1 Dependency-Descriptor model (docs/media-awareness.md): the
// shaper fills it; the controller/scheduler read it; the core stays codec-blind.
//
// This package is sans-I/O like the core: it reads no clock and opens no socket. It
// holds the descriptor type, the protection-tier taxonomy, the parse-free decodability
// oracle (which frames survive a given set of delivered units), and a synthetic GOP
// generator for exercising the UEP path deterministically before real codec parsers.
package shape

// PriorityClass is the protection tier a shaper assigns an access unit. Higher tiers
// are protected harder (a tighter decode-failure target ⇒ more repair, and at the top
// cross-path duplication). The order encodes the universal drop policy
// (docs/media-awareness.md §4): under budget pressure the lowest tier is sacrificed
// first, the top never. The numeric value is what crosses the waist as
// wire.Symbol.Priority — the core acts on the tier, not the codec.
type PriorityClass uint8

// The protection tiers, most-disposable first. The mapping from codec syntax to tier
// is each shaper's job (e.g. HEVC nuh_temporal_id_plus1 + NAL type); the core only
// ever sees the resulting tier byte.
const (
	// ClassDisposable: filler, redundant headers, non-HDR metadata, the highest
	// temporal layer — droppable with only graceful quality loss.
	ClassDisposable PriorityClass = iota
	// ClassEnhancement: mid temporal / spatial enhancement layers (still discardable).
	ClassEnhancement
	// ClassBase: base-layer reference frames — losing one propagates until the next RAP.
	ClassBase
	// ClassRAP: random-access points (IDR / CRA / KEY_FRAME) — the resync anchor.
	ClassRAP
	// ClassParamSet: parameter sets / sequence header / sticky HDR — tiny and
	// session-fatal on loss; the cheapest high-leverage bytes, protected hardest.
	ClassParamSet
	// NumClasses is the count of tiers.
	NumClasses
)

// Wire returns the priority byte the core acts on (wire.Symbol.Priority).
func (c PriorityClass) Wire() uint8 { return uint8(c) }

// Confidence records whether a descriptor field was read from the bitstream
// (Signaled) or guessed (Inferred). Where Inferred, a shaper is conservative —
// protect a little harder, drop a little more reluctantly — because the importance
// could be under-stated (AVC's weak signaling is the motivating case).
type Confidence uint8

// Confidence levels.
const (
	Inferred Confidence = iota
	Signaled
)

// Unit is the generic per-access-unit descriptor: a pragmatic subset of the
// DD-shaped MeldUnitDescriptor (docs/media-awareness.md §2) carrying the importance +
// dependency the codec-blind core and the decodability oracle need. The shaper fills
// it from the bitstream; RefersTo holds ABSOLUTE unit ids (the shaper has already
// resolved the codec's relative references).
type Unit struct {
	ID              uint32        // monotonic dependency key (one per access unit)
	Class           PriorityClass // protection tier
	RAP             bool          // random-access point (resync anchor)
	RecoveryRefresh bool          // coded reference slice inside a signaled intra-refresh recovery interval
	Discardable     bool          // nothing surviving references this unit
	Picture         bool          // a coded picture (a displayed frame), not a parameter set / SEI / metadata
	TemporalID      uint8         // enhancement-layer depth (drop high first → fewer fps)
	RefersTo        []uint32      // unit ids this AU decodes from (absolute)
	Confidence      Confidence    // signaled vs inferred importance
	Size            int           // AU size in bytes (how many coded symbols it spans)
}

// Decodable returns, for each unit, whether it is DECODABLE given the set of units the
// receiver fully delivered: a unit is decodable iff it was itself delivered AND every
// unit in its dependency closure (RefersTo, transitively) was delivered. This is the
// parse-free loss-propagation oracle — the same chain walk the receiver would do — and
// the basis of the decodable-frame-rate metric: a frame whose reference (or the RAP /
// parameter set beneath it) was lost is NOT decodable even if its own bytes arrived.
// Pure; memoized over the unit graph; tolerant of dangling references (treated as not
// delivered). Result is keyed by unit ID.
func Decodable(units []Unit, delivered map[uint32]bool) map[uint32]bool {
	byID := make(map[uint32]Unit, len(units))
	for _, u := range units {
		byID[u.ID] = u
	}
	memo := make(map[uint32]bool, len(units))
	var resolve func(id uint32, stack map[uint32]bool) bool
	resolve = func(id uint32, stack map[uint32]bool) bool {
		if v, ok := memo[id]; ok {
			return v
		}
		if stack[id] { // a dependency cycle (malformed) — fail safe to not-decodable
			return false
		}
		u, ok := byID[id]
		if !ok || !delivered[id] {
			memo[id] = false
			return false
		}
		stack[id] = true
		ok = true
		for _, ref := range u.RefersTo {
			if !resolve(ref, stack) {
				ok = false
				break
			}
		}
		delete(stack, id)
		memo[id] = ok
		return ok
	}
	out := make(map[uint32]bool, len(units))
	for _, u := range units {
		out[u.ID] = resolve(u.ID, map[uint32]bool{})
	}
	return out
}

// DecodableFrameRate is the fraction of coded PICTURES (displayed frames) that are
// decodable given the delivered set — the WP6 quality metric. Non-picture units
// (parameter sets, SEI, metadata) are excluded from the denominator — they are not
// displayed frames — but their loss still poisons every picture that depends on them, so
// they dominate the score by leverage.
func DecodableFrameRate(units []Unit, delivered map[uint32]bool) float64 {
	dec := Decodable(units, delivered)
	var frames, ok int
	for _, u := range units {
		if !u.Picture {
			continue
		}
		frames++
		if dec[u.ID] {
			ok++
		}
	}
	if frames == 0 {
		return 0
	}
	return float64(ok) / float64(frames)
}

// DecodableKeyframeRate is the fraction of random-access points (RAP / keyframe units)
// that are decodable — the headline WP6 metric. A keyframe is the resync anchor: lose
// it (or the parameter set beneath it) and the entire GOP hanging off it is undecodable
// until the next one, so keyframe survival dominates glass-to-glass quality. It is the
// sharpest test of "protect the unrecoverable cheaply": the keyframe + parameter set are
// a tiny fraction of the bytes, so a media-aware sizer keeps them alive under a budget
// that a flat sizer spreads too thin to hold.
func DecodableKeyframeRate(units []Unit, delivered map[uint32]bool) float64 {
	dec := Decodable(units, delivered)
	var raps, ok int
	for _, u := range units {
		if !u.RAP {
			continue
		}
		raps++
		if dec[u.ID] {
			ok++
		}
	}
	if raps == 0 {
		return 0
	}
	return float64(ok) / float64(raps)
}
