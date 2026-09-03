package shape

// AV1 OBU types (obu_type, AV1 bitstream spec §5.3.2).
const (
	av1SeqHeader     = 1
	av1TemporalDelim = 2
	av1FrameHeader   = 3
	av1TileGroup     = 4
	av1Metadata      = 5
	av1Frame         = 6 // combined frame header + tile group (the common streaming OBU)
	av1RedundantFH   = 7
	av1TileList      = 8
	av1Padding       = 15
)

// AV1 frame_type (uncompressed_header, §5.9.2).
const (
	av1KeyFrame    = 0
	av1InterFrame  = 1
	av1IntraOnly   = 2
	av1SwitchFrame = 3
)

// AV1Shaper maps an AV1 low-overhead-bitstream elementary stream to generic descriptors by
// parsing OBU headers and the uncompressed frame header — no tile or symbol decode. AV1 is
// the richest-signaled codec: a sequence header is the session-fatal parameter set, a
// KEY_FRAME is a RAP, the temporal id sets the tier, and show_existing_frame frames carry no
// new data. The DEPENDENCY is resolved EXACTLY against AV1's eight-slot reference buffer: the
// shaper tracks which unit last wrote each slot (refresh_frame_flags) and resolves an inter
// frame's references through its ref_frame_idx slot indices (reconstructed via set_frame_refs
// when the frame uses short signaling, §7.8). A show_existing_frame depends on the slot it
// displays; a hidden alt-ref frame is a reference but not a displayed picture (Picture is
// false until it is shown). When the headers cannot be parsed — no sequence header yet, or a
// truncated payload — it falls back to a frame_type peek + the previous-reference chain.
type AV1Shaper struct {
	nextID   uint32
	seqHdr   int64 // active sequence-header unit id, or -1
	prevRef  int64 // previous reference picture unit id, or -1 (fallback chain)
	seq      av1SeqHdr
	seqValid bool
	slotID   [av1NumRefFrames]int64 // unit id in each reference slot, or -1
	slotHint [av1NumRefFrames]int   // order hint of the frame in each slot (for set_frame_refs)
	slotKey  [av1NumRefFrames]bool  // whether the slot holds a key frame
}

// NewAV1Shaper returns a fresh AV1 shaper.
func NewAV1Shaper() *AV1Shaper {
	s := &AV1Shaper{seqHdr: -1, prevRef: -1}
	for i := range s.slotID {
		s.slotID[i] = -1
	}
	return s
}

// Shape parses an AV1 OBU stream and returns the protectable units in order. Sequence
// headers, temporal delimiters, frames, tile groups, and metadata become units. Temporal
// delimiters are essential framing in a low-overhead OBU stream and receive the same tiny,
// high-priority treatment as other decoder bootstrap material. Padding, redundant frame
// headers, and tile lists are dropped.
func (s *AV1Shaper) Shape(stream []byte) []Shaped {
	var out []Shaped
	for _, o := range av1OBUs(stream) {
		u := Unit{ID: s.nextID, Size: len(o.full), TemporalID: o.tid, Confidence: Signaled}
		switch o.typ {
		case av1SeqHeader:
			u.Class = ClassParamSet
			s.seqHdr = int64(u.ID)
			if seq, ok := parseAV1SeqHeader(o.payload); ok {
				s.seq, s.seqValid = seq, true
			}
		case av1TemporalDelim:
			u.Class = ClassParamSet
		case av1Frame, av1FrameHeader:
			s.shapeFrame(&u, o)
		case av1TileGroup:
			// Tile data for the current frame; carry it at the base tier, chained to the
			// frame it completes.
			u.Class = ClassBase
			u.RefersTo = s.refChain()
		case av1Metadata:
			u.Class, u.Confidence = ClassEnhancement, Inferred
			u.RefersTo = s.refChain()
		default:
			continue // padding, redundant header, tile list
		}
		out = append(out, Shaped{Unit: u, Payload: o.full})
		s.nextID++
	}
	return out
}

// seqRefs returns the active sequence-header dependency a RAP needs, or nil.
func (s *AV1Shaper) seqRefs() []uint32 {
	if s.seqHdr >= 0 {
		return []uint32{uint32(s.seqHdr)}
	}
	return nil
}

// refChain returns the previous-reference dependency (the cascade link), or nil.
func (s *AV1Shaper) refChain() []uint32 {
	if s.prevRef >= 0 {
		return []uint32{uint32(s.prevRef)}
	}
	return nil
}

// shapeFrame classifies a frame / frame_header OBU, resolving its references exactly through
// the reference slots when the sequence + frame headers parse, and degrading to a frame_type
// peek + the previous-reference chain otherwise.
func (s *AV1Shaper) shapeFrame(u *Unit, o av1OBU) {
	if s.seqValid {
		var roh [av1NumRefFrames]int
		for i := 0; i < av1NumRefFrames; i++ {
			roh[i] = s.slotHint[i]
		}
		if fi, ok := parseAV1FrameHeader(o.payload, s.seq, roh); ok {
			s.applyExact(u, fi)
			return
		}
	}
	s.applyPeek(u, o.payload, o.tid)
}

// applyExact fills the descriptor from a fully parsed frame header and updates the reference
// slots the frame writes.
func (s *AV1Shaper) applyExact(u *Unit, fi av1FrameInfo) {
	switch {
	case fi.showExisting:
		// Displays the frame already in the named slot; carries no new coded data.
		u.Class, u.Discardable, u.Picture = ClassDisposable, true, true
		if id := s.slotID[fi.showMapIdx]; id >= 0 {
			u.RefersTo = []uint32{uint32(id)}
		}
		if s.slotKey[fi.showMapIdx] { // showing a key frame refreshes every slot with it
			for i := 0; i < av1NumRefFrames; i++ {
				s.slotID[i], s.slotHint[i], s.slotKey[i] = s.slotID[fi.showMapIdx], s.slotHint[fi.showMapIdx], true
			}
		}
	case fi.intra:
		u.Picture = fi.show
		u.RefersTo = s.seqRefs()
		if fi.frameType == av1KeyFrame {
			u.Class, u.RAP = ClassRAP, true // a key frame resets the whole reference buffer
		} else {
			u.Class = classForTemporal(int(u.TemporalID), false) // intra-only: intra, not a clean RAP
		}
		s.prevRef = int64(u.ID)
		s.refresh(fi, u.ID, fi.frameType == av1KeyFrame)
	default: // INTER / SWITCH
		u.Picture = fi.show
		u.Class = classForTemporal(int(u.TemporalID), false)
		u.RefersTo = s.exactRefs(fi)
		s.prevRef = int64(u.ID)
		s.refresh(fi, u.ID, false)
	}
}

// applyPeek is the degraded path: read show_existing_frame + frame_type from the header's
// leading bits and chain to the previous reference (the pre-slot-tracking behavior).
func (s *AV1Shaper) applyPeek(u *Unit, payload []byte, tid uint8) {
	ft, showExisting := av1FrameType(payload)
	u.Picture = true
	switch {
	case showExisting:
		u.Class, u.Discardable = ClassDisposable, true
		u.RefersTo = s.refChain()
	case ft == av1KeyFrame || ft == av1IntraOnly:
		u.Class, u.RAP = ClassRAP, true
		u.RefersTo = s.seqRefs()
		s.prevRef = int64(u.ID)
	default: // INTER / SWITCH
		u.Class = classForTemporal(int(tid), false)
		u.RefersTo = s.refChain()
		s.prevRef = int64(u.ID)
	}
}

// exactRefs maps an inter frame's ref_frame_idx slot indices to the distinct unit ids that
// last wrote those slots, falling back to the chain if none resolve.
func (s *AV1Shaper) exactRefs(fi av1FrameInfo) []uint32 {
	var refs []uint32
	for _, slot := range fi.refIdx {
		if slot < 0 || slot >= av1NumRefFrames {
			continue
		}
		id := s.slotID[slot]
		if id < 0 {
			continue
		}
		dup := false
		for _, r := range refs {
			if r == uint32(id) {
				dup = true
				break
			}
		}
		if !dup {
			refs = append(refs, uint32(id))
		}
	}
	if len(refs) == 0 {
		return s.refChain()
	}
	return refs
}

// refresh writes this frame into the reference slots named by refresh_frame_flags.
func (s *AV1Shaper) refresh(fi av1FrameInfo, id uint32, key bool) {
	for i := 0; i < av1NumRefFrames; i++ {
		if fi.refreshFlags&(1<<i) != 0 {
			s.slotID[i], s.slotHint[i], s.slotKey[i] = int64(id), fi.orderHint, key
		}
	}
}

// av1FrameType reads show_existing_frame and (when absent) frame_type from the leading
// bits of a frame OBU payload, assuming reduced_still_picture_header == 0. The
// uncompressed header begins with show_existing_frame f(1); if 0, frame_type f(2).
func av1FrameType(payload []byte) (frameType uint8, showExisting bool) {
	if len(payload) < 1 {
		return av1KeyFrame, false
	}
	b := payload[0]
	if (b>>7)&1 == 1 {
		return 0, true
	}
	return (b >> 5) & 0x3, false
}

// av1OBU is one parsed Open Bitstream Unit.
type av1OBU struct {
	typ      uint8
	tid, sid uint8
	payload  []byte // the OBU payload (after header + extension + size), aliasing input
	full     []byte // the whole OBU (header through payload) for transmission
}

// av1OBUs splits a low-overhead AV1 bitstream into OBUs, reading each obu_header (type,
// extension flag → temporal/spatial id, has_size_field → leb128 size). Malformed lengths
// stop the scan rather than panic.
func av1OBUs(b []byte) []av1OBU {
	var out []av1OBU
	i := 0
	for i < len(b) {
		start := i
		hdr := b[i]
		i++
		typ := (hdr >> 3) & 0xF
		extFlag := (hdr>>2)&1 == 1
		sizeFlag := (hdr>>1)&1 == 1
		var tid, sid uint8
		if extFlag {
			if i >= len(b) {
				break
			}
			ext := b[i]
			i++
			tid = (ext >> 5) & 0x7
			sid = (ext >> 3) & 0x3
		}
		size := len(b) - i
		if sizeFlag {
			v, adv := leb128(b[i:])
			if adv == 0 {
				break
			}
			i += adv
			if v < size {
				size = v
			}
		}
		if size < 0 || i+size > len(b) {
			break
		}
		payload := b[i : i+size]
		i += size
		out = append(out, av1OBU{typ: typ, tid: tid, sid: sid, payload: payload, full: b[start:i]})
	}
	return out
}

// leb128 reads an unsigned LEB128 integer (AV1 §4.10.5), returning the value and the
// number of bytes consumed (0 on a truncated/over-long encoding).
func leb128(b []byte) (val, n int) {
	for n = 0; n < 8 && n < len(b); n++ {
		v := b[n]
		val |= int(v&0x7f) << (7 * n)
		if v&0x80 == 0 {
			return val, n + 1
		}
	}
	return 0, 0
}
