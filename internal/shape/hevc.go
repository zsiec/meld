package shape

// HEVC (H.265) NAL unit types (nal_unit_type, RFC 7798 / H.265 Table 7-1).
const (
	hevcTrailN  = 0  // trailing, sub-layer non-reference (SLNR)
	hevcTSA_N   = 2  // temporal sub-layer access, SLNR
	hevcSTSA_N  = 4  // step-wise TSA, SLNR
	hevcRADL_N  = 6  // random-access decodable leading, SLNR
	hevcRASL_N  = 8  // random-access skipped leading, SLNR
	hevcIRAPLo  = 16 // first IRAP (BLA_W_LP)
	hevcIRAPHi  = 23 // last reserved IRAP (RSV_IRAP_VCL23); 19/20 = IDR, 21 = CRA
	hevcVPS     = 32 // video parameter set
	hevcSPS     = 33 // sequence parameter set
	hevcPPS     = 34 // picture parameter set
	hevcPrefSEI = 39 // prefix SEI
	hevcSufSEI  = 40 // suffix SEI
)

// HEVCShaper maps an H.265 Annex-B elementary stream to generic descriptors. Tier and
// discardability come from the 2-byte NAL header — nal_unit_type and nuh_temporal_id_plus1
// — which HEVC signals richly (a native temporal id and explicit _N/_R sub-layer
// non-reference types), so importance is SIGNALED: the temporal id sets the tier (sublayer
// 0 → base, 1 → enhancement, ≥2 → disposable), an _N type marks the picture discardable,
// IRAP types are RAPs, and VPS/SPS/PPS are parameter sets. The DEPENDENCY is resolved by
// parsing just enough of the SPS + PPS + slice segment header (slice_type + picture order
// count, §8.3.1) to find each picture's references: a B-picture references its two
// bracketing anchors (nearest reference below and above it in display order — exact for the
// regular hierarchical-B structure, an approximation when an encoder picks farther
// references); other slices the previous reference; an IRAP the parameter sets. When the
// headers cannot be parsed it falls back to the single previous reference. Stateful. One
// unit per slice-segment NAL (multi-slice pictures yield multiple units referencing the
// same anchors — a benign approximation).
type HEVCShaper struct {
	nextID              uint32
	vpsID, spsID, ppsID int64 // active parameter-set unit ids, or -1
	prevRef             int64 // previous reference picture unit id, or -1

	sps                    hevcSPSInfo
	spsValid               bool
	pps                    hevcPPSInfo
	ppsValid               bool
	prevPocMsb, prevPocLsb int
	refs                   []pocRef // recent reference pictures (for B-frame bracketing), by POC
}

// NewHEVCShaper returns a fresh H.265 shaper.
func NewHEVCShaper() *HEVCShaper {
	return &HEVCShaper{vpsID: -1, spsID: -1, ppsID: -1, prevRef: -1}
}

// Shape parses an Annex-B buffer and returns the protectable units in order. Parameter
// sets, SEI, and coded slices become units; AUD / EOS / EOB / filler are dropped.
func (s *HEVCShaper) Shape(annexB []byte) []Shaped {
	var out []Shaped
	for _, nal := range nalUnits(annexB) {
		if len(nal) < 2 {
			continue
		}
		typ := (nal[0] >> 1) & 0x3F
		tid := int(nal[1]&0x7) - 1 // nuh_temporal_id_plus1 − 1
		if tid < 0 {
			tid = 0
		}
		u := Unit{ID: s.nextID, Size: len(nal), TemporalID: uint8(tid), Confidence: Signaled}
		switch {
		case typ == hevcVPS:
			u.Class = ClassParamSet
			s.vpsID = int64(u.ID)
		case typ == hevcSPS:
			u.Class = ClassParamSet
			s.spsID = int64(u.ID)
			if sps, ok := parseHEVCSPS(nal); ok {
				s.sps, s.spsValid = sps, true
			}
		case typ == hevcPPS:
			u.Class = ClassParamSet
			s.ppsID = int64(u.ID)
			if pps, ok := parseHEVCPPS(nal); ok {
				s.pps, s.ppsValid = pps, true
			}
		case typ >= hevcIRAPLo && typ <= hevcIRAPHi:
			u.Class, u.RAP, u.Picture = ClassRAP, true, true
			u.RefersTo = s.paramRefs()
			si, _ := s.sliceInfo(nal)
			poc := s.computePOC(si, true, true)
			s.refs = s.refs[:0]
			s.prevRef = int64(u.ID) // an IRAP resets the reference chain
			s.addRef(poc, u.ID)     // an IRAP is a reference
		case typ <= 9: // trailing / leading coded slices
			u.Picture = true
			isRef := typ%2 == 1 // odd types are references (_R); even are sub-layer non-reference (_N)
			si, ok := s.sliceInfo(nal)
			poc := s.computePOC(si, false, isRef)
			if ok && si.sliceType == hevcSliceB {
				u.RefersTo = s.bracket(poc) // a B-picture's two bracketing anchors (by POC)
			} else {
				u.RefersTo = s.refChain() // P / I / unparseable ⇒ the previous reference
			}
			if isRef {
				u.Class = classForTemporal(tid, false)
				s.prevRef = int64(u.ID)
				s.addRef(poc, u.ID)
			} else {
				u.Class, u.Discardable = classForTemporal(tid, true), true
			}
		case typ == hevcPrefSEI || typ == hevcSufSEI:
			u.Class, u.Confidence = ClassEnhancement, Inferred
			u.RefersTo = s.refChain()
		default:
			continue // AUD, EOS, EOB, filler, reserved
		}
		out = append(out, Shaped{Unit: u, Payload: nal})
		s.nextID++
	}
	return out
}

// classForTemporal maps a temporal sublayer to a protection tier: sublayer 0 is the
// base spine, 1 the enhancement layer, ≥2 disposable. A sub-layer non-reference picture
// (slnr) is never above the enhancement tier, since nothing references it.
func classForTemporal(tid int, slnr bool) PriorityClass {
	switch {
	case tid <= 0 && !slnr:
		return ClassBase
	case tid <= 1:
		return ClassEnhancement
	default:
		return ClassDisposable
	}
}

// paramRefs returns the active parameter-set ids a RAP depends on.
func (s *HEVCShaper) paramRefs() []uint32 {
	var r []uint32
	for _, id := range []int64{s.vpsID, s.spsID, s.ppsID} {
		if id >= 0 {
			r = append(r, uint32(id))
		}
	}
	return r
}

// refChain returns the previous-reference dependency (the cascade link), or nil.
func (s *HEVCShaper) refChain() []uint32 {
	if s.prevRef >= 0 {
		return []uint32{uint32(s.prevRef)}
	}
	return nil
}

// sliceInfo parses a coded-slice NAL for slice_type + POC, but only when both the SPS and
// PPS have been seen (the slice header walk depends on their fields); otherwise the shaper
// falls back to the reference chain.
func (s *HEVCShaper) sliceInfo(nal []byte) (sliceInfo, bool) {
	if !s.spsValid || !s.ppsValid {
		return sliceInfo{}, false
	}
	return parseHEVCSliceHeader(nal, s.sps, s.pps)
}

// computePOC derives a picture's order count from its slice header (H.265 §8.3.1), tracking
// the MSB across the lsb wrap. An IRAP resets the prediction state; a reference picture
// (isRef) advances prevPoc so later pictures predict from it. Returns 0 when the POC cannot
// be computed — bracketing is then skipped and the shaper uses the previous-reference chain.
func (s *HEVCShaper) computePOC(si sliceInfo, irap, isRef bool) int {
	if !s.spsValid || !si.hasPoc {
		return 0
	}
	maxLsb := 1 << s.sps.log2MaxPocLsb
	if irap {
		s.prevPocMsb, s.prevPocLsb = 0, 0
	}
	msb := pocMSB(s.prevPocMsb, s.prevPocLsb, si.pocLsb, maxLsb)
	poc := msb + si.pocLsb
	if isRef {
		s.prevPocMsb, s.prevPocLsb = msb, si.pocLsb
	}
	return poc
}

// bracket returns the two anchors a B-picture at display order poc depends on (the nearest
// reference below and above it in display order), or the previous-reference chain when no
// bracketing reference is available.
func (s *HEVCShaper) bracket(poc int) []uint32 {
	if r := bracketRefs(s.refs, poc); len(r) > 0 {
		return r
	}
	return s.refChain()
}

// addRef records a reference picture for future bracketing.
func (s *HEVCShaper) addRef(poc int, id uint32) { s.refs = appendRef(s.refs, poc, id) }
