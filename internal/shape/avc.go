package shape

// AVC (H.264) NAL unit types (nal_unit_type, RFC 6184 / H.264 Table 7-1).
const (
	avcNonIDR = 1  // coded slice of a non-IDR picture
	avcIDR    = 5  // coded slice of an IDR picture (RAP)
	avcSEI    = 6  // supplemental enhancement information
	avcSPS    = 7  // sequence parameter set
	avcPPS    = 8  // picture parameter set
	avcAUD    = 9  // access unit delimiter
	avcFiller = 12 // filler data
	avcSPSExt = 13 // sequence parameter set extension
	avcSubSPS = 15 // subset SPS (SVC/MVC base)
)

const avcSEIRecoveryPoint = 6

// AVCOptions controls optional source-side filtering. The default shaper is conservative
// and preserves SEI because it may carry captions, HDR, or recovery points.
type AVCOptions struct {
	// SourceConstrained means the caller has decided the encoded source is under a
	// real bandwidth/latency constraint. Only then may the shaper shed SEI NALs that
	// can be positively identified as non-recovery metadata. Malformed SEI is retained
	// fail-safe, and unconstrained sources preserve SEI exactly as received.
	SourceConstrained bool
	// DropDisposablePictures may shed non-reference coded pictures when SourceConstrained
	// is also true. These AVC slices have nal_ref_idc == 0, so no future picture should
	// depend on them; dropping them models an encoder/source that sacrifices top-layer
	// frame rate before transport to reduce burst exposure around the reference spine.
	DropDisposablePictures bool
}

// AVCShaper maps an H.264 Annex-B elementary stream to generic descriptors. Tier and
// discardability come from the NAL header (nal_ref_idc + nal_unit_type); the DEPENDENCY
// is resolved by parsing just enough of the SPS + slice header (slice_type + picture
// order count, §8.2.1) to find each picture's actual references. A B-frame references its
// two bracketing anchors (nearest reference below and above it in display order); a
// P-frame the previous reference; an IDR the parameter sets. When the SPS or slice header
// cannot be parsed it falls back to the single previous reference. Stateful across calls.
// One unit per slice NAL (multi-slice pictures yield multiple units referencing the same
// anchors — a benign approximation).
type AVCShaper struct {
	opts AVCOptions

	nextID       uint32
	spsID, ppsID int64 // active parameter-set unit ids, or -1
	prevRef      int64 // previous reference picture unit id, or -1

	sps                    spsInfo
	spsValid               bool
	prevPocMsb, prevPocLsb int
	refs                   []pocRef // recent reference pictures (for B-frame bracketing), by POC

	recoveryCountdown int
	recoveryRefs      []uint32
}

// NewAVCShaper returns a fresh H.264 shaper.
func NewAVCShaper() *AVCShaper {
	return &AVCShaper{spsID: -1, ppsID: -1, prevRef: -1, recoveryCountdown: -1}
}

// NewAVCShaperWithOptions returns a fresh H.264 shaper with explicit source-policy
// options.
func NewAVCShaperWithOptions(opts AVCOptions) *AVCShaper {
	s := NewAVCShaper()
	s.opts = opts
	return s
}

// Shape parses an Annex-B buffer and returns the protectable units in order. Parameter
// sets, SEI, and slices become units; AUD / filler / end-of-sequence are dropped. When
// SourceConstrained is true, non-recovery SEI is also dropped before source chunking.
func (s *AVCShaper) Shape(annexB []byte) []Shaped {
	var out []Shaped
	for _, nal := range nalUnits(annexB) {
		if len(nal) < 1 {
			continue
		}
		refIdc := (nal[0] >> 5) & 0x3
		typ := nal[0] & 0x1F
		u := Unit{ID: s.nextID, Size: len(nal)}
		switch typ {
		case avcSPS, avcSPSExt, avcSubSPS:
			u.Class, u.Confidence = ClassParamSet, Signaled
			s.spsID = int64(u.ID)
			if typ == avcSPS {
				if sps, ok := parseSPS(nal); ok {
					s.sps, s.spsValid = sps, true
				}
			}
		case avcPPS:
			u.Class, u.Confidence = ClassParamSet, Signaled
			s.ppsID = int64(u.ID)
		case avcIDR:
			u.Class, u.RAP, u.Picture, u.Confidence = ClassRAP, true, true, Signaled
			u.RefersTo = s.paramRefs()
			si, _ := parseSliceHeader(nal, s.sps)
			poc := s.computePOC(si, true, true)
			s.refs = s.refs[:0]
			s.recoveryCountdown = -1
			s.recoveryRefs = s.recoveryRefs[:0]
			s.prevRef = int64(u.ID) // an IDR resets the reference chain
			s.addRef(poc, u.ID)     // an IDR is a reference
		case avcNonIDR:
			u.Picture, u.Confidence = true, Inferred // AVC's deeper ref structure (LTRP/MMCO) is not modeled
			si, ok := parseSliceHeader(nal, s.sps)
			poc := s.computePOC(si, false, refIdc != 0)
			recoveryActive := s.recoveryCountdown >= 0
			recoveryComplete := s.consumeRecoveryCountdown()
			if ok && s.spsValid && si.sliceType == sliceB {
				u.RefersTo = s.bracket(poc) // a B-frame's two bracketing anchors (by POC)
			} else {
				u.RefersTo = s.refChain() // P / I / unparseable ⇒ the previous reference
			}
			if refIdc == 0 {
				if s.opts.SourceConstrained && s.opts.DropDisposablePictures {
					continue
				}
				u.Class, u.Discardable = ClassDisposable, true
			} else {
				u.Class = ClassBase
				if recoveryActive {
					u.RecoveryRefresh = true
				}
				if recoveryComplete {
					u.Class, u.RAP, u.Confidence = ClassRAP, true, Signaled
					u.RefersTo = append(s.paramRefs(), s.recoveryRefs...)
					s.refs = s.refs[:0]
					s.recoveryRefs = s.recoveryRefs[:0]
				}
				s.prevRef = int64(u.ID)
				s.addRef(poc, u.ID)
				if s.recoveryCountdown >= 0 {
					s.recoveryRefs = append(s.recoveryRefs, u.ID)
				}
			}
		case avcSEI:
			if s.opts.SourceConstrained && !avcSEIHasPayloadType(nal, avcSEIRecoveryPoint) {
				continue
			}
			if cnt, ok := avcSEIRecoveryFrameCnt(nal); ok {
				s.recoveryCountdown = cnt
				s.recoveryRefs = s.recoveryRefs[:0]
			}
			// Conservative: SEI may carry a recovery point or HDR — protect at a mid tier
			// rather than treat as disposable; classifying its payload is later work.
			u.Class, u.Confidence = ClassEnhancement, Inferred
			u.RefersTo = s.refChain()
		default:
			continue // AUD, filler, end-of-seq/stream, reserved
		}
		out = append(out, Shaped{Unit: u, Payload: nal})
		s.nextID++
	}
	return out
}

// paramRefs returns the active parameter-set ids a RAP depends on.
func (s *AVCShaper) paramRefs() []uint32 {
	var r []uint32
	if s.spsID >= 0 {
		r = append(r, uint32(s.spsID))
	}
	if s.ppsID >= 0 {
		r = append(r, uint32(s.ppsID))
	}
	return r
}

// refChain returns the previous-reference dependency (the cascade link), or nil.
func (s *AVCShaper) refChain() []uint32 {
	if s.prevRef >= 0 {
		return []uint32{uint32(s.prevRef)}
	}
	return nil
}

func (s *AVCShaper) consumeRecoveryCountdown() bool {
	if s.recoveryCountdown < 0 {
		return false
	}
	if s.recoveryCountdown == 0 {
		s.recoveryCountdown = -1
		return true
	}
	s.recoveryCountdown--
	return false
}

// computePOC derives a picture's order count from its slice header (pic_order_cnt_type 0,
// H.264 §8.2.1), tracking the MSB across the lsb wrap. idr resets the prediction state; a
// reference picture (isRef) advances prevPoc so later pictures predict from it. Returns 0
// when the POC cannot be computed (non-zero poc_type, missing SPS) — bracketing is then
// skipped and the shaper falls back to the previous-reference chain.
func (s *AVCShaper) computePOC(si sliceInfo, idr, isRef bool) int {
	if !s.spsValid || s.sps.pocType != 0 || !si.hasPoc {
		return 0
	}
	maxLsb := 1 << s.sps.log2MaxPocLsb
	if idr {
		s.prevPocMsb, s.prevPocLsb = 0, 0
	}
	msb := pocMSB(s.prevPocMsb, s.prevPocLsb, si.pocLsb, maxLsb)
	poc := msb + si.pocLsb
	if isRef {
		s.prevPocMsb, s.prevPocLsb = msb, si.pocLsb
	}
	return poc
}

// bracket returns the two anchors a B-frame at display order poc depends on (the nearest
// reference below and above it in display order), or the previous-reference chain when no
// bracketing reference is available.
func (s *AVCShaper) bracket(poc int) []uint32 {
	if r := bracketRefs(s.refs, poc); len(r) > 0 {
		return r
	}
	return s.refChain()
}

// addRef records a reference picture for future bracketing.
func (s *AVCShaper) addRef(poc int, id uint32) { s.refs = appendRef(s.refs, poc, id) }
