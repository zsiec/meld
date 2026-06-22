package shape

// Minimal AV1 bitstream parsing for exact reference resolution: enough of the sequence
// header and the uncompressed frame header to recover show_existing_frame, frame_type,
// show_frame, refresh_frame_flags, and the seven ref_frame_idx slot indices (deriving them
// via set_frame_refs when the frame uses short signaling). No tile or symbol decode. Spec:
// AV1 bitstream & decoding process §5.5 (sequence header), §5.9 (frame header), §7.8
// (set_frame_refs), §5.9.3 (get_relative_dist). AV1 OBU payloads carry no
// emulation-prevention bytes, so the bitReader reads them directly (no ebspToRbsp).
//
// Unlike AVC/HEVC there is no picture order count: AV1 manages an eight-slot reference
// buffer (the DPB). Each inter frame names up to seven of those slots; the shaper maps the
// slot indices to the unit ids that last wrote them. set_frame_refs reconstructs the seven
// indices from the slots' order hints when the bitstream signals only two of them.

// AV1 reference-management constants (§3 "Symbols and abbreviated terms").
const (
	av1NumRefFrames    = 8 // ref slots (the DPB)
	av1RefsPerFrame    = 7 // references an inter frame may name
	av1SelectScreen    = 2 // SELECT_SCREEN_CONTENT_TOOLS
	av1SelectIntegerMV = 2 // SELECT_INTEGER_MV
	av1PrimaryRefNone  = 7 // PRIMARY_REF_NONE
)

// av1SeqHdr holds the sequence-header fields the frame-header parse needs.
type av1SeqHdr struct {
	reducedStill         bool
	frameIDPresent       bool
	idLen                int
	deltaFrameIDLen      int
	decoderModelPresent  bool
	equalPictureInterval bool
	bufRemovalTimeLen    int
	framePresentTimeLen  int
	opCnt                int
	opIDC                [32]int
	decoderModelForOp    [32]bool
	frameWidthBits       int
	frameHeightBits      int
	enableOrderHint      bool
	orderHintBits        int
	forceScreenContent   int
	forceIntegerMV       int
}

// av1FrameInfo is the resolved frame header: how it displays and what it references.
type av1FrameInfo struct {
	showExisting bool
	showMapIdx   int
	frameType    int
	show         bool   // displayed this access unit (show_frame, or shown via show_existing)
	intra        bool   // KEY_FRAME or INTRA_ONLY_FRAME — references no other picture
	orderHint    int    // display order hint (for slot bookkeeping + set_frame_refs)
	refreshFlags int    // refresh_frame_flags: which slots this frame writes
	refIdx       [7]int // slot indices this frame references (inter only)
}

// uvlc reads an unsigned variable-length code (AV1 §4.10.3), capped to avoid a runaway read.
func uvlc(r *bitReader) uint {
	zeros := 0
	for {
		if r.bit() == 1 {
			break
		}
		zeros++
		if zeros >= 32 {
			return (1 << 32) - 1
		}
	}
	return r.bits(zeros) + (1 << zeros) - 1
}

// parseAV1SeqHeader parses a sequence_header_obu payload for the fields the frame-header
// walk depends on (§5.5.1). Returns false if it ran off the end (a truncated or synthetic
// payload), so the shaper falls back to the frame_type peek.
func parseAV1SeqHeader(p []byte) (av1SeqHdr, bool) {
	r := newBitReader(p)
	var sh av1SeqHdr
	r.bits(3) // seq_profile
	r.bit()   // still_picture
	sh.reducedStill = r.bit() == 1
	bufferDelayLen := 0
	if sh.reducedStill {
		r.bits(5) // seq_level_idx[0]
		sh.opCnt = 1
	} else {
		if r.bit() == 1 { // timing_info_present_flag
			r.bits(32) // num_units_in_display_tick
			r.bits(32) // time_scale
			sh.equalPictureInterval = r.bit() == 1
			if sh.equalPictureInterval {
				uvlc(r) // num_ticks_per_picture_minus_1
			}
			sh.decoderModelPresent = r.bit() == 1
			if sh.decoderModelPresent {
				bufferDelayLen = int(r.bits(5)) + 1
				r.bits(32) // num_units_in_decoding_tick
				sh.bufRemovalTimeLen = int(r.bits(5)) + 1
				sh.framePresentTimeLen = int(r.bits(5)) + 1
			}
		}
		initialDisplayDelay := r.bit() == 1
		sh.opCnt = int(r.bits(5)) + 1
		for i := 0; i < sh.opCnt && i < len(sh.opIDC); i++ {
			sh.opIDC[i] = int(r.bits(12))
			seqLevelIdx := int(r.bits(5))
			if seqLevelIdx > 7 {
				r.bit() // seq_tier[i]
			}
			if sh.decoderModelPresent {
				sh.decoderModelForOp[i] = r.bit() == 1
				if sh.decoderModelForOp[i] {
					r.bits(bufferDelayLen) // decoder_buffer_delay
					r.bits(bufferDelayLen) // encoder_buffer_delay
					r.bit()                // low_delay_mode_flag
				}
			}
			if initialDisplayDelay {
				if r.bit() == 1 { // initial_display_delay_present_for_this_op
					r.bits(4) // initial_display_delay_minus_1
				}
			}
		}
	}
	sh.frameWidthBits = int(r.bits(4)) + 1
	sh.frameHeightBits = int(r.bits(4)) + 1
	r.bits(sh.frameWidthBits)  // max_frame_width_minus_1
	r.bits(sh.frameHeightBits) // max_frame_height_minus_1
	if !sh.reducedStill {
		sh.frameIDPresent = r.bit() == 1
	}
	if sh.frameIDPresent {
		sh.deltaFrameIDLen = int(r.bits(4)) + 2 // delta_frame_id_length_minus_2 + 2
		add := int(r.bits(3)) + 1               // additional_frame_id_length_minus_1 + 1
		sh.idLen = sh.deltaFrameIDLen + add
	}
	r.bit() // use_128x128_superblock
	r.bit() // enable_filter_intra
	r.bit() // enable_intra_edge_filter
	if sh.reducedStill {
		sh.forceScreenContent = av1SelectScreen
		sh.forceIntegerMV = av1SelectIntegerMV
	} else {
		r.bit() // enable_interintra_compound
		r.bit() // enable_masked_compound
		r.bit() // enable_warped_motion
		r.bit() // enable_dual_filter
		sh.enableOrderHint = r.bit() == 1
		if sh.enableOrderHint {
			r.bit() // enable_jnt_comp
			r.bit() // enable_ref_frame_mvs
		}
		if r.bit() == 1 { // seq_choose_screen_content_tools
			sh.forceScreenContent = av1SelectScreen
		} else {
			sh.forceScreenContent = int(r.bit())
		}
		if sh.forceScreenContent > 0 {
			if r.bit() == 1 { // seq_choose_integer_mv
				sh.forceIntegerMV = av1SelectIntegerMV
			} else {
				sh.forceIntegerMV = int(r.bit())
			}
		} else {
			sh.forceIntegerMV = av1SelectIntegerMV
		}
		if sh.enableOrderHint {
			sh.orderHintBits = int(r.bits(3)) + 1
		}
	}
	if r.overran() {
		return av1SeqHdr{}, false
	}
	return sh, true
}

// parseAV1FrameHeader parses an uncompressed frame header from a frame or frame_header OBU
// payload (§5.9.2), up to and including the reference slot indices. RefOrderHint carries the
// order hint stored in each of the eight slots (for set_frame_refs); the shaper supplies it.
// Returns false on overrun so the shaper falls back to the frame_type peek.
func parseAV1FrameHeader(p []byte, sh av1SeqHdr, refOrderHint [av1NumRefFrames]int) (av1FrameInfo, bool) {
	r := newBitReader(p)
	var fi av1FrameInfo
	allFrames := (1 << av1NumRefFrames) - 1

	if sh.reducedStill {
		fi.frameType = av1KeyFrame
		fi.intra = true
		fi.show = true
		fi.orderHint = 0
		fi.refreshFlags = allFrames
		if r.overran() {
			return av1FrameInfo{}, false
		}
		return fi, true
	}

	fi.showExisting = r.bit() == 1
	if fi.showExisting {
		fi.showMapIdx = int(r.bits(3))
		if sh.decoderModelPresent && !sh.equalPictureInterval {
			r.bits(sh.framePresentTimeLen) // temporal_point_info: frame_presentation_time
		}
		if sh.frameIDPresent {
			r.bits(sh.idLen) // display_frame_id
		}
		fi.show = true
		if r.overran() {
			return av1FrameInfo{}, false
		}
		return fi, true // the shaper resolves the shown slot and any key-frame refresh
	}

	fi.frameType = int(r.bits(2))
	fi.intra = fi.frameType == av1KeyFrame || fi.frameType == av1IntraOnly
	fi.show = r.bit() == 1
	if fi.show && sh.decoderModelPresent && !sh.equalPictureInterval {
		r.bits(sh.framePresentTimeLen) // temporal_point_info
	}
	if !fi.show {
		r.bit() // showable_frame
	}
	errResilient := false
	if fi.frameType == av1SwitchFrame || (fi.frameType == av1KeyFrame && fi.show) {
		errResilient = true
	} else {
		errResilient = r.bit() == 1
	}

	r.bit() // disable_cdf_update
	allowScreen := sh.forceScreenContent
	if sh.forceScreenContent == av1SelectScreen {
		allowScreen = int(r.bit())
	}
	if allowScreen > 0 && sh.forceIntegerMV == av1SelectIntegerMV {
		r.bit() // force_integer_mv
	}
	if sh.frameIDPresent {
		r.bits(sh.idLen) // current_frame_id
	}
	if fi.frameType != av1SwitchFrame { // frame_size_override_flag (1 for SWITCH, else signalled)
		r.bit()
	}
	fi.orderHint = int(r.bits(sh.orderHintBits))
	if !fi.intra && !errResilient {
		r.bits(3) // primary_ref_frame
	}
	if sh.decoderModelPresent {
		if r.bit() == 1 { // buffer_removal_time_present_flag
			for op := 0; op < sh.opCnt; op++ {
				if sh.decoderModelForOp[op] {
					r.bits(sh.bufRemovalTimeLen) // buffer_removal_time[op]
				}
			}
		}
	}

	if fi.frameType == av1SwitchFrame || (fi.frameType == av1KeyFrame && fi.show) {
		fi.refreshFlags = allFrames
	} else {
		fi.refreshFlags = int(r.bits(8))
	}
	if (!fi.intra || fi.refreshFlags != allFrames) && errResilient && sh.enableOrderHint {
		for i := 0; i < av1NumRefFrames; i++ {
			r.bits(sh.orderHintBits) // ref_order_hint[i]
		}
	}

	if fi.intra {
		if r.overran() {
			return av1FrameInfo{}, false
		}
		return fi, true // intra frames reference no other picture
	}

	shortSignaling := false
	if sh.enableOrderHint {
		shortSignaling = r.bit() == 1
	}
	if shortSignaling {
		lastIdx := int(r.bits(3))
		goldIdx := int(r.bits(3))
		fi.refIdx = setFrameRefs(lastIdx, goldIdx, fi.orderHint, sh.orderHintBits, refOrderHint)
	} else {
		for i := 0; i < av1RefsPerFrame; i++ {
			fi.refIdx[i] = int(r.bits(3)) // ref_frame_idx[i]
			if sh.frameIDPresent {
				r.bits(sh.deltaFrameIDLen) // delta_frame_id_minus_1
			}
		}
	}
	if r.overran() {
		return av1FrameInfo{}, false
	}
	return fi, true
}

// getRelativeDist returns the signed order-hint distance a−b on the wrapped OrderHintBits
// ring (§5.9.3).
func getRelativeDist(a, b, orderHintBits int) int {
	if orderHintBits == 0 {
		return 0
	}
	diff := a - b
	m := 1 << (orderHintBits - 1)
	return (diff & (m - 1)) - (diff & m)
}

// setFrameRefs reconstructs the seven ref_frame_idx slot indices from the two signalled
// indices and the slots' order hints (§7.8), the algorithm an AV1 decoder runs when a frame
// uses frame_refs_short_signaling.
func setFrameRefs(lastIdx, goldIdx, orderHint, orderHintBits int, refOrderHint [av1NumRefFrames]int) [7]int {
	var refIdx [7]int
	for i := range refIdx {
		refIdx[i] = -1
	}
	refIdx[0] = lastIdx // LAST_FRAME
	refIdx[3] = goldIdx // GOLDEN_FRAME
	var used [av1NumRefFrames]bool
	used[lastIdx] = true
	used[goldIdx] = true
	curHint := 1 << (orderHintBits - 1)
	var shifted [av1NumRefFrames]int
	for i := 0; i < av1NumRefFrames; i++ {
		shifted[i] = curHint + getRelativeDist(refOrderHint[i], orderHint, orderHintBits)
	}
	find := func(forward bool, latest bool) int {
		ref, best := -1, 0
		for i := 0; i < av1NumRefFrames; i++ {
			h := shifted[i]
			backward := h >= curHint
			if used[i] || backward == forward { // want backward refs when forward==false
				continue
			}
			if ref < 0 || (latest && h >= best) || (!latest && h < best) {
				ref, best = i, h
			}
		}
		return ref
	}
	if ref := find(false, true); ref >= 0 { // ALTREF: latest backward
		refIdx[6] = ref
		used[ref] = true
	}
	if ref := find(false, false); ref >= 0 { // BWDREF: earliest backward
		refIdx[4] = ref
		used[ref] = true
	}
	if ref := find(false, false); ref >= 0 { // ALTREF2: earliest backward
		refIdx[5] = ref
		used[ref] = true
	}
	for _, name := range []int{1, 2, 4, 5, 6} { // LAST2, LAST3, BWDREF, ALTREF2, ALTREF
		if refIdx[name] < 0 {
			if ref := find(true, true); ref >= 0 { // latest forward
				refIdx[name] = ref
				used[ref] = true
			}
		}
	}
	earliest, best := -1, 0
	for i := 0; i < av1NumRefFrames; i++ {
		if earliest < 0 || shifted[i] < best {
			earliest, best = i, shifted[i]
		}
	}
	for i := range refIdx {
		if refIdx[i] < 0 {
			refIdx[i] = earliest
		}
	}
	return refIdx
}
