package shape

// Minimal H.265 (HEVC) bitstream parsing for exact reference resolution: enough of the
// SPS, PPS, and slice segment header (Exp-Golomb over the RBSP) to recover slice_type and
// the picture order count, so a B-picture's two bracketing anchors can be identified by
// display order. No pixel/residual decode. Spec: ITU-T H.265 §7.3.2.2 (SPS), §7.3.2.3
// (PPS), §7.3.6.1 (slice segment header), §8.3.1 (POC). Reuses the shared bitReader and
// ebspToRbsp from h264parse.go (the Exp-Golomb reader is codec-neutral). The two-byte NAL
// header precedes every RBSP, so parsing starts at nal[2:].

// HEVC slice_type values (H.265 §7.4.7.1) — note the order differs from H.264.
const (
	hevcSliceB = 0
	hevcSliceP = 1
	hevcSliceI = 2
)

// HEVC IDR NAL unit types: a picture order count of zero with no signaled lsb.
const (
	hevcIDRWRADL = 19
	hevcIDRNLP   = 20
)

// hevcSPSInfo holds the SPS fields the slice-header parse and POC computation need.
type hevcSPSInfo struct {
	log2MaxPocLsb       int
	separateColourPlane bool
}

// hevcPPSInfo holds the PPS fields needed to walk the slice segment header.
type hevcPPSInfo struct {
	dependentSliceSegments bool
	outputFlagPresent      bool
	numExtraSliceHeaderBit int
}

// parseHEVCSPS parses an SPS NAL (including its 2-byte header) for log2_max_pic_order_cnt_lsb
// and separate_colour_plane_flag (H.265 §7.3.2.2.1).
func parseHEVCSPS(nal []byte) (hevcSPSInfo, bool) {
	if len(nal) < 3 {
		return hevcSPSInfo{}, false
	}
	r := newBitReader(ebspToRbsp(nal[2:]))
	r.bits(4) // sps_video_parameter_set_id
	maxSubLayersMinus1 := int(r.bits(3))
	r.bit() // sps_temporal_id_nesting_flag
	skipProfileTierLevel(r, maxSubLayersMinus1)
	r.ue() // sps_seq_parameter_set_id
	var s hevcSPSInfo
	if chroma := r.ue(); chroma == 3 {
		s.separateColourPlane = r.bit() == 1
	}
	r.ue()            // pic_width_in_luma_samples
	r.ue()            // pic_height_in_luma_samples
	if r.bit() == 1 { // conformance_window_flag
		r.ue() // conf_win_left_offset
		r.ue() // conf_win_right_offset
		r.ue() // conf_win_top_offset
		r.ue() // conf_win_bottom_offset
	}
	r.ue() // bit_depth_luma_minus8
	r.ue() // bit_depth_chroma_minus8
	s.log2MaxPocLsb = int(r.ue()) + 4
	if s.log2MaxPocLsb < 4 || s.log2MaxPocLsb > 16 { // spec range; guards bits(n) against a runaway count
		return hevcSPSInfo{}, false
	}
	return s, true
}

// skipProfileTierLevel consumes a profile_tier_level structure with profilePresentFlag set
// (H.265 §7.3.3): a fixed 96-bit general block, then per-sub-layer present flags and their
// optional 88-bit profile / 8-bit level blocks.
func skipProfileTierLevel(r *bitReader, maxSubLayersMinus1 int) {
	r.bits(32) // general_profile_space(2) + tier(1) + profile_idc(5) + compatibility[24]
	r.bits(32) // compatibility[8] + progressive/interlaced/non-packed/frame-only(4) + constraints[20]
	r.bits(24) // constraints[23] + general_inbld_flag/reserved(1)
	r.bits(8)  // general_level_idc
	profilePresent := make([]bool, maxSubLayersMinus1)
	levelPresent := make([]bool, maxSubLayersMinus1)
	for i := 0; i < maxSubLayersMinus1; i++ {
		profilePresent[i] = r.bit() == 1
		levelPresent[i] = r.bit() == 1
	}
	if maxSubLayersMinus1 > 0 {
		for i := maxSubLayersMinus1; i < 8; i++ {
			r.bits(2) // reserved_zero_2bits
		}
	}
	for i := 0; i < maxSubLayersMinus1; i++ {
		if profilePresent[i] {
			r.bits(32)
			r.bits(32)
			r.bits(24) // sub-layer profile block, no level (88 bits)
		}
		if levelPresent[i] {
			r.bits(8) // sub_layer_level_idc
		}
	}
}

// parseHEVCPPS parses a PPS NAL (including its 2-byte header) for the fields that govern
// the slice segment header walk (H.265 §7.3.2.3.1).
func parseHEVCPPS(nal []byte) (hevcPPSInfo, bool) {
	if len(nal) < 3 {
		return hevcPPSInfo{}, false
	}
	r := newBitReader(ebspToRbsp(nal[2:]))
	r.ue() // pps_pic_parameter_set_id
	r.ue() // pps_seq_parameter_set_id
	var p hevcPPSInfo
	p.dependentSliceSegments = r.bit() == 1
	p.outputFlagPresent = r.bit() == 1
	p.numExtraSliceHeaderBit = int(r.bits(3))
	return p, true
}

// parseHEVCSliceHeader parses a coded-slice NAL (including its 2-byte header) far enough to
// get slice_type and slice_pic_order_cnt_lsb (H.265 §7.3.6.1). It only resolves the FIRST
// slice segment of a picture; a dependent or non-first segment needs CTB-address math we do
// not carry, so it returns hasPoc=false and the shaper falls back to the reference chain.
func parseHEVCSliceHeader(nal []byte, sps hevcSPSInfo, pps hevcPPSInfo) (sliceInfo, bool) {
	if len(nal) < 3 {
		return sliceInfo{}, false
	}
	typ := (nal[0] >> 1) & 0x3F
	idr := typ == hevcIDRWRADL || typ == hevcIDRNLP
	irap := typ >= hevcIRAPLo && typ <= hevcIRAPHi
	r := newBitReader(ebspToRbsp(nal[2:]))
	var si sliceInfo
	si.idr = idr
	firstSlice := r.bit() == 1
	if !firstSlice {
		return sliceInfo{}, false // non-first segment: bail to the reference-chain fallback
	}
	if irap {
		r.bit() // no_output_of_prior_pics_flag
	}
	r.ue() // slice_pic_parameter_set_id
	for i := 0; i < pps.numExtraSliceHeaderBit; i++ {
		r.bit() // slice_reserved_flag[i]
	}
	si.sliceType = int(r.ue())
	if pps.outputFlagPresent {
		r.bit() // pic_output_flag
	}
	if sps.separateColourPlane {
		r.bits(2) // colour_plane_id
	}
	if !idr {
		si.pocLsb = int(r.bits(sps.log2MaxPocLsb))
	}
	si.hasPoc = true // an IDR has POC 0; others carry slice_pic_order_cnt_lsb
	return si, true
}
