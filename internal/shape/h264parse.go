package shape

// Minimal H.264 bitstream parsing for exact reference resolution: enough of the SPS and
// slice header (Exp-Golomb over the RBSP) to recover slice_type and the picture order
// count, so a B-frame's two bracketing anchors can be identified by display order (POC).
// No pixel/residual decode. Spec: ITU-T H.264 §7.3.2.1.1 (SPS), §7.3.3 (slice header),
// §8.2.1 (POC, pic_order_cnt_type 0). switchframe (the sibling lab) parses NALs but
// decodes via openh264, so there is no Go reference for this — it is written to spec.

// bitReader reads bits MSB-first over an RBSP, returning 0 past the end (so a truncated
// header degrades to zeros rather than panicking — the no-panic rule).
type bitReader struct {
	data []byte
	pos  int // bit position
}

func newBitReader(rbsp []byte) *bitReader { return &bitReader{data: rbsp} }

func (r *bitReader) bit() uint {
	if r.pos >= len(r.data)*8 {
		r.pos++
		return 0
	}
	b := (r.data[r.pos>>3] >> uint(7-(r.pos&7))) & 1
	r.pos++
	return uint(b)
}

func (r *bitReader) bits(n int) uint {
	var v uint
	for i := 0; i < n; i++ {
		v = (v << 1) | r.bit()
	}
	return v
}

// overran reports whether the reader has consumed past the end of the buffer — a parse
// that ran off the end read zeros and its result must not be trusted.
func (r *bitReader) overran() bool { return r.pos > len(r.data)*8 }

// ue reads an unsigned Exp-Golomb code.
func (r *bitReader) ue() uint {
	zeros := 0
	for r.pos < len(r.data)*8 && r.bit() == 0 {
		zeros++
		if zeros > 31 {
			return 0
		}
	}
	if zeros == 0 {
		return 0
	}
	return (uint(1) << zeros) - 1 + r.bits(zeros)
}

// se reads a signed Exp-Golomb code.
func (r *bitReader) se() int {
	k := r.ue()
	if k&1 == 0 {
		return -int(k >> 1)
	}
	return int((k + 1) >> 1)
}

// spsInfo holds the SPS fields the slice-header parse and POC computation need.
type spsInfo struct {
	log2MaxFrameNum     int
	pocType             int
	log2MaxPocLsb       int
	separateColourPlane bool
	frameMbsOnly        bool
}

// avcHighProfile reports whether profile_idc carries the chroma/scaling SPS extension.
func avcHighProfile(p uint) bool {
	switch p {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		return true
	}
	return false
}

// parseSPS parses an SPS NAL (including its header byte) for the fields needed downstream.
func parseSPS(nal []byte) (spsInfo, bool) {
	if len(nal) < 2 {
		return spsInfo{}, false
	}
	r := newBitReader(ebspToRbsp(nal[1:]))
	profile := r.bits(8)
	r.bits(8) // constraint flags + reserved
	r.bits(8) // level_idc
	r.ue()    // seq_parameter_set_id
	var s spsInfo
	if avcHighProfile(profile) {
		chroma := r.ue()
		if chroma == 3 {
			s.separateColourPlane = r.bit() == 1
		}
		r.ue()            // bit_depth_luma_minus8
		r.ue()            // bit_depth_chroma_minus8
		r.bit()           // qpprime_y_zero_transform_bypass_flag
		if r.bit() == 1 { // seq_scaling_matrix_present_flag
			n := 8
			if chroma == 3 {
				n = 12
			}
			for i := 0; i < n; i++ {
				if r.bit() == 1 { // scaling_list_present_flag[i]
					size := 16
					if i >= 6 {
						size = 64
					}
					skipScalingList(r, size)
				}
			}
		}
	}
	s.log2MaxFrameNum = int(r.ue()) + 4
	if s.log2MaxFrameNum < 4 || s.log2MaxFrameNum > 16 { // spec range; guards bits(n) against a runaway count
		return spsInfo{}, false
	}
	s.pocType = int(r.ue())
	if s.pocType == 0 {
		s.log2MaxPocLsb = int(r.ue()) + 4
		if s.log2MaxPocLsb < 4 || s.log2MaxPocLsb > 16 {
			return spsInfo{}, false
		}
	} else if s.pocType == 1 {
		r.bit() // delta_pic_order_always_zero_flag
		r.se()  // offset_for_non_ref_pic
		r.se()  // offset_for_top_to_bottom_field
		for n := int(r.ue()); n > 0; n-- {
			r.se() // offset_for_ref_frame[i]
		}
	}
	r.ue()  // max_num_ref_frames
	r.bit() // gaps_in_frame_num_value_allowed_flag
	r.ue()  // pic_width_in_mbs_minus1
	r.ue()  // pic_height_in_map_units_minus1
	s.frameMbsOnly = r.bit() == 1
	return s, true
}

// skipScalingList consumes a scaling list (§7.3.2.1.1.1) without retaining it.
func skipScalingList(r *bitReader, size int) {
	last, next := 8, 8
	for j := 0; j < size; j++ {
		if next != 0 {
			delta := r.se()
			next = (last + delta + 256) & 255
		}
		if next != 0 {
			last = next
		}
	}
}

// sliceInfo holds the slice-header fields needed for reference resolution.
type sliceInfo struct {
	sliceType int // 0=P, 1=B, 2=I (mod 5)
	pocLsb    int
	hasPoc    bool
	idr       bool
}

// H.264 slice_type values mod 5.
const (
	sliceP = 0
	sliceB = 1
	sliceI = 2
)

// parseSliceHeader parses a coded-slice NAL (including its header byte) far enough to get
// slice_type and pic_order_cnt_lsb (pic_order_cnt_type 0, the common case).
func parseSliceHeader(nal []byte, sps spsInfo) (sliceInfo, bool) {
	if len(nal) < 1 {
		return sliceInfo{}, false
	}
	idr := nal[0]&0x1f == avcIDR
	r := newBitReader(ebspToRbsp(nal[1:]))
	var si sliceInfo
	si.idr = idr
	r.ue() // first_mb_in_slice
	si.sliceType = int(r.ue()) % 5
	r.ue() // pic_parameter_set_id
	if sps.separateColourPlane {
		r.bits(2) // colour_plane_id
	}
	r.bits(sps.log2MaxFrameNum) // frame_num
	if !sps.frameMbsOnly {
		if r.bit() == 1 { // field_pic_flag
			r.bit() // bottom_field_flag
		}
	}
	if idr {
		r.ue() // idr_pic_id
	}
	if sps.pocType == 0 {
		si.pocLsb = int(r.bits(sps.log2MaxPocLsb))
		si.hasPoc = true
	}
	return si, true
}

// ebspToRbsp removes the emulation_prevention_three_byte (0x03 after 0x00 0x00) so the
// RBSP can be bit-read directly.
func ebspToRbsp(b []byte) []byte {
	out := make([]byte, 0, len(b))
	zeros := 0
	for _, c := range b {
		if zeros >= 2 && c == 0x03 {
			zeros = 0
			continue
		}
		out = append(out, c)
		if c == 0 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

// avcSEIHasPayloadType reports whether an AVC SEI NAL contains a SEI message with the
// requested payload type. Malformed SEI returns true so source filtering fails safe and
// preserves metadata it cannot classify.
func avcSEIHasPayloadType(nal []byte, want int) bool {
	if len(nal) < 2 || nal[0]&0x1f != avcSEI {
		return true
	}
	rbsp := ebspToRbsp(nal[1:])
	for off := 0; off < len(rbsp); {
		if rbsp[off] == 0x80 && trailingZeroBits(rbsp[off+1:]) {
			return false
		}
		payloadType, ok := readSEIExtValue(rbsp, &off)
		if !ok {
			return true
		}
		payloadSize, ok := readSEIExtValue(rbsp, &off)
		if !ok || payloadSize < 0 || off+payloadSize > len(rbsp) {
			return true
		}
		if payloadType == want {
			return true
		}
		off += payloadSize
	}
	return false
}

func avcSEIRecoveryFrameCnt(nal []byte) (int, bool) {
	if len(nal) < 2 || nal[0]&0x1f != avcSEI {
		return 0, false
	}
	rbsp := ebspToRbsp(nal[1:])
	for off := 0; off < len(rbsp); {
		if rbsp[off] == 0x80 && trailingZeroBits(rbsp[off+1:]) {
			return 0, false
		}
		payloadType, ok := readSEIExtValue(rbsp, &off)
		if !ok {
			return 0, false
		}
		payloadSize, ok := readSEIExtValue(rbsp, &off)
		if !ok || payloadSize < 0 || off+payloadSize > len(rbsp) {
			return 0, false
		}
		if payloadType == avcSEIRecoveryPoint {
			r := newBitReader(rbsp[off : off+payloadSize])
			cnt := int(r.ue())
			if r.overran() {
				return 0, false
			}
			return cnt, true
		}
		off += payloadSize
	}
	return 0, false
}

func readSEIExtValue(b []byte, off *int) (int, bool) {
	if *off >= len(b) {
		return 0, false
	}
	v := 0
	for *off < len(b) && b[*off] == 0xff {
		v += 255
		*off += 1
	}
	if *off >= len(b) {
		return 0, false
	}
	v += int(b[*off])
	*off += 1
	return v, true
}

func trailingZeroBits(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}
