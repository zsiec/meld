package shape

// Codec-neutral picture-order-count (POC) bracketing, shared by the AVC and HEVC shapers.
// Both codecs predict a picture's display order from the previous reference's POC by the
// same lsb-wrap rule (H.264 §8.2.1, H.265 §8.3.1), and both resolve a B-picture to the two
// references that bracket it in display order. The codec-specific bit parsing lives in
// h264parse.go / hevcparse.go; the prediction and bracketing math live here.

// pocRef is a recent reference picture: its display order (POC) and its unit id.
type pocRef struct {
	poc int
	id  uint32
}

// bracketDPB bounds the reference history a shaper keeps for bracketing — a
// decoded-picture-buffer-sized window past which older references are pruned.
const bracketDPB = 32

// pocMSB predicts a picture's POC most-significant bits from the previous reference's POC
// (prevMsb/prevLsb) and the current picture's lsb, per the identical H.264 §8.2.1 /
// H.265 §8.3.1 wrap rule: the msb steps up or down a full lsb period when the lsb jumps
// more than half a period in the opposite direction.
func pocMSB(prevMsb, prevLsb, lsb, maxLsb int) int {
	switch {
	case lsb < prevLsb && prevLsb-lsb >= maxLsb/2:
		return prevMsb + maxLsb
	case lsb > prevLsb && lsb-prevLsb > maxLsb/2:
		return prevMsb - maxLsb
	default:
		return prevMsb
	}
}

// bracketRefs returns the anchors that bracket display order poc among refs: the reference
// with the nearest-lower POC and the one with the nearest-higher POC — a B-picture's
// backward and forward prediction sources. Either may be absent (returns 0, 1, or 2 ids).
func bracketRefs(refs []pocRef, poc int) []uint32 {
	below, above := int64(-1), int64(-1)
	belowPOC, abovePOC := 0, 0
	for _, r := range refs {
		if r.poc < poc && (below < 0 || r.poc > belowPOC) {
			below, belowPOC = int64(r.id), r.poc
		}
		if r.poc > poc && (above < 0 || r.poc < abovePOC) {
			above, abovePOC = int64(r.id), r.poc
		}
	}
	var out []uint32
	if below >= 0 {
		out = append(out, uint32(below))
	}
	if above >= 0 {
		out = append(out, uint32(above))
	}
	return out
}

// appendRef records a reference picture for future bracketing, bounding the history to the
// DPB window.
func appendRef(refs []pocRef, poc int, id uint32) []pocRef {
	refs = append(refs, pocRef{poc: poc, id: id})
	if len(refs) > bracketDPB {
		refs = refs[len(refs)-bracketDPB:]
	}
	return refs
}
