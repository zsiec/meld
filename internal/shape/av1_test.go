package shape

import "testing"

// av1Stream concatenates OBUs (AV1's low-overhead bitstream has no start codes; OBUs
// carry their own size field).
func av1Stream(obus ...[]byte) []byte {
	var b []byte
	for _, o := range obus {
		b = append(b, o...)
	}
	return b
}

// TestAV1ShaperClassifies: an AV1 OBU stream maps to the right tiers, RAP/discardable
// flags, temporal ids, and dependency chain from OBU headers + the frame_type peek.
func TestAV1ShaperClassifies(t *testing.T) {
	// obu_header bits: type<<3 | ext<<2 | has_size<<1 | reserved.
	seqHdr := []byte{0x0A, 0x02, 0x00, 0x00} // type 1, has_size, size 2
	keyFrame := []byte{0x32, 0x01, 0x00}     // type 6, size 1, payload show_existing=0 frame_type=KEY(00)
	interFr := []byte{0x32, 0x01, 0x20}      // type 6, payload frame_type=INTER(01)
	tempDelim := []byte{0x12, 0x00}          // type 2, size 0 — dropped
	metadata := []byte{0x2A, 0x01, 0x01}     // type 5, size 1

	sh := NewAV1Shaper().Shape(av1Stream(seqHdr, keyFrame, interFr, tempDelim, metadata))
	if len(sh) != 4 {
		t.Fatalf("got %d units, want 4 (temporal delimiter dropped)", len(sh))
	}
	assertUnit(t, sh, 0, ClassParamSet, false, false, 0, Signaled)       // sequence header (id 0)
	assertUnit(t, sh, 1, ClassRAP, true, false, 0, Signaled, 0)          // KEY frame → seq header
	assertUnit(t, sh, 2, ClassBase, false, false, 0, Signaled, 1)        // INTER frame → KEY (prevRef)
	assertUnit(t, sh, 3, ClassEnhancement, false, false, 0, Inferred, 2) // metadata → INTER (prevRef)
}

// TestAV1ShaperTemporalAndShowExisting: a temporal-extension INTER frame tiers by its
// temporal id, and a show_existing_frame frame is disposable.
func TestAV1ShaperTemporalAndShowExisting(t *testing.T) {
	seqHdr := []byte{0x0A, 0x01, 0x00}
	keyFrame := []byte{0x32, 0x01, 0x00}
	// type 6, ext flag (tid 2), has_size: header 0x36; ext byte tid=2<<5=0x40; payload INTER.
	interT2 := []byte{0x36, 0x40, 0x01, 0x20}
	// show_existing_frame: payload top bit 1.
	showExisting := []byte{0x32, 0x01, 0x80}

	sh := NewAV1Shaper().Shape(av1Stream(seqHdr, keyFrame, interT2, showExisting))
	assertUnit(t, sh, 2, ClassDisposable, false, false, 2, Signaled, 1) // tid 2 INTER → disposable tier
	assertUnit(t, sh, 3, ClassDisposable, false, true, 0, Signaled, 2)  // show_existing → discardable
}
