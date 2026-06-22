package shape

import "testing"

// TestJPEGXSShaperPerFrameRAP: each codestream becomes one intra RAP unit with no
// dependency and not discardable (the degenerate intra-only model), and the frames are
// split on the SOC marker.
func TestJPEGXSShaperPerFrameRAP(t *testing.T) {
	soc := []byte{0xFF, 0x10}
	frame1 := append(append([]byte{}, soc...), 0x12, 0x34, 0x56)
	frame2 := append(append([]byte{}, soc...), 0x78, 0x9A)
	stream := append(append([]byte{}, frame1...), frame2...)

	sh := NewJPEGXSShaper().Shape(stream)
	if len(sh) != 2 {
		t.Fatalf("got %d units, want 2 frames", len(sh))
	}
	assertUnit(t, sh, 0, ClassRAP, true, false, 0, Signaled)
	assertUnit(t, sh, 1, ClassRAP, true, false, 0, Signaled)
	if at(t, sh, 0).Unit.Size != len(frame1) || at(t, sh, 1).Unit.Size != len(frame2) {
		t.Fatalf("frame sizes %d/%d, want %d/%d", sh[0].Unit.Size, sh[1].Unit.Size, len(frame1), len(frame2))
	}

	// Intra-only ⇒ the dependency model is EXACT by construction: every codestream is a
	// coded picture with NO inter-frame reference, so a frame is decodable iff it was
	// delivered (no cascade to model, no reference to get wrong).
	var units []Unit
	for _, s := range sh {
		units = append(units, s.Unit)
		if !s.Unit.Picture || len(s.Unit.RefersTo) != 0 {
			t.Fatalf("JPEG XS unit %d: Picture=%v RefersTo=%v, want a picture with no references", s.Unit.ID, s.Unit.Picture, s.Unit.RefersTo)
		}
	}
	d := map[uint32]bool{units[0].ID: true, units[1].ID: false}
	dec := Decodable(units, d)
	if !dec[units[0].ID] || dec[units[1].ID] {
		t.Fatal("intra frames must be independent (no cascade)")
	}
	// Both the keyframe and the frame metric equal the delivered fraction exactly.
	if r := DecodableKeyframeRate(units, d); r != 0.5 {
		t.Fatalf("keyframe rate %.2f, want 0.5 (one of two intra frames lost)", r)
	}
	if r := DecodableFrameRate(units, d); r != 0.5 {
		t.Fatalf("frame rate %.2f, want 0.5 (decodable == delivered for intra-only)", r)
	}
}

// TestJPEGXSNoSOC: a pre-framed payload with no SOC marker is treated as a single frame.
func TestJPEGXSNoSOC(t *testing.T) {
	sh := NewJPEGXSShaper().Shape([]byte{0x01, 0x02, 0x03})
	if len(sh) != 1 || !sh[0].Unit.RAP {
		t.Fatalf("want 1 RAP unit for a pre-framed payload, got %d", len(sh))
	}
}
