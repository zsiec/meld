package shape

import (
	"os"
	"testing"
)

// TestHEVCShaperRealStream runs the H.265 shaper over a real Annex-B clip (a 48-frame
// testsrc2 extract encoded with hierarchical B-frames) and confirms it classifies real NAL
// streams: it finds the parameter sets, two IDR keyframes, many coded pictures, and —
// crucially — resolves bidirectional B-pictures to TWO bracketing anchors via the parsed
// POC, not the single-previous-reference approximation. The decodability cascade holds
// (losing a parameter set the keyframe needs poisons the keyframe). Validates the SPS/PPS/
// slice parser on real bitstream signaling.
func TestHEVCShaperRealStream(t *testing.T) {
	data, err := os.ReadFile("testdata/bbb.h265")
	if err != nil {
		t.Skip("no real H.265 sample in testdata")
	}
	sh := NewHEVCShaper().Shape(data)
	if len(sh) == 0 {
		t.Fatal("shaped no units from a real H.265 stream")
	}
	var params, raps, pics, bracketed int
	var units []Unit
	for _, s := range sh {
		units = append(units, s.Unit)
		switch {
		case s.Unit.Class == ClassParamSet:
			params++
		case s.Unit.RAP:
			raps++
		}
		if s.Unit.Picture {
			pics++
		}
		if len(s.Unit.RefersTo) == 2 && !s.Unit.RAP {
			bracketed++ // a B-picture resolved to both its bracketing anchors
		}
		if s.Unit.Size != len(s.Payload) {
			t.Fatalf("unit %d size %d != payload %d", s.Unit.ID, s.Unit.Size, len(s.Payload))
		}
	}
	t.Logf("real HEVC: %d units — %d param sets, %d keyframes, %d pictures, %d bracketed B-pictures",
		len(sh), params, raps, pics, bracketed)
	if params < 3 {
		t.Fatalf("expected VPS+SPS+PPS, got %d parameter sets", params)
	}
	if raps != 2 {
		t.Fatalf("expected 2 IDR keyframes, got %d", raps)
	}
	if pics < 40 {
		t.Fatalf("expected ~48 coded pictures, got %d", pics)
	}
	// The point of the increment: real B-pictures resolve to their two bracketing anchors.
	if bracketed < 5 {
		t.Fatalf("expected several bracketed B-pictures (2 refs each), got %d — POC bracketing not engaging", bracketed)
	}

	all := make(map[uint32]bool, len(units))
	for _, u := range units {
		all[u.ID] = true
	}
	if r := DecodableKeyframeRate(units, all); r != 1 {
		t.Fatalf("keyframe rate %.2f with all delivered, want 1", r)
	}
	// Drop a parameter set the first keyframe depends on ⇒ that keyframe is poisoned.
	var rapRefs []uint32
	for _, u := range units {
		if u.RAP {
			rapRefs = u.RefersTo
			break
		}
	}
	if len(rapRefs) == 0 {
		t.Fatal("the keyframe should reference its parameter sets")
	}
	d := make(map[uint32]bool, len(units))
	for id := range all {
		d[id] = id != rapRefs[0]
	}
	if r := DecodableKeyframeRate(units, d); r >= 1 {
		t.Fatalf("keyframe rate %.2f after losing a parameter set, want < 1 (poisoned)", r)
	}
}
