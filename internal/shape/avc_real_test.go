package shape

import (
	"os"
	"testing"
)

// TestAVCShaperRealStream runs the H.264 shaper over a real Annex-B clip (a 12-frame
// Big Buck Bunny extract, CC-BY) and confirms it classifies real NAL streams: it finds
// the parameter sets, exactly one IDR keyframe, several P-frames, and the cascade holds
// through the decodability oracle (losing a parameter set the keyframe needs poisons the
// keyframe). This validates the shaper on real bitstream signaling, not just synthetic
// NAL headers — realistic input from a glass-to-glass media run.
func TestAVCShaperRealStream(t *testing.T) {
	data, err := os.ReadFile("testdata/bbb.h264")
	if err != nil {
		t.Skip("no real H.264 sample in testdata")
	}
	sh := NewAVCShaper().Shape(data)
	if len(sh) == 0 {
		t.Fatal("shaped no units from a real H.264 stream")
	}
	var params, raps, frames, sei int
	var units []Unit
	for _, s := range sh {
		units = append(units, s.Unit)
		switch s.Unit.Class {
		case ClassParamSet:
			params++
		case ClassRAP:
			raps++
		case ClassEnhancement:
			sei++
		default:
			frames++
		}
		if s.Unit.Size != len(s.Payload) {
			t.Fatalf("unit %d size %d != payload %d", s.Unit.ID, s.Unit.Size, len(s.Payload))
		}
	}
	t.Logf("real BBB H.264: %d units — %d param sets, %d keyframes, %d P-frames, %d SEI",
		len(sh), params, raps, frames, sei)
	if params < 2 {
		t.Fatalf("expected SPS+PPS, got %d parameter sets", params)
	}
	if raps != 1 {
		t.Fatalf("expected exactly 1 IDR keyframe, got %d", raps)
	}
	if frames < 5 {
		t.Fatalf("expected several P-frames, got %d", frames)
	}

	all := make(map[uint32]bool, len(units))
	for _, u := range units {
		all[u.ID] = true
	}
	if r := DecodableKeyframeRate(units, all); r != 1 {
		t.Fatalf("keyframe rate %.2f with all delivered, want 1", r)
	}
	// Drop a parameter set the keyframe depends on ⇒ the keyframe is poisoned.
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
	if r := DecodableKeyframeRate(units, d); r != 0 {
		t.Fatalf("keyframe rate %.2f after losing a parameter set, want 0 (poisoned)", r)
	}
}
