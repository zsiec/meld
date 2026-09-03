package shape

import (
	"os"
	"testing"
)

// TestAV1ShaperRealStream runs the AV1 shaper over a real low-overhead OBU stream (a
// 48-frame testsrc2 extract encoded by libaom with hidden alt-ref frames and
// show_existing_frame) and confirms the EXACT slot-based reference model: it finds the
// sequence headers, two KEY frames, exactly 48 displayed pictures (matching the decoder),
// hidden reference frames that are not displayed, show_existing pictures that depend on the
// slot they show, and inter frames that resolve to one or more reference slots. The
// decodability cascade holds (losing the sequence header poisons the keyframes).
func TestAV1ShaperRealStream(t *testing.T) {
	data, err := os.ReadFile("testdata/bbb.obu")
	if err != nil {
		t.Skip("no real AV1 sample in testdata")
	}
	sh := NewAV1Shaper().Shape(data)
	if len(sh) == 0 {
		t.Fatal("shaped no units from a real AV1 stream")
	}
	var seqs, delimiters, keys, pics, hidden, showExist, multiRef int
	var units []Unit
	for _, s := range sh {
		units = append(units, s.Unit)
		switch {
		case len(s.Payload) > 0 && (s.Payload[0]>>3)&0xf == av1SeqHeader:
			seqs++
		case len(s.Payload) > 0 && (s.Payload[0]>>3)&0xf == av1TemporalDelim:
			delimiters++
		case s.Unit.RAP:
			keys++
		}
		if s.Unit.Picture {
			pics++
		} else if s.Unit.Class != ClassParamSet && s.Unit.Class != ClassEnhancement {
			hidden++ // a coded frame that is not displayed (a hidden alt-ref reference)
		}
		if s.Unit.Discardable && len(s.Unit.RefersTo) == 1 {
			showExist++ // a show_existing picture depends on the single slot it displays
		}
		if len(s.Unit.RefersTo) >= 2 {
			multiRef++ // an inter frame resolved to several reference slots
		}
	}
	t.Logf("real AV1: %d units — %d seq headers, %d temporal delimiters, %d keyframes, %d displayed, %d hidden refs, %d show_existing, %d multi-ref",
		len(sh), seqs, delimiters, keys, pics, hidden, showExist, multiRef)
	if seqs < 1 {
		t.Fatalf("expected a sequence header, got %d", seqs)
	}
	if delimiters < 1 {
		t.Fatal("expected temporal delimiters required by low-overhead OBU framing")
	}
	if keys != 2 {
		t.Fatalf("expected 2 KEY frames, got %d", keys)
	}
	if pics != 48 {
		t.Fatalf("expected 48 displayed pictures (the decoder's frame count), got %d", pics)
	}
	if hidden == 0 {
		t.Fatal("expected hidden alt-ref frames (coded but not displayed)")
	}
	if showExist == 0 {
		t.Fatal("expected show_existing pictures depending on a displayed slot")
	}
	if multiRef == 0 {
		t.Fatal("expected inter frames resolving to multiple reference slots — slot model not engaging")
	}

	all := make(map[uint32]bool, len(units))
	for _, u := range units {
		all[u.ID] = true
	}
	if r := DecodableKeyframeRate(units, all); r != 1 {
		t.Fatalf("keyframe rate %.2f with all delivered, want 1", r)
	}
	// Drop the sequence header the first keyframe depends on ⇒ that keyframe is poisoned
	// (each GOP re-sends its own sequence header, so one loss poisons one keyframe).
	var keyRef uint32
	for _, u := range units {
		if u.RAP && len(u.RefersTo) > 0 {
			keyRef = u.RefersTo[0]
			break
		}
	}
	d := make(map[uint32]bool, len(units))
	for id := range all {
		d[id] = id != keyRef
	}
	if r := DecodableKeyframeRate(units, d); r >= 1 {
		t.Fatalf("keyframe rate %.2f after losing a sequence header, want < 1 (poisoned)", r)
	}
}
