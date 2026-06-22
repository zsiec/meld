package shape

import "testing"

// TestAVCShaperClassifies: an H.264 NAL sequence maps to the right tiers, RAP/discardable
// flags, and dependency chain from the NAL headers alone.
func TestAVCShaperClassifies(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00}       // type 7, nal_ref_idc 3
	pps := []byte{0x68, 0xCE}             // type 8
	idr := []byte{0x65, 0x88, 0x80, 0x00} // type 5 (IDR slice), ref
	ref := []byte{0x61, 0x9A}             // type 1, nal_ref_idc 3 (reference P slice)
	non := []byte{0x01, 0x9A}             // type 1, nal_ref_idc 0 (non-reference)
	aud := []byte{0x09, 0x10}             // type 9 (access unit delimiter) — dropped
	sei := []byte{0x06, 0x05}             // type 6 (SEI)

	sh := NewAVCShaper().Shape(annexB(sps, pps, idr, ref, non, aud, sei))
	if len(sh) != 6 {
		t.Fatalf("got %d units, want 6 (AUD dropped)", len(sh))
	}
	check := func(i int, cls PriorityClass, rap, disc bool, conf Confidence, refs []uint32) {
		u := at(t, sh, i).Unit
		if u.Class != cls || u.RAP != rap || u.Discardable != disc || u.Confidence != conf {
			t.Fatalf("unit %d: class=%d rap=%v disc=%v conf=%d, want class=%d rap=%v disc=%v conf=%d",
				i, u.Class, u.RAP, u.Discardable, u.Confidence, cls, rap, disc, conf)
		}
		if len(u.RefersTo) != len(refs) {
			t.Fatalf("unit %d refs=%v, want %v", i, u.RefersTo, refs)
		}
		for k := range refs {
			if u.RefersTo[k] != refs[k] {
				t.Fatalf("unit %d refs=%v, want %v", i, u.RefersTo, refs)
			}
		}
	}
	check(0, ClassParamSet, false, false, Signaled, nil)            // SPS (id 0)
	check(1, ClassParamSet, false, false, Signaled, nil)            // PPS (id 1)
	check(2, ClassRAP, true, false, Signaled, []uint32{0, 1})       // IDR → SPS+PPS
	check(3, ClassBase, false, false, Inferred, []uint32{2})        // ref P → IDR
	check(4, ClassDisposable, false, true, Inferred, []uint32{3})   // non-ref → ref P (prevRef unchanged)
	check(5, ClassEnhancement, false, false, Inferred, []uint32{3}) // SEI → ref P
}

// TestAVCShaperCascade: through the shaper + the decodability oracle, losing the SPS
// poisons the whole sequence, while losing the non-reference slice is local.
func TestAVCShaperCascade(t *testing.T) {
	sps := []byte{0x67, 0x42}
	pps := []byte{0x68, 0xCE}
	idr := []byte{0x65, 0x88}
	ref := []byte{0x61, 0x9A}
	non := []byte{0x01, 0x9A}
	var units []Unit
	for _, s := range NewAVCShaper().Shape(annexB(sps, pps, idr, ref, non)) {
		units = append(units, s.Unit)
	}
	all := map[uint32]bool{}
	for _, u := range units {
		all[u.ID] = true
	}
	if r := DecodableKeyframeRate(units, all); r != 1 {
		t.Fatalf("all delivered: keyframe rate %.2f, want 1", r)
	}
	// Drop the SPS (id 0): the IDR depends on it, everything chains off the IDR.
	d := map[uint32]bool{}
	for id := range all {
		d[id] = id != 0
	}
	if r := DecodableKeyframeRate(units, d); r != 0 {
		t.Fatalf("SPS lost: keyframe rate %.2f, want 0 (whole sequence poisoned)", r)
	}
	// Drop only the non-reference slice (id 4): local — keyframe stays decodable.
	d = map[uint32]bool{}
	for id := range all {
		d[id] = id != 4
	}
	if r := DecodableKeyframeRate(units, d); r != 1 {
		t.Fatalf("non-ref lost: keyframe rate %.2f, want 1 (local loss)", r)
	}
}
