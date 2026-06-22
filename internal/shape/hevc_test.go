package shape

import "testing"

// TestHEVCShaperClassifies: an H.265 NAL sequence maps to the right tiers, RAP/
// discardable flags, temporal ids, and dependency chain from the 2-byte NAL headers.
func TestHEVCShaperClassifies(t *testing.T) {
	vps := []byte{0x40, 0x01, 0x0C} // type 32, tid 0
	sps := []byte{0x42, 0x01, 0x01} // type 33, tid 0
	pps := []byte{0x44, 0x01, 0xC1} // type 34, tid 0
	idr := []byte{0x26, 0x01, 0xAF} // type 19 (IDR_W_RADL), tid 0 — RAP
	tr0 := []byte{0x02, 0x01, 0xD0} // type 1 (TRAIL_R), tid 0 — base reference
	te1 := []byte{0x02, 0x02, 0xD0} // type 1 (TRAIL_R), tid 1 — enhancement reference
	tn2 := []byte{0x00, 0x03, 0xD0} // type 0 (TRAIL_N), tid 2 — disposable SLNR
	aud := []byte{0x46, 0x01, 0x50} // type 35 (AUD) — dropped

	sh := NewHEVCShaper().Shape(annexB(vps, sps, pps, idr, tr0, te1, tn2, aud))
	if len(sh) != 7 {
		t.Fatalf("got %d units, want 7 (AUD dropped)", len(sh))
	}
	check := func(i int, cls PriorityClass, rap, disc bool, tid uint8, refs []uint32) {
		u := at(t, sh, i).Unit
		if u.Class != cls || u.RAP != rap || u.Discardable != disc || u.TemporalID != tid || u.Confidence != Signaled {
			t.Fatalf("unit %d: class=%d rap=%v disc=%v tid=%d conf=%d, want class=%d rap=%v disc=%v tid=%d Signaled",
				i, u.Class, u.RAP, u.Discardable, u.TemporalID, u.Confidence, cls, rap, disc, tid)
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
	check(0, ClassParamSet, false, false, 0, nil)            // VPS (id 0)
	check(1, ClassParamSet, false, false, 0, nil)            // SPS (id 1)
	check(2, ClassParamSet, false, false, 0, nil)            // PPS (id 2)
	check(3, ClassRAP, true, false, 0, []uint32{0, 1, 2})    // IDR → VPS+SPS+PPS
	check(4, ClassBase, false, false, 0, []uint32{3})        // TRAIL_R tid0 → IDR
	check(5, ClassEnhancement, false, false, 1, []uint32{4}) // TRAIL_R tid1 → TRAIL_R tid0 (prevRef)
	check(6, ClassDisposable, false, true, 2, []uint32{5})   // TRAIL_N tid2 → TRAIL_R tid1 (prevRef)
}

// TestHEVCShaperCascade: through the shaper + oracle, losing a base reference cascades to
// the frames hanging off it, while losing an SLNR leaf is local.
func TestHEVCShaperCascade(t *testing.T) {
	vps := []byte{0x40, 0x01}
	sps := []byte{0x42, 0x01}
	pps := []byte{0x44, 0x01}
	idr := []byte{0x26, 0x01}
	tr0 := []byte{0x02, 0x01} // base ref (id 4)
	tn1 := []byte{0x00, 0x02} // SLNR leaf off tr0 (id 5)
	var units []Unit
	for _, s := range NewHEVCShaper().Shape(annexB(vps, sps, pps, idr, tr0, tn1)) {
		units = append(units, s.Unit)
	}
	all := map[uint32]bool{}
	for _, u := range units {
		all[u.ID] = true
	}
	frames := func(d map[uint32]bool) float64 { return DecodableFrameRate(units, d) }
	if frames(all) != 1 {
		t.Fatalf("all delivered: frame rate %.2f, want 1", frames(all))
	}
	// Drop the base reference tr0 (id 4): the SLNR leaf (id 5) cascades.
	d := map[uint32]bool{}
	for id := range all {
		d[id] = id != 4
	}
	dec := Decodable(units, d)
	if dec[5] {
		t.Fatal("leaf should cascade when its base reference is lost")
	}
	if !dec[3] {
		t.Fatal("the IDR (before the lost base) should stay decodable")
	}
	// Drop only the SLNR leaf (id 5): local.
	d = map[uint32]bool{}
	for id := range all {
		d[id] = id != 5
	}
	dec = Decodable(units, d)
	if dec[5] || !dec[4] {
		t.Fatal("SLNR leaf loss should be local (base stays decodable)")
	}
}
