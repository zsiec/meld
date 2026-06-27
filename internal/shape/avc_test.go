package shape

import (
	"bytes"
	"testing"
)

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

func TestAVCShaperConstrainedSourceDropsOnlyNonRecoverySEI(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00}
	pps := []byte{0x68, 0xCE}
	idr := []byte{0x65, 0x88, 0x80, 0x00}
	userDataSEI := []byte{0x06, 5, 0, 0x80}     // payloadType 5, empty payload, trailing bits
	recoverySEI := []byte{0x06, 6, 1, 0, 0x80}  // payloadType 6 recovery_point
	malformedSEI := []byte{0x06, 5, 0xff, 0xff} // malformed size extension: retain fail-safe
	stream := annexB(sps, pps, idr, userDataSEI, recoverySEI, malformedSEI)

	unconstrained := NewAVCShaper().Shape(stream)
	if got, want := len(unconstrained), 6; got != want {
		t.Fatalf("unconstrained shaped %d units, want %d", got, want)
	}

	constrained := NewAVCShaperWithOptions(AVCOptions{SourceConstrained: true}).Shape(stream)
	if got, want := len(constrained), 5; got != want {
		t.Fatalf("constrained shaped %d units, want %d", got, want)
	}
	var seis [][]byte
	for _, sh := range constrained {
		if len(sh.Payload) == 0 || sh.Payload[0]&0x1f != avcSEI {
			continue
		}
		seis = append(seis, sh.Payload)
	}
	if len(seis) != 2 || !bytes.Equal(seis[0], recoverySEI) || !bytes.Equal(seis[1], malformedSEI) {
		t.Fatalf("constrained retained SEI payloads % x, want recovery and malformed fail-safe", seis)
	}
}

func TestAVCShaperRecoveryPointSEIResetsReferenceChain(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00}
	pps := []byte{0x68, 0xCE}
	idr := []byte{0x65, 0x88, 0x80, 0x00}
	recoverySEI := []byte{0x06, 6, 1, 0x50, 0x80} // recovery_frame_cnt = 1
	ref1 := []byte{0x61, 0x9A}
	ref2 := []byte{0x61, 0x9B}
	ref3 := []byte{0x61, 0x9C}

	sh := NewAVCShaper().Shape(annexB(sps, pps, idr, recoverySEI, ref1, ref2, ref3))
	if got, want := len(sh), 7; got != want {
		t.Fatalf("shaped %d units, want %d", got, want)
	}
	if u := sh[4].Unit; !u.RecoveryRefresh || u.RAP {
		t.Fatalf("refresh interval unit = %+v, want non-RAP recovery refresh slice", u)
	}
	if u := sh[5].Unit; !u.RAP || u.Class != ClassRAP {
		t.Fatalf("recovery-complete unit = %+v, want soft RAP", u)
	}
	if u := sh[5].Unit; !u.RecoveryRefresh {
		t.Fatalf("recovery-complete unit = %+v, want recovery refresh tag", u)
	}
	wantRefs := []uint32{0, 1, 4}
	if got := sh[5].Unit.RefersTo; !sameU32s(got, wantRefs) {
		t.Fatalf("recovery-complete refs = %v, want %v", got, wantRefs)
	}
	if got, want := sh[6].Unit.RefersTo, []uint32{5}; !sameU32s(got, want) {
		t.Fatalf("post-recovery refs = %v, want %v", got, want)
	}
	if sh[6].Unit.RecoveryRefresh {
		t.Fatalf("post-recovery unit = %+v, want ordinary reference slice", sh[6].Unit)
	}
}

func TestAVCShaperImmediateRecoveryPointSEIIsSoftRAP(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00}
	pps := []byte{0x68, 0xCE}
	idr := []byte{0x65, 0x88, 0x80, 0x00}
	recoverySEI := []byte{0x06, 6, 1, 0xC4, 0x80} // recovery_frame_cnt = 0
	ref := []byte{0x61, 0x9A}

	sh := NewAVCShaper().Shape(annexB(sps, pps, idr, recoverySEI, ref))
	if got, want := len(sh), 5; got != want {
		t.Fatalf("shaped %d units, want %d", got, want)
	}
	if u := sh[4].Unit; !u.RAP || u.Class != ClassRAP {
		t.Fatalf("immediate recovery unit = %+v, want soft RAP", u)
	}
	if u := sh[4].Unit; !u.RecoveryRefresh {
		t.Fatalf("immediate recovery unit = %+v, want recovery refresh tag", u)
	}
	if got, want := sh[4].Unit.RefersTo, []uint32{0, 1}; !sameU32s(got, want) {
		t.Fatalf("immediate recovery refs = %v, want %v", got, want)
	}
}

func TestAVCShaperConstrainedSourceMayDropDisposablePictures(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00}
	pps := []byte{0x68, 0xCE}
	idr := []byte{0x65, 0x88, 0x80, 0x00}
	ref := []byte{0x61, 0x9A}
	disposable := []byte{0x01, 0x9A}
	stream := annexB(sps, pps, idr, ref, disposable)

	unconstrained := NewAVCShaperWithOptions(AVCOptions{DropDisposablePictures: true}).Shape(stream)
	if got, want := len(unconstrained), 5; got != want {
		t.Fatalf("unconstrained shaped %d units, want %d; disposable picture must be preserved by default", got, want)
	}
	seiOnly := NewAVCShaperWithOptions(AVCOptions{SourceConstrained: true}).Shape(stream)
	if got, want := len(seiOnly), 5; got != want {
		t.Fatalf("SEI-only constrained shaped %d units, want %d; disposable picture should remain without explicit drop", got, want)
	}
	dropped := NewAVCShaperWithOptions(AVCOptions{SourceConstrained: true, DropDisposablePictures: true}).Shape(stream)
	if got, want := len(dropped), 4; got != want {
		t.Fatalf("drop-disposable constrained shaped %d units, want %d", got, want)
	}
	for _, sh := range dropped {
		if sh.Unit.Discardable {
			t.Fatalf("retained disposable unit %+v", sh.Unit)
		}
	}
}

func sameU32s(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
