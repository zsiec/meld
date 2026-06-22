package shape

import (
	"bytes"
	"testing"
)

// annexB frames each NAL with a 4-byte start code into one Annex-B buffer.
func annexB(nals ...[]byte) []byte {
	var b []byte
	for _, n := range nals {
		b = append(b, 0, 0, 0, 1)
		b = append(b, n...)
	}
	return b
}

// TestNALUnitSplit: the splitter recovers each NAL's bytes (sans start code) from a mix
// of 3- and 4-byte start codes.
func TestNALUnitSplit(t *testing.T) {
	stream := []byte{
		0, 0, 1, 0x67, 0x42, // 3-byte start code + NAL
		0, 0, 0, 1, 0x68, 0xCE, 0x00, // 4-byte start code + NAL (trailing 0 belongs to next)
		0, 0, 1, 0x65, 0x88,
	}
	nals := nalUnits(stream)
	want := [][]byte{{0x67, 0x42}, {0x68, 0xCE}, {0x65, 0x88}}
	if len(nals) != len(want) {
		t.Fatalf("got %d NALs, want %d", len(nals), len(want))
	}
	for i := range want {
		if !bytes.Equal(nals[i], want[i]) {
			t.Fatalf("NAL %d = % x, want % x", i, nals[i], want[i])
		}
	}
	if nalUnits([]byte{1, 2, 3, 4}) != nil {
		t.Fatal("a buffer with no start code should yield no NALs")
	}
}

// at returns the shaped unit at index i, failing if out of range.
func at(t *testing.T, sh []Shaped, i int) Shaped {
	t.Helper()
	if i >= len(sh) {
		t.Fatalf("only %d units shaped, wanted index %d", len(sh), i)
	}
	return sh[i]
}

// assertUnit checks a shaped unit's descriptor fields and dependency list.
func assertUnit(t *testing.T, sh []Shaped, i int, cls PriorityClass, rap, disc bool, tid uint8, conf Confidence, refs ...uint32) {
	t.Helper()
	u := at(t, sh, i).Unit
	if u.Class != cls || u.RAP != rap || u.Discardable != disc || u.TemporalID != tid || u.Confidence != conf {
		t.Fatalf("unit %d: class=%d rap=%v disc=%v tid=%d conf=%d; want class=%d rap=%v disc=%v tid=%d conf=%d",
			i, u.Class, u.RAP, u.Discardable, u.TemporalID, u.Confidence, cls, rap, disc, tid, conf)
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
