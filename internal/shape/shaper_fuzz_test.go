package shape

import "testing"

// shapeNoPanic runs one shaper over arbitrary bytes and asserts the universal descriptor
// invariants: a unit's Size equals its payload length, and every reference is to an
// EARLIER unit (dependencies always point backward in decode order — the property the
// decodability oracle relies on). The point of the fuzz is that no malformed bitstream may
// panic a shaper (the parsers read untrusted headers).
func shapeNoPanic(t *testing.T, name string, shaped []Shaped) {
	for _, s := range shaped {
		if s.Unit.Size != len(s.Payload) {
			t.Fatalf("%s: unit %d Size %d != payload %d", name, s.Unit.ID, s.Unit.Size, len(s.Payload))
		}
		for _, r := range s.Unit.RefersTo {
			if r >= s.Unit.ID {
				t.Fatalf("%s: unit %d references %d, not strictly earlier", name, s.Unit.ID, r)
			}
		}
	}
}

// FuzzShapers feeds arbitrary bytes to every codec shaper: a malformed elementary stream
// must never panic the bitstream parsers, and the descriptors it produces must satisfy the
// backward-reference invariant.
func FuzzShapers(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0x00, 0x01, 0x67, 0x42})                   // AVC-ish Annex-B
	f.Add([]byte{0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0C})             // HEVC-ish Annex-B
	f.Add([]byte{0x0A, 0x02, 0x00, 0x00, 0x32, 0x01, 0x00})             // AV1-ish OBU
	f.Add([]byte{0xFF, 0x10, 0x12, 0x34, 0xFF, 0x10, 0x56})             // JPEG XS SOC frames
	f.Add([]byte{0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x21, 0xFF, 0x03}) // 3-byte start code + emulation
	f.Fuzz(func(t *testing.T, data []byte) {
		shapeNoPanic(t, "avc", NewAVCShaper().Shape(data))
		shapeNoPanic(t, "hevc", NewHEVCShaper().Shape(data))
		shapeNoPanic(t, "av1", NewAV1Shaper().Shape(data))
		shapeNoPanic(t, "jpegxs", NewJPEGXSShaper().Shape(data))
	})
}
