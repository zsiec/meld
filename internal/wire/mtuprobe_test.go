package wire

import "testing"

// TestMTUProbeRoundTrip checks the probe/ack codecs: a probe is padded to exactly the
// requested size, decodes to the size that arrived, and the ack round-trips.
func TestMTUProbeRoundTrip(t *testing.T) {
	for _, size := range []int{mtuProbeHdr, 1200, 1400, 1500, 9000} {
		d := EncodeMTUProbe(nil, 0x01020304, size)
		if len(d) != size {
			t.Fatalf("probe size: encoded %d bytes, want %d", len(d), size)
		}
		n, sz, err := DecodeMTUProbe(d)
		if err != nil || n != 0x01020304 || sz != size {
			t.Fatalf("probe decode: n=%#x sz=%d err=%v", n, sz, err)
		}
		ack := EncodeMTUProbeAck(nil, n, uint16(sz))
		an, asz, err := DecodeMTUProbeAck(ack)
		if err != nil || an != n || int(asz) != size {
			t.Fatalf("ack decode: n=%#x sz=%d err=%v", an, asz, err)
		}
	}
	// A probe smaller than the header is clamped up, not corrupted.
	if d := EncodeMTUProbe(nil, 1, 2); len(d) != mtuProbeHdr {
		t.Fatalf("undersized probe not clamped: %d", len(d))
	}
	// Short buffers error, never panic.
	if _, _, err := DecodeMTUProbe([]byte{lead(typeMTUProbe)}); err == nil {
		t.Fatalf("short probe should error")
	}
	if _, _, err := DecodeMTUProbeAck([]byte{lead(typeMTUProbeAck), 1, 2}); err == nil {
		t.Fatalf("short ack should error")
	}
}
