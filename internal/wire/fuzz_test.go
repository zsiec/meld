package wire

import (
	"bytes"
	"testing"
)

// FuzzDecodeNoPanic asserts the decoders never panic on arbitrary input and that
// anything they accept survives a semantic round-trip: decode(encode(decode(b)))
// equals decode(b). (Re-encoding is not byte-identical to b because reserved
// header bytes are intentionally ignored on decode for forward compatibility, so
// the property is over the decoded values, not the raw bytes.)
func FuzzDecodeNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{typeSystematic})
	f.Add(EncodeSymbol(nil, Symbol{Flow: 1, Kind: Repair, N: 4, Payload: []byte("abc")}))
	f.Add(EncodeSymbol(nil, Symbol{Flow: 1, Kind: SparseRepair, SparseIDs: []uint32{3, 8, 13}, Payload: []byte("abc")}))
	f.Add(EncodeFeedback(nil, Feedback{Flow: 9, Deficit: 2}))
	f.Add(EncodeMTUProbe(nil, 0xdeadbeef, 1400))
	f.Add(EncodeMTUProbeAck(nil, 0xdeadbeef, 1400))

	f.Fuzz(func(t *testing.T, b []byte) {
		typ, terr := PeekType(b)
		if n1, sz1, err := DecodeMTUProbe(b); err == nil {
			if terr != nil || !IsMTUProbe(typ) {
				t.Fatalf("DecodeMTUProbe ok but PeekType says %d/%v", typ, terr)
			}
			n2, sz2, err := DecodeMTUProbe(EncodeMTUProbe(nil, n1, sz1))
			if err != nil || n1 != n2 || sz1 != sz2 {
				t.Fatalf("probe round-trip unstable: err=%v (%d,%d)!=(%d,%d)", err, n1, sz1, n2, sz2)
			}
		}
		if n1, sz1, err := DecodeMTUProbeAck(b); err == nil {
			if terr != nil || !IsMTUProbeAck(typ) {
				t.Fatalf("DecodeMTUProbeAck ok but PeekType says %d/%v", typ, terr)
			}
			n2, sz2, err := DecodeMTUProbeAck(EncodeMTUProbeAck(nil, n1, sz1))
			if err != nil || n1 != n2 || sz1 != sz2 {
				t.Fatalf("probe-ack round-trip unstable: err=%v", err)
			}
		}
		if s1, err := DecodeSymbol(b); err == nil {
			if terr != nil || !IsSymbol(typ) {
				t.Fatalf("DecodeSymbol ok but PeekType says %d/%v", typ, terr)
			}
			s2, err := DecodeSymbol(EncodeSymbol(nil, s1))
			if err != nil || !symbolEqual(s1, s2) {
				t.Fatalf("symbol round-trip unstable: err=%v\n %+v\n %+v", err, s1, s2)
			}
		}
		if f1, err := DecodeFeedback(b); err == nil {
			if terr != nil || !IsFeedback(typ) {
				t.Fatalf("DecodeFeedback ok but PeekType says %d/%v", typ, terr)
			}
			f2, err := DecodeFeedback(EncodeFeedback(nil, f1))
			if err != nil || !feedbackEqual(f1, f2) {
				t.Fatalf("feedback round-trip unstable: err=%v", err)
			}
		}
	})
}

func symbolEqual(a, b Symbol) bool {
	return a.Flow == b.Flow && a.Epoch == b.Epoch && a.PathID == b.PathID && a.Kind == b.Kind && a.WindowBase == b.WindowBase &&
		a.SrcIndex == b.SrcIndex && a.N == b.N && a.RepairKey == b.RepairKey &&
		u32eq(a.SparseIDs, b.SparseIDs) &&
		a.Priority == b.Priority && a.Deadline == b.Deadline && a.SendTimestamp == b.SendTimestamp &&
		a.HasFrameDesc == b.HasFrameDesc && a.FrameLen == b.FrameLen && a.FrameStart == b.FrameStart && u32eq(a.FrameRefs, b.FrameRefs) &&
		a.FrameRAP == b.FrameRAP && a.FrameRecoveryRefresh == b.FrameRecoveryRefresh &&
		a.FrameDiscardable == b.FrameDiscardable && a.FrameNonPicture == b.FrameNonPicture &&
		bytes.Equal(a.Payload, b.Payload)
}
