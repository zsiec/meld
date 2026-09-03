package wire

import (
	"bytes"
	"testing"
)

func u32eq(a, b []uint32) bool {
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

func TestSymbolRoundTrip(t *testing.T) {
	cases := []Symbol{
		{
			Flow: 1, Kind: Systematic, WindowBase: 100, SrcIndex: 105, Priority: 4,
			Deadline: 1 << 40, HasSourceLength: true, SourceLength: 7, Payload: []byte("hello systematic"),
		},
		{
			Flow: 0xDEADBEEF, Epoch: 0x1234, PathID: 1, Kind: Repair, WindowBase: 1000, SrcIndex: 7, N: 32,
			RepairKey: 0xBEEF, Priority: 0, Deadline: -1, Payload: bytes.Repeat([]byte{0xAB}, 1316),
		},
		{
			Flow: 2, Kind: Repair, WindowBase: 9, SrcIndex: 4, N: 8, RepairKey: 17,
			HasSourceLength: true, SourceLength: 23, Payload: bytes.Repeat([]byte{0xCD}, 35),
		},
		{Flow: 0, Kind: Systematic, Payload: nil},
		{
			Flow: 7, PathID: 255, Kind: Systematic, SrcIndex: 3, SendTimestamp: 1_700_000_000_000_000,
			Deadline: 1 << 41, Payload: []byte("with send ts on a path"),
		},
		{
			Flow: 9, Kind: Systematic, SrcIndex: 50, Priority: 4, HasFrameDesc: true,
			FrameStart: 12, FrameLen: 7, FrameRefs: []uint32{8, 20}, FrameRAP: true, FrameRecoveryRefresh: true, Payload: []byte("with frame desc"),
		},
		{
			Flow: 9, Kind: Systematic, SrcIndex: 51, HasFrameDesc: true, FrameStart: 13, FrameLen: 1,
			FrameDiscardable: true, FrameNonPicture: true, SendTimestamp: 42, Payload: []byte("desc + ts"),
		},
		{
			Flow: 9, Kind: Systematic, SrcIndex: 52, Priority: 2, HasFrameDesc: true, FrameStart: 14,
			FrameLen: 3, FrameRefs: []uint32{2}, FrameLTR: true, Payload: []byte("ltr candidate"),
		},
		{
			Flow: 10, Kind: SparseRepair, SrcIndex: 99, N: 4, RepairKey: 123, Priority: 3, Deadline: 55,
			SendTimestamp: 44, SparseIDs: []uint32{12, 21, 24, 33}, Payload: []byte("sparse repair"),
		},
		{
			Flow: 11, Kind: UnitRepair, WindowBase: 77, SrcIndex: 77, N: 1, Priority: 2,
			Deadline: 99, SendTimestamp: 66, HasSourceLength: true, SourceLength: 5, Payload: []byte("short"),
		},
	}
	for i, want := range cases {
		enc := EncodeSymbol(nil, want)
		typ, err := PeekType(enc)
		if err != nil || !IsSymbol(typ) {
			t.Fatalf("case %d: PeekType=%d err=%v", i, typ, err)
		}
		got, err := DecodeSymbol(enc)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if got.Flow != want.Flow || got.Epoch != want.Epoch || got.PathID != want.PathID || got.Kind != want.Kind ||
			got.WindowBase != want.WindowBase || got.SrcIndex != want.SrcIndex || got.N != want.N ||
			got.RepairKey != want.RepairKey || !u32eq(got.SparseIDs, want.SparseIDs) ||
			got.Priority != want.Priority || got.Deadline != want.Deadline || got.SendTimestamp != want.SendTimestamp ||
			got.HasSourceLength != want.HasSourceLength || got.SourceLength != want.SourceLength ||
			got.HasFrameDesc != want.HasFrameDesc || got.FrameLen != want.FrameLen || got.FrameStart != want.FrameStart ||
			!u32eq(got.FrameRefs, want.FrameRefs) || got.FrameRAP != want.FrameRAP ||
			got.FrameRecoveryRefresh != want.FrameRecoveryRefresh ||
			got.FrameDiscardable != want.FrameDiscardable || got.FrameNonPicture != want.FrameNonPicture ||
			got.FrameLTR != want.FrameLTR {
			t.Fatalf("case %d: header mismatch: got %+v want %+v", i, got, want)
		}
		// The leading byte carries the current version in its high nibble.
		if enc[0]>>4 != Version {
			t.Fatalf("case %d: leading byte %#x missing version nibble %d", i, enc[0], Version)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("case %d: payload mismatch", i)
		}
		// Byte-stable re-encode.
		if !bytes.Equal(EncodeSymbol(nil, got), enc) {
			t.Fatalf("case %d: re-encode not byte-stable", i)
		}
	}
}

// feedbackEqual compares two Feedback values including the variable per-path slices.
func feedbackEqual(a, b Feedback) bool {
	if a.Flow != b.Flow || a.Epoch != b.Epoch || a.DecodedLowEdge != b.DecodedLowEdge ||
		a.HighestSeen != b.HighestSeen || a.Deficit != b.Deficit || a.EcnCE != b.EcnCE ||
		a.LossRate != b.LossRate || a.Deficits != b.Deficits ||
		a.CongestionLoss != b.CongestionLoss || a.Burstiness != b.Burstiness ||
		a.Frames != b.Frames || a.DecodableFrames != b.DecodableFrames ||
		a.Keyframes != b.Keyframes || a.DecodableKeyframes != b.DecodableKeyframes ||
		a.NewestDecodableLTR != b.NewestDecodableLTR || a.BrokenAnchors != b.BrokenAnchors ||
		a.DeadPaths != b.DeadPaths || a.Missing != b.Missing ||
		a.SettledLost != b.SettledLost || a.OutageRun != b.OutageRun {
		return false
	}
	return u16eq(a.PathLoss, b.PathLoss) && u16eq(a.SlotDist, b.SlotDist)
}

func u16eq(a, b []uint16) bool {
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

func TestFeedbackRoundTrip(t *testing.T) {
	want := Feedback{
		Flow: 42, Epoch: 9, DecodedLowEdge: 1000, HighestSeen: 1050, Deficit: 7, EcnCE: 3,
		LossRate: 19661, Deficits: [MaxFeedbackGens]uint8{7, 0, 3, 0, 1, 0, 0, 255},
		CongestionLoss: 1234, Burstiness: 512,
		PathLoss: []uint16{26214, 6553, 3276}, SlotDist: []uint16{52000, 9000, 3000, 535},
		Frames: 120, DecodableFrames: 117, Keyframes: 4, DecodableKeyframes: 3,
		NewestDecodableLTR: 887, BrokenAnchors: 6, DeadPaths: 0b10, Missing: 0xDEAD_BEEF_0000_0101,
		SettledLost: 321, OutageRun: 73,
	}
	enc := EncodeFeedback(nil, want)
	typ, err := PeekType(enc)
	if err != nil || !IsFeedback(typ) {
		t.Fatalf("PeekType=%d err=%v", typ, err)
	}
	got, err := DecodeFeedback(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !feedbackEqual(got, want) {
		t.Fatalf("feedback mismatch: got %+v want %+v", got, want)
	}
	if !bytes.Equal(EncodeFeedback(nil, got), enc) {
		t.Fatal("re-encode not byte-stable")
	}
	single := want
	single.PathLoss, single.SlotDist = nil, nil
	singleGot, err := DecodeFeedback(EncodeFeedback(nil, single))
	if err != nil || !feedbackEqual(singleGot, single) {
		t.Fatalf("single-path media feedback: got %+v err=%v", singleGot, err)
	}
	for n := 0; n < len(enc); n++ {
		if _, err := DecodeFeedback(enc[:n]); err != ErrShort {
			t.Fatalf("truncated length %d: want ErrShort, got %v", n, err)
		}
	}
	if _, err := DecodeFeedback(append(append([]byte(nil), enc...), 0)); err != ErrInvalid {
		t.Fatalf("trailing byte: want ErrInvalid, got %v", err)
	}
}

func TestDecodeShortAndBadType(t *testing.T) {
	if _, err := DecodeSymbol([]byte{typeSystematic, 1, 2}); err != ErrShort {
		t.Fatalf("want ErrShort, got %v", err)
	}
	if _, err := DecodeFeedback([]byte{typeFeedback}); err != ErrShort {
		t.Fatalf("want ErrShort, got %v", err)
	}
	// Correct version, unknown type nibble → ErrType.
	bad := make([]byte, symbolHeader)
	bad[0] = lead(0x0F)
	if _, err := DecodeSymbol(bad); err != ErrType {
		t.Fatalf("want ErrType, got %v", err)
	}
	if _, err := PeekType(nil); err != ErrShort {
		t.Fatalf("want ErrShort, got %v", err)
	}
}

func TestSparseRepairRejectsOversizedIDList(t *testing.T) {
	ids := make([]uint32, sparseMaxIDs+1)
	for i := range ids {
		ids[i] = uint32(i)
	}
	enc := EncodeSymbol(nil, Symbol{Flow: 1, Kind: SparseRepair, SparseIDs: ids, Payload: []byte("x")})
	if _, err := DecodeSymbol(enc); err != ErrInvalid {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestSourceLengthExtensionTruncation(t *testing.T) {
	enc := EncodeSymbol(nil, Symbol{Kind: Systematic, HasSourceLength: true, SourceLength: 7})
	for n := symbolHeader; n < symbolHeader+sourceLenLen; n++ {
		if _, err := DecodeSymbol(enc[:n]); err != ErrShort {
			t.Fatalf("length %d: want ErrShort, got %v", n, err)
		}
	}
}

func TestSymbolRejectsUnknownFlags(t *testing.T) {
	enc := EncodeSymbol(nil, Symbol{Kind: Systematic, Payload: []byte("x")})
	enc[29] = 0x80
	if _, err := DecodeSymbol(enc); err != ErrInvalid {
		t.Fatalf("unknown symbol flag: want ErrInvalid, got %v", err)
	}
}

func TestFrameDescriptorRejectsUnknownFlagsAndExcessReferences(t *testing.T) {
	enc := EncodeSymbol(nil, Symbol{Kind: Systematic, HasFrameDesc: true})
	badFlags := append([]byte(nil), enc...)
	badFlags[symbolHeader+6] = 0x80
	if _, err := DecodeSymbol(badFlags); err != ErrInvalid {
		t.Fatalf("unknown descriptor flag: want ErrInvalid, got %v", err)
	}

	tooMany := append([]byte(nil), enc...)
	tooMany[symbolHeader+7] = descMaxRefs + 1
	tooMany = append(tooMany, make([]byte, 4*(descMaxRefs+1))...)
	if _, err := DecodeSymbol(tooMany); err != ErrInvalid {
		t.Fatalf("excess descriptor references: want ErrInvalid, got %v", err)
	}
}

// TestVersionMismatch: a datagram stamped with a version this build does not know
// is rejected with ErrVersion (never decoded as silent garbage) — the guard that
// prevents a different format from being parsed under the version-1 schema.
func TestVersionMismatch(t *testing.T) {
	sym := EncodeSymbol(nil, Symbol{Flow: 1, Kind: Systematic, Payload: []byte("x")})
	fb := EncodeFeedback(nil, Feedback{Flow: 1})
	future := uint8((Version + 1) << 4)
	for _, tc := range []struct {
		name string
		b    []byte
	}{{"symbol", sym}, {"feedback", fb}} {
		b := append([]byte(nil), tc.b...)
		b[0] = future | (b[0] & typeMask) // bump the version nibble, keep the type
		if _, err := PeekType(b); err != ErrVersion {
			t.Fatalf("%s PeekType: want ErrVersion, got %v", tc.name, err)
		}
		if _, err := DecodeSymbol(b); tc.name == "symbol" && err != ErrVersion {
			t.Fatalf("symbol decode: want ErrVersion, got %v", err)
		}
		if _, err := DecodeFeedback(b); tc.name == "feedback" && err != ErrVersion {
			t.Fatalf("feedback decode: want ErrVersion, got %v", err)
		}
	}
}

func TestAppendPreservesPrefix(t *testing.T) {
	prefix := []byte("PFX")
	enc := EncodeSymbol(append([]byte(nil), prefix...), Symbol{Flow: 1, Payload: []byte("x")})
	if !bytes.HasPrefix(enc, prefix) {
		t.Fatal("EncodeSymbol did not preserve dst prefix")
	}
}

// TestClockSyncRoundTrip: the clock-offset probe/echo encode, decode, and demux
// cleanly, and carry the version nibble like every other message.
func TestClockSyncRoundTrip(t *testing.T) {
	p := ClockProbe{T0: 1234567890}
	pe := EncodeClockProbe(nil, p)
	if typ, err := PeekType(pe); err != nil || !IsClockProbe(typ) {
		t.Fatalf("probe PeekType=%d err=%v", typ, err)
	}
	if got, err := DecodeClockProbe(pe); err != nil || got != p {
		t.Fatalf("probe round-trip: got %+v err=%v", got, err)
	}
	e := ClockEcho{T0: 1234567890, T1: 1234567999, T2: 1234568050}
	ee := EncodeClockEcho(nil, e)
	if typ, err := PeekType(ee); err != nil || !IsClockEcho(typ) {
		t.Fatalf("echo PeekType=%d err=%v", typ, err)
	}
	if got, err := DecodeClockEcho(ee); err != nil || got != e {
		t.Fatalf("echo round-trip: got %+v err=%v", got, err)
	}
	// Cross-type decodes are refused by type, not silently misparsed (use an
	// echo-sized buffer carrying the probe tag so the length check passes first).
	wrongType := make([]byte, clockEchoLen)
	wrongType[0] = lead(typeClockProbe)
	if _, err := DecodeClockEcho(wrongType); err != ErrType {
		t.Fatalf("echo-decode of a probe tag: want ErrType, got %v", err)
	}
	if _, err := DecodeClockProbe(ee[:clockProbeLen]); err != ErrType {
		t.Fatalf("probe-decode of an echo: want ErrType, got %v", err)
	}
}
