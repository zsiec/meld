package flow

import (
	"bytes"
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

func FuzzExpandRepairPayloadNoPanic(f *testing.F) {
	f.Add(uint8(1), uint8(16), uint32(5), append([]byte{1, 2, 3, 4, 5}, make([]byte, codedSymbolMetadataBytes)...))
	f.Add(uint8(0), uint8(8), uint32(0), make([]byte, 8+codedSymbolMetadataBytes))
	f.Add(uint8(1), uint8(0), uint32(0), make([]byte, codedSymbolMetadataBytes))

	f.Fuzz(func(t *testing.T, mode, width uint8, sourceLength uint32, payload []byte) {
		symbolSize := int(width)
		sym := wire.Symbol{
			Kind:            wire.Repair,
			HasSourceLength: mode&1 != 0,
			SourceLength:    sourceLength,
			Payload:         payload,
		}
		expanded, ok := expandRepairPayload(sym, symbolSize)
		if !ok {
			return
		}
		if len(expanded) != codedSymbolSize(symbolSize) {
			t.Fatalf("accepted width %d expanded to %d bytes", symbolSize, len(expanded))
		}
		if !sym.HasSourceLength {
			if !bytes.Equal(expanded, payload) {
				t.Fatal("full-width repair changed during validation")
			}
			return
		}
		prefix := int(sourceLength)
		if prefix > symbolSize || len(payload) != prefix+codedSymbolMetadataBytes {
			t.Fatal("accepted malformed compact repair")
		}
		if !bytes.Equal(expanded[:prefix], payload[:prefix]) ||
			!bytes.Equal(expanded[symbolSize:], payload[prefix:]) {
			t.Fatal("compact repair prefix or metadata changed during expansion")
		}
		for _, b := range expanded[prefix:symbolSize] {
			if b != 0 {
				t.Fatal("compact repair expanded with a nonzero omitted byte")
			}
		}
	})
}
