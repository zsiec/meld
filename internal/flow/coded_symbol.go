package flow

import (
	"encoding/binary"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

const codedSymbolMetadataBytes = 12 // original length (uint32) + exact deadline (int64)

func codedSymbolSize(symbolSize int) int { return symbolSize + codedSymbolMetadataBytes }

func makeCodedSource(data []byte, symbolSize int, deadline clock.Timestamp) []byte {
	b := make([]byte, codedSymbolSize(symbolSize))
	n := len(data)
	if n > symbolSize {
		n = symbolSize
	}
	copy(b, data[:n])
	binary.BigEndian.PutUint32(b[symbolSize:], uint32(n))
	binary.BigEndian.PutUint64(b[symbolSize+4:], uint64(deadline))
	return b
}

func addCodedSource(enc *code.Encoder, data []byte, symbolSize int, deadline clock.Timestamp) uint32 {
	n := len(data)
	if n > symbolSize {
		n = symbolSize
	}
	var metadata [codedSymbolMetadataBytes]byte
	binary.BigEndian.PutUint32(metadata[:4], uint32(n))
	binary.BigEndian.PutUint64(metadata[4:], uint64(deadline))
	return enc.AddWithSuffix(data[:n], symbolSize, metadata[:])
}

func medianCadence(samples *[9]int64, n int) int64 {
	if n <= 0 {
		return 0
	}
	if n > len(samples) {
		n = len(samples)
	}
	values := *samples
	for i := 1; i < n; i++ {
		v := values[i]
		j := i
		for j > 0 && values[j-1] > v {
			values[j] = values[j-1]
			j--
		}
		values[j] = v
	}
	return values[n/2]
}

func parseCodedSource(data []byte, symbolSize int) (payload []byte, n int, deadline clock.Timestamp, ok bool) {
	if len(data) != codedSymbolSize(symbolSize) {
		return nil, 0, 0, false
	}
	n64 := binary.BigEndian.Uint32(data[symbolSize:])
	if uint64(n64) > uint64(symbolSize) {
		return nil, 0, 0, false
	}
	dl := clock.Timestamp(int64(binary.BigEndian.Uint64(data[symbolSize+4:])))
	return data[:int(n64)], int(n64), dl, true
}

// systematicSourceLength validates the two unambiguous source forms: an exact
// payload carrying SourceLength, or a full-width payload whose length is already
// implied by the configured symbol size.
func systematicSourceLength(sym wire.Symbol, symbolSize int) (int, bool) {
	if !sym.HasSourceLength {
		if len(sym.Payload) != symbolSize {
			return 0, false
		}
		return symbolSize, true
	}
	if sym.SourceLength > uint32(symbolSize) {
		return 0, false
	}
	n := int(sym.SourceLength)
	if len(sym.Payload) != n {
		return 0, false
	}
	return n, true
}

// compactRepairPayload removes only the guaranteed-zero tail of a coded
// equation's application region. The protected length/deadline suffix is kept
// verbatim at the end of the compact payload. The receiver restores the omitted
// zeros before the equation enters GF arithmetic, so coefficients and rank are
// unchanged. The extension costs four bytes; require a net wire saving.
func compactRepairPayload(data []byte, symbolSize int) (payload []byte, prefix int, compact bool) {
	if len(data) != codedSymbolSize(symbolSize) {
		return data, 0, false
	}
	prefix = symbolSize
	for prefix > 0 && data[prefix-1] == 0 {
		prefix--
	}
	if symbolSize-prefix <= 4 {
		return data, symbolSize, false
	}
	payload = make([]byte, prefix+codedSymbolMetadataBytes)
	copy(payload, data[:prefix])
	copy(payload[prefix:], data[symbolSize:])
	return payload, prefix, true
}

// expandRepairPayload validates and reconstructs a repair equation. With no
// compact-prefix extension, the payload must already have the full algebraic
// width. With the extension, SourceLength names the transmitted application
// prefix and the final 12 bytes remain the coded metadata suffix.
func expandRepairPayload(sym wire.Symbol, symbolSize int) ([]byte, bool) {
	return expandRepairPayloadInto(sym, symbolSize, nil)
}

// expandRepairPayloadInto is expandRepairPayload with reusable compact-repair
// storage. Full-width payloads continue to alias the decoded datagram; compact
// payloads use dst when it has sufficient capacity.
func expandRepairPayloadInto(sym wire.Symbol, symbolSize int, dst []byte) ([]byte, bool) {
	if !sym.HasSourceLength {
		return sym.Payload, len(sym.Payload) == codedSymbolSize(symbolSize)
	}
	if sym.SourceLength > uint32(symbolSize) {
		return nil, false
	}
	prefix := int(sym.SourceLength)
	if len(sym.Payload) != prefix+codedSymbolMetadataBytes {
		return nil, false
	}
	fullSize := codedSymbolSize(symbolSize)
	var full []byte
	if cap(dst) >= fullSize {
		full = dst[:fullSize]
		clear(full)
	} else {
		full = make([]byte, fullSize)
	}
	copy(full, sym.Payload[:prefix])
	copy(full[symbolSize:], sym.Payload[prefix:])
	return full, true
}

// encodeSymbol applies zero-tail repair serialization while
// returning the full-width charge used by the recovery controller. Keeping the
// charge invariant prevents a representation saving from changing which
// equations the adaptive policy emits; only the actual datagram gets smaller.
func encodeSymbol(sym wire.Symbol, symbolSize int) (datagram []byte, repairCharge int) {
	if sym.Kind != wire.Repair && sym.Kind != wire.SparseRepair {
		datagram = wire.EncodeSymbol(nil, sym)
		if sym.Kind == wire.UnitRepair {
			return datagram, len(datagram)
		}
		return datagram, 0
	}
	full := wire.EncodeSymbol(nil, sym)
	repairCharge = len(full)
	payload, prefix, ok := compactRepairPayload(sym.Payload, symbolSize)
	if !ok {
		return full, repairCharge
	}
	sym.HasSourceLength = true
	sym.SourceLength = uint32(prefix)
	sym.Payload = payload
	return wire.EncodeSymbol(nil, sym), repairCharge
}
