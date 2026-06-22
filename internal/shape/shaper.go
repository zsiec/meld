package shape

// Shaped pairs a unit descriptor with the access-unit bytes the shaper classified, so
// the caller chunks Payload into coded symbols and writes each with Unit.Class.Wire() as
// the priority (meld.Sender.WriteUnit). The descriptor alone (Unit) drives the core's
// unequal protection and the decodability oracle; Payload is the media to send.
type Shaped struct {
	Unit    Unit
	Payload []byte // the NAL bytes including the NAL header; aliases the shaper input
}

// Shaper maps a codec elementary stream to generic descriptors ABOVE the waist — the
// only media-aware code in Meld. Each codec is a fill-in adapter (AVCShaper, HEVCShaper,
// …) reading bitstream headers only — no entropy/slice decode — so the sans-I/O core
// stays codec-blind, acting solely on the resulting Priority/Deadline/dependency. It is
// stateful across calls (tracks the active parameter sets and the reference chain for
// the dependency model) but reads no clock and does no I/O.
type Shaper interface {
	// Shape parses an Annex-B buffer (one or more NAL units) and returns the protectable
	// units in stream order.
	Shape(annexB []byte) []Shaped
}

// nalUnits splits an Annex-B byte stream into NAL units, returning each NAL's bytes
// WITHOUT the leading start code (0x000001 or 0x00000001), aliasing b. Only the NAL
// header is consulted by the shapers, so any trailing-zero ambiguity at a NAL's tail is
// harmless. A buffer with no start code yields nothing.
func nalUnits(b []byte) [][]byte {
	var out [][]byte
	start := nextStartCode(b, 0)
	for start >= 0 {
		nalStart := start + 3 // past the 3-byte 0x000001 (the 4-byte form's extra 0x00
		// is left on the previous NAL's tail, which never matters for header reads)
		next := nextStartCode(b, nalStart)
		end := len(b)
		if next >= 0 {
			end = next
			for end > nalStart && b[end-1] == 0 { // trim the next start code's leading zeros
				end--
			}
		}
		if end > nalStart {
			out = append(out, b[nalStart:end])
		}
		start = next
	}
	return out
}

// nextStartCode returns the index of the next 0x000001 at or after off, or -1.
func nextStartCode(b []byte, off int) int {
	for i := off; i+2 < len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			return i
		}
	}
	return -1
}
