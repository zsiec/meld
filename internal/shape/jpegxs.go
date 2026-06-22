package shape

// JPEGXSShaper maps a JPEG XS elementary stream to generic descriptors. JPEG XS is
// INTRA-ONLY — every frame (codestream) is independently decodable — so the dependency
// model is degenerate: each codestream is its own random-access point with no
// inter-frame reference, and nothing is discardable (a dropped frame is a dropped
// picture, not a quality layer). UEP collapses to WITHIN-frame importance (codestream
// header > slices > high-frequency subbands); extracting that needs slice/precinct/
// subband parsing — a later refinement. This first cut emits one RAP unit per
// codestream, split on the SOC (start-of-codestream) marker 0xFF10.
//
// This is the faithful degenerate case of the descriptor (every AU a RAP, no chain,
// not discardable) — the cross-codec view's JPEG XS column (docs/media-awareness.md §4).
type JPEGXSShaper struct{ nextID uint32 }

// NewJPEGXSShaper returns a fresh JPEG XS shaper.
func NewJPEGXSShaper() *JPEGXSShaper { return &JPEGXSShaper{} }

// Shape splits a JPEG XS stream into codestreams (frames) on the SOC marker and emits one
// RAP unit per frame.
func (s *JPEGXSShaper) Shape(stream []byte) []Shaped {
	var out []Shaped
	for _, frame := range jpegxsFrames(stream) {
		out = append(out, Shaped{
			Unit:    Unit{ID: s.nextID, Class: ClassRAP, RAP: true, Picture: true, Confidence: Signaled, Size: len(frame)},
			Payload: frame,
		})
		s.nextID++
	}
	return out
}

// jpegxsFrames splits a buffer on SOC markers (0xFF10), each frame running from one SOC
// to the next. A buffer with no SOC is treated as a single frame (e.g. a pre-framed
// payload). SOC bytes inside the entropy-coded data are not expected (markers are
// reserved), so a stray split is rare and harmless.
func jpegxsFrames(b []byte) [][]byte {
	var starts []int
	for i := 0; i+1 < len(b); i++ {
		if b[i] == 0xFF && b[i+1] == 0x10 {
			starts = append(starts, i)
		}
	}
	if len(starts) == 0 {
		if len(b) > 0 {
			return [][]byte{b}
		}
		return nil
	}
	out := make([][]byte, 0, len(starts))
	for k, st := range starts {
		end := len(b)
		if k+1 < len(starts) {
			end = starts[k+1]
		}
		out = append(out, b[st:end])
	}
	return out
}
