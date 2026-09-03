# JPEG XS shaping

`shape.JPEGXSShaper` implements the intra-only case of the generic media descriptor.
Every codestream is independently decodable, so each emitted unit is a random-access
picture with no inter-frame references and no discardable flag.

## Framing

The shaper splits a byte stream at JPEG XS start-of-codestream markers (`0xFF10`). A
non-empty buffer with no marker is treated as one already-framed codestream. Empty input
produces no units.

Each frame is emitted unchanged as:

```text
Class      = RAP
RAP        = true
Picture    = true
Confidence = Signaled
RefersTo   = empty
```

## Limits

The current implementation is frame-aware, not sub-frame-aware. It does not parse
headers, slices, precincts, subbands, or refinement bitplanes, so it cannot assign
within-frame unequal protection. All bytes in a codestream receive the same RAP class.
Callers that already have authoritative frame boundaries should pass one codestream per
`Shape` call to avoid relying on marker scanning.

JPEG XS codestream terminology follows ISO/IEC 21122.
