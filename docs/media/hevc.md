# HEVC shaping

`shape.HEVCShaper` accepts H.265 Annex-B data and emits descriptors for retained NAL
units. It uses the two-byte NAL header plus a bounded SPS/PPS/slice-header parser; it
does not decode pixels.

## Classification

| HEVC material | Meld class and flags |
|---|---|
| VPS, SPS, PPS | parameter set |
| IRAP NAL types 16–23 | random-access point and picture |
| reference coded slice | class derived from temporal ID |
| sub-layer non-reference slice | discardable picture |
| prefix or suffix SEI | enhancement, conservatively inferred |
| AUD, end markers, filler, reserved NALs | omitted |

Temporal layer 0 reference pictures form the base class, layer 1 uses enhancement, and
higher layers are disposable. Explicit non-reference (`_N`) types are never treated as
part of the reference spine.

The shaper tracks active VPS/SPS/PPS units and makes each IRAP depend on them. When SPS,
PPS, and slice syntax are available, it derives picture order count. B-pictures are
linked to the nearest retained reference pictures on either side in display order;
other slices use the previous reference. Unsupported or truncated headers fall back to
that conservative previous-reference chain.

## Limits

- Bracketing is exact for regular hierarchical-B layouts but may approximate streams
  that select farther references explicitly.
- Long-term-reference and gradual-decoding-refresh semantics are not modeled.
- The current unit is a slice-segment NAL, so a multi-slice picture is represented by
  multiple units.

The parser follows ITU-T H.265 for parameter-set, slice-header, and POC syntax; NAL
transport terminology follows RFC 7798.
