# AV1 shaping

`shape.AV1Shaper` accepts the AV1 low-overhead bitstream format and emits descriptors
for retained OBUs. It parses OBU, sequence-header, and uncompressed-frame-header syntax;
it does not decode tiles or pixels.

## Classification

| AV1 material | Meld class and flags |
|---|---|
| sequence header, temporal delimiter | parameter set / essential framing |
| shown key frame | random-access point and picture |
| intra-only frame | temporal class and picture |
| inter or switch frame | temporal class; picture when shown |
| `show_existing_frame` | disposable picture depending on the displayed slot |
| tile group | base material linked to the current frame chain |
| metadata | enhancement, conservatively inferred |
| padding, redundant frame header, tile list | omitted |

Temporal IDs determine the normal base/enhancement/disposable class. A sequence header
is retained as the dependency of independently decodable frames.

## Reference resolution

The shaper models AV1's eight reference slots. For a successfully parsed inter frame it
maps `ref_frame_idx` entries to the units currently stored in those slots, including
short-signaling reconstruction, and applies `refresh_frame_flags` afterward.
`show_existing_frame` depends on the selected slot. Hidden frames update reference
state but are not counted as displayed pictures until a later frame shows them.

If a sequence or frame header cannot be parsed, the shaper peeks at the leading frame
type and uses the previous-reference chain. This preserves a conservative dependency
instead of declaring uncertain content disposable.

## Input and limits

- Input must use low-overhead OBUs. Each OBU must either carry a size field or occupy the
  remaining input buffer.
- Spatial IDs are parsed from the OBU extension but are not represented in the current
  generic descriptor.
- Separate frame-header and tile-group OBUs are represented as separate units linked by
  the fallback chain; the combined frame OBU is the best-resolved form.
- Malformed or overlong LEB128 lengths stop the scan without panicking.

Syntax names and reference-buffer behavior follow the AV1 Bitstream & Decoding Process
Specification.
