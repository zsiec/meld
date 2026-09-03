# AVC shaping

`shape.AVCShaper` accepts H.264 Annex-B data and emits one `shape.Shaped` value for
each retained NAL unit. It parses only the syntax needed for protection and dependency
classification; it does not decode pixels.

## Classification

| AVC material | Meld class and flags |
|---|---|
| SPS, SPS extension, subset SPS, PPS | parameter set |
| IDR slice | random-access point and picture |
| reference non-IDR slice (`nal_ref_idc != 0`) | base picture |
| non-reference slice | disposable picture |
| SEI | enhancement, conservatively inferred |
| AUD, filler, end markers, reserved NALs | omitted |

The shaper tracks the active SPS and PPS and makes each IDR depend on them. It parses
Exp-Golomb-coded SPS and slice-header fields needed for `slice_type` and picture order
count. For `pic_order_cnt_type == 0`, a B-picture is linked to its nearest retained
reference pictures on either side in display order. Other pictures use the previous
reference. If parsing is incomplete or unsupported, the same previous-reference chain
is the safe fallback.

Recovery-point SEI starts a countdown. Reference slices within that interval are marked
as recovery refresh; the completing slice becomes a random-access point and depends on
the active parameter sets and accumulated refresh references.

## Optional source filtering

`AVCOptions.SourceConstrained` enables source-side filtering:

- non-recovery SEI may be omitted;
- when `DropDisposablePictures` is also set, non-reference pictures may be omitted.

Malformed or ambiguous SEI is retained. With default options, the shaper does not apply
either filter.

## Limits

- The detailed picture-order path supports POC type 0. Other modes use the conservative
  chain fallback.
- Long-term-reference and memory-management operations are not modeled.
- The current unit is a slice NAL, so a multi-slice picture is represented by multiple
  units.

The parser follows ITU-T H.264 for SPS, slice-header, POC, and recovery-point syntax;
Annex-B/NAL transport terminology follows RFC 6184.
