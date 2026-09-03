# Media awareness

Meld keeps codec parsing above the transport core. A shaper turns codec-specific
bitstream units into a small generic descriptor; the core uses only that descriptor for
unequal protection, dependency tracking, frame statistics, and optional early eviction.

The implementation is in `internal/shape`:

- [AVC](media/avc.md)
- [HEVC](media/hevc.md)
- [AV1](media/av1.md)
- [JPEG XS](media/jpegxs.md)

## Descriptor

`shape.Unit` is the in-process descriptor produced by a shaper:

| Field | Meaning |
|---|---|
| `ID` | Monotonic access-unit identity. |
| `Class` | Protection tier, from disposable through parameter-set material. |
| `RAP` | Random-access point. |
| `RecoveryRefresh` | Reference material participating in signaled gradual refresh. |
| `Discardable` | No surviving unit is expected to depend on this unit. |
| `Picture` | Displayed coded picture rather than metadata or parameter material. |
| `TemporalID` | Temporal-layer depth used for prioritization and shedding. |
| `RefersTo` | Absolute IDs of units required to decode this unit. |
| `Confidence` | Whether classification was signaled or conservatively inferred. |
| `Size` | Source-unit size in bytes. |

The wire-facing `meld.FrameDesc` carries the subset required by the receiver:
priority, frame identity, references, chunk count, temporal ID, and the RAP,
recovery-refresh, discardable, non-picture, and long-term-reference flags. Deadlines are
assigned by the transport when chunks are written; they are not parsed from the codec.

## Protection classes

Classes are ordered from easiest to sacrifice to hardest:

1. disposable material;
2. enhancement layers;
3. base references;
4. random-access points;
5. parameter sets and essential framing.

The sender tightens the decode-failure target for higher classes. Under a rate limit,
repair for lower classes is dropped first. Source data itself is not silently discarded
unless an explicit source-shedding policy is enabled.

## Dependency behavior

The receiver can determine whether a frame is decodable without parsing its payload. A
frame is decodable only when all of its chunks and the transitive closure of its
references are available. `FrameStats` reports this picture-level result.

`Config.EvictBrokenFrames` may advance past a frame and its dependent subtree once they
are known to be unusable, allowing the next random-access point to resume delivery.
`Config.FrameAtomic` provides all-or-nothing frame delivery in generation mode. Both
options require descriptors supplied through `WriteFrame`.

## Failure policy

Parsers are bounded and must not panic on malformed input. When a shaper cannot resolve
syntax precisely, it falls back to a conservative reference chain and avoids labeling
uncertain material as disposable. This favors excess protection over accidental loss of
the dependency spine.

The model is intentionally smaller than a general codec dependency descriptor. It does
not carry decode-target templates, spatial-layer masks, or codec syntax into the
transport core.
