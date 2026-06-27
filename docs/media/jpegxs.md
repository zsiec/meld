# Media awareness — the JPEG XS shaper

> Per-codec note. Folds into [`../media-awareness.md`](../media-awareness.md).
> JPEG XS is **fundamentally different** from the temporal codecs: it is an
> **intra-only, visually-lossless, ultra-low-latency mezzanine/contribution
> codec** (ISO/IEC 21122) over ST 2110-22, with sub-frame (a few video lines)
> end-to-end latency. There is **no inter-frame dependency** — so the protection
> model is **intra-frame spatial/quality unequal protection**, not temporal
> reference protection. This is the most novel shaper, and a degenerate case of
> the generic descriptor (one decode target, every AU a RAP, `chain_prev` always
> null).

---

## 1. Reuse audit — nothing to reuse; design from spec

Neither sibling repo parses JPEG XS codestreams:
- **prism** — zero JPEG XS references (H.264/H.265/AAC over TS/MoQ).
- **switchframe** — one reference, and it's only a boolean: `Compressed bool //
  ST 2110-22 (JPEG XS) when true` at `server/st2110/types.go:21`. ST 2110-22
  transport is delegated to Intel's MTL behind a build tag; **no** codestream,
  marker, precinct, slice, or subband logic anywhere.

**Conclusion:** the shaper is designed from ISO/IEC 21122-1 (codestream) + RFC
9134 (RTP packetization). Reuse only switchframe's safe-payload sizing discipline
(one RTP/UDP-safe MTU's worth of media per wire unit) as the symbol-MTU guardrail.

---

## 2. Survivability-relevant structure

JPEG XS is intra-only; all importance is *spatial and frequency* importance within
a single picture. Outermost to innermost:

```
Codestream  = Header segment (SOC + capabilities + picture/component/weights markers)
              ├─ Slice 0   (full image width, ~16 image lines tall)
              │    ├─ Precinct 0  (one spatial region: ALL subbands touching it)
              │    │     ├─ LF packet  (low-frequency subbands)   → significance / bitplane-count / data
              │    │     └─ HF packet  (high-frequency subbands)  → significance / bitplane-count / data
              │    ├─ Precinct 1 ...
              │    └─ Precinct N
              ├─ Slice 1 ...
              └─ EOC marker  (in the last slice's packetization unit)
```

- **Markers / header segment** (SOC, capabilities `CAP`, picture header `PIH`,
  component table `CDT`, weights table `WGT`). Defines geometry, decomposition
  depth, and the **quantization/gain weights**. **If lost, the entire picture is
  undecodable.** The single most catastrophic loss class — *not* gracefully
  degradable.
- **Precincts** — all wavelet coefficients across all subbands for one spatial
  region (group of consecutive lines). **Independently decodable** (per-precinct,
  per-packet resync headers). Losing one loses one bounded spatial region.
- **Slices** — an integer number of precincts, full image width, **~16 image
  lines**. A slice = **4/8/16 precincts** for **2/1/0 vertical decomposition
  levels**. Vertical MSB-prediction across slice boundaries is *prohibited*, so
  **slices are fully independently reconstructable and confine errors to one
  slice's worth of lines** — JPEG XS's native concealment granularity and the
  natural unit for Meld's coding alignment.
- **Subbands & the LL/HF hierarchy (the key insight).** A separable DWT (typically
  5/3 LeGall), ~5 horizontal × 1–2 vertical levels. Each level splits into:
  - **LL** — low-pass both directions: the **coarse approximation** (at the
    deepest level, a thumbnail of the region).
  - **LH / HL / HH** — high-pass: progressively finer edge/detail/texture.

  > **Losing the LL / low-frequency band destroys the region** (no coarse image to
  > refine). **Losing LH/HL/HH high-frequency detail only softens/blurs it** — the
  > picture survives, degraded but coherent.

  Within each precinct: **significance → bitplane-count → data** (MSB down).
  Significance + MSB bitplanes dominate; trailing refinement bitplanes contribute
  least — a *second* degradation axis below subband level.
- **Progression order** controls the LF/HF + bitplane byte layout within a
  precinct. The shaper must read it from the header to label sub-precinct ranges;
  if it can only see slice/precinct boundaries (slice-mode RTP), it falls back to
  per-precinct uniform + per-*slice* unequal protection.

---

## 3. Latency model — the buffer is a handful of lines, so ARQ is dead

End-to-end latency is **sub-frame — lines, not frames**: DWT algorithmic latency
alone is **3 lines (1 vertical level) or 9 lines (2 levels)**; total encode→decode
targets are commonly **≤ 32 lines**. At 1080p60 a line is ~14.8 µs, so 32 lines ≈
**0.5 ms for the whole pipeline**. Consequences for Meld's core:

- **Deadlines are extremely tight and near-uniform within a frame.** All symbols
  of a picture share essentially one decode-by epoch (the 90 kHz RTP timestamp +
  line budget), unlike a temporal codec where an I-frame's deadline is far looser
  than a trailing B's. Micro-stagger by slice-arrival epoch (top slices decode &
  display first in raster order).
- **The coding window is tiny** — bounded by *lines in flight*, not frames. The
  eviction horizon is roughly one-to-a-few slices (≈16–48 lines). Past-deadline
  symbols must be evicted immediately — there is no "catch it next frame."
- **ARQ is essentially non-viable.** A single RTT of feedback blows a 0.5 ms line
  budget. Meld runs **near-pure proactive coding (sliding-window FEC) for JPEG
  XS**, not NACK/retransmit. ARQ is admissible *only* as an out-of-band rescue for
  the header-segment class (§4), where a re-request that arrives "late" still beats
  a destroyed frame, and only when the app tolerates an occasional one-frame stall.
- **Repair must be intra-frame and intra-epoch.** Generate coded symbols **across
  slices/precincts/subbands that share the same arrival epoch**, so any *k-of-n*
  recovers within the line budget. Mix repair across *spatial* units of equal
  class (e.g. the LL packets of several precincts coded together), never across
  time. Because slices are independently decodable, a slice-aligned coding block
  is released the instant *its* symbols are recovered.

---

## 4. Protection mapping (the ordering is the load-bearing part)

| JPEG XS unit | priority_class | deadline | discardable? / graceful degradation |
|---|---|---|---|
| **Codestream header segment** (SOC, CAP, PIH, CDT, WGT + slice/precinct resync headers) | **T0 — highest** | frame epoch (tightest); earliest bytes, must arrive first | **No. Never discardable.** Loss = whole frame undecodable. Max repair; **duplicate across all paths**; the only class where a late ARQ rescue is considered. |
| **LL / deepest-level low-frequency** of each precinct (significance + MSB bitplanes) | **T1** | frame epoch (per-slice micro-stagger) | **No.** Loss = region destroyed. Strong repair; spread across paths. |
| **Mid-level low-frequency** (LH/HL of shallower-but-still-low levels; significance maps) | **T2** | frame epoch | Soft-discardable under congestion only. Loss = structural blur. Moderate repair. |
| **High-frequency detail** (HL/LH/HH of the finest levels; trailing refinement bitplanes) | **T3 — lowest** | frame epoch | **Yes — the designed sacrifice.** Under-protect first; **drop these first** when budget/link is tight. Loss = graceful softening, picture stays coherent. Little/no repair; single path OK. |
| **EOC marker** | T0-adjacent | end of frame | Cheap; piggyback on the last slice's protected block (signals completion / resync). |

Two orthogonal degradation axes:
1. **Subband/level axis** — drop highest-frequency subbands first (T3 → T2). The
   primary graceful-degradation knob; maps onto JPEG XS quality scalability.
2. **Bitplane axis** — within a precinct, drop trailing refinement bitplanes before
   MSB/significance bytes. Finer-grained sacrifice when whole-subband dropping is
   too coarse.

Both reduce the **discardable** sub-stream first and **never** touch T0/T1 →
degradation is monotonic and visually graceful (softer image), never catastrophic
(dead region / dead frame).

---

## 5. Packetization (RFC 9134 + ST 2110-22) → symbol chunking

RFC 9134 packetizes each JPEG XS **frame as one ADU**; an ADU is a sequence of
**packetization units**. A unit may span multiple RTP packets, all the **same size
except the last** (MTU-driven). RTP clock **90 kHz**; all packets of a frame share
one timestamp; the **M bit** marks the last packet of the frame/field.

Two packetization modes — the choice dictates whether UEP is even possible without
decoding:
- **Slice mode (K=1)** — the packetization unit is **one slice** (the first unit
  carries the header segment; the last carries EOC). **The mode Meld wants** —
  slice boundaries are visible at the RTP layer, so the shaper aligns symbols to
  slices *and* their vertical position **without decoding the codestream**, and
  exploits slice-level independent decodability directly.
- **Codestream mode (K=0)** — the unit is the **entire picture segment**; slice/
  precinct structure is invisible at the RTP boundary. Subband-level UEP then
  requires **codestream introspection** (walk markers, find slice/precinct/LF-HF
  offsets via progression order). Falls back to whole-frame uniform protection.

The 4-byte RTP payload header the shaper reads: **T** (transmission order), **K**
(mode — picks the strategy), **L** (last packet of the unit → a coding-block
boundary), **I** (interlace), **F** (frame counter mod 32 → the ADU/epoch group),
**SEP** (slice index in slice mode → the slice identifier & vertical-position key),
**P** (packet counter within the unit).

**Symbolization:** chunk the ADU into coded symbols on **packetization-unit (slice)
boundaries**, keyed by `(F, SEP)`. Code *within* a class across same-epoch units
(LL-of-many-precincts together; HF together or bare). The header-segment unit is
its own T0 coding block, duplicated across paths. RFC 9134 already guarantees
equal-size payloads within a unit and slice independence → symbols are naturally
uniform-sized and independently releasable, a clean fit for *k-of-n*. **ST 2110-22**
carries this as CBR-compressed video essence, so symbol cadence is steady and
predictable — the proactive-FEC scheduler exploits that.

---

## 6. JPEG XS-specific gotchas

1. **The core tension: ultra-low latency vs. strong protection.** No time for ARQ
   or big windows, yet contribution demands quality. Resolve by spending the budget
   *unequally and proactively*: pour repair into T0/T1 (headers + LL), starve T3
   (HF). You cannot protect everything within a line budget — protect the
   unrecoverable parts, let discardable HF detail absorb loss as graceful blur.
2. **Header loss is an absolute cliff, not a slope.** Unlike temporal codecs (a
   lost reference degrades *future* frames), a lost header kills *this* frame with
   no concealment. T0 must be over-provisioned and path-duplicated even though it's
   a tiny fraction of the bytes — asymmetric repair cost is the whole point.
3. **UEP requires understanding the codestream layout.** Subband-level protection
   is impossible from packet boundaries alone in **codestream mode** — the shaper
   must walk markers and know the **progression order** (encoder-configurable;
   read it, don't assume). Prefer/negotiate **slice mode** so slice boundaries are
   free; treat full introspection as the heavier fallback.
4. **Tiny window ⇒ aggressive, correct eviction.** The window is lines, not frames.
   A symbol whose slice deadline passed is dead weight that *must* leave the window
   immediately, or it starves repair capacity for live slices. Eviction-by-deadline
   is load-bearing, not an optimization.
5. **Raster-order display couples deadline to vertical position.** Top slices
   display before bottom; the *effective* deadline rises down the image.
   Micro-stagger per-slice deadlines by `SEP` rather than treating the frame as one
   instant — else top-of-frame repair arrives uselessly late while bottom-of-frame
   is over-protected.
6. **Don't cross-code across classes or across time.** Mixing a T0 header symbol
   with T3 refinement bytes drags the header's recovery probability down to the
   block's worst member; coding across frame epochs violates the latency budget.
   Coding blocks must be **single-class and single-epoch**.
7. **Equal-size payload guarantee is a gift — keep it.** RFC 9134's equal-size
   constraint within a unit means uniform symbols without padding games; preserve
   slice/unit alignment so you don't re-fragment and lose that property.
8. **Interlaced = two ADUs/epochs per frame.** With `I`=field1/field2 the picture
   is two passes with two deadlines; never merge fields into one coding epoch.

> **Implementation status:** the current shaper emits one RAP unit per codestream.
> Header/slice/subband-aware JPEG XS UEP is still a refinement, and the shipped coding
> engine remains the project-wide systematic RLNC path described in [`../coding.md`](../coding.md).

---

**Sources:** RFC 9134 (RTP payload for JPEG XS); ISO/IEC 21122-1 (JPEG XS
codestream); SMPTE ST 2110-22. Reuse audit: `~/dev/prism`, `~/dev/switchframe`
(only `server/st2110/types.go:21`).
