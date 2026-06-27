# Media awareness

How Meld protects the **picture**, not just the bytes. Meld is built for **AV1,
HEVC, AVC, and JPEG XS**. A thin, per-codec **shaper** sits *above* the
media-blind, sans-I/O core and maps each codec access unit / packetization unit to
**one generic descriptor**; the core does unequal error protection (UEP),
deadline-based eviction, and graceful degradation over that descriptor alone — it
never sees an OBU/NAL/codestream.

This file is the **index + the canonical descriptor + the cross-codec view**. The
per-codec design notes:

- [AV1](media/av1.md) — richest structure; the model's source (OBUs, temporal
  units, the 8-slot reference buffer, the **Dependency Descriptor** with chains).
- [HEVC](media/hevc.md) — `TemporalId` + `_N`/`_R` SLNR + IRAP/RASL open-GOP.
- [AVC](media/avc.md) — the baseline; weakest signaling (`nal_ref_idc` + IDR).
- [JPEG XS](media/jpegxs.md) — the outlier: intra-only, spatial/quality UEP,
  sub-frame latency (ARQ is dead).

Code-family fit (which erasure code protects which class):
[coding.md](coding.md).

---

## 1. The shaper contract

The shaper takes a codec AU / packetization unit and emits the generic descriptor.
It **never** touches the wire, clock, or a socket (`now`/PTS enter as explicit
arguments) — same discipline as the core. The four fields the core consumes:

- **`priority_class`** — small integer tier; higher = protect harder (more repair
  budget, and possibly duplicate/spread across paths).
- **`deadline`** — when the unit must be decodable; past it the core **evicts** its
  symbols from the coding window (no more repair spent on it).
- **`dependency`** — what references what, so loss propagation is computable
  parse-free.
- **`discardable`** — droppable with only graceful quality loss.

All codec intelligence lives in the shaper; the core stays codec-blind.

---

## 2. The canonical generic descriptor (DD-shaped)

All four codec deep-dives independently converged on the same recommendation:
**adopt one descriptor at the waist, modeled on the AV1 Dependency Descriptor
(DD)**, and make it the *only* dependency/importance information the core consumes.
The DD was designed for the identical problem — let a Selective Forwarding
middlebox **forward/drop without parsing the codec**. Meld replaces "forward/drop"
with "how much repair + which paths + evict when," but the input information is the
same: dependencies, importance tiers, decode targets, chains.

```
MeldUnitDescriptor {
  // --- identity / ordering ---
  unit_id            u32   // monotonic dependency key (DD frame_number widened to 32b — never wrap mid-window)
  start_of_au, end_of_au  bool  // AU framing for symbol packing (DD start/end_of_frame)

  // --- importance / UEP ---
  priority_class     u8    // 0 = most disposable .. K = protect hardest
  discardable        bool  // no surviving unit references this (DD "Discardable" on every active target)
  is_switch          bool  // soft-RAP for ≥1 decode target (DD "Switch" / AV1 SWITCH_FRAME / HEVC TSA-ish)
  is_rap             bool  // hard random-access point (AV1 KEY_FRAME shown / HEVC IDR,CRA / AVC IDR)

  // --- deadline ---
  decode_deadline    ts    // must be decodable by this (DTS); past max(deadline) the core EVICTS its symbols
  display_deadline   ts    // when shown (>= decode_deadline; matters for show_existing_frame / B-pyramid)

  // --- dependency (the DD model, generalized) ---
  refers_to          []u32 // unit_ids this unit decodes from (deltas resolved to absolute ids in the shaper)
  decode_targets     u16   // bitmask: which decode targets this unit belongs to
  dti[target]        enum{NotPresent,Discardable,Switch,Required}  // per-target importance (DD DTI, 2b)
  chain[target]   -> chain_id   // which chain protects each target
  chain_prev[chain]  u32        // previous unit_id in this chain → O(1), parse-free "did this loss break a target I protect?"

  // --- scalability hint (path/budget policy) ---
  temporal_id        u8    // enhancement-layer depth (drop high T first → fewer fps)
  spatial_id         u8    // resolution layer (drop high S after high T)

  // --- provenance (Meld addition; see §3) ---
  confidence         enum{Signaled,Inferred}  // was this descriptor read from the bitstream, or guessed?
}
```

**Why this is the right altitude.** Chains are the loss-impact oracle the coded
core wants: on a `unit_id` gap, the receiver chases each active chain's
`chain_prev` pointer and learns *immediately, without decoding*, whether a missing
unit broke a target it is protecting — exactly how the controller decides whether
to keep spending repair on a window or let it go. Templates keep it cheap (send the
structure on the RAP; steady-state descriptors are a handful of bytes). Meld
carries the descriptor in its own coded framing — it need not be an RTP extension
on the wire — but the *information model* is the DD's.

---

## 3. How each codec fills it (and the confidence flag)

The descriptor is the **union target**; each shaper is a fill-in adapter. They
differ sharply in how much they can fill *from the bitstream* vs. *by inference* —
which is why Meld adds a per-descriptor **`confidence`** field (the AVC deep-dive's
key refinement). Where a field is `Inferred`, the protection policy is
**conservative**: protect a little harder, drop a little more reluctantly.

| Codec | Fills from | Confidence | Notes |
|---|---|---|---|
| **AV1** | the real DD (or synthesized from OBU frame headers on TS/raw ingest) | **Signaled** | Near-1:1: full chains, multi-target DTI, switch frames, T/S layers. |
| **HEVC** | `nuh_temporal_id_plus1` (→`temporal_id`), IRAP types (→`is_rap`), TSA/STSA (→`is_switch`), `_N`/`_R` SLNR (→`discardable`) | **Signaled** (near-lossless) | Chains degenerate to "temporal sublayer up to TID." LTRP/GDR force conservative fallback. |
| **AVC** | `nal_ref_idc` (→`discardable`), IDR/recovery-point (→`is_rap`), prefix-NAL `temporal_id` when present; else **inferred** from slice-header heuristics | **mostly Inferred** | The worst case — true propagation horizon (LTRPs) and temporal depth often unrecoverable from a baseline stream. |
| **JPEG XS** | intra-only → every AU a RAP, **one** decode target, `chain_prev` always null, `discardable=false` | **Signaled** (degenerate) | UEP collapses to **intra-frame** slice/precinct/subband importance: low-frequency (LL) protected over high. |

### As built — the shapers resolve dependencies exactly (`internal/shape/`)

The shipped shapers parse the headers directly (no `prism` dependency) and resolve
references more exactly than the first-cut "Fills from" column promised:

- **AVC / HEVC** parse the SPS/PPS/slice headers for `slice_type` + the picture order
  count (§8.2.1 / §8.3.1) and resolve each B-picture to its **two bracketing anchors**
  (nearest reference below and above it in display order) — exact for the regular
  hierarchical-B structure, falling back to the previous reference otherwise. So AVC's
  bidirectional-B propagation is no longer "mostly Inferred" for the common case; it is
  signaled-exact.
- **AV1** parses the sequence + uncompressed frame headers and tracks the **eight-slot
  reference buffer**, resolving `ref_frame_idx` (with `set_frame_refs` for short
  signaling, §7.8), `show_existing_frame`, and the hidden-vs-displayed-frame distinction —
  the real DD-equivalent reference model, not a frame_type peek.
- **JPEG XS** is intra-exact by construction.
- A `Picture` flag separates displayed frames from parameter sets / SEI / metadata (so
  the decodable-frame metric counts pictures, not headers).

Proven glass-to-glass: a real decoder (ffprobe — libx264 / libx265 / libdav1d) decodes
**exactly** the model's predicted decodable picture set across protection modes and seeds,
and `FuzzShapers` proves no malformed bitstream panics a shaper.

### Media-aware early eviction (`Config.EvictBrokenFrames`)

The receiver does not just *score* decodability — under `Config.EvictBrokenFrames`
(`internal/flow`, off by default) it **acts** on the model, the "evict when" of §1 applied
to dependency rather than deadline. The moment a frame is known undecodable — one of its
own source ids was lost, or a reference's whole sub-tree is dead — the receiver **abandons
the frame's dependent sub-tree immediately** instead of waiting out each id's deadline. Two
wins follow:

- **Faster resync.** The delivery cursor advances past the dead GOP to the next
  independently-decodable frame (the next keyframe), so the picture resyncs sooner instead
  of stalling on symbols that can never complete a frame.
- **Reclaimed repair budget.** Because the sender retires every generation below the
  receiver's reported cursor (`DecodedLowEdge`), the cursor advance *is* the implicit
  "stop repairing this GOP" signal — reactive repair budget is freed for live generations
  with no extra wire field.

It trades byte-completeness (a doomed-but-recoverable id is dropped) for
**picture-completeness**: every *decodable* frame is still delivered whole, in order, never
late — which is why it is a media-only option, gated by frame descriptors
(`Sender.WriteFrame`) and a no-op for plain byte streams.

---

## 4. Cross-codec view

| | AV1 | HEVC | AVC | JPEG XS |
|---|---|---|---|---|
| **Dependency model** | temporal (rich: DD + chains) | temporal (TID + SLNR) | temporal (weak: NRI + IDR) | **none** (intra-only) |
| **Random-access point** | KEY / SWITCH frame | IDR / CRA / BLA | IDR / recovery-point | every frame |
| **Free "discardable" signal** | DTI=Discardable | even `_N` NAL type | `nal_ref_idc==0` | high-freq subbands |
| **Primary degradation axis** | drop high temporal, then spatial | drop high temporal sublayer | drop NRI==0, then temporal | drop high-frequency subbands / refinement bitplanes |
| **Session-fatal unit** | Sequence Header OBU (+ sticky HDR) | VPS/SPS/PPS | SPS/PPS | codestream header segment |
| **ARQ viable?** | yes (frame buffer) | yes | yes | **no** (~0.5 ms line budget) |
| **Coding window** | frames | frames | frames | **lines / slices** (tiny) |
| **Symbol alignment** | OBU / tile-group | NAL / slice | NAL / slice | slice / precinct / subband |

### The unified principle (works for all four)
1. **Protect the unrecoverable cheaply.** The session-fatal unit (parameter sets /
   sequence header / codestream header) is tiny and catastrophic on loss — pin it
   to the top tier, **duplicate it across paths**, and repeat it on every RAP. The
   cheapest high-leverage bytes in any stream.
2. **Protect the RAP.** It's the resync anchor; breaking it poisons everything
   until the next one.
3. **Spend the remaining budget down the dependency tree;** let the leaves
   (`discardable` / high temporal / high spatial / high frequency) degrade
   gracefully — that's the picture surviving, not failing.
4. **Evict by deadline.** A symbol past `max(decode,display)_deadline` leaves the
   window so it stops starving repair for live units. Load-bearing for JPEG XS
   (lines), useful for everyone.
5. **Fail safe on parse error / low confidence.** Unknown unit → base-tier
   reference, **not** discardable (over-protect), never fail-discardable.

### Drop order under budget pressure (most disposable → never)
non-HDR metadata / redundant headers / filler → highest temporal layer → next
temporal layer → highest spatial layer → (JPEG XS: high-frequency subbands →
refinement bitplanes) → … → **never drop** base layer, switch/recovery points,
RAPs, parameter sets / sequence headers, or sticky HDR metadata.

---

## 5. Implications for the core and the build

- **The core's UEP / eviction / degradation logic is written once** against
  `MeldUnitDescriptor`; every codec is a fill-in-the-descriptor shaper.
- **`confidence` gates aggressiveness:** signaled streams (AV1/HEVC) let the
  controller drop disposable units confidently; inferred streams (baseline AVC)
  get conservative protection.
- **Code family is class-driven, not codec-driven** — the current shipped code uses one
  systematic RLNC engine. Earlier RaptorQ/block-fountain exploration is closed in
  [coding.md](coding.md); revisit only if a future target needs a distinct block engine.
- **The sliding media path uses descriptors now**: the shapers parse their own
  bitstream headers (AV1/HEVC/AVC slice/OBU parsers + first-cut JPEG XS codestream
  framing, no `prism` dependency), resolve dependencies for the supported descriptors,
  and are scored on **decodable-keyframe %** glass-to-glass against ffprobe. The
  default sliding path uses descriptors for UEP, recovery-refresh metadata, and
  parse-free frame stats. Frame-atomic delivery remains generation-only.

---

## Provenance

Synthesized from four codec deep-dives (read-only audits of `~/dev/prism` and
`~/dev/switchframe`; spec/RFC research). Per-codec citations live in each note. The
generic descriptor derives from the AV1 RTP **Dependency Descriptor** (templates,
DTI, chains, decode targets); the `confidence` field is a Meld addition motivated
by AVC's weak signaling.
