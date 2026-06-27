# Media awareness — the AVC (H.264) shaper

> Per-codec note. Folds into [`../media-awareness.md`](../media-awareness.md)
> (the index + the canonical generic descriptor). AVC is the baseline: simple,
> robust, deployed everywhere, and — critically — the codec with the **weakest
> built-in dependency signaling**, so this shaper sets the floor the core must
> tolerate (see the index's note on the per-field *confidence flag*).

Maps each H.264 access unit / NAL to the generic descriptor `{priority_class,
deadline, dependency, discardable}` for the sliding-window-coding core.

---

## 1. Reuse audit (`prism` and `switchframe`)

Both repos parse H.264 only as far as their jobs (TS demux, resolution stats,
captions, clip validation) require. Reuse the *plumbing*; build the importance
bits from scratch — neither extracts the two fields the shaper most needs
(`nal_ref_idc`, `slice_type`).

**Directly reusable (port, don't reinvent):**

| Capability | prism | switchframe |
|---|---|---|
| Annex-B start-code scan + NAL split | `demux/h264.go:459` `parseAnnexBGeneric`, `:517` `ParseAnnexB` | `server/codec/nalu.go:154` `splitAnnexBNALUs` |
| AVCC ⇄ Annex-B convert | `moq/format.go:12` `AnnexBToAVC1`; `moqclient/moqclient.go:140` | `server/codec/nalu.go:14/80`, `:126` `ExtractNALUs` |
| `nal_unit_type` = `b & 0x1F` + consts | `demux/h264.go:518`; consts `:9-17` | inline `nalu[0]&0x1F` (no enum) |
| **SPS parse → profile/level/res, incl. EPB strip + Exp-Golomb reader** | **`demux/h264.go:149` `ParseSPS`, `:435` `removeEmulationPrevention`, `:57-144` `bitReader`** | `clip/validator.go:439` `ParseSPSDimensions`, `:587` `bitReader` |
| IDR/keyframe (type==5) | `demux/h264.go:522` `IsKeyframe`; AU keyframe `demux/mpegts.go:304-319` | `clip/demux.go:265`, `clip/validator.go:358` |
| SEI framing (payloadType/size FF-loop) | `demux/h264.go:540` `ParsePicTimingSEI` | `server/caption/sei.go:386` `ExtractCCPairsFromSEI` |
| PTS/DTS | `mpegts/pes.go:52/90`; 90 kHz→µs `demux/mpegts.go:270`, DTS←PTS fallback `:278` | `clip/demux.go:296`; MP4 `demux_mp4.go:548` `buildDTSTable` |

The **SPS parser + EPB strip + Exp-Golomb bit reader** (`prism/demux/h264.go:57-144,149,435`)
is the single most valuable reusable asset — it's exactly what slice-header
parsing needs and which neither repo currently uses for slices.

**Gaps the shaper must build (absent in BOTH):**
- **`nal_ref_idc`** — never extracted; both mask `& 0x1F` and discard the top
  bits. *The* most useful importance bit, one mask away: `(b >> 5) & 0x03`.
- **Slice header** — `slice_type` (I/P/B), `first_mb_in_slice`, `frame_num`, POC:
  none parsed.
- **Recovery-point SEI** (payloadType 6): undetected; open-GOP RA unrepresentable.
- **AUD as boundary**: prism *drops* AUDs (`demux/mpegts.go:300`); switchframe
  ignores them. Both derive AU boundaries from the **container** (one PES = one AU).
- **Sub-keyframe importance / disposability / temporal-layer / SVC-prefix-NAL:**
  none. Importance is binary `IsKeyframe`; droppability is GOP-coarse
  (`prism distribution/session_helpers.go:13` drops *all* deltas of a damaged GOP).

Net: reuse the NAL iterator, AVCC converters, SPS parser, SEI framer, PTS/DTS
plumbing. Build a small **NAL-header decode** (`nal_ref_idc` + type) and a
**minimal slice-header decode** (`first_mb_in_slice`, `slice_type`, `frame_num`;
POC optional) on the ported bit reader.

---

## 2. Importance & dependency model

**NAL unit types (Table 7-1) the shaper classifies:**

| Type | Name | VCL? | Role |
|---|---|---|---|
| 7 / 8 | SPS / PPS | no | Decode config. Lose either → every dependent slice undecodable. Tiny, infrequent. |
| 5 | IDR coded slice | yes | Closed random-access point. Lose → whole GOP dead. |
| 1 | non-IDR coded slice | yes | I/P/B slice. Importance = `nal_ref_idc` × `slice_type`. |
| 6 | SEI | no | Metadata. **recovery_point(6)** = open-GOP RAP; pic_timing(1); captions(4); buffering_period(0). |
| 9 | AUD | no | AU boundary hint. Disposable for decode; useful as a parse marker. |
| 2-4 | partition A/B/C | yes | Data-partitioned slices (rare). A > B > C. |
| 13 | SPS extension | no | Aux-coded-picture config (rare). |
| 14 | prefix NAL | no | **Carries SVC/temporal-scalability info for the following type-1/5 NAL.** |
| 15 / 20 | subset SPS / slice ext | mixed | SVC/MVC enhancement layers. |
| 12 | filler | no | **Always discardable.** Never code as repair-worthy. |

**`nal_ref_idc` (2 bits) — the one importance bit AVC gives you for free.**
`nal_ref_idc == 0` ⇒ the slice belongs to a **non-reference picture**: losing it
degrades exactly one frame and propagates nowhere. `> 0` ⇒ reference; loss
propagates down the prediction chain until the next refresh. The spec *requires*
`!= 0` for SPS/PPS/IDR/reference slices — so a **zero NRI is a hard, reliable
"disposable" signal**. The 3 nonzero levels are encoder-assigned relative priority
(hierarchical-B encoders set deeper temporal layers lower) — a **monotone
ordering**, not absolute tiers.

**Slice types (I/P/B):** I depends only on SPS/PPS (intra). P depends on earlier
references. B depends on earlier *and* later — but a **B-slice is a propagation
hazard only if `nal_ref_idc > 0`**; a non-reference B (common) is a leaf. Useful
combined signal: **`nal_ref_idc` first, `slice_type` second**: `(NRI>0, I/P) ≫
(NRI>0, B) > (NRI==0, anything)`.

**`frame_num`/POC and propagation:**
- `frame_num` increments per *reference* frame (mod `2^…`). A jump ⇒ a reference
  was lost. `gaps_in_frame_num_value_allowed_flag` (SPS) says whether the decoder
  may synthesize missing references or must conceal — a survivability hint.
- POC orders *display*, decoupled from decode order — needed for deadlines when
  DTS≠PTS (B-frames). The shaper can lean on container DTS/PTS and skip POC in the
  baseline.
- **Propagation rule:** a lost reference (NRI>0) corrupts its whole **reference
  chain** until the next IDR/recovery point; a lost disposable (NRI==0) corrupts
  only itself. The entire justification for UEP.

**Recovery-point SEI (payloadType 6):** marks an **open-GOP** RA point — an
I-frame that is *not* an IDR but guarantees clean output `recovery_frame_cnt`
frames later. Treat a recovery-point-tagged AU like an IDR (top tier).

**SVC/MVC + prefix NAL:** Annex G SVC (subset-SPS 15, prefix 14, slice-ext 20) /
Annex H MVC layer the stream with explicit `temporal_id`/`dependency_id`/
`quality_id`. Some encoders emit a **prefix NAL (14) before each VCL NAL** purely
to carry `temporal_id` in otherwise-AVC streams — a *gift* of explicit importance;
pass it straight to the descriptor (the closest AVC gets to AV1's DD). Baseline
AVC has none of it.

**The classic failure this shaper prevents:** *lose one SPS/PPS or one IDR slice
and the entire GOP is undecodable.* These are tiny and rare, so spending
disproportionate repair (and multipath duplication) on them is nearly free — the
maximal UEP win.

---

## 3. Protection mapping

`priority_class` ascending = harder protection.

| NAL class (type + `nal_ref_idc` + `slice_type`) | priority_class | deadline | discardable / graceful |
|---|---|---|---|
| **SPS / PPS** (7,8) | **T4 (max)** + duplicate on all paths | AU PTS it configures (or +∞ if cached/out-of-band) | No. Loss = GOP dead. Tiny — protect lavishly. |
| **IDR slice** (5) + **recovery-point AU** | **T4** | AU PTS | No. Stream resync anchor. |
| **I-slice, non-IDR, NRI>0** (1) | T3 | AU PTS | No. Mid-GOP intra; still a reference. |
| **P-slice, NRI>0** (1) | T3 | AU PTS | No — propagation hazard. |
| **B-slice, NRI>0** (reference B / hierarchical anchor) (1) | T2 | AU PTS | Partially — dropping loses the sub-tree below it. |
| **Any slice, NRI==0** (non-reference; typ. leaf B) (1) | T1 | AU PTS | **Yes — naturally discardable.** Drop one frame, zero propagation. Frame-rate scaling drops here first. |
| **SEI: recovery_point(6)** | follows its AU (T4) | AU PTS | No (open-GOP anchor). |
| **SEI: captions(4)/other** | T1-T2 | AU PTS | Mostly yes; captions degrade gracefully. |
| **AUD(9) / filler(12)** | **T0** | n/a | **Yes, fully** — strip filler; AUD is a 2-byte hint. |
| **Data partition A/B/C** (2/3/4) | T3/T2/T1 | AU PTS | A no; B/C increasingly yes (lose C → lose residual detail, keep motion). |
| **SVC/MVC base** (1/5, `dependency_id==0`) | as base AVC | AU PTS | No. |
| **SVC/MVC enhancement / higher `temporal_id`** (14/15/20) | T1 per layer depth | AU PTS | **Yes — drop top layers first** for resolution/quality/frame-rate scaling. |

**Deadline:** `deadline = AU_presentation_time` (PTS) mapped to Meld's clock; all
NALs of one AU share it. For reference frames, optionally use **DTS** (earlier
than PTS for B-pyramids) since a late-but-pre-PTS reference still helps dependents.
Practical: `deadline = DTS if available else PTS` (mirrors
`prism demux/mpegts.go:278`).

---

## 4. Packetization (RFC 6184) → symbol chunking

Three NAL-to-RTP modes:
- **Single NAL** (mode 0): one NAL = one payload, header verbatim.
- **STAP-A** (type **24**): concatenate small NALs (`[16-bit size][NAL]…`).
  Non-interleaved only. `NRI = max`, `F = OR` of members. (STAP-B/MTAP add DON,
  interleaved-only.)
- **FU-A** (type **28**): split one large NAL. FU indicator `[F|NRI|28]`; FU header
  `[S|E|R|Type]` (S=first, E=last, Type = original 5-bit type). Original NAL header
  reconstructed from indicator+header. (FU-B adds DON.)
- **Interleaved** (mode 2) decouples transmission from decode order via DON;
  needed only when reordering on the wire. **Low-latency contribution uses
  non-interleaved (mode 1)** — no DON, no reorder buffer.

**Implications (Meld is not RTP — the NAL-alignment wisdom transfers):**
1. **Symbolize on NAL/slice boundaries**, never mid-NAL across unrelated NALs.
2. **Bundle tiny config + anchor NALs into one high-priority symbol group**
   (STAP-A insight): SPS+PPS+recovery/SEI+AUD into one **T4** set — the cheapest,
   highest-leverage protection in the codec.
3. **FU-A-style fragmentation for large slices:** keep all fragments of one NAL at
   the same `priority_class` (all-or-nothing). Meld's k-of-n **replaces** FU-A's
   "lose one fragment = lose the NAL" cliff with graceful recovery.
4. **Don't cross-bundle priority tiers in one symbol** (mixing a T4 SPS with a T1
   disposable B over-protects the B or under-protects the SPS). Aggregate *within*
   a tier — the discipline STAP-A's max-NRI rule hints at, Meld enforces.
5. **Preserve decode-order metadata out-of-band** (the DON job lifted into the
   descriptor): the window is reorder-tolerant, so no DON in symbols, but tag each
   symbol's AU/decode position so deadline eviction + dependency tracking work.

---

## 5. AVC-specific gotchas

- **Emulation-prevention bytes (EPB).** Strip `0x000003` before parsing slice
  headers/SEI (reuse the audited stripper) but **code the bytes verbatim** (EPB
  intact) — the decoder expects them. Strip for *inspection*, never *transmission*.
- **`first_mb_in_slice` & multi-slice pictures.** `first_mb_in_slice == 0` marks a
  new picture → use it (with type / `frame_num` / POC change) to detect AU
  boundaries from the bitstream rather than trusting container framing. Default
  H.264 slices are independently decodable (no FMO/ASO), so you *can* tier slices
  within a picture — but only if you confirm no inter-slice dependency.
- **Filler (12) / SEI padding** can be large and pure waste — never spend repair on
  them; drop pre-coding.
- **SEI source shedding is constrained-only.** By default, preserve SEI because it
  can carry captions, HDR, buffering, or recovery-point metadata. When the encoder
  or benchmark explicitly models a constrained source, drop only SEI that is
  positively identified as non-recovery metadata; retain recovery_point(6) and
  retain malformed/ambiguous SEI fail-safe.
- **`nal_ref_idc` is necessary but not sufficient.** A `NRI>0` B (hierarchical-B
  anchor) *is* a reference — dropping it kills its sub-tree. Always read NRI; never
  infer disposability from `slice_type` alone.
- **`gaps_in_frame_num`.** If the flag is 0, a lost reference is *fatal* (no
  synthesis); if 1, the decoder fakes it (degraded but alive). Bump protection on
  reference NALs when the SPS forbids gaps.
- **Long-term reference pictures (MMCO).** A single frame can be referenced far
  beyond the normal window (e.g. a static background). Its propagation horizon is
  huge — promote it. Detecting it requires parsing `dec_ref_pic_marking()` in the
  slice header (future-protection hook).
- **IDR vs recovery-point ambiguity.** A non-IDR I-frame is *not* a clean RA point
  unless a recovery-point SEI says so. Parse type 5 **and** recovery_point SEI —
  treating every I-slice as an anchor over-protects; only-IDR misses open-GOP.
- **Redundant coded pictures** (built-in AVC resilience): a lower-fidelity
  duplicate of a primary picture. Keep at a modest tier; never drop both primary
  and redundant.
- **No bitstream AU boundary in legacy muxers.** Both repos lean on container
  PES/PTS and drop AUDs; for raw Annex-B without reliable AUDs, reconstruct AU
  boundaries from `first_mb_in_slice==0` + `frame_num`/POC/type transitions.

---

## 6. Cross-codec note (why the generic descriptor matters most here)

AVC's *entire* built-in dependency vocabulary is **`nal_ref_idc`** (1 disposable
bit + 2 ordering bits) **and the IDR flag** — plus whatever the shaper *infers*
(slice_type, frame_num gaps, POC, MMCO long-term refs, recovery-point SEI). Some
of it (true propagation horizon under LTRPs, exact temporal-layer depth without
prefix NALs) is **not reliably recoverable** from a baseline stream at all. This is
the worst case in Meld's codec lineup.

HEVC improves (`TemporalId` explicit per NAL; `_N`/`_R` separates sub-layer
reference from non-reference). AV1 goes furthest (the Dependency Descriptor gives
the exact prediction graph).

**Design implication:** the core must operate on the **generic descriptor only**
and never branch on codec — an AV1-DD-shaped descriptor as the union target. AV1
fills it directly (lossless); HEVC near-losslessly; **AVC fills it from
`nal_ref_idc`/IDR/recovery-point/prefix-NAL and *inference*, marking inferred
fields low-confidence.** That **confidence flag** is load-bearing: where AV1/HEVC
give a precise graph, AVC gives an estimate, and Meld's policy should be
correspondingly conservative (protect a little harder, drop a little more
reluctantly) when the descriptor is inferred. Unify on the DD-shaped descriptor →
the core's UEP/eviction/degradation logic is written **once**; every codec's shaper
is a fill-in-the-descriptor adapter, AVC's being the most parsing-heavy and least
certain — precisely because AVC tells you the least. (Canonical descriptor: see
[`../media-awareness.md`](../media-awareness.md).)

---

**Sources:** RFC 6184 (H.264 RTP payload); H.264 `nal_ref_idc` / recovery-point
semantics (ITU-T H.264). Reuse audit: direct reads of `~/dev/prism` and
`~/dev/switchframe` (citations inline in §1).
