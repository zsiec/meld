# Media awareness — the HEVC (H.265) shaper

> Per-codec note. Folds into [`../media-awareness.md`](../media-awareness.md)
> (the index + the canonical generic descriptor). The shaper is a thin,
> codec-specific mapper that runs **above** the media-agnostic sans-I/O core. It
> consumes HEVC access units (AUs) — already de-payloadized — and emits, per
> packetization unit, the generic descriptor `{priority_class, deadline,
> dependency, discardable}`. It never touches a clock, socket, or goroutine;
> `now`/PTS enter as explicit arguments, matching the RIST-core discipline.

HEVC's survivability is a lattice: **parameter sets** (session-fatal) → **IRAP
anchors** (GOP-fatal) → **temporal sublayers** (frame-rate, gracefully sheddable)
→ **leading pictures** (cosmetic). The shaper reads each NAL's two-byte header
(+ light SPS state) and places it on that lattice.

---

## 1. Reuse audit (`prism` and `switchframe`)

Both repos parse HEVC at the byte/NAL level (real decode is FFmpeg in
switchframe). Reusable Go surface is small but solid; everything
dependency/priority-relevant is **net-new**.

**Directly reusable (copy or import the pattern):**

| Capability | prism | switchframe |
|---|---|---|
| Annex B split (3/4-byte start codes) | `parseAnnexBGeneric` `demux/h264.go:459`; HEVC `ParseAnnexBHEVC` `demux/h265.go:46` | `splitAnnexBNALUs` `server/codec/nalu.go:154` |
| Length-prefixed (hvcC) NAL walk | emits only (`moq/format.go:12`) | `ExtractNALUs` `server/codec/nalu.go:126` (the better base) |
| `nal_unit_type` = `(b0>>1)&0x3F` | `HEVCNALType` `demux/h265.go:24` | `HEVCNALUType` `server/codec/nalu_hevc.go:25` |
| NAL-type constants | partial `demux/h265.go:9-20` | fuller `server/codec/nalu_hevc.go:4-20` |
| IRAP/keyframe test (16..21) | `IsHEVCKeyframe` `demux/h265.go:30` | `IsHEVCKeyframe` `server/codec/nalu_hevc.go:31` |
| VCL test (type≤31) | — | `IsHEVCVCL` `server/codec/nalu_hevc.go:36` |
| VPS/SPS/PPS detect/extract | `IsHEVCVPS/SPS/PPS` `demux/h265.go:35-41` | `ExtractHEVCParamSets` `server/codec/nalu_hevc.go:139` |
| **HEVC SPS field parse** (PTL, conformance-window W/H, chroma, bit-depth) | **`ParseHEVCSPS` `demux/h265.go:101`** (the only real one in either tree) | — |
| EPB strip + Exp-Golomb bit reader | `removeEmulationPrevention` `demux/h264.go:435`, `readBits/readUE/readSE` `:57-126` | **absent** (gap noted `caption/sei.go:386`) |
| Per-AU PTS/DTS | `media.VideoFrame.PTS/DTS` `media/frame.go:18-19` (90 kHz→µs `demux/mpegts.go:271-280`) | `BufferedFrame.pts` `clip/types.go:122` (PTS only; **no DTS**) |
| GOP labeling on IRAP | `GroupID++` `demux/mpegts.go:393-396` | keyframe-walk `clip/validator.go:220-233` |

**Net-new (absent in both — shaper-owned):**

1. **`nuh_temporal_id_plus1`** — `TID = (data[1] & 0x07) - 1`. Neither repo reads
   NAL header byte 1; the single most important missing field for UEP.
2. **`_N`/`_R` SLNR discardability** — the even/odd rule (`type≤15 && type%2==0` ⇒
   sub-layer non-reference). No code keys off it.
3. **IDR-vs-CRA/BLA + RADL/RASL leading-picture handling** — `IsHEVCKeyframe`
   collapses all IRAP into one class; open-GOP semantics absent.
4. **Full NAL-type enum** — add TRAIL/TSA/STSA/RADL/RASL (types 2-9).
5. **POC / slice-header / reference graph** — none anywhere.
6. **RFC 7798 FU/AP/PACI de-payloadization** — absent (only SRT/TS + MoQ/RIST
   transports). Needed only for RTP ingest.
7. **NAL-level AU boundary** (`first_slice_segment_in_pic_flag`) — both rely on
   PES boundaries.

> Inherited bug to avoid: prism parses HEVC caption SEI with the H.264 helper
> (`demux/mpegts.go:377`), an off-by-one for HEVC's 2-byte header — use
> `ExtractCaptionsHEVC` / strip 2 bytes.

**Recommendation:** base the NAL layer on switchframe's exported `package codec`
(`ExtractNALUs`, `HEVCNALUType`, `IsHEVCVCL`, `ExtractHEVCParamSets`); port
prism's `ParseHEVCSPS` + bit reader for the one-time SPS decode. Everything in
"net-new" is shaper-owned.

---

## 2. Importance & dependency model

### NAL header (the whole per-NAL input)
2 bytes, big-endian: `forbidden_zero_bit(1) | nal_unit_type(6) | nuh_layer_id(6) |
nuh_temporal_id_plus1(3)`.
- `nal_unit_type = (b0>>1) & 0x3F`
- `nuh_layer_id = ((b0&1)<<5) | (b1>>3)` — `0` single-layer; non-zero ⇒
  scalable/multiview (a separate, lower-base-priority dependency chain).
- `TemporalId = (b1 & 0x07) - 1` — `plus1` is never 0; `TemporalId==0` is base.

### NAL classes and loss propagation

- **Parameter sets — VPS(32)/SPS(33)/PPS(34).** Activation-scoped. Loss ⇒ every
  dependent picture until the next in-band repeat is undecodable.
  **Session-critical; never discardable; longest effective deadline** (tiny;
  resent at every IRAP). Aggregate VPS+SPS+PPS into one repair group (§4).
- **IRAP family (VCL 16-23) — the random-access anchors:**
  - **IDR_W_RADL(19)/IDR_N_LP(20):** closed GOP. Resets POC, flushes DPB; nothing
    after it references before it. Hard recovery point; highest VCL priority.
  - **CRA(21):** **open GOP.** Decodable standalone, but its associated **RASL**
    leaders reference pre-CRA pictures that don't exist on a clean tune-in
    (`NoRaslOutputFlag`). CRA frame data is anchor-critical; its RASL leaders are
    disposable.
  - **BLA_W_LP(16)/BLA_W_RADL(17)/BLA_N_LP(18):** "broken link access" — a spliced
    CRA. Same anchor priority; RASL after a BLA are *required* to be discarded, so
    protecting them is pure overhead.
  - Anchor loss = **GOP-fatal** → top tier, candidate for **path duplication**.
- **Leading pictures (follow IRAP in decode, precede in output):**
  - **RADL_N(6)/RADL_R(7)** — decodable after a clean random access. Mid priority;
    RADL_N is also SLNR (discardable).
  - **RASL_N(8)/RASL_R(9)** — reference pre-IRAP pictures; on clean tune-in to a
    CRA/BLA they are **undecodable and discarded by spec**. **Always
    discardable** — never trigger repair or path duplication. The cleanest free
    win in HEVC UEP.
- **Trailing (TRAIL_N(0)/TRAIL_R(1)):** the bulk B/P pictures; priority from the
  two axes below.
- **Temporal sublayer access — TSA(2/3)/STSA(4/5):** up-switch points for adaptive
  frame-rate. Dropping a whole sublayer is artifact-free **only** from a TSA/STSA
  boundary.

### The two graceful-degradation axes
- **(A) SLNR — `_N`/`_R` even/odd (VCL 0-9):** even `nal_unit_type` is
  **sub-layer non-reference** — no later same-sublayer picture references it.
  Loss corrupts exactly one displayed frame, never propagates → **discardable**.
  Odd (`_R`) propagates within the sublayer until the next `_R`/anchor.
- **(B) Temporal layering — `TemporalId`:** sublayers are strictly nested; a
  `TID==T` picture is referenced only by `TID≥T`. The highest sublayer is always
  a leaf → **shed highest `TemporalId` first** (clean 60→30→15 fps), only at
  TSA/STSA boundaries for artifact-free up-switch.

Sacrifice order (most disposable → least): `RASL_* → highest-TID _N →
highest-TID _R → … → base _N → base _R → leading-to-IRAP → IRAP → parameter sets`.

### Open-GOP hazard
At a CRA, a receiver that joins/recovers **must suppress RASL output**
(`NoRaslOutputFlag=1`) — those leaders reference frames it never had. So the
shaper must (1) never spend repair on RASL and (2) mark RASL evictable the instant
the window is pressured. RADL after the same CRA *are* decodable — do not lump
RADL with RASL.

---

## 3. Protection mapping

`priority_class`: higher = protect harder. Proposed 5-tier scale (0 lowest).
**Deadline:** anchor each AU at its **DTS** (decode-by), not PTS:
`deadline = AU.DTS + jitter_slack`. If only PTS is available, approximate
`DTS ≈ PTS − max_reorder_delay` from SPS `sps_max_num_reorder_pics`. Relax the
deadline for higher sublayers (`deadline_relax = k·TemporalId`). Parameter sets
inherit the deadline of the earliest AU they activate.

| HEVC NAL class | type | priority_class | deadline | discardable / graceful |
|---|---|---|---|---|
| VPS / SPS / PPS | 32/33/34 | **4 (top)** + multipath dup | earliest activating AU's DTS | No. Session-fatal. Aggregate into one symbol group. |
| IDR (W_RADL/N_LP) | 19/20 | **4** + dup | AU DTS + slack | No. Closed-GOP anchor. |
| CRA | 21 | **4** + dup | AU DTS + slack | No (frame data). Open-GOP anchor. |
| BLA (W_LP/W_RADL/N_LP) | 16/17/18 | **4** + dup | AU DTS + slack | No (frame data). Splice anchor. |
| Base ref trailing (`TID==0`,`_R`) | TRAIL_R(1) | **3** | AU DTS + slack | No. Propagates through GOP. |
| Base non-ref (`TID==0`,`_N`) | TRAIL_N(0) | **2** | AU DTS + slack | **Yes** — single-frame loss only. |
| Higher-sublayer ref (`TID>0`,`_R`) | TRAIL_R / TSA_R(3) / STSA_R(5) | **max(1, 3−TID)** | DTS + `k·TID` | **Yes**, shedding that sublayer at a TSA/STSA boundary. |
| Higher-sublayer non-ref (`TID>0`,`_N`) | TRAIL_N / TSA_N(2) / STSA_N(4) | **max(0, 2−TID)** | DTS + `k·TID` | **Yes** — first to drop (leaf + non-ref). |
| RADL_R | 7 | **2** | PTS (output-tied, early) | Partly — decodable post-RA, real quality. |
| RADL_N | 6 | **1** | PTS | **Yes** (leading + SLNR). |
| **RASL_N / RASL_R** | 8/9 | **0 (lowest)** | PTS, short | **Always yes.** Never spend repair; evict first. |
| AUD(35) / EOS(36) / EOB(37) / FILLER(38) | — | n/a | — | Structural/none; AUD may be dropped or used as AU boundary. |
| Prefix/Suffix SEI(39/40) | by message | **per-message** | AU DTS | Mostly yes (see below). |

**SEI sub-classing** (walk the SEI message loop, classify by `payloadType`):
buffering-period(0)/pic-timing(1)/active-parameter-sets(129) → **tier 3**, not
discardable (HRD/timing; loss can stall/mistime the decoder). recovery-point(6) →
**tier 3** (soft anchor for gradual-refresh RA). decoded-picture-hash(132) →
tier 0. T.35/unregistered(4/5) → **tier 2** if captions/HDR10+ (visible), else
tier 0. mastering-display(137)/CLL(144) → tier 2 (static HDR; carry with the
anchor).

---

## 4. Packetization (RFC 7798) → symbol chunking

RFC 7798 payload header is 2 bytes (= NAL header fields). Modes:
- **Single NAL** — `Type` = the NAL's own type (0-47).
- **Aggregation Packet (AP), Type=48** — ≥2 NALs, each 16-bit-size-prefixed
  (first optionally a 16-bit DONL, others 8-bit DOND). `F = OR`, `LayerId/TID =
  min` over members.
- **Fragmentation Unit (FU), Type=49** — one large NAL split; FU header byte
  `S|E|FuType`. DONL only in the first fragment when `sprop-max-don-diff>0`.
- **PACI, Type=50** — wrapper; the only standard use is **TSCI** (`F0=1`,
  `PHSsize=3`): `TL0PICIDX | IrapPicID | S | E | RES` — explicit
  temporal-scalability + start/end-of-picture a middlebox reads without decoding.
- **DONL/DOND** present iff `sprop-max-don-diff > 0` (interleaved/aggregated);
  absent in the common in-order case.

**Implications (Meld is not RTP — mirror the discipline):**
1. **NAL/slice-aligned symbol boundaries** so a symbol's contents share one
   descriptor; never mix a discardable RASL with a TRAIL_R in one symbol (forces
   protect-the-max, wastes repair).
2. **Aggregate the tiny high-priority NALs (AP analog):** VPS+SPS+PPS (+
   active-parameter-set SEI) into **one top-tier symbol group**, replicated at
   every IRAP.
3. **Fragment large slices (FU analog), descriptor on every fragment:** a
   reference NAL's fragments are **all-or-nothing** for that NAL's recoverability
   (k-of-n satisfiable across the whole NAL, not per-fragment); a *discardable*
   NAL's fragments can be partially evicted under pressure.
4. **Carry TSCI-equivalent metadata in-band to the shaper, not on the wire** — the
   shaper already knows TID, SLNR, start/end-of-picture; stamp them on the
   descriptor.
5. **Decode-order (DONL) is the shaper's concern, not the core's:** supply DTS so
   the core's deadline eviction respects decode order even when output (PTS) order
   differs (B-pyramid).

---

## 5. HEVC-specific gotchas

- **Multi-slice frames break "1 NAL = 1 frame."** A picture is N slice-segment
  NALs (`first_slice_segment_in_pic_flag` is in the slice header). Losing **any**
  slice of a reference picture corrupts it and propagates — per-picture
  recoverability must span **all** its slices. Independent slices give natural
  spatial-region UEP if you parse slice addresses (advanced/optional).
- **Tiles & WPP → spatial error isolation, but only with `loop_filter_across_*`
  disabled.** A lost tile/CTU-row corrupts only its region *iff* in-loop filtering
  doesn't cross the boundary (PPS flags). The shaper **must not assume isolation**
  unless it parses the PPS to confirm — else a "contained" loss smears.
- **Long-term reference pictures (LTRPs) defeat the temporal-leaf heuristic.** An
  LTRP can be referenced many GOPs later and may be a `_N`-typed / low-`TID`
  picture you'd otherwise shed. Without slice-header RPS parsing the shaper can't
  know, so the safe default: **never treat `_N` as discardable when the SPS
  advertises LTRP usage** unless confirmed. Same caution for GDR
  (recovery_point-SEI streams with no IRAPs) — protect the whole refresh interval.
- **B-pyramid: output ≠ decode ≠ importance order.** A `TID==0` reference B is
  decoded after, displayed between, frames that depend on it. Deadlines key on
  **DTS**; priority on **reference role (`_R` + `TID`)**, not display position.
- **`nuh_layer_id != 0` (SHVC/MV-HEVC).** Enhancement-layer NALs ride the same
  stream; a separate dependency chain atop the base. Default: base = full ladder;
  enhancement = strictly below the base picture it depends on, **fully
  discardable** for graceful quality/resolution downscaling.
- **CRA/BLA after splice = `NoRaslOutputFlag` trap.** Naively protecting RASL
  burns budget on frames the decoder is *required* to discard on the very recovery
  event the repair was meant to serve — strictly negative value. RASL hard-wired
  to lowest tier, first-evicted.
- **In-band parameter-set repetition cadence is your real recovery granularity.**
  If PS are sent only once (legacy out-of-band hvcC), a single early loss is
  unrecoverable forever. Detect PS repetition rate; if not repeated at each IRAP,
  **escalate the one-time PS set to the absolute top + path duplication** — the
  highest-leverage, lowest-byte-cost protection in the stream.
- **EPB + short-buffer safety.** Strip emulation-prevention before bit-parsing
  (port prism's `removeEmulationPrevention`; switchframe has none); never panic on
  truncated/malformed NALs. On any parse failure, **fail safe**: treat as base-tier
  reference, **not** discardable (over-protect), never fail-discardable.

---

**Sources:** RFC 7798 (HEVC RTP payload); HEVC NAL types & SLNR /
`temporal_id_plus1` semantics (ITU-T H.265). Reuse audit: direct reads of
`~/dev/prism` and `~/dev/switchframe` (citations inline in §1).
