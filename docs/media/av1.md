# Media awareness — the AV1 shaper (and the generic dependency model)

> Per-codec note. Folds into [`../media-awareness.md`](../media-awareness.md)
> (the index). This note is **AV1-led** because AV1 has the richest survivability
> structure of Meld's target codecs; the generic descriptor first designed here
> (§3) is the source of the canonical `MeldUnitDescriptor` in the index, which
> serves all four codecs (the index adds the per-field `confidence` flag for the
> weaker-signaling codecs — see [`avc.md`](avc.md)).

The shaper is a thin, per-codec adapter that sits **above** Meld's media-blind,
sans-I/O coded core. It never touches the wire, the clock, or a socket. It takes
a codec access unit (AU) / packetization unit and emits a **generic descriptor**
the core uses to drive unequal error protection (UEP), deadline-based eviction
from the coding window, and graceful degradation:

- `priority_class` — small integer tier; higher = protect harder (more repair
  budget, and possibly duplicate/spread across multiple paths).
- `deadline` — when the unit must be decodable; past it, the symbol is evicted
  from the coding window (no more repair spent on it).
- `dependency` — what references what, so loss propagation is computable.
- `discardable` — droppable with only graceful quality loss.

The core stays **codec-blind**: it reasons over the generic descriptor only. All
AV1 (or HEVC/AVC/JXS) intelligence lives in the shaper.

---

## 0. Reuse audit (what's already parsed in the sibling repos)

**Verdict: there is no AV1 OBU parser to reuse.** AV1 is unparsed in both
`prism` and `switchframe`. The AV1 shaper must be built from the AV1 spec and the
AV1 RTP payload format / Dependency Descriptor spec. What *is* reusable is
generic scaffolding (bitstream readers, start-code iteration shape, PES PTS/DTS,
keyframe→GOP hooks), and — most importantly — `prism` is the right *upstream* to
**add** an AV1 path to later, because its frame/GOP model is the structure Meld's
shaper consumes.

### prism — H.264 + HEVC only, no AV1

- Supported codecs are H.264 (TS stream type `0x1B`) and HEVC (`0x24`); AAC
  audio (`0x0F`).
- **Reusable, codec-generic:**
  - An Exp-Golomb / emulation-prevention bit reader (`readBit`/`readBits`/`readUE`/
    `readSE`). AV1 OBUs use raw bits + **leb128**, not Exp-Golomb, so this reader
    transfers only partially — the leb128 reader is new.
  - Annex-B NAL iteration (3- and 4-byte start codes). The *shape* (length-delimited
    unit iteration) maps onto OBU iteration; AV1 uses leb128 `obu_size`, not start
    codes.
  - PES PTS/DTS @ 90 kHz → µs conversion — the **deadline clock source** for the
    shaper.
  - Keyframe→GOP hooks: an `IsKeyframe` flag and a `GroupID` incremented per
    keyframe. The resulting `VideoFrame{PTS, DTS, IsKeyframe, GroupID, …}` model is
    essentially the per-AU record the shaper annotates.
- **Not present (must build for AV1):** OBU header (`obu_type`,
  `obu_extension_flag`, `obu_has_size_field`, `temporal_id`/`spatial_id`),
  leb128, sequence header (profile / operating points / decoder model), frame
  header (`frame_type`, `show_frame`, `show_existing_frame`,
  `refresh_frame_flags`, `ref_frame_idx`, `primary_ref_frame`), temporal-unit
  framing, tile groups. No RTP depacketization and **no Dependency Descriptor**
  anywhere in prism.

### switchframe — AV1 via FFmpeg, not parsed in-repo

- AV1 is recognized only as an FFmpeg codec id and decoded by libavcodec; the
  Go pipeline operates on decoded YUV and **re-encodes to H.264/HEVC/VP9, never
  back to AV1**. No OBU-level intelligence exists in-repo; keyframe/PTS come from
  FFmpeg frame flags.
- **RIST ↔ AV1 connection is *transport only*, not codec-aware:** RIST carries
  **MPEG-TS bytes** in and out via `ristgo`; AV1 is just one possible elementary
  stream inside that TS. RIST output chunks TS into ~1316-byte payloads (7 TS
  packets). The one genuinely reusable fact: that **safe payload sizing** discipline
  (one RTP/UDP-safe MTU's worth of media per wire unit) is the same constraint Meld's
  symbol sizer faces.

**Bottom line for the build:** write a fresh, allocation-light AV1 OBU + sequence
+ frame-header parser in the shaper. Borrow prism's bit-reader idioms and PTS
plumbing; treat prism's `VideoFrame`/GOP model as the target annotation record;
take switchframe's safe-payload sizing as the symbol-MTU guardrail.

---

## 1. AV1 structure for survivability

### 1.1 OBU types (and what each means for protection)

AV1 is a stream of **OBUs** (Open Bitstream Units). Each has a 1-byte header
(`obu_type` 4 bits, `obu_extension_flag`, `obu_has_size_field`), an optional
1-byte extension header (`temporal_id` 3 bits, `spatial_id` 2 bits), and an
optional leb128 `obu_size`.

| `obu_type` | OBU | Survivability role |
|---|---|---|
| 1 | **Sequence Header** | Decoder config (profile, resolution, operating points, decoder model). **Without it nothing decodes.** Highest priority; cache + repeat. |
| 2 | **Temporal Delimiter** | Frames AU boundary marker. Zero payload value over Meld — **strip it** (the AV1 RTP format removes TDs; receivers re-insert). |
| 3 | **Frame Header** | `frame_type`, `show_frame`, `show_existing_frame`, `refresh_frame_flags`, `ref_frame_idx[]`, `primary_ref_frame`. **The dependency oracle reads this.** Tiny but load-bearing. |
| 4 | **Tile Group** | The actual coded pixels (one or more tiles). Bulk of the bytes; importance inherited from its frame. |
| 5 | **Metadata** | HDR (CLL/MDCV), ITU-T T.35, scalability, timecode. Mostly discardable for *decode*, but **HDR static metadata is sticky** (drop once → wrong colors for the whole stream). |
| 6 | **Frame** | Frame Header **+** Tile Group fused into one OBU (the common RTP/low-latency case). Inherits the frame's priority; you cannot split header from tiles. |
| 7 | **Redundant Frame Header** | A *repeat* of a frame header for error resilience. Naturally **discardable** to Meld (it's belt-and-suspenders for someone else's channel). |
| 8 | **Tile List** | Large-scale-tile / film use. Not supported by the AV1 RTP format; **strip + ignore**. |
| 15 | **Padding** | Discard. |

### 1.2 Temporal units (the AU the shaper keys on)

A **temporal unit (TU)** = a temporal delimiter OBU plus all OBUs up to the next
TD. TUs always advance in **display order**. Without scalability a TU contains
exactly **one shown frame** (`show_frame==1` or a `show_existing_frame==1`
header). With scalability a TU contains one coded frame **per spatial layer** of
the operating point. **The TU is the shaper's atomic AU**: deadline is derived
per-TU (its display PTS), and the shaper walks the OBUs inside it to assign
per-frame priority.

### 1.3 Frame types

- **KEY_FRAME** (`frame_type==0`): intra, and a **decoder reset** when
  `show_frame==1` (sets `RefValid`/refresh of all 8 slots). The random-access
  point; the single most important frame in the stream. Top tier.
- **INTER_FRAME** (1): predicted from reference slots. Importance depends on
  *who references it* (see refresh/DTI), not on being inter.
- **INTRA_ONLY_FRAME** (2): intra but **not** a full decoder reset (no temporal
  scalability reset, can't be shown as a sequence start the way a key frame is).
  A mid-stream intra refresh; high value but not a clean RAP for layered streams.
- **SWITCH_FRAME** (3): an inter frame constrained so it can act as a
  **bitstream switch point** — it implicitly forces `refresh_frame_flags=0xFF`
  (overwrites *all* 8 ref slots) and `error_resilient_mode`, so everything after
  it is decodable from it *without* an intra frame. This is the codec's native
  **"resync without an IDR"** — Meld should treat a SWITCH_FRAME like a soft RAP
  and protect it near key-frame tier.

### 1.4 `show_existing_frame` — the cheap, fragile display command

A frame header with `show_existing_frame==1` carries **no pixels**: it tells the
decoder to display the already-decoded frame in slot `frame_to_show_map_idx`.
This is how AV1 does B-frame-like reordering (decode order ≠ display order) and
alt-ref ("ARF") frames. Survivability consequences:

- It is **tiny** but its *deadline* is the display PTS of the TU it sits in, and
  it **fails silently** if the referenced slot frame was lost — you get a frozen
  / wrong frame, not a decode error. Protect the *referenced* frame to its later
  display deadline, not just its decode deadline.
- If `show_existing_frame` points at a `KEY_FRAME`, it triggers a deferred key
  decode (`frame_type` read from the stored frame). That's a hidden RAP.

### 1.5 The 8-slot reference buffer & `refresh_frame_flags`

AV1 keeps **8 reference slots** (the virtual buffer, `RefFrame[0..7]`). Two
syntax elements drive all of loss propagation:

- **`refresh_frame_flags`** (8-bit mask): which slots this frame **overwrites**
  after decoding. KEY_FRAME (shown) and SWITCH_FRAME force `0xFF` (refresh all).
- **`ref_frame_idx[0..6]`** + **`primary_ref_frame`**: which of the 8 slots this
  frame **reads** (up to 7 references; `primary_ref_frame` selects the slot whose
  CDF/loop-filter/segmentation context is inherited).

**Loss propagation rule the shaper computes:** a frame F is *undecodable* if any
slot in its `ref_frame_idx`/`primary_ref_frame` set currently holds a frame that
was lost (or itself undecodable). A lost frame keeps poisoning the picture until
a frame **overwrites every slot it had polluted** — i.e. until a KEY_FRAME, a
SWITCH_FRAME, or an accumulation of `refresh_frame_flags` that retires all
poisoned slots. **This is exactly what AV1 SVC "chains" encode at the RTP layer
(§3) so you don't have to re-derive it per packet** — but the shaper *can* derive
it from the frame header for the non-RTP (raw OBU / TS) ingest path.

### 1.6 Scalability: `temporal_id`, `spatial_id`, operating points

The OBU extension header tags every OBU with `temporal_id` (0–7) and `spatial_id`
(0–3). The sequence header enumerates **operating points**, each a
`(seq_level_idx, seq_tier, operating_point_idc)` where `operating_point_idc` is a
12-bit mask selecting which `(spatial_id, temporal_id)` layers belong to that
point. Decode-target semantics:

- **Temporal layers (T):** higher `temporal_id` = higher frame rate, and (by
  convention) **not referenced by lower layers** → the natural first thing to
  drop. T2 frames in L1T3 are pure enhancement.
- **Spatial layers (S):** higher `spatial_id` = higher resolution; may use
  inter-layer prediction (S references S-1 within the TU). Dropping S_top
  degrades resolution; S_base must survive.

This `(temporal_id, spatial_id)` → operating-point structure is the codec-side
truth behind the RTP **decode targets** (§3). The shaper maps it to
`priority_class` directly: base layer high, each enhancement layer one tier
lower.

---

## 2. The AV1 RTP Dependency Descriptor (DD) — and why Meld should adopt its model

The DD is an **RTP header extension** (RFC 8285) defined by the AV1 RTP payload
spec. Its entire purpose is to let a **Selective Forwarding Middleware (SFU)
make forward/drop decisions without parsing the codec bitstream**. That is
*precisely Meld's core's situation*: the core must allocate repair and evict by
deadline without knowing AV1 from HEVC. The DD is the proven, deployed answer to
"rich dependency + importance, codec-agnostically."

### 2.1 Mandatory part (every packet, 3 bytes)

- `start_of_frame` (1b), `end_of_frame` (1b) — frame boundary within the RTP
  stream.
- `frame_dependency_template_id` (6b) — index 0–63 into a **template** that
  pre-declares this frame's whole dependency/DTI/chain pattern.
- `frame_number` (16b) — monotonic per-frame id (wraps mod 2¹⁶).

The genius is the **template**: the full structure is sent once (in the
keyframe), and steady-state frames carry only **3 bytes** referencing a template.
Overhead is "almost negligible" in steady state.

### 2.2 Extended part (when present)

- `template_dependency_structure` — sent on the first packet of a coded video
  sequence: `template_id_offset` (6b), `dt_cnt_minus_one` (5b → number of
  **decode targets**), optional render resolutions, and per-template:
  - `template_dti[dt][tmpl]` (2b each) — **Decode Target Indications**.
  - `template_fdiff[tmpl][]` — referenced-frame deltas (`frame_number` diffs).
  - `template_chain_fdiff[chain][tmpl]` (4b) — distance to previous frame in
    each **chain**.
  - layer assignment per template via `next_layer_idc` (same / T+1 / S+1,T=0 /
    end).
- `active_decode_targets_bitmask` (DtCnt bits) — which decode targets are
  currently live.
- Custom per-frame overrides: `custom_dtis_flag` → `frame_dti[]`,
  `custom_fdiffs_flag` → `frame_fdiff[]` (size-tagged 4/8/16-bit deltas),
  `custom_chains_flag` → `frame_chain_fdiff[]` (8b per chain).

### 2.3 Decode Target Indications (DTI) — the importance enum

Per frame, **per decode target**, a 2-bit value:

| Val | DTI | Meaning for Meld |
|---|---|---|
| 0 | **Not present** | This frame contributes nothing to this target → for that target, **drop-safe**. |
| 1 | **Discardable** | Present, but **no future frame in this target references it** → `discardable=true`, lowest protect tier. |
| 2 | **Switch** | Present, and **all subsequent frames in this target decode if this one did** → a **resync/RAP for that target**: top protect tier. |
| 3 | **Required** | Present, and **future frames depend on it** → must survive: high tier. |

A single frame can be `Switch` for the base target and `Required`/`Discardable`
for others simultaneously — the importance is *per decode target*, which is
exactly the granularity Meld wants for UEP.

### 2.4 Chains — O(1) loss-impact detection

A **chain** is the minimal frame sequence that must arrive for a set of decode
targets to stay decodable "without further recovery." Each frame carries, per
chain, the `frame_number` of the **previous frame in that chain**
(`chain_fdiff`). Each decode target is **"protected by"** exactly one chain.

Loss detection becomes O(1) and parse-free: on a `frame_number` gap, the receiver
checks each active chain's `previous-frame` pointer; if a missing frame is the
previous link of a chain protecting a target you care about, **that target is
broken** and needs repair / a switch / an LRR — and you learn this *immediately*
on the first surviving packet after the gap, without decoding anything.

> This is the single most important idea to steal: **chains turn "did this loss
> matter?" into a pointer-chase, not a bitstream parse.**

---

## 3. THE KEY QUESTION — should Meld's waist carry a DD-like codec-agnostic descriptor?

**Recommendation: Yes. Adopt a DD-derived generic descriptor at the narrow waist,
and make it the *only* dependency/importance information the core consumes.** Do
**not** invent a codec-specific structure per codec inside the core; do **not**
make the core peek at OBUs/NALs. This is the cleanest way to honor Meld's
"media-blind core, thin per-codec shaper" architecture, and it matches the
README's stated intent (a "DD-like" descriptor "modeled on the AV1 Dependency
Descriptor so the core stays codec-agnostic").

Rationale:

1. **It's the right altitude.** The DD was designed for the *identical* problem
   (forward/drop without parsing the codec). Meld replaces "forward/drop" with
   "how much repair budget + which paths + evict when," but the *input
   information* is the same: dependencies, importance tiers, decode targets,
   chains. One model serves AV1, HEVC, AVC, and even JPEG XS (which is intra-only
   — a degenerate DD with one decode target and trivial chains).
2. **It generalizes across codecs cleanly.** AVC/HEVC have no native equivalent
   of AV1's chains/decode-targets, yet WebRTC *already* emits the DD for VP8/VP9/
   H.264 via "generic" frame descriptors. The shaper synthesizes the descriptor
   from NAL `nal_ref_idc`/temporal-id (AVC), `nuh_temporal_id_plus1`/IRAP types
   (HEVC), or OBU headers (AV1). The core never knows which.
3. **Chains are the loss-impact oracle the coded core wants.** Meld's whole bet
   is "recover the bytes the *picture* depends on." Chains tell you, parse-free,
   whether a given erasure actually broke a decode target — which is exactly how
   the core decides whether to keep spending repair on a window or let it go.
4. **Templates keep it cheap.** Steady-state descriptors are a handful of bytes;
   send the structure on the RAP. Meld can carry the descriptor in its own coded
   framing (it need not be an RTP extension on the wire), but the *information
   model* is the DD's.

### 3.1 Meld's generic descriptor — proposed fields

The shaper emits one descriptor per AU (and per coded-frame within an AU for
layered streams). Field names are Meld's; provenance noted.

```
MeldUnitDescriptor {
  // --- identity / ordering ---
  unit_id            u32   // monotonic; the core's dependency key (DD frame_number widened to 32b — never wrap mid-window)
  start_of_au, end_of_au  bool  // AU framing for symbol packing (DD start/end_of_frame)

  // --- importance / UEP ---
  priority_class     u8    // 0 = most disposable .. K = protect hardest. Derived from DTI + layer (see §4)
  discardable        bool  // true iff no surviving unit references this (DD "Discardable" on every active target)
  is_switch          bool  // soft-RAP for at least one decode target (DD "Switch" / AV1 SWITCH_FRAME)
  is_rap             bool  // hard random-access point (AV1 KEY_FRAME shown / HEVC IDR/CRA / AVC IDR)

  // --- deadline ---
  decode_deadline    ts    // when it must be decodable to be used (display PTS, or earlier if referenced)
  display_deadline   ts    // when it is shown (>= decode_deadline; matters for show_existing_frame)
  // past max(deadline) the core EVICTS the unit's symbols from the coding window

  // --- dependency (the DD model, generalized) ---
  refers_to          []u32 // unit_ids this unit decodes from (DD frame_fdiff resolved to ids). Empty for RAP.
  decode_targets     u16   // bitmask: which decode targets this unit belongs to (DD active_decode_targets)
  dti[decode_target] enum{NotPresent,Discardable,Switch,Required}  // per-target importance (DD DTI, 2b)
  chain[decode_target] -> chain_id        // which chain protects each target (DD "protected by")
  chain_prev[chain_id] u32                // previous unit_id in this chain (DD chain_fdiff resolved). Enables O(1) break detection.

  // --- scalability hint (optional, for path/budget policy) ---
  temporal_id        u8    // enhancement-layer depth (drop high T first)
  spatial_id         u8    // resolution layer (drop high S after high T)
}
```

The **core uses**: `priority_class` (repair/duplication budget), `decode_target`
+ `chain` + `chain_prev` (parse-free "did this loss break anything I'm
protecting?"), `decode_deadline`/`display_deadline` (window eviction), and
`discardable`/`is_switch`/`is_rap` (degradation & resync policy). It **never**
sees an OBU.

### 3.2 What this buys per codec

- **AV1:** near-1:1 from the real DD (or synthesized from OBU frame headers on
  TS/raw ingest). Full chains, multi-target DTI, switch frames.
- **HEVC:** `temporal_id` from `nuh_temporal_id_plus1`; IRAP (IDR/CRA/BLA) →
  `is_rap`; TSA/STSA → switch-ish; sub-layer non-reference (`TRAIL_N`,
  `TSA_N`...) → `discardable`. Chains degenerate to "temporal sublayer up to
  TID."
- **AVC:** `temporal_id` from prefix-NAL SVC ext or `nal_ref_idc==0` →
  `discardable`; IDR → `is_rap`. Single decode target unless SVC.
- **JPEG XS:** intra-only → every AU is a RAP, one decode target, `chain_prev`
  always null, `discardable=false`. UEP collapses to **slice/precinct
  importance within a frame** (low-frequency wavelet bands protected over high).

---

## 4. Protection mapping (AV1 unit/frame-class → Meld descriptor)

`K` = top tier (most repair + multipath duplication). Tiers are relative; the
budget allocator turns them into code rate per the deadline and current loss
estimate. "Deadline" is always derived from the TU's display PTS (the prism PES
PTS clock); `decode_deadline ≤ display_deadline`, and a referenced frame's
effective deadline is pulled **earlier** to the *latest display deadline of
anything that references it*.

| AV1 unit / frame-class | priority_class | deadline derivation | discardable? / graceful degradation |
|---|---|---|---|
| **Sequence Header OBU** | **K (max)** | none per-AU; **persistent** — must precede first use, repeat on every RAP | No. Loss = total decode failure. Cache + duplicate on all paths; re-send with each RAP. |
| **KEY_FRAME (shown)** | **K** | display PTS of its TU | No. `is_rap`. The resync anchor; protect hardest, spread across paths. |
| **SWITCH_FRAME** | **K−0** (near key) | display PTS | No. `is_switch`. Soft RAP (refreshes all 8 slots); protect like a key. |
| **INTRA_ONLY_FRAME** | **K−1** | display PTS | No. Mid-stream intra; valuable but not a clean layered RAP. |
| **INTER base-layer, T0/S0, DTI=Required** | **K−1** | display PTS | No. Reference anchor for the GOP; breaking it poisons slots until next RAP. |
| **INTER base-layer, DTI=Switch** | **K−0** | display PTS | No. Per-target resync point. |
| **INTER enhancement, higher T (T1,T2…)** | **K−2 / K−3** (one tier per T level) | display PTS of that sub-frame | **Yes** (DTI=Discardable on its target). Drop **highest temporal layer first**: frame rate halves, base keeps playing. |
| **INTER/INTRA enhancement, higher S (spatial)** | one tier below its T peer | display PTS | **Yes**, after temporal. Drop **highest spatial layer next**: resolution drops, base resolution survives. |
| **`show_existing_frame` header** | inherit the **referenced** frame's tier | **display** PTS of *this* TU (later than the referenced frame's decode) | Itself no-payload; pull the referenced frame's `display_deadline` out to here so repair isn't evicted before it's shown. |
| **Redundant Frame Header OBU** | **0 (min)** | n/a | **Yes.** It's external error resilience; Meld's coding supersedes it. Strippable. |
| **Tile Group / Frame OBU (pixels)** | **inherit its frame header's tier** | frame's deadline | Inherits. Can't split header importance from tiles in a fused `Frame` OBU. |
| **Metadata: HDR CLL/MDCV, T.35** | **K−1** (sticky) | persistent | **No, in practice.** Static; drop once → wrong colors for the rest of the stream. Cache + repeat like seq header. |
| **Metadata: scalability / timecode** | **0–1** | per-AU | **Yes.** Informational. |
| **Temporal Delimiter / Tile List OBU** | n/a | n/a | **Strip before coding** (re-inserted at decode; not carried). |

**Degradation order under budget pressure (drop first → last):**
redundant frame headers / non-HDR metadata → highest temporal layer →
next temporal layer → highest spatial layer → … → never drop base T0/S0,
switch frames, key frames, seq header, or HDR metadata.

---

## 5. Packetization facts → how Meld should chunk a TU into coded symbols

### 5.1 AV1 RTP payload format (the facts)

- **One aggregation-header byte** per RTP packet: `Z|Y|W(2)|N|---`.
  - **Z** = first OBU element is a **continuation** of a fragment from the prior
    packet.
  - **Y** = last OBU element **continues** into the next packet.
  - **W** (2b) = number of OBU elements in this packet (0 = "use a leb128 length
    on *every* element"; 1–3 = that many elements, and the **last one omits its
    length** — its size is the remaining payload).
  - **N** = this packet starts a **new coded video sequence** (`N=1 ⇒ Z=0`); a
    cue to expect a fresh sequence header / template structure.
- **OBU elements** are length-prefixed by **leb128** (except the last when
  `W≠0`). OBUs **may be fragmented** across packets (Z/Y mark the seams). A
  packet must not mix OBUs from **different temporal units**.
- **`obu_has_size_field` SHOULD be 0** on the wire (the RTP layer carries size),
  removing redundant size bytes.
- **Temporal Delimiter and Tile List OBUs are removed** by the sender and ignored
  by receivers. **Sequence header**, if present, **SHOULD be first** in the
  packet, and **SHOULD be aggregated with the base layer** (scalable) or **with
  each spatial layer** (S-mode simulcast).
- If multiple OBUs in one packet carry extension headers, their `temporal_id`/
  `spatial_id` **MUST be identical** — i.e. a packet is single-(T,S).

### 5.2 What it implies for Meld's symbol chunking

Meld does **not** have to use RTP on the wire (it carries coded symbols), but the
aggregation rules are the right *segmentation discipline* for a TU:

1. **Symbol = OBU-element-aligned, never cross-TU.** Make a coded symbol's source
   data respect TU boundaries so a window's deadline is well-defined (one TU =
   one deadline). Carry Meld's equivalent of the aggregation header (W/Z/Y/N) so
   the receiver can reassemble OBUs and know "new sequence" without decoding.
2. **Pack many small OBUs into one symbol; fragment big ones across symbols.**
   Exactly the W (count) vs Z/Y (fragment) split. Seq header + a small frame
   header + a small tile group of the **same (T,S) and same priority** can share
   one symbol (leb128-delimited). A large tile group spans multiple symbols (Z/Y
   continuation). **Crucially: only co-pack OBUs of the same `priority_class`**,
   or the symbol inherits the *max* priority and you over-protect cheap bytes (or
   under-protect dear ones). The AV1 rule "same (T,S) per packet" is a *free
   approximation* of "same priority per symbol" — keep it.
3. **Keep the sequence header / template in its own protection class.** Mirror
   "seq header first, aggregated with base layer": carry the seq header (and the
   DD template structure, §3) as top-tier symbols, repeated on every RAP, so a
   late joiner or a post-loss receiver can resync without an upstream request.
4. **Align coding-window generations to TUs / layers.** Because a packet is
   single-(T,S), enhancement-layer symbols are naturally separable — the core can
   give them their own (lower-rate) repair generation and **evict them first**
   under deadline pressure without touching base-layer symbols.

---

## 6. Gotchas — AV1 traps for a coding-based transport

1. **`show_existing_frame` deadlines.** A zero-payload header displays a frame
   decoded much earlier (alt-ref/B-pyramid). The referenced frame's symbols must
   **not be evicted at its decode deadline** — pull its `display_deadline`
   forward to the later TU. Miss this and you "successfully" recover a frame the
   receiver already gave up showing, or you freeze.

2. **Switch frames are RAPs without IDRs.** A SWITCH_FRAME (or DTI=Switch) makes
   the stream decodable again *without* an intra frame and *without* a sequence
   header repeat. If Meld only treats KEY_FRAMEs as resync anchors, it will
   over-protect (chasing repair on a window that a switch frame already healed)
   and mis-time eviction. Treat `is_switch` as a near-equal resync anchor.

3. **The 8-slot poison spreads silently.** A lost reference frame produces
   *correct-looking* but wrong output (decode "succeeds"), and keeps poisoning
   every slot it refreshed until a frame overwrites **all** polluted slots. The
   core can't see this from byte recovery — it must read it from the
   descriptor's chains. **If you don't carry chains/refresh info, you cannot tell
   a fatal loss from a cosmetic one.** This is the core reason §3 is non-optional.

4. **Strip TD / Tile List before coding, restore on decode.** Coding raw OBU
   streams that still contain temporal delimiters wastes bytes and confuses
   TU-boundary logic; the RTP spec strips them for a reason. Do the same and have
   the depacketizer re-insert a TD per TU.

5. **`obu_has_size_field` ambiguity.** On the RTP path OBUs *should* drop their
   size field (size lives in leb128 element headers). On a TS/raw ingest path
   they usually *keep* it. The shaper's OBU iterator must handle **both** (size
   from `obu_size` when present, else from the framing) or it will desync — a
   classic AV1 parser bug.

6. **`frame_number` / `unit_id` wrap.** DD `frame_number` is 16-bit and wraps mod
   2¹⁶; `chain_fdiff`/`fdiff` are small deltas. Widen to 32-bit `unit_id` at the
   waist (like ristgo widens 16-bit seqs) so dependency math never wraps inside a
   coding window. Resolve all `fdiff`/`chain_fdiff` deltas to absolute
   `unit_id`s in the shaper; never hand raw deltas to the core.

7. **Single sequence header, big blast radius (and HDR stickiness).** The seq
   header and HDR static metadata (CLL/MDCV) are sent rarely but govern the whole
   stream. They're tiny, so people under-protect them; losing one is catastrophic
   (no decode) or chronic (wrong color). Pin them to top tier, cache, and repeat
   on every RAP — they are the cheapest high-leverage bytes in the stream.

8. **Spatial inter-layer prediction couples S-layers within a TU.** Dropping a
   spatial *base* layer (`spatial_id=0`) inside a TU can kill the enhancement
   layers that predicted from it, even though they "arrived." Order degradation
   **temporal-before-spatial**, and within spatial drop **top-down**, never the
   base. The DTI/chain per decode target encodes this — trust it over a naive
   "high spatial_id = droppable" heuristic.

9. **Tiles ≠ independent recovery units in general.** Tile groups *can* be
   independently decodable, but only when the frame header enables it
   (`disable_cdf_update`, no dependent context across tiles). Don't assume a
   surviving tile group is useful on its own unless the frame header says so;
   default to frame-granular importance, treat sub-frame tile recovery as an
   optimization gated on the header.

---

## Appendix — provenance

- AV1 bitstream: frame types, 8-slot reference buffer / `refresh_frame_flags` /
  `ref_frame_idx` / `primary_ref_frame`, `show_existing_frame` /
  `frame_to_show_map_idx` / `showable_frame`, temporal-unit definition, operating
  points — AV1 Bitstream & Decoding Process Specification
  (aomediacodec.github.io/av1-spec).
- AV1 RTP payload format: aggregation header Z/Y/W/N, leb128 OBU elements,
  fragmentation, TD/Tile-List stripping, seq-header placement, and the
  **Dependency Descriptor** (templates, DTI, chains, decode targets, frame_number)
  — RTP Payload Format For AV1 (aomediacodec.github.io/av1-rtp-spec).
- Chains / decode targets SFU semantics — "Mastering the AV1 SVC chains"
  (Medooze/Millicast).
- Reuse audit — what existing H.264/HEVC TS pipelines already provide vs. what an
  AV1 shaper must build (summarized in §0).
