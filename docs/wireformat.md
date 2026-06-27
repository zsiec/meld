# Meld wire format (pinned, v1)

The on-wire encoding of the narrow waist (`internal/wire`): the **Symbol** (media-
bearing) and the **Feedback** report. This document is the authority; the codec
mirrors it. All multi-byte fields are **big-endian**. The codec never panics on
malformed input — short, mis-versioned, or corrupt buffers return a sentinel error
(`ErrShort`, `ErrType`, `ErrVersion`).

> **Status:** v1, pinned 2026-06-21 (roadmap N0). Meld is pre-deployment; v0 (the
> unversioned 27-byte prototype headers) was never shipped and has no compatibility
> obligation. v1 is the first explicitly-versioned format.

## Versioning — the forcing function

Every datagram's **leading byte** packs the format version and the message type:

```
byte0 = (Version << 4) | type      // Version = 1 (high nibble), type (low nibble)
```

| type nibble | message |
|---|---|
| `0x1` | Systematic symbol (one source symbol, verbatim) |
| `0x2` | Repair symbol (random linear combination over a window) |
| `0x3` | Feedback report (receiver → sender) |
| `0x4` | Clock probe (receiver → sender): `T0` (9 bytes) — N4 offset handshake |
| `0x5` | Clock echo (sender → receiver): `T0,T1,T2` (25 bytes) — the probe reply |
| `0x6` | Handshake message 1 (initiator → responder) — encryption (below) |
| `0x7` | Handshake message 2 (responder → initiator) |
| `0x8` | Cookie reply (responder → initiator) — mac2 anti-amplification, under load |

A decoder checks the version nibble **first**: a datagram whose version it does not
understand returns `ErrVersion` rather than misparsing — so a field added in a later
revision can never decode as silent garbage in an older peer. `ErrVersion` is
reported before `ErrType` (a mis-versioned datagram is named as such, not as an
unknown type).

### Extension policy (how N1/N2/N4 add fields without colliding)

- **Additive optional field** → append to the **tail** under the *same* version,
  gated by a Symbol `Flags` bit (symbols) or a length check (feedback). An older
  decoder reads the base layout it knows and ignores the rest. **No version bump.**
- **Incompatible base-layout change** (moving/resizing an existing field) → **bump
  the version nibble.** Old peers cleanly reject with `ErrVersion`.

The near-term roadmap fields have all landed this way:

| field | message | how |
|---|---|---|
| `CongestionLoss` (pre-recovery wire loss, N1) | Feedback | tail append, length-gated |
| `Burstiness` (loss-run / GE estimate, N2) | Feedback | tail append, length-gated |
| `EcnCE` (CE-marked fraction, N3 / L4S) | Feedback | base field, now populated |
| per-path loss + erasure-count histogram (N5, **N paths**) | Feedback | variable tail section (below) |
| media damage counters (N6) | Feedback | tail append after the per-path section |
| frame dependency (`FrameRefs`, WP6) | Symbol | `flagDesc` tail extension (below) |

`PathID` (N5) is the one exception that is **not** a tail extension: a path id is a
structural property of every symbol (single path is just path 0), not an optional
measurement, so it lands as a **base header field**. Meld is pre-deployment with no
compatibility obligation (see Status), so the v1 base layout absorbed it directly
rather than spending a `Flags` bit and an extension on a field every multipath symbol
carries; the version nibble stays 1.

The cross-host clock offset (N4) is carried by the dedicated probe/echo messages
above (cheap, periodic — not on every symbol), so the per-symbol `SendTimestamp`
(now implemented as the `flagSendTS` header extension above) is only a *refinement*
(finer one-way-delay-variation tracking, the QUIC-TS trick), not required for the
offset itself.

## Symbol header (30-byte base, then optional extension, then payload)

| off | size | field | meaning |
|----:|----:|---|---|
| 0 | 1 | `ver\|type` | version nibble + `0x1`/`0x2` |
| 1 | 4 | `Flow` | flow id |
| 5 | 2 | `Epoch` | flow generation; bumps on flow reset / key update (reserved; 0) |
| 7 | 1 | `PathID` | host-stamped path the symbol was sent on (0 = single path / path 0; N5) |
| 8 | 4 | `WindowBase` | low edge of the coding window |
| 12 | 4 | `SrcIndex` | Systematic: source id · Repair: repair counter |
| 16 | 2 | `N` | Repair: window width covered |
| 18 | 2 | `RepairKey` | Repair: GF(2⁸) coefficient PRNG seed |
| 20 | 1 | `Priority` | descriptor: protection tier (0 = most disposable) |
| 21 | 8 | `Deadline` | descriptor: decode-by, clock microseconds (i64) |
| 29 | 1 | `Flags` | bit 0 `flagSendTS` ⇒ send-timestamp ext; bit 1 `flagDesc` ⇒ frame-descriptor ext |
| +8 | 8 | `SendTimestamp` | ext, present iff `flagSendTS` — sender clock µs at emission (N4) |
| ext | 8 + 4·`nRefs` | frame descriptor | ext, present iff `flagDesc` — head `FrameStart`(4) `FrameLen`(2) `descFlags`(1) `nRefs`(1), then `nRefs`×`RefStart`(4); WP6 |
| … | — | `Payload` | coded bytes, sized to the validated path MTU |

The two extensions follow the 30-byte base in a **fixed order** (send timestamp, then frame
descriptor), each gated by its Flags bit, so the decoder walks them by flag. The **frame
descriptor** (WP6) is stamped on SYSTEMATIC symbols only and lets the receiver compute loss
propagation parse-free: `FrameStart` is the access unit's first source id (its identity, and
with `FrameLen` its exact id range `[FrameStart, FrameStart+FrameLen)`); `descFlags` bit 0 =
RAP (keyframe), bit 1 = discardable, bit 2 = non-picture metadata/parameter material;
`nRefs` (≤ 15) is the number of dependency frames, each a `RefStart` (a referenced frame's first source id). The references are **exact and
variable-length** — a B-picture carries its two bracketing anchors, a P-picture one — so the
receiver tracks the dependency tree the shaper resolved (POC bracketing / AV1's reference
buffer). Coding rebuilds payloads, not headers, so a recovered symbol carries no descriptor —
the receiver infers frames it did not directly receive (`internal/flow` FrameStats).

A **Systematic** symbol carries one source symbol verbatim (`N`/`RepairKey` unused);
a **Repair** symbol carries a random linear combination, where `WindowBase`+`N`
delimit the spanned window and `RepairKey` regenerates the coefficients
(`internal/code.GenCoeffs`). `PathID` is set by the multipath scheduler
(`internal/flow.pathScheduler`); the host transmits the symbol on that path and the
receiver attributes its arrival/loss to that path for the co-loss estimate.

## Feedback report (53-byte base + length-gated tails)

Cumulative and idempotent — *state, not events* — so a lost report costs nothing;
the next carries the same truth, advanced. The encoder always writes the fullest
form; the decoder reads each tail group only when the buffer is long enough, so an
earlier-prefix peer ignores fields it does not know (no version bump).

| off | size | field | meaning |
|----:|----:|---|---|
| 0 | 1 | `ver\|type` | version nibble + `0x3` |
| 1 | 4 | `Flow` | flow id |
| 5 | 2 | `Epoch` | flow generation (mirrors Symbol.Epoch; reserved; 0) |
| 7 | 4 | `DecodedLowEdge` | everything below this source id is recovered + delivered |
| 11 | 4 | `HighestSeen` | highest source id observed (gap = work outstanding) |
| 15 | 2 | `Deficit` | extra independent symbols the live window needs (== `Deficits[0]`) |
| 17 | 2 | `EcnCE` | CE-marked fraction of received symbols this interval, parts per 65535 (N3 / L4S) |
| 19 | 2 | `LossRate` | smoothed channel erasure estimate, parts per 65535 |
| 21 | 32 | `Deficits[32]` | rank deficit of the 32 generations from the cursor (saturating u8) |
| 53 | 2 | `CongestionLoss` | pre-recovery wire-loss count since last report (N1; tail, length-gated) |
| 55 | 2 | `Burstiness` | smoothed mean loss-run length, Q8 (256 = i.i.d.; N2; tail) |
| 57 | 1 | `nPaths` | multipath: paths reported (0 = single path); N5 variable tail |
| +0 | 2·`nPaths` | `PathLoss[nPaths]` | per-path marginal erasure rates, parts per 65535 (weights the scheduler) |
| +… | 2·(`nPaths`+1) | `SlotDist[nPaths+1]` | per-slot erasure-COUNT histogram: fraction of aligned N-path slots with exactly *j* of *N* paths erased, parts per 65535 (drives the joint-tail sizer) |
| next | 16 | media stats | cumulative `Frames`, `DecodableFrames`, `Keyframes`, `DecodableKeyframes` as u32 counters (N6; zero when absent/no descriptors) |

The per-path section is **variable** and bound-gated (`nPaths ≤ 8`): a forged count can neither
over-read nor over-allocate. `SlotDist` is the exact sufficient statistic the correlation-aware
joint-tail sizer (`internal/flow.repairForJointTailN`) convolves — union-decode failure depends
only on the total erasure count, so the count histogram embeds the cross-path correlation an
i.i.d.-union sizer misses, without enumerating which paths failed.

The media-stats tail lets the sender's single `meld-auto` loop tell ordinary wire loss from
decode-damaging dependency loss. Combined with `Burstiness`, it drives the advisory encoder
recovery-cadence request (`EncoderControl.RecoveryCadenceFrames`) without introducing separate
deployable profiles. A sender that cannot influence the encoder simply ignores that actuator and
keeps the same transport loop.

## The generic media descriptor

Every symbol carries the minimal slice the core's sizing acts on — `Priority` (unequal
protection) and `Deadline` (per-symbol eviction) — and systematic symbols additionally carry
the `flagDesc` frame descriptor above (`FrameStart`, `FrameLen`, `FrameRefs[]`, RAP /
discardable), so the receiver reconstructs the dependency tree and computes decodable-frame
loss propagation parse-free. The media shaper (`internal/shape`) fills it from the bitstream;
see `docs/media-awareness.md`.

## Encryption (handshake + ciphertext) — opt-in

When a flow is encrypted (`meld.Config.Passphrase`), three handshake messages establish
keys before any media, and every Symbol's `Payload` becomes AEAD ciphertext. The design,
rationale, and citations are in [`docs/encryption.md`](encryption.md); this section pins the
bytes. The deterministic sans-I/O core never sees a key — encryption lives in the host
(`internal/session`), so these messages and the payload transform sit entirely on the host
side of the waist.

### Handshake messages

All three are framed `byte0 = (Version<<4) | type` followed by an opaque crypto payload
(`internal/crypto`); the lengths are fixed by the X25519 + ML-KEM-768 sizes.

| type | name | payload bytes | layout |
|---|---|---:|---|
| `0x6` | message 1 (init → resp) | 1248 | `X25519_pub`(32) ‖ `MLKEM768_encap_key`(1184) ‖ `mac1`(16) ‖ `mac2`(16) |
| `0x7` | message 2 (resp → init) | 1152 | `X25519_pub`(32) ‖ `MLKEM768_ciphertext`(1088) ‖ `confirm`(16) ‖ `mac1`(16) |
| `0x8` | cookie reply (resp → init) | 56 | `nonce`(24) ‖ XChaCha20-Poly1305(`cookie`(16) ‖ `tag`(16)) under a PSK-derived key |

Message 1 carries no freshness counter. A restarted sender re-handshakes **commit-after-confirm**:
the responder stages a new handshake as a PENDING session (matched to the live one by the
initiator's ephemeral keys — identical keys ⇒ a retransmit, resend the cached message 2 to the
established peer; different keys ⇒ a new handshake) and keeps the live session running until an
inbound symbol AUTHENTICATES under the pending keys, only then promoting it and resetting the
receive core. A replayed or forged message 1 can never produce such a symbol, so it can never
displace a live session — replacing the need for a wire freshness/anti-replay field. `mac1` is
HMAC-SHA256 (truncated to 16) keyed by the Argon2id-derived PSK over `pubs` — the always-on
gate that rejects a non-PSK flood before any asymmetric work. `mac2`
is the WireGuard-style cookie (HMAC keyed by the cookie the responder issues in `0x8`),
zero unless the responder is under load; it proves return-routability of the source
address. `confirm` is HMAC under a master-derived key over the handshake transcript,
authenticating the responder. The two ephemeral shared secrets (X25519 ECDH + ML-KEM-768
decapsulation) are HKDF-chained into the master secret — a hybrid that survives the break
of either primitive (PQNoise / PQ-WireGuard lineage).

### Encrypted Symbol payload

The Symbol **header is unchanged and cleartext** (a relay still reads `WindowBase`, `N`,
`RepairKey`, `PathID` to route and recode), but its `Payload` is the AEAD output:

```
Payload = ChaCha20-Poly1305(K_epoch, nonce, media_chunk, AAD)   // len = len(media_chunk) + 16
```

- **Encrypt-then-code:** the host seals each SOURCE symbol *before* the coder; repair
  symbols are GF(2⁸) linear combinations of the sealed bytes, so a recoding relay
  recombines ciphertext with **no key**, and the 16-byte tag (carried as coded payload)
  gives end-to-end integrity through the code.
- **Nonce** = `Epoch`(2) ‖ `SrcIndex`(4), zero-padded to 96 bits — **derived, never on the
  wire** (the receiver knows the delivered source id and the epoch). Refuse-to-reuse: the
  sender rekeys before `SrcIndex` wraps.
- **`Epoch` is the key-update counter** (the reserved Symbol/Feedback field, now used):
  `Epoch = SrcIndex / EpochSize`, and the directional traffic secret ratchets forward each
  epoch (intra-session forward secrecy). Both ends derive the epoch from the source id with
  no extra wire field.
- **AAD** = `Flow`(4) ‖ `Epoch`(2) ‖ `SrcIndex`(4) — the routing identity both ends know
  for any delivered symbol (including a recovered one, which carries no header); binding
  `Epoch` is the lightweight key-commitment that closes the partitioning-oracle gap.

Encrypted media chunks are therefore at most `SymbolSize − 16` bytes. The frame-descriptor
(`flagDesc`) and `SendTimestamp` extensions are unaffected (cleartext header extensions).

### Encrypted control plane

On an encrypted flow the control datagrams — Feedback (`0x3`), Clock probe (`0x4`), Clock
echo (`0x5`) — are authenticated so a forged or replayed one cannot retire recovery state or
skew the deadline frame. Each carries an 8-byte sequence and a 16-byte tag as a **trailer**,
leaving the leading type byte at offset 0 so a relay/host still dispatches on it:

```
datagram ‖ Seq(8) ‖ HMAC-SHA256(K_dir, datagram ‖ Seq)[:16]
```

Each direction uses its OWN key — `K_i2r = HKDF-Expand(master, "control-i2r")` for
initiator→responder, `K_r2i` for the reverse — so one direction's datagram can never verify
in the other (no cross-direction reflection); one side's send key equals the other's receive
key. The receiver of a control datagram verifies the tag and runs `Seq` through a 64-entry
sliding replay window (RFC 6479), dropping replays and stale sequences. Control datagrams are
dropped (and never emitted) until the handshake has established the keys, so the
pre-establishment window is not an unauthenticated hole. Cleartext flows carry no trailer.

## Coefficient encoding (resolved for v1)

v1 uses **seeded coefficients**: `RepairKey` is the PRNG seed both ends expand into
the GF(2⁸) coefficient vector over `[WindowBase, WindowBase+N)` — compact (no
on-wire vector), the RFC 8681 RLC lineage. A `density` byte (fraction of the window
mixed) is reserved for sparse codes.

Two alternatives are deliberately **not** v1, and adopting either is a version bump:

- **Explicit coefficient vectors** — *forced* the moment recoding at relays is in
  scope, because a recoded symbol's coefficients are no longer derivable from a single
  seed. Reserve for it.
- **Deterministic Vandermonde** (`α^((src·key) mod 2^m)`, RFC 9407 Tetrys) — the
  price of wire-interop with Tetrys. Adopt only if interop is wanted.
