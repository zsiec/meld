# Meld wire format (version 1)

This document defines the on-wire encoding implemented by `internal/wire`. All
multi-byte integers are big-endian. A decoder returns `ErrShort`, `ErrType`,
`ErrVersion`, or `ErrInvalid` for malformed input and never panics.

Version 1 is the sole research format. The repository does not encode or decode
any preceding layout.

## Datagram lead byte

Every datagram begins with:

```text
byte0 = (1 << 4) | type
```

| type | message |
|---|---|
| `0x1` | systematic symbol |
| `0x2` | contiguous repair symbol |
| `0x3` | feedback report |
| `0x4` | clock probe |
| `0x5` | clock echo |
| `0x6` | handshake message 1 |
| `0x7` | handshake message 2 |
| `0x8` | handshake cookie reply |
| `0x9` | MTU probe |
| `0xA` | MTU probe acknowledgement |
| `0xB` | sparse repair symbol |
| `0xC` | exact unit repair |

The version nibble is validated before the type nibble. Any version other than
1 is rejected with `ErrVersion`.

## Symbol messages

Systematic, contiguous repair, sparse repair, and unit repair share this
30-byte base header:

| offset | size | field | meaning |
|---:|---:|---|---|
| 0 | 1 | `ver\|type` | version 1 and symbol type |
| 1 | 4 | `Flow` | flow identifier |
| 5 | 2 | `Epoch` | flow/key generation |
| 7 | 1 | `PathID` | host-selected path; zero on a single path |
| 8 | 4 | `WindowBase` | low edge of the coding set |
| 12 | 4 | `SrcIndex` | source id or repair sequence |
| 16 | 2 | `N` | coding width or sparse-id count |
| 18 | 2 | `RepairKey` | coefficient key |
| 20 | 1 | `Priority` | protection tier |
| 21 | 8 | `Deadline` | decode-by time in sender-clock microseconds |
| 29 | 1 | `Flags` | bit 0 send time, bit 1 frame descriptor, bit 2 source length |

Flagged extensions follow in a fixed order:

1. If bit 0 is set, `SendTimestamp` is an 8-byte signed timestamp.
2. If bit 1 is set, a frame descriptor contains `FrameStart` (4), `FrameLen`
   (2), descriptor flags (1), reference count (1), then that many 4-byte source
   ids. The reference count is bounded to 15 when decoded by the flow.
3. If bit 2 is set, `SourceLength` is a 4-byte unsigned integer.

A sparse repair then carries `N` 4-byte source ids before its payload. `N` must
be in `[1,64]`.

### Source representation

A systematic symbol normally sets `SourceLength` and carries exactly that many
source bytes. A full-width systematic payload may omit the field when its length
is exactly the configured `SymbolSize`. A unit repair always names one retained
source (`WindowBase=SrcIndex`, `N=1`) and carries its exact bytes. Receivers reject
ambiguous or over-wide source representations.

The flow's algebraic source width is `SymbolSize + 12`. The private 12-byte
suffix holds the source length as a uint32 and its exact deadline as an int64.
The receiver reconstructs this suffix before admitting a systematic or unit
value to the decoder. Consequently a recovered source retains its own length and
deadline.

### Repair representation

A contiguous repair covers:

```text
[WindowBase, WindowBase + N)
```

A sparse repair covers the explicit source ids carried in the packet. For either
kind, `RepairKey` regenerates the coefficient vector over GF(2⁸).

The full repair payload is `SymbolSize + 12` bytes. When its application region
ends in at least five zero bytes, the sender uses the compact representation:
`SourceLength` is the transmitted application-prefix width and the payload is
that prefix followed by the coded 12-byte metadata suffix. The receiver restores
the omitted zero interval before GF arithmetic. Compaction is used only when the
four-byte length field produces a net saving, and control accounting continues
to charge the full equation width.

Generation repair and fixed 16-source repair epochs use the Cauchy-MDS key
namespace (`RepairKey & 0xC000 == 0xC000`). Other keys select deterministic
seeded RLNC coefficients. A repair epoch uses ordinary type `0x2` packets: each
systematic announces the same 16-source `WindowBase`/`N` pair and the epoch rows
are decoded in separate bounded block state before recovered values are injected
into ordered sliding recovery.

## Feedback report

Feedback type `0x3` is cumulative recovery state. Version 1 requires the complete
layout; a truncated report returns `ErrShort` and trailing bytes return
`ErrInvalid`.

| offset | size | field | meaning |
|---:|---:|---|---|
| 0 | 1 | `ver\|type` | version 1 and `0x3` |
| 1 | 4 | `Flow` | flow identifier |
| 5 | 2 | `Epoch` | flow/key generation |
| 7 | 4 | `DecodedLowEdge` | all lower source ids are resolved |
| 11 | 4 | `HighestSeen` | highest observed source-id edge |
| 15 | 2 | `Deficit` | independent values needed at the cursor |
| 17 | 2 | `EcnCE` | CE-marked fraction, parts per 65535 |
| 19 | 2 | `LossRate` | smoothed pre-recovery erasure rate |
| 21 | 32 | `Deficits[32]` | generation deficits or sliding closure continuation |
| 53 | 2 | `CongestionLoss` | pre-recovery loss count for the interval |
| 55 | 2 | `Burstiness` | mean loss-run length in Q8; 256 is iid |
| 57 | 1 | `nPaths` | number of reported paths; zero for single path |
| next | `2*nPaths` | `PathLoss` | per-path loss fractions |
| next | `2*(nPaths+1)` | `SlotDist` | aligned-slot erasure-count histogram |
| next | 16 | media counters | frames, decodable frames, keyframes, decodable keyframes |
| next | 6 | LTR state | newest decodable LTR and broken-anchor count |
| next | 1 | `DeadPaths` | receiver-classified path-outage bitmap |
| next | 8 | `Missing` | rank-closing free-column bitmap from `DecodedLowEdge` |
| next | 2 | `SettledLost` | reorder-settled losses in the interval |
| next | 2 | `OutageRun` | largest classified outage run in source symbols |

`nPaths` is bounded to 8. If it is nonzero, both `PathLoss[nPaths]` and
`SlotDist[nPaths+1]` are required. A single-path report is 93 bytes; an N-path
report is `95 + 4*N` bytes.

In generation mode, `Deficits[i]` is the saturating rank deficit for generation
`i` from the delivery cursor. In sliding mode, `Missing` covers offsets 0--63
from `DecodedLowEdge` and `Deficits` carries the continuation. Its default form is
four big-endian 64-bit words covering offsets 64--319. Run form begins with
`ff 43 <count> 00`, sets bit `0x80` in byte 31, and stores one to six
`(start uint16, length uint16)` big-endian pairs beginning at byte 4. Runs are
ordered, non-overlapping, begin at offset 64 or later, and end by the 2,048-source
sliding-window limit. `Deficit` remains the hard cap on exact answers.

The combined closure representation names only free columns in the receiver's
reduced system, below decode coverage. Each exact answer removes one independent
degree of freedom; unresolved pivot columns are omitted because an arbitrary set
of them need not close rank. Unsent or merely in-flight ids are never named.
`SettledLost` is the
reorder-tolerant clean/dirty signal. `OutageRun` preserves fade duration separately
from the recoverable-run distribution used for repair-rate sizing.

## Other control messages

- Clock probe (`0x4`) is 9 bytes: lead byte and `T0` as int64.
- Clock echo (`0x5`) is 25 bytes: lead byte and `T0`, `T1`, `T2` as int64.
- MTU probe (`0x9`) is a 4-byte nonce followed by zero padding to the candidate
  datagram size.
- MTU acknowledgement (`0xA`) is 7 bytes: lead byte, nonce, and observed size as
  uint16.
- Handshake types `0x6`, `0x7`, and `0x8` contain opaque payloads owned by the
  session crypto layer.

On encrypted flows, source bytes are sealed before coding. Symbol headers remain
cleartext, while the exact ciphertext length is protected by the same source
metadata. Feedback, clock, and other protected control packets receive the
sequence/tag trailer defined in [Encryption](encryption.md).

## Media descriptor

The base header always carries `Priority` and `Deadline`. Systematic symbols may
also carry the frame descriptor extension. Its flags identify RAP,
recovery-refresh, discardable, non-picture, and long-term-reference units;
`FrameStart`, `FrameLen`, and the reference source ids let the receiver compute
dependency damage without parsing the media payload.
