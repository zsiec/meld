# Meld Protocol

This document describes the protocol as implemented today. It is meant to be the
source material for a future formal spec, not the formal spec itself.

## Goals

Meld is built for live media under a deadline. The main goal is to maximize
decodable media at a fixed latency budget when loss recovery cannot wait for a
named-packet retransmission round trip.

The current credible frontier is:

- iid or tail-erasure loss
- low latency
- playout budget at or below roughly one RTT
- same encoded source as SRT/RIST baselines

The protocol is not optimized around separate deployable profiles. The intended
deployment model is one adaptive profile that behaves conservatively outside the
frontier.

## Model

Meld sends fixed-size **source symbols** and **repair symbols**.

- A source symbol carries one application media chunk.
- A repair symbol carries a random linear combination of source symbols.
- A receiver can recover missing source symbols when the linear system has
  sufficient rank.
- Feedback adjusts future repair and reports delivery state; recovery does not
  require naming a particular missing packet.

All coding is over GF(2^8). Coefficients are deterministic from wire metadata, so
repair packets do not carry coefficient vectors.

## Layers

Meld has three layers.

### Application And Media Layer

The application writes chunks with one of three APIs:

- `Write`: base-priority byte-stream chunks.
- `WriteUnit`: byte-stream chunks with an explicit protection priority.
- `WriteFrame`: chunks with a full frame descriptor.

`WriteFrame` is the media-aware path. It lets a packetizer or encoder describe
what a chunk belongs to:

- frame id
- frame references
- chunk count for the access unit
- protection priority
- random-access point marker
- recovery-refresh marker
- temporal layer
- discardability
- non-picture metadata marker

The descriptor is generic. Codec-specific shapers map AVC, HEVC, AV1, JPEG XS,
or an application's own dependency model into this shape.

### Flow Core

The flow core is sans-I/O:

- no sockets
- no real clock reads
- no goroutines
- deterministic state transitions from explicit inputs

The core owns:

- coding window state
- source and repair emission
- deadline eviction
- feedback processing
- loss, burstiness, and reorder estimates
- repair sizing
- frame decodability accounting
- encoder-control advisory state

The public package wraps the core through `internal/session`.

### Session Host

The session host owns deployment mechanics:

- UDP sockets, or a caller-provided datagram substrate
- goroutines and timers
- transmit pacing
- write backpressure
- encryption handshake and record layer
- DPLPMTUD probing
- clock and feedback plumbing

The host does not decide the coding policy. It drains datagrams from the core and
feeds inbound datagrams back into it.

## Default Coding Path

`meld.DefaultConfig()` selects the band-form sliding-window coder.

The sender emits every source symbol immediately, then emits repair over a
trailing coding window. The repair window is elastic: it is capped by
`CodingWindow`, but reduced when the configured playout budget cannot safely
cover the full span.

This is the important low-latency property:

- repair is already in flight before a loss is known
- a received repair packet can help any missing symbol in its window
- recovery is not blocked on a NACK returning

The receiver delivers source symbols in order once they are known. If a symbol
misses its deadline, it is declared lost and the cursor advances.

## Generation Fallback

The generation coder still exists and is used by the multipath path. It partitions
source symbols into generations, sends systematic symbols, and emits repair for
each generation.

Generation mode is useful for:

- fallback/control tests
- multipath diversity coding
- comparison with the sliding path
- regimes where latency is generous and feedback can land

It is not the default low-latency media path.

## Repair Sizing

The sender has a proactive repair floor and an adaptive repair controller.

Relevant config:

- `Redundancy`: floor proactive repair rate.
- `TargetFailure`: decode-failure probability target.
- `RepairWithinBudget`: cap proactive repair inside the sender rate budget.
- `MaxBitrate`: aggregate media+repair ceiling.
- `BufferMicros`: deadline budget.

The controller uses feedback to estimate loss and burstiness. On the sliding path,
early cold-start repair is intentionally conservative because the first symbols
cannot be saved by feedback that has not arrived yet.

Repair is not allowed to create unbounded latency. When the rate budget binds,
repair is shed before source media. The host pacer then smooths what remains and
backpressures `Write` when the queue would exceed the deadline budget.

## Feedback

Feedback reports enough state for the sender to adapt without making recovery
depend on a named retransmission.

Conceptually, feedback includes:

- decoded low edge
- rank deficit
- highest source seen
- loss estimate
- burstiness estimate
- pre-recovery wire loss
- frame decodability counters
- keyframe decodability counters

The rank deficit can trigger reactive repair when it can still help. The loss and
burstiness estimates tune future proactive repair.

Pre-recovery wire loss is not decremented when repair later reconstructs a symbol.
That makes it the honest congestion/loss signal.

## Deadlines

`BufferMicros` is the playout budget. Each source symbol receives a deadline when
written. The receiver skips a symbol once it can no longer arrive or decode in
time.

Deadlines are load-bearing for media:

- they bound decode state
- they stop the sender and receiver from spending repair on dead media
- they keep the transport from turning loss recovery into accumulated delay

For media-aware flows, frame descriptors let the receiver compute frame
decodability from delivered source symbols and dependencies. This is separate
from byte delivery.

## Media Awareness

Meld's core is codec-blind, but not media-blind. It understands a generic frame
descriptor:

- priority
- access-unit identity
- dependency references
- RAP and recovery-refresh markers
- temporal layer
- discardability
- non-picture metadata

This is how the protocol avoids treating every packet equally. Parameter sets,
RAP anchors, base references, and recovery-critical metadata can receive stronger
protection than disposable enhancement data.

Recovery-refresh labels are currently observational. The rejected
refresh-island sparse-repair policy is not part of the deployable path.

## Encoder Control

The sender exposes an advisory `EncoderControl`:

```go
ctrl := sender.EncoderControl()
```

`RecoveryCadenceFrames` asks an attached encoder to bound recovery distance when
feedback shows long bursts causing frame damage. The encoder may implement that
with intra-refresh, recovery-point SEI, or keyframes.

This is not a profile switch. It is an actuator for the same adaptive transport
loop. If the encoder cannot comply, Meld continues with transport-only repair.

## Transmit Pacer

`Pace` is on by default.

The pacer:

- releases datagrams at the current core rate budget
- smooths keyframe/source bursts
- applies backpressure to `Write`
- keeps source FIFO
- does not run its own congestion controller

Rejected packet-placement interleaving experiments are not part of the pacer.
The pacer is intentionally a rate and queue discipline, not a dependency-layout
optimizer.

## Encryption

When `Passphrase` is set, Meld encrypts source chunks before coding.

High-level flow:

1. The session host establishes keys with a hybrid X25519 + ML-KEM-768 handshake.
2. Each source chunk is AEAD-sealed.
3. The coder treats ciphertext plus tag as the source payload.
4. Repair symbols are linear combinations of ciphertext bytes.
5. The receiver solves the linear system and then authenticates/decrypts the
   recovered source payload.

This preserves coded recovery and relay recoding without exposing plaintext to
the coding layer.

See [Encryption](encryption.md).

## Multipath

Multipath uses a coding-native model: source and repair symbols are spread across
paths and decoded from the union. This is diversity coding, not ST 2022-7-style
duplication.

The public multipath API currently routes through the generation core. Sliding
multipath remains future work.

## Wire Model

The main wire object is a symbol. A symbol can be:

- systematic source
- dense repair over a window
- sparse repair over explicit ids

Important symbol fields include:

- flow id
- kind
- source index
- window base and width
- repair key
- deadline
- send timestamp
- priority
- optional frame descriptor extension
- payload

Feedback and control messages are separate wire objects. See
[Wire Format](wireformat.md) for the current encoding.

## Invariants

The implementation is tested around these invariants:

- no duplicate delivered source ids
- in-order delivery
- no delivery past deadline
- no false recovery beyond the rank supported by the received equations
- bounded receiver state under forged or oversized windows
- deterministic core behavior under explicit clocks

The rank oracle and glassbench are part of the protocol's validation strategy.

## What Is Intentionally Not In The Protocol Surface

The following were explored and removed or kept internal:

- deployable placement/interleaver profiles
- refresh-island sparse repair switch
- manual protected-repair phasing knobs
- fixed-keyint source shortcuts as a protocol strategy
- full emitted-stream lookahead simulators as a sender model

They remain documented only in decision notes so future work does not repeat the
same local optimization loop.
