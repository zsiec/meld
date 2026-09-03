# Meld Protocol

This document describes the protocol as implemented today. The byte-level working
specification is maintained in [the LaTeX specification](spec/README.md).

## Goals

Meld is built for live media under a deadline. The main goal is to maximize
decodable media at a fixed latency budget when loss recovery cannot wait for a
named-packet retransmission round trip.

The protocol is not divided into deployable recovery profiles. One adaptive
path measures loss, burst persistence, reorder, source cadence, RTT, deadline
slack, and available byte headroom, then chooses recovery work that can still
arrive on time. Measured envelopes belong in generated benchmark artifacts rather
than the protocol definition; [Benchmarking](bench.md) defines the required method.

## Model

Meld codes fixed-width **algebraic source symbols** and **repair symbols**.

- A source symbol carries one application media chunk of up to `SymbolSize`
  bytes. A systematic datagram carries only the exact application bytes; zero
  padding and exact length/deadline metadata exist inside the algebraic symbol.
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

The sender emits every source symbol immediately, then emits recovery work over
a trailing coding window. The repair window is elastic: it is capped by
`CodingWindow`, but reduced when the configured playout budget cannot safely
cover the full span.

This is the important low-latency property:

- repair is already in flight before a loss is known
- a received repair packet can help any missing symbol in its window
- recovery is not blocked on a NACK returning

The automatic recovery path has four complementary actions:

- fungible sliding RLNC equations for unknown or burst-correlated loss;
- isolated 16-source Cauchy-MDS blocks whose continuously adjusted share reflects
  measured channel memory, source cadence, deadline geometry,
  reactive reachability, and recent source-wire cost;
- exact compact unit closure for persistent isolated holes, or for clustered
  residuals at their last useful dispatch when repair headroom is sufficient;
- delayed compact copies, spaced by the measured mean fade length, when a
  reactive cycle cannot fit but a second proactive copy can.

Every action spends the shared recovery allowance; none creates a second user
configuration or bypasses the source-first byte ledger.

During an automatic MDS epoch, all 16 systematics announce one stable source
range. The sender accumulates the same proactive credit that normally produces
moving-window equations, then releases bounded Cauchy rows when the block closes.
The receiver keeps those rows in an isolated fixed decoder, folds in exact values
learned from every recovery lane, and injects decoded sources into the common
ordered sliding decoder. Stable boundaries and isolated state prevent one row
from changing meaning as the sliding window advances.

A new epoch also compares one full algebraic row with a bounded 64-source mean
encoded systematic. A cold probe requires the row to cost no more than two mean
systematics; after repeated burst or outage evidence, sustained selection may use
the established three-systematic crossover. This keeps the decision media-agnostic:
full-width sources can select MDS, while a padded row cannot crowd out several
compact delayed-copy opportunities on a memoryless ragged source.

The receiver delivers source symbols in order once they are known. If a symbol
misses its deadline, it is declared lost and the cursor advances.

## Generation Fallback

The generation coder still exists and is used by the multipath path. It partitions
source symbols into bounded generations, sends systematic symbols, and emits
Cauchy-MDS repair for each generation. Any generation-width set of source and
repair rows reconstructs the block; the cap keeps systematic and parity points
disjoint in GF(256).

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
- `CongestionControl`: optionally replace the static ceiling with a delay/ECN
  budget. Sliding mode seeds startup from its measured application-limited
  source-plus-recovery offer so the first feedback does not collapse a live
  stream to a two-packet window.
- `BufferMicros`: deadline budget.

The controller uses feedback to estimate loss and burstiness. On the sliding
path, early cold-start repair is intentionally conservative because the first
symbols cannot be saved by feedback that has not arrived yet. Once measurements
are credible, memoryless loss with sufficient slack favors compact exact closure;
correlated loss normally favors equations whose value is not tied to one erased
source. A correlated residual switches to compact units only when feedback takes
at least four live-band spans, the deadline holds the 1.5-cycle closure gate, and
retained content shows an equation costs more than three units.
When no feedback cycle fits, a measured burst may instead enable one compact copy
delayed by the estimated fade duration.
If outage-aware estimation classifies a run beyond the recovery horizon, the
receiver reports that run separately from the censored burst estimate. For the
next two deadline budgets, and only while no reactive cycle fits and ordinary
recoverable-burst evidence is absent, the sender moves one quarter of already-
earned proactive RLNC equations across the measured span. This is time diversity,
not extra redundancy: the code-rate credit is consumed when the equation is
scheduled, source traffic remains first, and the pending queue is bounded by the
coding window. A first isolated outage shorter than one fifth of
`min(RTT, BufferMicros)` starts at one eighth instead; repeated outage evidence
restores the quarter share. This adjacent-regime probe prevents a short ge6 tail
from selecting the full deep-fade action.

The same feedback continuously controls an automatic epoch share. The sender
tracks Q8 demand, repeated burst correlation, and confirmed outage memory. A
new sender begins with a decaying uncertainty prior; ordinary loss receives a
small exploration share, repeated mean bursts of at least 2.0 symbols raise it,
and an `OutageRun` of at least 16 symbols requests the strongest allocation.
Clean offered reports release the demand, more quickly when a reactive cycle can
take over; idle feedback freezes it. There is no timed epoch mode or user-selected
transition.

At each safe 16-source boundary, demand maps continuously to a epoch fraction
of already-earned proactive credit. A viable reactive cycle multiplies the share
by one eighth, and post-propagation slack above 75 ms scales it by
`75 ms / slack`; neither condition disables MDS. Cadence, block-fill deadline,
effective-band and measured row/source economics remain hard safety
checks. An announced block always finishes with stable geometry, and the next
block immediately recomputes its share from live observations.

Repair is not allowed to create unbounded latency. When the rate budget binds,
repair is shed before source media. The host pacer then smooths what remains and
backpressures `Write` when the queue would exceed the deadline budget. Its queue
order is source, feedback-proven exact repair, then fungible recovery; all three
still spend the same aggregate rate budget.

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
- a bounded missing-neighborhood bitmap
- largest receiver-classified outage run in the report interval

The rank deficit can trigger reactive repair when it can still help. A missing
bitmap can close residual holes exactly when the return transmission fits. The
sender gives in-flight coding one feedback opportunity for isolated loss, but a
clustered residual switches to exact values before another feedback interval
would make them late, provided the measured source rate leaves enough recovery
headroom to preserve fungible coding.
The loss and burstiness estimates tune future proactive repair and
decide whether named retransmission is appropriate for the observed channel. The
separate outage run preserves fade geometry deliberately excluded from those rate
estimators, allowing the sender to change equation timing or sustain a
fixed-block epoch without poisoning its loss-rate set-point.

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
- long-term-reference candidates

This is how the protocol avoids treating every packet equally. Parameter sets,
RAP anchors, base references, and recovery-critical metadata can receive stronger
protection than disposable enhancement data.

Recovery-refresh labels describe the source dependency structure. They do not
trigger a separate whole-refresh-island repair schedule.

## Encoder Control

The sender exposes an advisory `EncoderControl`:

```go
ctrl := sender.EncoderControl()
```

`TargetBitrateBps` asks an attached encoder to reduce source payload when the
current source plus the bounded recovery allowance cannot fit inside the live
total-rate budget. The calculation prices systematic headers and recovery serialization,
uses sustained-overload/clear hysteresis, and bounds recovery's share so packet
completeness cannot be bought by collapsing source quality. A zero value means no
active reduction request.

`RecoveryCadenceFrames` asks an attached encoder to bound recovery distance when
feedback shows long bursts causing frame damage. The encoder may implement that
with intra-refresh, recovery-point SEI, or keyframes.

`Resync` asks the encoder to code its next frame against `ResyncRefFrameID`, an
LTR candidate the receiver has confirmed decodable, after the live reference
chain breaks. An encoder that no longer retains the named LTR may ignore it.

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

The pacer is a rate and queue discipline, not a dependency-layout optimizer.

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

The public multipath API uses the generation core; the sliding core is single-path.

## Wire Model

The main wire object is a symbol. A symbol can be:

- compact systematic source
- dense repair over a window, optionally transmitted without its zero tail
- sparse repair over explicit ids, with the same optional compaction
- compact exact unit repair

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

Wire version 1 makes exact source length and deadline part of every coded
equation and uses bounded Cauchy-MDS keys for generation and isolated epoch
repair. Systematic packets carry exact source bytes, or omit the explicit length
only when the payload already has full symbol width. A repair may omit a trailing
zero run from its application region; the coded length/deadline suffix stays on
the wire, and the receiver restores the zeros before GF arithmetic. This
preserves the coefficient vector, equation, and rank while avoiding full-width
packets for short application chunks. The same format defines
persistence-gated missing repair, exact unit repair, and stable 16-source block
announcements with isolated epoch rows. Feedback must carry the complete
version-1 layout.

## Invariants

The implementation is tested around these invariants:

- no duplicate delivered source ids
- in-order delivery
- no delivery past deadline
- no false recovery beyond the rank supported by the received equations
- bounded receiver state under forged or oversized windows
- deterministic core behavior under explicit clocks

The rank oracle and glassbench are part of the protocol's validation strategy.
