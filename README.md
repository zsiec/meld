# Meld

Meld is a low-latency coded media transport for live video. It sends media as
compact systematic source symbols plus adaptive sliding-window recovery, so the
receiver can recover from erasures even when a named-packet retransmission cannot
make a round trip before playout.

The current product direction is deliberately narrow:

- one deployable profile: `meld-auto`
- automatic adaptation, not a menu of profiles
- source mostly FIFO
- recovery bytes capped by the configured rate budget
- proactive coded repair where latency is too tight for ARQ
- fixed-geometry repair epochs when measured deep fades make continuous
  sliding geometry less effective
- compact coded equations when their zero tail can be omitted without changing rank
- compact exact closure when feedback can still help
- burst-spaced compact copies when a measured fade fits but a reactive cycle does not
- conservative fallback behavior when the channel is uncertain

Applications do not choose among these mechanisms. The sender selects them from
measured RTT, deadline slack, source cadence, loss memory, and available byte
headroom.

## Validation Status

The deterministic unit, integration, fuzz-seed, and media-shaper suites exercise the
coding invariants, deadline behavior, malformed-input bounds, encryption, multipath, and
codec dependency models. `cmd/glassbench` provides capacity-matched, same-source
comparisons against external SRT and RIST tools with source and ideal oracle rows.

Benchmark results are intentionally not frozen into this README: they depend on the
source, tool versions, host, revision, capacity, impairment model, and matched seeds. See
[Benchmarking](docs/bench.md) for the current suites, required artifacts, and acceptance
bar.

## What Meld Optimizes

Meld is optimized for media completion under a playout deadline, not raw packet
delivery. The benchmark gate is glass-to-glass:

- `ffprobe` decoded frames
- decodable frame percentage
- decodable keyframe percentage
- same source packet and byte counts across transports
- latency budget relative to RTT
- oracle rows showing source ceiling and ideal transport ceiling

This matters because packet recovery is not the same thing as video recovery. A
lost parameter set, random-access anchor, or early reference chain can destroy a
large dependency island. A lost disposable enhancement packet may not matter at
all. Meld's media-aware path lets the sender tag chunks with generic frame
metadata so the transport can protect dependency-critical data harder while still
remaining codec-blind at the core.

## Protocol In One Page

Meld has three layers:

1. **Application/media layer**: writes media chunks up to `SymbolSize` bytes. It
   may attach a `FrameDesc` describing priority, references,
   RAP/recovery-refresh markers, temporal layer, and discardability.
2. **Sans-I/O flow core**: emits source and repair symbols, tracks deadlines,
   estimates loss/burstiness/reorder, sizes repair, and decodes from any
   sufficient rank. The core does not open sockets or read clocks.
3. **Session host**: owns UDP sockets or a caller-provided datagram substrate,
   pacing, timers, encryption, DPLPMTUD, and goroutines.

The default sender uses the band-form sliding-window coder and continuously
allocates some proactive repair to isolated 16-source Cauchy-MDS blocks without
an application mode switch:

- source symbols are sent immediately
- systematic packets carry only their exact application bytes
- repair symbols cover an elastic trailing window
- compact repair omits only a zero equation tail and reconstructs the
  identical full-width equation before decoding
- feedback adjusts future repair rate and closes persistent holes when useful
- deadline-admissible fixed blocks receive a continuously adjusted
  share of the same proactive credit; loss memory raises the share, while
  reactive reachability and long slack reduce it without turning MDS off
- measured bursts may trigger one delayed compact copy when ARQ cannot fit
- `BufferMicros` is the playout deadline
- `MaxBitrate` and `RepairWithinBudget` keep repair inside the rate budget
- the host pacer smooths output and backpressures `Write` instead of queueing
  media past its deadline; within that budget it releases source first, then
  feedback-proven exact repair, then fungible recovery

The receiver delivers recovered source chunks in order. It also reports
pre-recovery wire loss, recovered source count, rejected symbols, evictions, and
frame-level decodability when frame descriptors are present.

For the deep mechanics, see [Protocol](docs/protocol.md).

## Quick Start

```sh
go get github.com/zsiec/meld
```

### Sender

```go
package main

import (
	"io"
	"log"

	"github.com/zsiec/meld"
)

func send(remote string, media io.Reader) error {
	cfg := meld.DefaultConfig()
	cfg.Flow = 1
	cfg.BufferMicros = 75_000

	s, err := meld.NewSender(remote, cfg)
	if err != nil {
		return err
	}
	defer s.Close()

	buf := make([]byte, cfg.MaxChunk())
	for {
		n, err := media.Read(buf)
		if n > 0 {
			if _, werr := s.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			s.Flush()
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func main() {
	if err := send("203.0.113.10:5000", openMedia()); err != nil {
		log.Fatal(err)
	}
}
```

### Receiver

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1
cfg.BufferMicros = 75_000

r, err := meld.NewReceiver(":5000", cfg)
if err != nil {
	log.Fatal(err)
}
defer r.Close()

buf := make([]byte, cfg.SymbolSize)
for {
	n, err := r.Read(buf)
	if err != nil {
		break
	}
	writeMedia(buf[:n])
}

st := r.Stats()
log.Printf("delivered=%d recovered=%d lost=%d wire_lost=%d",
	st.Delivered, st.Recovered, st.Lost, st.WireLost)
```

`Write`/`Read` are byte-stream chunk APIs. For media-aware protection, use
`WriteFrame`.

## Media-Aware Sending

`WriteFrame` attaches the generic media descriptor that Meld stamps onto the wire.
Use it when your encoder or packetizer knows frame boundaries and references.

```go
fd := meld.FrameDesc{
	Priority:    3,          // higher means protect harder
	FrameID:     frame.ID,   // monotonic access-unit id
	RefFrameIDs: frame.Refs, // dependency frame ids
	Chunks:      uint16(len(frame.Chunks)),
	TemporalID:  frame.TemporalID,
	RAP:         frame.IsRandomAccess,
	Discardable: frame.Discardable,
	NonPicture:  frame.MetadataOnly,
}

for _, chunk := range frame.Chunks {
	if _, err := s.WriteFrame(chunk, fd); err != nil {
		return err
	}
}
```

Descriptor fields are generic, not codec-specific. AVC, HEVC, AV1, and JPEG XS
shapers map their codec structures into this shape. The repo contains internal
shapers used by `glassbench`; a stable public shaper package is not exposed yet.
Integrators can still use `WriteFrame` directly from their existing encoder,
RTP, container, or dependency metadata.

See [Integration](docs/integration.md) and [Media Awareness](docs/media-awareness.md).

## Encoder Control

Meld does not expose multiple deployable profiles for source structure. Instead,
the sender can produce an advisory encoder request:

```go
ctrl := s.EncoderControl()
if ctrl.TargetBitrateBps != 0 {
	encoder.SetBitrate(ctrl.TargetBitrateBps)
}
if ctrl.RecoveryCadenceFrames != 0 {
	encoder.SetMaxRecoveryInterval(int(ctrl.RecoveryCadenceFrames))
}
if ctrl.Resync {
	encoder.RequestLTRResync(ctrl.ResyncRefFrameID)
}
```

`TargetBitrateBps` asks the encoder to leave enough of the live aggregate rate
budget for measured recovery demand. It activates only after sustained overload,
remains stable when the encoder complies, and relaxes only after sustained spare
capacity. Source media always retains the majority of the budget, so a large
burst estimate cannot collapse picture quality merely to maximize packet counts.

`RecoveryCadenceFrames` asks the encoder to shorten dependency damage with a
bounded recovery point cadence. Encoders may implement this with intra-refresh,
recovery-point SEI, or keyframes. If the encoder cannot comply, Meld continues
with the same transport loop.

`Resync` names an encoder-retained LTR that the receiver has confirmed decodable
and asks the encoder to reference it for the next frame after the live chain
breaks. If the encoder no longer retains that LTR, it can ignore the request.

This is intentionally advisory. It is an actuator for `meld-auto`, not a second
profile.

## Configuration

Start with:

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1
cfg.BufferMicros = 75_000
```

Important fields:

| Field | Default | Use |
|---|---:|---|
| `Flow` | `0` | Wire flow id. Both ends must match. |
| `SymbolSize` | `1316` | Maximum application chunk and fixed algebraic width. Systematic packets carry only the bytes written. Use `MaxChunk()` when encrypted. |
| `BufferMicros` | `200000` | Playout/deadline budget. This is the main latency knob. |
| `Sliding` | `true` | Default low-latency sliding-window coder. |
| `CodingWindow` | `0` | Max sliding band width. `0` selects the internal default. |
| `Redundancy` | `0.15` | Floor proactive repair rate. The controller raises it as needed. |
| `TargetFailure` | `1e-3` | Decode-failure target used by repair sizing. |
| `MaxBitrate` | `0` | Aggregate media+repair ceiling. `0` selects the host default. |
| `RepairWithinBudget` | `true` | Keep proactive repair inside the rate budget. |
| `CongestionControl` | `false` | Derive the sliding or generation sender's total rate budget from delay and ECN. Sliding startup is seeded from its measured media-plus-recovery offer. |
| `Pace` | `true` | Smooth datagrams to the budget and backpressure `Write`. |
| `AutoReorderHoldoff` | `true` | Receiver adapts loss estimation under reorder. |
| `Passphrase` | empty | Enables encrypted sessions. |
| `ProbeMTU` | `false` | Enables DPLPMTUD discovery and black-hole reporting. |

Generation-mode controls (`GenSize`, `AutoGenSize`, `AdaptiveGenSize`,
`NominalRTTMicros`, `NominalBitrateBps`) remain for the generation fallback and
multipath path. The default media path is sliding.

For a full integration reference, see [Integration](docs/integration.md).

## Encryption

Set the same passphrase on both ends:

```go
cfg.Passphrase = os.Getenv("MELD_PASSPHRASE")
```

Meld runs a hybrid X25519 + ML-KEM-768 handshake and encrypts source chunks before
coding them. Repair symbols are linear combinations of ciphertext bytes, so
coded recovery and relay recoding do not require plaintext access.

Encrypted chunks have 16 bytes less media payload because of the AEAD tag. Use
`cfg.MaxChunk()` for sender buffers. See [Encryption](docs/encryption.md).

## Custom Datagram Substrates

UDP is the built-in host, but the public API also supports caller-provided
datagram transports:

```go
s, err := meld.NewSenderOver(substrate, cfg)
r, err := meld.NewReceiverOver(substrate, cfg)
```

The substrate interface is the datagram subset of `net.PacketConn`:
`ReadFrom`, `WriteTo`, `LocalAddr`, and `Close`. This is the seam for in-process
tests, WebTransport-style hosts, and other datagram runtimes. See
[Substrate](docs/substrate.md).

## Multipath

Meld also has a coding-native multipath API:

```go
ms, err := meld.NewMultipathSender([]string{pathA, pathB}, cfg)
mr, err := meld.NewMultipathReceiver([]string{bindA, bindB}, cfg)
```

Multipath currently uses the generation core internally. It is diversity coding,
not packet duplication: source and repair symbols are spread across paths and
decoded from their union.

## Repository Map

| Path | Purpose |
|---|---|
| `meld.go` | Public API. |
| `internal/flow` | Sans-I/O sender/receiver state machines and control loops. |
| `internal/code` | RLNC encoder/decoder and band decoder. |
| `internal/gf` | GF(2^8) arithmetic and SIMD multiply-add. |
| `internal/session` | UDP/custom-substrate host, pacer, timers, crypto, DPLPMTUD. |
| `internal/shape` | Internal media shapers used by tests and glassbench. |
| `internal/wire` | Symbol, feedback, handshake, and control encodings. |
| `cmd/glassbench` | Glass-to-glass benchmark harness against SRT, RIST, and oracles. |
| `docs` | Protocol, integration, benchmark, media, and wire documentation. |

## Documentation

Start here:

- [Documentation Index](docs/README.md)
- [Protocol](docs/protocol.md)
- [Specification](docs/spec/README.md)
- [Integration](docs/integration.md)
- [Benchmarking](docs/bench.md)
- [Coding](docs/coding.md)
- [Media Awareness](docs/media-awareness.md)
- [Wire Format](docs/wireformat.md)
- [Encryption](docs/encryption.md)
- [Substrate](docs/substrate.md)

## Build And Test

```sh
go test ./...
go test -race ./...
go run ./cmd/glassbench -h
```

Some glassbench arms require SRT/RIST tools available on the host. Generated
benchmark output belongs under `scratchpad/`, which is ignored by git.

## Status

Meld is an active research/prototype codebase with a real benchmark harness and
one automatic recovery path. The current direction is:

1. keep one adaptive profile
2. choose recovery from measured physical opportunity rather than operator modes
3. use deterministic simulation and macro frontier runs as gates
4. preserve oracle rows so we know whether the protocol, the source structure, or
   the benchmark ceiling is responsible for a gap

The formal protocol artifact now lives in [docs/spec](docs/spec/README.md). It
should track the implementation closely as the wire/control terminology settles.
