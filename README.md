# Meld

Meld is a low-latency coded media transport for live video. It sends media as
systematic source symbols plus sliding-window repair symbols, so the receiver can
recover from erasures without waiting for a named-packet retransmission to make a
round trip.

The current product direction is deliberately narrow:

- one deployable profile: `meld-auto`
- automatic adaptation, not a menu of profiles
- source mostly FIFO
- repair count capped by the configured rate budget
- proactive coded repair where latency is too tight for ARQ
- conservative fallback behavior when the channel is uncertain

The strongest measured frontier is not "Meld beats SRT/RIST everywhere." The
credible frontier is **iid or tail-erasure loss at tight playout budgets,
especially when the budget is below one RTT**. In that region, coded repair can
land before an ARQ round trip can return. As the budget grows to one RTT and
beyond, SRT/RIST catch up and the advantage compresses.

## Current Frontier Result

The latest curated macro run compared the same encoded source across Meld, SRT,
RIST, and oracle rows:

```sh
go run ./cmd/glassbench -macrofrontier -buf 0 \
  -arms oracle-source,oracle-ideal,meld-auto,libsrt,librist \
  -frontierlosses 0.05,0.10 \
  -frontierbursts 0 \
  -rtts 50,100,200 \
  -frontiermults 0.75,1,1.5 \
  -reps 8 \
  -reportdir scratchpad/glassbench-results/frontier-iid-samesource-oracles-reps8-20260627
```

Summary:

| Cell | Meld | Best ARQ | Delta | Notes |
|---|---:|---:|---:|---|
| 10% iid loss, 0.75x RTT budget, RTT 50/100/200 ms | 144.0 frames | 121.1 frames, RIST | +22.9 frames | Meld reached the source ceiling in all three RTT cells. |
| 5% iid loss, 0.75x RTT budget, RTT 50/100/200 ms | 143.8-144.0 frames | 133.6 frames, RIST | about +10 frames | Stable win across RTTs. |
| 5-10% iid loss, 1x RTT budget | 144.0 frames in several cells | SRT usually best ARQ | small single-digit gains | ARQ starts to catch up. |
| 5-10% iid loss, 1.5x RTT budget | near ceiling | SRT/RIST near ceiling | no stable Meld advantage | Budget is generous enough for ARQ. |

No stable Meld deficits appeared in that iid frontier run.

The burst48/burst96 experiments did not produce a stable deployable target.
Repair placement, refresh-island repair, and fixed-keyint source shortcuts are not
kept as user-facing profiles. See [Benchmarking](docs/bench.md) and the decision
notes in [docs/decisions](docs/decisions).

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

1. **Application/media layer**: writes fixed-size media chunks. It may attach a
   `FrameDesc` describing priority, references, RAP/recovery-refresh markers,
   temporal layer, and discardability.
2. **Sans-I/O flow core**: emits source and repair symbols, tracks deadlines,
   estimates loss/burstiness/reorder, sizes repair, and decodes from any
   sufficient rank. The core does not open sockets or read clocks.
3. **Session host**: owns UDP sockets or a caller-provided datagram substrate,
   pacing, timers, encryption, DPLPMTUD, and goroutines.

The default sender uses the band-form sliding-window coder:

- source symbols are sent immediately
- repair symbols cover an elastic trailing window
- feedback adjusts future repair rate and sends reactive top-up when useful
- `BufferMicros` is the playout deadline
- `MaxBitrate` and `RepairWithinBudget` keep repair inside the rate budget
- the host pacer smooths output and backpressures `Write` instead of queueing
  media past its deadline

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

## Encoder Cadence Actuator

Meld does not expose multiple deployable profiles for source structure. Instead,
the sender can produce an advisory encoder request:

```go
ctrl := s.EncoderControl()
if ctrl.RecoveryCadenceFrames != 0 {
	encoder.SetMaxRecoveryInterval(int(ctrl.RecoveryCadenceFrames))
}
```

`RecoveryCadenceFrames` asks the encoder to shorten dependency damage with a
bounded recovery point cadence. Encoders may implement this with intra-refresh,
recovery-point SEI, or keyframes. If the encoder cannot comply, Meld continues
with the same transport loop.

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
| `SymbolSize` | `1316` | Fixed coded-symbol payload size. Use `MaxChunk()` when encrypted. |
| `BufferMicros` | `200000` | Playout/deadline budget. This is the main latency knob. |
| `Sliding` | `true` | Default low-latency sliding-window coder. |
| `CodingWindow` | `0` | Max sliding band width. `0` selects the internal default. |
| `Redundancy` | `0.15` | Floor proactive repair rate. The controller raises it as needed. |
| `TargetFailure` | `1e-3` | Decode-failure target used by repair sizing. |
| `MaxBitrate` | `0` | Aggregate media+repair ceiling. `0` selects the host default. |
| `RepairWithinBudget` | `true` | Keep proactive repair inside the rate budget. |
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
| `cmd/glassbench` | Glass-to-glass benchmark harness against SRT/RIST and oracles. |
| `docs` | Protocol, integration, benchmark, media, and decision notes. |

## Documentation

Start here:

- [Documentation Index](docs/README.md)
- [Protocol](docs/protocol.md)
- [Integration](docs/integration.md)
- [Benchmarking](docs/bench.md)
- [Coding](docs/coding.md)
- [Media Awareness](docs/media-awareness.md)
- [Wire Format](docs/wireformat.md)
- [Encryption](docs/encryption.md)
- [Substrate](docs/substrate.md)

Decision notes:

- [Macro frontier discovery](docs/decisions/2026-06-27-macro-frontier-discovery.md)
- [Cleanup after frontier discovery](docs/decisions/2026-06-27-cleanup-after-frontier.md)

## Build And Test

```sh
go test ./...
go test -race ./...
go run ./cmd/glassbench -h
```

Some glassbench arms require SRT/RIST tools available on the host. Generated
benchmark output belongs under `scratchpad/`, which is ignored by git.

## Status

Meld is an active research/prototype codebase with a real benchmark harness and a
clear current frontier. The default direction is no longer "try every possible
coding trick." It is:

1. keep one adaptive profile
2. optimize the tight-latency iid/tail-erasure frontier deliberately
3. use macro frontier runs as the gate
4. preserve oracle rows so we know whether the protocol, the source structure, or
   the benchmark ceiling is responsible for a gap

The next formal artifact should be a protocol specification. A LaTeX spec is a
good fit once the docs and wire/control terminology settle.
