# Meld

*Media Erasure-coded Live Delivery*

**A coded, media-aware, survivable transport for live and contribution-grade
video.** Meld is a clean-sheet sibling to my SRT (`srtgo`/`srtrust`)
and RIST (`ristgo`/`ristrust`) stacks. It exists to answer one question: *given
everything we learned building SRT and RIST twice each, and a deterministic
impairment lab to prove it — what does a transport designed for **survivability**
from a blank page look like?*

Every wire packet is a **coded symbol** over a sliding window of source media.
Recovery needs *any k-of-n* symbols, not the specific missing packet, so loss
recovery is decoupled from the RTT chain that makes named-packet ARQ fall over at
high loss × RTT. Feedback **tightens** the repair rate; it does not **gate**
recovery on round trips. Multipath is one erasure channel spread across paths —
**diversity, not duplication**. Redundancy is allocated by **deadline and media
importance**, protecting the bytes the *picture* depends on. The whole flow is a
**sans-I/O deterministic state machine** so an oracle can score the exact
recoverable set.

**Unpacking the jargon above** — each phrase points at concrete code, design notes, and specs:

- **coded symbol over a sliding window**, **recovery from any _k_-of-_n_ symbols** — sliding-window
  random linear network coding (RLNC) over GF(2⁸): [`internal/code`](internal/code) and
  [`docs/coding.md`](docs/coding.md). The seeded-coefficient sliding-window RLC lineage is
  [RFC 8681](https://www.rfc-editor.org/rfc/rfc8681); the alternative block code is RaptorQ
  ([RFC 6330](https://www.rfc-editor.org/rfc/rfc6330)).
- **feedback _tightens_ the repair rate but does not _gate_ recovery on round trips** — the
  feed-forward redundancy controller (`repairForTarget` sizes proactive repair to a target
  decode-failure probability; the rank-deficit feedback only trims a reactive residual):
  [`internal/flow`](internal/flow), [`docs/coding.md`](docs/coding.md).
- **multipath is one erasure channel spread across paths — diversity, not duplication** — a
  generation is spread over the paths and decoded from their union, sized by a correlation-aware
  joint-tail estimator (`repairForJointTailN`): [`internal/flow/multipath.go`](internal/flow/multipath.go).
- **redundancy allocated by deadline and media importance** — unequal error protection steered by
  per-codec shapers that read the codec's dependency structure
  ([AV1 Dependency Descriptor](https://aomediacodec.github.io/av1-rtp-spec/#dependency-descriptor-rtp-header-extension);
  [RFC 6184](https://www.rfc-editor.org/rfc/rfc6184) / [RFC 7798](https://www.rfc-editor.org/rfc/rfc7798)
  for AVC/HEVC): [`internal/shape`](internal/shape), [`docs/media-awareness.md`](docs/media-awareness.md).
- **sans-I/O deterministic state machine scored by an oracle** — the core never reads a clock or
  opens a socket ([sans-I/O](https://sans-io.readthedocs.io/)), and a rank oracle bounds the
  recoverable set it may ever claim: [`internal/flow`](internal/flow) and
  [`internal/code`](internal/code) (the `TestRankOracle` property).

> **Meld** is a working codename (you weave coded threads across paths into one
> fabric). Rename freely. The Go module path is `github.com/zsiec/meld`.

---

## Contents

- [The bet](#the-bet) — where SRT/RIST structurally cap out
- [Pillars](#pillars) — the six things Meld does differently
- [Architecture](#architecture) — the three layers and the narrow waist
- [Quick start](#quick-start) — install, send, receive, encrypt, bond
- [Configuration](#configuration) — the `Config` reference
- [Public API](#public-api) — the surface of the `meld` package
- [Media awareness](#media-awareness) — the per-codec shapers
- [Encryption](#encryption) — the post-quantum hybrid crypto layer
- [Wire format](#wire-format) — the on-wire Symbol and Feedback
- [Substrate](#substrate) — UDP today, QUIC-DATAGRAM behind a seam
- [Benchmarks](#benchmarks) — status (no published claims yet)
- [Project layout](#project-layout) — the package map
- [Build & test](#build--test) — make targets, race, fuzz
- [Status & roadmap](#status--roadmap) — what ships, what's deferred
- [Further reading](#further-reading) — the full docs index
- [References](#references) — the specs cited in the code
- [Dependencies & license](#dependencies--license)

---

## The bet

SRT and RIST are the same animal: RTP-ish/UDP + sequence numbers + **named-packet
ARQ** + **rigid block FEC** + **duplicate-everything bonding** (ST 2022-7). Five
structural gaps, each instrumented in the lab:

1. **ARQ is RTT-bound and 1-for-1.** Each NACK recovers one named packet at a cost
   of ≥1 round trip; a lost retransmit costs another. As loss × RTT × bitrate
   grows you run out of round trips before the playout deadline.
2. **Block FEC is rigid and latency-heavy.** A fixed L×D matrix recovers a fixed
   count per block, pays its overhead whether or not loss happens, can't ride a
   burst longer than its interleave, and forces a full-block wait.
3. **Bonding is duplication, not diversity.** ST 2022-7 costs N× bandwidth to
   survive one path dying and gains nothing when two paths are merely lossy — even
   though the information that arrived may sum to more than 100%.
4. **Recovery is packet-centric, not picture-centric.** Both retransmit a B-frame
   as hard as an SPS/PPS or an IDR slice. Decodable-keyframe %, time-to-first-frame,
   and A/V skew — the metrics that matter — are left to luck.
5. **Loss vs congestion is ambiguous; session identity is fragile.** Loss-as-
   congestion on wireless, NAT rebind, interface flap.

Meld collapses ARQ + FEC + bonding into **one coded primitive** and adds a
delay-based congestion signal that survives loss-masking. The architecture is
detailed in the sections below and the deep-dive notes under [`docs/`](docs).

---

## Pillars

1. **Coded recovery substrate** — sliding-window network coding (RLNC over GF(2⁸),
   RFC 8681 lineage) with a SIMD-accelerated multiply-accumulate (NEON on arm64,
   AVX2 on amd64, each byte-for-byte identical to the scalar golden reference).
   Recovery needs *any k-of-n* symbols, not the specific missing packet; ARQ
   degenerates to a feedback-tightened fallback driven by the receiver's rank
   deficit. Two coders ship behind one waist: a **generation** coder (the default
   profile, for a generous latency budget) and a **band-form sliding-window** coder
   (`Config.Sliding`, for budgets tighter than the RTT). See
   [`docs/coding.md`](docs/coding.md) and [`docs/sliding-window.md`](docs/sliding-window.md).
2. **Coding-native multipath** — N paths as one erasure channel; the receiver
   measures the **cross-path erasure correlation** and the sender sizes repair
   against the **joint** tail, so two lossy paths add diversity by *pooling* coded
   symbols rather than duplicating every packet the way ST 2022-7 bonding does.
3. **Importance- & deadline-aware unequal protection (UEP)** — a thin per-codec
   shaper tags each access unit with a generic `(priority, deadline, dependency)`
   descriptor; the codec-blind core steers a fixed repair budget up the dependency
   spine (parameter sets, IDR/IRAP, base layers) and lets disposable detail degrade
   gracefully, optimizing **decodable-frame %**, not raw byte recovery. Broken
   sub-trees are evicted early so the next keyframe resyncs sooner. See
   [`docs/media-awareness.md`](docs/media-awareness.md).
4. **Loss-agnostic congestion control + L4S/ECN** — coding masks loss, so loss is
   not a usable congestion signal; a delay-based, Copa-style controller owns the
   send-rate budget and the redundancy sizer allocates repair *within* it (never on
   top). An L4S/DCTCP response to ECN **CE** marks rides delivered packets, the one
   congestion signal orthogonal to coding. Plus a pre-recovery wire-loss counter as
   the honest signal, and resource-safety admission caps + a token bucket. Opt-in;
   see [`docs/research-2026.md`](docs/research-2026.md) §N1/N3.
5. **Modern, survivable encryption** — an X25519 + ML-KEM-768 **hybrid
   post-quantum** handshake (Noise NNpsk0), ChaCha20-Poly1305 AEAD with
   encrypt-then-code, per-epoch forward-secret key ratcheting, authenticated
   control plane, and a WireGuard-style return-routability cookie. One config knob
   (`Passphrase`). See [`docs/encryption.md`](docs/encryption.md).
6. **Sans-I/O deterministic core** — the GF coder, redundancy sizer, and path
   scheduler are pure functions; the whole flow (`internal/flow`) is a
   deterministic state machine that never reads a clock, opens a socket, or spawns a
   goroutine. A CI-enforced import gate keeps it that way. This is the only way a
   coded transport stays testable — and it is the moat.

---

## Architecture

Three layers, split at a **narrow waist** (`wire.Symbol`). Media awareness lives
*above* the waist; the core is codec-blind; the host below it is a dumb pump with
no protocol logic.

```
            ┌────────────────────────────────────────────────────────┐
  media in  │  SHAPER  (internal/shape, per codec: AV1/HEVC/AVC/JXS)  │
  ────────► │  access units → symbols + generic                      │
            │  (priority, deadline, dependency) descriptor            │
            └───────────────────────────┬────────────────────────────┘
                                        │  wire.Symbol  (the narrow waist)
            ┌───────────────────────────▼────────────────────────────┐
            │  CORE  (internal/flow) — pure, deterministic.           │
            │  No clock, no socket, no goroutine. Inputs are typed    │
            │  methods; outputs are drained effect/event queues.      │
            │   • coder (sliding-window / generation RLNC, pure GF)   │
            │   • redundancy sizer (variance- & burst-aware)          │
            │   • congestion controller (delay-based + L4S/ECN)       │
            │   • multipath scheduler + co-loss estimator             │
            │   • window/deadline manager (evict past-deadline)       │
            └───────────────────────────┬────────────────────────────┘
                                        │  effects: SendSymbol / SetTimer /
                                        │  AdaptRate / Deliver / PathDead …
            ┌───────────────────────────▼────────────────────────────┐
            │  HOST  (internal/session) — owns RealClock, timer wheel,│
            │  goroutines, the UDP substrate, pacer, DPLPMTUD, clock  │
            │  sync, crypto. A thin pump; no protocol logic.          │
            └─────────────────────────────────────────────────────────┘
```

- **Sans-I/O core** (`internal/flow`) — encode/decode over the coding window,
  dedup, in-order delivery, deadline eviction, redundancy control, feedback
  cadence, and multipath merge. Time enters only via explicit `now` arguments.
- **The coder** (`internal/code` over `internal/gf`) — pure functions over GF(2⁸):
  systematic + repair symbols, on-the-fly Gaussian elimination, a band-form decoder
  with O(1) window advance and O(b²) per-symbol decode.
- **The host** (`internal/session`) — `clock.RealClock`, the timer wheel, the
  goroutines, the swappable `Substrate` datagram seam, the transmit pacer, DPLPMTUD,
  the cross-host clock-offset handshake, and the crypto layer.

The effects/feedback contract is the typed method/queue surface of `internal/flow`;
the sans-I/O import gate is enforced in CI by `make check-core-imports`.

---

## Quick start

```
go get github.com/zsiec/meld
```

Requires Go 1.25+.

### Sender

```go
package main

import (
	"log"

	"github.com/zsiec/meld"
)

func main() {
	cfg := meld.DefaultConfig() // 1316-byte chunks, generation 16, 200 ms budget
	cfg.Flow = 1                // both ends must agree on Flow

	s, err := meld.NewSender("203.0.113.10:5000", cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	buf := make([]byte, cfg.MaxChunk()) // SymbolSize, minus the AEAD tag if encrypted
	for {
		n, err := readMediaChunk(buf) // your packetizer (e.g. an MPEG-TS slice)
		if err != nil {
			break
		}
		if _, err := s.Write(buf[:n]); err != nil {
			log.Fatal(err)
		}
	}
	s.Flush() // protect and emit the final partial generation
}
```

### Receiver

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1

r, err := meld.NewReceiver(":5000", cfg)
if err != nil {
	log.Fatal(err)
}
defer r.Close()

buf := make([]byte, cfg.SymbolSize)
for {
	n, err := r.Read(buf) // blocks until the next in-order chunk decodes
	if err != nil {
		break
	}
	writeMediaChunk(buf[:n]) // your depacketizer/decoder
}

st := r.Stats() // Delivered / Lost / Recovered / WireLost / …
```

`Read` returns chunks **in order, byte-exact**, recovering loss from repair without
ever naming a missing packet. Both ends must agree on `Flow`, `SymbolSize`,
`GenSize`, and `BufferMicros`.

### Encrypted

Set the same `Passphrase` on both ends — nothing else changes. The Sender and
Receiver run the hybrid post-quantum handshake before any media and AEAD-seal every
chunk (`NewSender` blocks until the channel is established):

```go
cfg.Passphrase = "correct horse battery staple"
cfg.Salt = "my-deployment-v1" // domain-separate distinct deployments
```

### Multipath (coding-native bonding)

Spread one coded flow across two paths and decode from the union — diversity, not
duplication:

```go
s, _ := meld.NewMultipathSender(
	[]string{"a.example:5000", "b.example:5000"}, cfg)
r, _ := meld.NewMultipathReceiver(
	[]string{":5000", ":5001"}, cfg)
```

### Media-aware unequal protection

Feed the per-access-unit descriptor a shaper produces (see
[`internal/shape`](internal/shape)) and the core protects the picture, not just the
bytes — and the receiver reports parse-free decodable-frame stats:

```go
s.WriteFrame(chunk, meld.FrameDesc{
	Priority:    7,                 // 0 = disposable … higher = protect harder
	FrameID:     42,
	RefFrameIDs: []uint32{40, 44},  // a B-frame's two anchors
	Chunks:      3,
	RAP:         true,              // a keyframe / random-access point
})
// ... on the receiver:
fs := r.FrameStats() // DecodableFrames / DecodableKeyframes / …
```

---

## Configuration

`meld.Config` (constructed via `meld.DefaultConfig()` and overridden). Both ends
must agree on `Flow`, `SymbolSize`, `GenSize`, and `BufferMicros`; the crypto and
multipath fields must also match.

| Field | Default | Meaning |
|---|---|---|
| `Flow` | `0` | Flow identifier on the wire. |
| `SymbolSize` | `1316` | Fixed media-chunk / coded-symbol size (bytes). 1316 = 7×188 MPEG-TS. |
| `GenSize` | `16` | Coding generation (window) size in source symbols. |
| `Redundancy` | `0.15` | **Floor** proactive code rate (repair per source symbol); the controller raises it as measured loss requires. |
| `TargetFailure` | `1e-3` | Per-generation decode-failure probability the sizer targets (the QoS knob). |
| `BufferMicros` | `200000` | Playout / deadline budget (µs). |
| `Sliding` | `false` | Use the band-form sliding-window coder instead of generations. Reach for it when the budget is **tighter than the RTT** (low-latency long-haul); see [`docs/sliding-window.md`](docs/sliding-window.md). |
| `CodingWindow` | `0` (auto) | Max sliding band width (source symbols); the sender adapts the span below it to fit the deadline. Ignored unless `Sliding`. |
| `CongestionControl` | `false` | Enable the delay-based controller that owns the send-rate budget (repair throttled within it). Leave off until validated on your paths. |
| `Pace` | `true` | Host transmit pacer: smooth coded datagrams to the budget and backpressure `Write` past the deadline, so a keyframe burst isn't dumped as a microburst. |
| `MaxBitrate` | `0` (≈100 Mbps) | Cap on the aggregate emitted rate (bits/s); media is never dropped, repair is throttled to hold the ceiling. |
| `EvictBrokenFrames` | `false` | Media-aware early eviction of undecodable frames + their dead sub-trees (requires `WriteFrame`). |
| `Passphrase` | `""` | Enable encryption (empty ⇒ cleartext). See [Encryption](#encryption). |
| `Salt` | `""` | Domain-separates the Argon2id passphrase stretch. Distinct deployments → distinct salts. |
| `Argon2Time` / `Argon2MemoryKiB` / `Argon2Threads` | `3` / `65536` / `4` | Argon2id work factor (RFC 9106). |
| `EpochSize` | `0` (default) | Source symbols sealed per epoch key before the forward-secrecy ratchet. |
| `CookieThreshold` | `0` (dormant) | Handshake attempts/tick above which the return-routability cookie engages. |

`Config.MaxChunk()` returns the largest chunk `Write` accepts (`SymbolSize`, or
`SymbolSize − 16` when a `Passphrase` is set, for the AEAD tag).

---

## Public API

The entire surface lives in the [`meld`](meld.go) package — a thin facade over the
sans-I/O core and the UDP host.

**Single path**

- `NewSender(remote string, cfg Config) (*Sender, error)` — `Write`, `WriteUnit`,
  `WriteFrame`, `Flush`, `Stats`, `Close`.
- `NewReceiver(bind string, cfg Config) (*Receiver, error)` — `Read`,
  `SetReadDeadline`, `LocalAddr`, `Stats`, `FrameStats`, `Close`.

**Multipath**

- `NewMultipathSender(remotes []string, cfg Config) (*MultipathSender, error)`
- `NewMultipathReceiver(binds []string, cfg Config) (*MultipathReceiver, error)`

**Write variants** (sender side)

- `Write(p []byte)` — one chunk at the base protection tier.
- `WriteUnit(p []byte, priority uint8)` — carry a protection tier (UEP).
- `WriteFrame(p []byte, fd FrameDesc)` — carry the full access-unit descriptor
  (tier + dependency) so the receiver computes decodable-frame stats parse-free.

**Telemetry**

- `SenderStats{ Source, Repair, ReactiveRepair, Throttled }`
- `ReceiverStats{ Delivered, Lost, Recovered, Duplicates, WireLost, Rejected, Evicted }`
- `FrameStats{ Frames, DecodableFrames, Keyframes, DecodableKeyframes }`

---

## Media awareness

Meld is explicitly built for **AV1, HEVC, AVC, and JPEG XS**. A thin per-codec
**shaper** (`internal/shape`) sits *above* the media-blind core and maps each codec
access unit to **one generic descriptor** — modeled on the AV1 Dependency
Descriptor so the core stays codec-agnostic. The shapers parse only headers (no
entropy/slice decode) and resolve each frame's dependencies **exactly**: AVC/HEVC
POC bracketing of a B-picture's two anchors, AV1's eight-slot reference buffer,
JPEG XS intra-exact. The dependency model is proven **glass-to-glass** against
`ffprobe` — a real decoder decodes exactly the model's predicted decodable picture
set.

- [`docs/media-awareness.md`](docs/media-awareness.md) — the index, the canonical
  descriptor, and the cross-codec view.
- [`docs/media/av1.md`](docs/media/av1.md) — AV1 (the richest survivability
  structure; the descriptor was designed here).
- [`docs/media/hevc.md`](docs/media/hevc.md) — HEVC (H.265).
- [`docs/media/avc.md`](docs/media/avc.md) — AVC (H.264; the weakest-signaling
  floor the core must tolerate).
- [`docs/media/jpegxs.md`](docs/media/jpegxs.md) — JPEG XS (intra-only mezzanine /
  ST 2110-22; spatial/quality UEP).

---

## Encryption

A clean-sheet protocol gets to skip the crypto debt SRT and RIST carry. Meld's
layer (enabled by `Config.Passphrase`) is:

- **Hybrid post-quantum handshake** — Noise **NNpsk0** with an **X25519 +
  ML-KEM-768** combiner: forward-secret against a classical attacker today and a
  harvest-now-decrypt-later quantum one tomorrow.
- **AEAD** — ChaCha20-Poly1305, **encrypt-then-code** (seal before the network
  code), with a structured nonce (epoch ‖ source index).
- **Forward secrecy within a session** — per-epoch HKDF key ratcheting.
- **Authenticated, replay-protected control plane** — feedback, clock probes, and
  MTU acks are MAC'd with a sliding replay window.
- **Anti-amplification** — a WireGuard-style mac2 return-routability cookie under
  handshake flood (`CookieThreshold`).
- **Passphrase hardening** — Argon2id (RFC 9106) stretch, salted per deployment.

The full decision record — threat model, why each primitive beats the SRT/RIST
baseline, and the nonce/epoch construction — is in
[`docs/encryption.md`](docs/encryption.md). The handshake message bytes are pinned
in [`docs/wireformat.md`](docs/wireformat.md).

---

## Wire format

The on-wire encoding of the narrow waist (`internal/wire`) is **pinned at v1** and
documented authoritatively in [`docs/wireformat.md`](docs/wireformat.md). Highlights:

- A **version nibble** in the leading byte — an unknown version yields `ErrVersion`,
  never silent garbage — with a tail-extension policy so new fields don't collide.
- The **Symbol** (media-bearing): flow, kind (systematic/repair), coding-window
  base, source index, generation width, per-symbol deadline, a host-stamped
  `path_id`, and flag-gated extensions (send timestamp, frame descriptor).
- The **Feedback** report (receiver → sender): decoded low edge, rank deficit, loss
  rate, RTT samples, ECN CE count, and per-path marginal + co-loss histograms.
  Feedback is **cumulative and idempotent** — a lost report costs nothing.
- The codec **never panics** on malformed input (fuzz-enforced); short, mis-typed,
  or mis-versioned buffers return sentinel errors.

All multi-byte fields are big-endian.

---

## Substrate

The host runs over a small `Substrate` datagram seam (the datagram subset of
`net.PacketConn`). The shipping core is **pure-stdlib UDP**; a **QUIC-DATAGRAM**
adapter is a few-line opt-in behind the same seam. UDP was chosen for the core
because a QUIC datagram is itself congestion-controlled by QUIC's CC, which would
double-control the rate beneath Meld's own delay-based controller — and Meld's
coding already provides QUIC's main service (loss/reorder resilience). The
rationale and the A/B comparison are in [`docs/substrate.md`](docs/substrate.md).

---

## Benchmarks

**No performance claims yet.** Meld is validated for *correctness* — the four
delivery invariants (no duplicate delivered, in-order output, nothing past
deadline, completeness under recoverable loss), the decoder's agreement with an
independent rank oracle, and glass-to-glass dependency resolution confirmed against
a real decoder.

Comparative **performance** numbers are deliberately **not published here.** The
figures produced so far are single-host loopback and have not been validated
cross-host or against the canonical SRT/RIST/Zixi stacks on real networks, so this
README makes no throughput, latency, or delivery claims. Reproducible,
independently verifiable benchmarks are planned; see
[`docs/bench.md`](docs/bench.md).

---

## Project layout

```
meld.go              public API facade (the meld package)
e2e_test.go          end-to-end tests over real UDP sockets
internal/
  gf/                GF(2⁸) field arithmetic + SIMD AXPY (NEON / AVX2)
  code/              sliding-window RLNC: encoder, RREF + band-form decoders, rank oracle
  flow/              the sans-I/O core: sender, receiver, congestion, ecn, sliding, multipath
  wire/              the Symbol + Feedback codec (the narrow waist)
  clock/             clock.Timestamp + RealClock / Manual seam
  crypto/            hybrid PQ handshake, AEAD, KDF, control-plane auth, cookies
  session/           the UDP host: pacer, pmtud, multipath sockets, clock sync, secure
  shape/             per-codec shapers (AV1 / HEVC / AVC / JPEG XS) + dependency resolver
docs/                the deep-dive design notes (see Further reading)
```

The sans-I/O core (`internal/flow`) imports only `internal/{clock,wire,code,gf}` +
the standard library — no substrate, crypto, or shaper. `make check-core-imports`
asserts it in CI.

---

## Build & test

```
make build               # go build ./...
make lint                # gofmt check + go vet ./...
make test                # go test -race -count=1 -timeout 120s ./...
make bench               # go test -bench=. -benchmem ./...
make check-deps          # dependency allowlist gate (stdlib + x/{crypto,net,sys})
make check-core-imports  # internal/flow import gate
go test -fuzz=FuzzXxx ./internal/<pkg>   # run a fuzz target (code, wire, shape)
```

Everything runs under `-race`. The coder, wire codec, and every shaper are fuzzed
for no-panic-on-arbitrary-input; the decoder is checked against an independent
**rank oracle** (it may never claim recovery the window rank doesn't support).

---

## Status & roadmap

**The coded core ships, with the deployability and media-aware layers built and
tested.** Done and tested under `go test -race`:

- **Coder** (`internal/{gf,code}`) — sliding-window + generation RLNC over GF(2⁸),
  SIMD AXPY, rank oracle, fuzzed.
- **Sans-I/O core** (`internal/flow`) — in-order delivery, erasure recovery,
  deadline eviction, variance- & burst-aware redundancy sizing, delay-based
  congestion control with L4S/ECN, and media-aware UEP with early eviction.
- **Coding-native multipath** — correlation-aware N-path joint-tail sizing + co-loss
  estimation, wired end-to-end over an N-socket host.
- **Media shapers** (`internal/shape`) — AVC/HEVC/AV1/JPEG XS with exact dependency
  resolution, validated glass-to-glass against `ffmpeg`.
- **Crypto** (`internal/crypto`) — the full hybrid PQ handshake + AEAD + ratchet +
  authenticated control plane.
- **Host** (`internal/session`) — UDP pump over a swappable `Substrate`, transmit
  pacer, DPLPMTUD (RFC 8899), cross-host clock-offset handshake.

**Deferred / honest gaps:**

- The **QUIC-DATAGRAM** substrate adapter (designed; UDP ships).
- Reading real **ECN CE** marks off the socket is **Linux-only** (the macOS dev box
  can't surface TOS via `x/net/ipv4`); the in-core L4S response is proven, the
  kernel read is deferred.
- The **sliding** coder accepts but ignores the WP6 frame descriptor (UEP runs on
  the generation coder).
- **Relay recoding** across multiple hops (single-endpoint diversity ships).
- **Cross-host** benchmarks (loopback numbers today).

---

## Further reading

| Doc | What it covers |
|---|---|
| [`docs/coding.md`](docs/coding.md) | The erasure-code family; why RLNC, why not RaptorQ / streaming codes. |
| [`docs/sliding-window.md`](docs/sliding-window.md) | The band-form low-latency profile and its niche. |
| [`docs/media-awareness.md`](docs/media-awareness.md) | The generic descriptor + cross-codec UEP model. |
| [`docs/media/{av1,hevc,avc,jpegxs}.md`](docs/media) | Per-codec shaper design. |
| [`docs/wireformat.md`](docs/wireformat.md) | The pinned v1 on-wire Symbol + Feedback. |
| [`docs/encryption.md`](docs/encryption.md) | The post-quantum hybrid crypto decision record. |
| [`docs/substrate.md`](docs/substrate.md) | UDP vs QUIC-DATAGRAM, settled empirically. |
| [`docs/bench.md`](docs/bench.md) | Benchmark status — correctness bar; performance numbers withheld pending validation. |
| [`docs/research-2026.md`](docs/research-2026.md) | The architecture review + next-build decision report. |

---

## References

Cited in the code by spec section (never by library file path — the ristgo rule):
RFC 8681 (sliding-window RLC FEC), RFC 9407 (Tetrys on-the-fly coding), RFC 6330
(RaptorQ), RFC 9221 (QUIC DATAGRAM), RFC 9000 (QUIC), RFC 9330/9331/9332 (L4S /
ECN / DualPI2), RFC 8899 (DPLPMTUD), RFC 9265 (FEC & congestion control), RFC 2914
(congestion-collapse principles), RFC 8083 (RTP circuit breaker), Copa (NSDI'18),
RFC 9106 (Argon2), RFC 7798 (HEVC RTP), RFC 6184 (AVC RTP), RFC 9134 (JPEG XS RTP)
+ ST 2110-22, and the AV1 Dependency Descriptor. Each is cited inline in the code
(by spec section) and the relevant `docs/` note.

---

## Dependencies & license

**Dependencies:** the Go standard library, **`golang.org/x/crypto`** (the AEAD,
X25519, ML-KEM, and Argon2 primitives behind the encryption layer), and its
transitive **`golang.org/x/sys`** (CPU-feature detection for the SIMD AEAD / Argon2
paths). Nothing else — the allowlist is CI-enforced by `make check-deps`. The
sans-I/O core itself imports only the standard library + sibling `internal`
packages.

**License:** [MIT](LICENSE).
