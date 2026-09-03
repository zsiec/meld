# Integration Guide

This guide is for someone adding Meld to an application. It focuses on the public
Go API in `meld.go`.

## Install

```sh
go get github.com/zsiec/meld
```

## Basic Byte-Stream Mode

Byte-stream mode is the simplest integration. The sender writes media chunks up
to `MaxChunk()` bytes; the receiver reads the exact recovered chunks in order.

### Sender

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1
cfg.BufferMicros = 75_000

s, err := meld.NewSender("receiver.example:5000", cfg)
if err != nil {
	return err
}
defer s.Close()

buf := make([]byte, cfg.MaxChunk())
for {
	n, err := source.Read(buf)
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
```

### Receiver

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1
cfg.BufferMicros = 75_000

r, err := meld.NewReceiver(":5000", cfg)
if err != nil {
	return err
}
defer r.Close()

buf := make([]byte, cfg.SymbolSize)
for {
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	sink.Write(buf[:n])
}
```

## Media-Aware Mode

Use `WriteFrame` when the application knows frame boundaries and dependencies.
This is the path that lets Meld optimize decodable video rather than raw bytes.

```go
fd := meld.FrameDesc{
	Priority:        priorityFor(frame),
	FrameID:         frame.ID,
	RefFrameIDs:     frame.References,
	Chunks:          uint16(len(frame.Chunks)),
	TemporalID:      frame.TemporalID,
	RAP:             frame.IsRandomAccess,
	RecoveryRefresh: frame.InRecoveryRefresh,
	Discardable:     frame.Discardable,
	NonPicture:      frame.MetadataOnly,
}

for _, chunk := range frame.Chunks {
	if _, err := sender.WriteFrame(chunk, fd); err != nil {
		return err
	}
}
```

Guidance:

- Use the same `FrameDesc` for every chunk of the same access unit.
- `FrameID` must be monotonic within the flow.
- `RefFrameIDs` should name the access units required to decode this frame.
- `Chunks` should be the number of Meld chunks in the access unit.
- Higher `Priority` means the repair sizer protects the data harder.
- Mark leaf frames `Discardable` when dropping them does not damage future frames.
- Mark parameter sets and metadata as `NonPicture` so frame stats do not count
  them as displayed frames.

The repo's codec shapers are currently internal. External users should either:

- use byte-stream mode first
- generate `FrameDesc` from their existing encoder/RTP/container metadata
- copy the mapping logic into their application until a public shaper API exists

## Priority Convention

The core treats priority as an ordered tier. The current shapers use this rough
shape:

| Priority | Meaning |
|---:|---|
| `0` | disposable/non-picture filler |
| `1` | enhancement or leaf media |
| `2` | normal base/reference media |
| `3` | RAP, parameter set, or other critical anchor |

Use fewer tiers if your media pipeline does not have that detail. Correct
relative ordering matters more than exact numeric names.

## Encoder Control

Poll encoder control from the sender:

```go
ctrl := sender.EncoderControl()
if ctrl.TargetBitrateBps != 0 {
	encoder.SetTargetBitrate(ctrl.TargetBitrateBps)
}
if ctrl.RecoveryCadenceFrames != 0 {
	encoder.SetMaxRecoveryInterval(int(ctrl.RecoveryCadenceFrames))
} else {
	encoder.RelaxRecoveryInterval()
}
```

The request is advisory. Meld continues operating even if the encoder ignores it.

Recommended behavior:

- apply `TargetBitrateBps` as an encoder payload ceiling, not an additional
  transport pacer
- satisfy with bounded intra-refresh or recovery-point cadence when possible
- avoid frequent full IDR/keyint shortcuts unless that is the codec's only
  actuator
- relax the encoder request when Meld returns `0`
- do not expose this as a separate user profile

The bitrate request is derived from the sender's measured source packet cadence,
payload/header split, live total-rate budget, and uncapped recovery set point.
Activation and relaxation are hysteretic. A recovery-share bound preserves most
of the aggregate budget for media even when a burst-tail estimate is extreme.

## Configuration Recipes

Recovery does not require a recipe. `DefaultConfig` uses one automatic policy:

- before feedback is credible, conservative sliding equations cover cold start;
- burst-correlated loss keeps recovery fungible across a window;
- the sender continuously allocates a measured share of proactive repair
  to isolated 16-source Cauchy-MDS blocks when cadence, deadline, and row
  economics admit them; correlated loss raises the share, while reactive room
  and long slack reduce it without turning MDS off;
- coded equations automatically omit a zero payload
  tail when the four-byte length extension still produces a net byte saving;
- a persistent residual can receive compact exact closure when a feedback cycle
  fits its deadline;
- when a feedback cycle cannot fit, one copy may be delayed past the measured
  fade if one-way transit and the byte budget still fit;
- source bytes are charged first and every recovery action shares the remaining
  `MaxBitrate` allowance.

Exact-unit, compact-equation, and epoch repair are part of wire version 1.
Applications set the latency and capacity contract; they do not switch recovery
algorithms as the path changes.

### Low-Latency Frontier

```go
cfg := meld.DefaultConfig()
cfg.Flow = 1
cfg.BufferMicros = 75_000 // example: 75 ms
cfg.MaxBitrate = 0        // default ceiling unless the app has a known cap
```

This is the main target. Leave `Sliding`, `Pace`, `RepairWithinBudget`, and
`AutoReorderHoldoff` at their defaults.

### Fixed Bandwidth Ceiling

```go
cfg.MaxBitrate = 12_000_000 // media + repair bits per second
```

Source media is not dropped by the core. Repair is shed first. With `Pace` on,
`Write` can backpressure the application when the queue would exceed the deadline.

### Encrypted Session

```go
cfg.Passphrase = passphrase
buf := make([]byte, cfg.MaxChunk())
```

Use `MaxChunk()` rather than `SymbolSize` for sender buffers because encryption
adds an AEAD tag.

### Custom Datagram Host

```go
sender, err := meld.NewSenderOver(substrate, cfg)
receiver, err := meld.NewReceiverOver(substrate, cfg)
```

The substrate must implement:

- `ReadFrom([]byte) (int, net.Addr, error)`
- `WriteTo([]byte, net.Addr) (int, error)`
- `LocalAddr() net.Addr`
- `Close() error`

The built-in UDP host owns pacing and timers. With a custom substrate, Meld still
owns coding; the host transport owns its own network behavior.

### Multipath

```go
sender, err := meld.NewMultipathSender([]string{addrA, addrB}, cfg)
receiver, err := meld.NewMultipathReceiver([]string{bindA, bindB}, cfg)
```

Multipath is coding-native diversity. It is not packet duplication. It currently
uses the generation core internally.

## Reading Stats

Sender stats:

```go
st := sender.Stats()
log.Printf("source=%d repair=%d reactive=%d arq=%d burst_copy=%d outage_delayed=%d block_mds=%d mds_demand=%d/256 mds_correlation=%d/256 mds_memory=%d/256 mds_mix=%d/256 compact=%d saved_bytes=%d throttled=%d cadence=%d",
	st.Source,
	st.Repair,
	st.ReactiveRepair,
	st.RepairExact,
	st.RepairBurstDuplicate,
	st.RepairOutageDiversity,
	st.RepairEpoch,
	st.EpochDemandQ8,
	st.EpochCorrelationQ8,
	st.EpochMemoryQ8,
	st.EpochShareQ8,
	st.RepairCompacted,
	st.RepairBytesSaved,
	st.Throttled,
	st.RecoveryCadenceFrames,
)
```

`RepairExact` is a subset of deficit repair; `RepairBurstDuplicate`,
`RepairOutageDiversity`, and `RepairEpoch` are subsets of proactive repair.
Do not add them to `Repair` when computing total traffic. Outage diversity counts
existing RLNC credits moved in time; automatic MDS counts existing proactive
credit emitted as fixed Cauchy rows. Neither is additional redundancy.
`EpochDemandQ8`, `EpochCorrelationQ8`, and
`EpochMemoryQ8` expose the live automatic evidence. `EpochShareQ8`
is the epoch fraction selected for the latest stable block; 256 means all of that
block's proactive opportunities were assigned to MDS.
`RepairCompacted` and `RepairBytesSaved` describe wire serialization savings;
they do not add equations or weaken control-budget accounting.

Receiver stats:

```go
st := receiver.Stats()
log.Printf("delivered=%d recovered=%d lost=%d wire_lost=%d rejected=%d evicted=%d",
	st.Delivered,
	st.Recovered,
	st.Lost,
	st.WireLost,
	st.Rejected,
	st.Evicted,
)
```

Frame stats:

```go
fs := receiver.FrameStats()
log.Printf("frames=%d decodable=%d keyframes=%d decodable_keys=%d",
	fs.Frames,
	fs.DecodableFrames,
	fs.Keyframes,
	fs.DecodableKeyframes,
)
```

Interpretation:

- `Recovered` is good: it means coded repair reconstructed source data.
- `WireLost` is pre-recovery loss and should not be subtracted after recovery.
- `Lost` is unrecoverable before deadline.
- `Rejected` indicates resource-safety admission rejected inbound symbols.
- `Evicted` indicates media-aware early eviction skipped data known to be
  undecodable.
- `FrameStats` is meaningful when `WriteFrame` descriptors are present.

## Packetization And Chunk Size

`SymbolSize` is the maximum application chunk and the fixed algebraic width. The
default, 1316 bytes, matches seven 188-byte MPEG-TS packets and fits common
RTP/UDP MTU practice. Systematic datagrams carry only the exact bytes written;
repair equations internally zero-pad values to this width.

Rules:

- do not pass chunks larger than `cfg.MaxChunk()`
- keep chunk boundaries stable for the receiver's media pipeline
- for `WriteFrame`, avoid mixing unrelated frame data in one chunk
- for encrypted flows, always size buffers with `MaxChunk()`

## Deadlines

`BufferMicros` is both a protocol and product decision. It should reflect the
receiver's playout deadline, not a generic socket buffer size.

If the budget is below one RTT, ARQ cannot reliably rescue missing data in time.
That is where Meld's proactive coded repair is most relevant.

If the budget is well above one RTT, SRT/RIST-style retransmission can often catch
up. Meld should still behave conservatively, but the macro advantage is expected
to shrink.

## Operational Checklist

Before a real deployment:

- set the same `Flow`, `SymbolSize`, and `BufferMicros` on both ends; generation
  deployments must also agree on `GenSize`
- use `MaxChunk()` for sender buffers
- keep `Pace` enabled unless your host layer already enforces the same deadline
  contract
- set `MaxBitrate` when the transport must stay under a known capacity
- enable `Passphrase` for non-test links
- read `Stats()` and `FrameStats()` in telemetry
- benchmark with the same encoded source against SRT/RIST
- write generated benchmark output under `scratchpad/`

## What Not To Tune First

The following were explored and are not current deployment levers:

- packet placement interleaving
- refresh-island sparse repair
- manual singleton/protected repair gap tuning
- fixed-keyint source shortcuts
- frequent IDR/keyint as a proxy for bounded recovery cadence

Use macro frontier runs to justify reopening any of those branches.
