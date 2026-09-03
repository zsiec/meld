# Sliding-window recovery

Meld's default single-path flow uses a band-form sliding decoder. Source symbols
are emitted immediately and retained in an elastic trailing window; repair
symbols are linear equations over a bounded portion of that window. The flow is
deterministic and sans I/O. Its caller supplies timestamps and transports the
datagrams it emits.

## Window geometry

`Config.CodingWindow` is the maximum band width and therefore the decode-cost
cap. The sender may choose a smaller effective band when source cadence,
one-way propagation, and the configured playout budget leave insufficient time
for a full-width equation to arrive. The receiver bounds admitted windows and
live state independently, so forged geometry cannot cause unbounded allocation.

The band decoder keeps rows by absolute pivot and performs incremental
Gauss-Jordan elimination. An innovative row adds one degree of freedom. A source
is delivered as soon as its column is solved and every preceding source is
resolved; an unresolved source is skipped when its exact deadline expires.

## Source and repair values

Application chunks are at most `SymbolSize` bytes. The wire sends exact source
bytes, while the algebraic value is fixed-width:

```text
zero-padded application region (SymbolSize)
source length (uint32)
deadline (int64)
```

Every repair equation therefore covers `SymbolSize + 12` bytes. The coded
metadata makes the length and deadline recoverable rather than inferred from
neighboring packets. Compact repair omits a guaranteed-zero suffix of the
application region when doing so saves bytes; the receiver restores it before
elimination, so the equation and rank do not change.

`WindowBase`, `N`, and `RepairKey` identify a contiguous equation. Sparse repair
uses an explicit bounded source-id list. Exact unit repair carries one retained
source value and enters the decoder as a known systematic while remaining
repair-class traffic for pacing and accounting.

## Recovery allocation

All recovery actions share one source-first byte ledger:

- Sliding RLNC provides immediate fungible equations over the moving band.
- Rank feedback can add reactive equations while their return path still fits
  the affected deadline.
- A bounded free-column bitmap can trigger exact unit repair. Every named value
  removes one independent degree of freedom, up to the remaining rank deficit.
  Proactive coding gets the first opportunity. A clustered residual crosses to
  exact at its last useful dispatch when measured repair headroom can fund it;
  isolated holes wait for one persistent report.
- A measured fade may move a bounded share of already-earned equations later in
  time, or schedule one delayed exact copy, when a feedback cycle cannot fit.
- Stable groups of 16 sources may receive Cauchy-MDS rows in isolated epoch
  decoders when cadence, deadline, loss memory, and row cost admit them.

These are internal actions of the single automatic policy. They do not create
additional profiles or repair credit, and source traffic is admitted before
repair whenever the configured rate ceiling binds.

## Feedback

The receiver reports cumulative progress and bounded current observations:

- decoded low edge and highest covered source;
- rank deficits;
- pre-recovery loss rate and interval loss count;
- loss-run burstiness and classified outage duration;
- reorder-settled loss evidence;
- rank-closing free-column closure map over the receive window;
- optional path and media observations carried inside the complete version-1
  feedback message.

Loss and burst measurements size future equations. Settled loss supplies the
clean/dirty signal without treating ordinary reordering as loss. The first 64
closure bits travel in `Missing`; the fixed `Deficits` bytes extend that map to
320 offsets directly or describe up to six runs anywhere in the 2,048-source
receive window. Named ids are unresolved free columns below decode coverage, so
each exact answer advances rank and unsent or merely in-flight sources cannot
solicit repair.

Exact crossover combines residual state, remaining time, and capacity. A
measured clustered channel or a bulk closure is eligible when one more ordinary
feedback interval would consume the retained source's final useful dispatch and
the measured source rate leaves enough repair headroom. Isolated holes retain
the persistence gate, avoiding unconditional retransmission on reorder-prone
paths. Scarce-headroom senders preserve fungible coded repair.

## Fixed repair epochs

An epoch contains exactly 16 consecutive sources. Each systematic in the epoch
announces the same `WindowBase` and `N=16`. Cauchy rows over that range enter a
separate bounded decoder; exact values learned by direct reception, unit repair,
or the moving band are shared with it. Recovered epoch values are injected into
the common ordered decoder. Epoch rows are never mixed into a matrix whose
column set moves.

The sender recomputes the epoch share only at safe block boundaries. It uses
measured loss correlation, outage memory, reactive reachability, remaining
deadline slack, source cadence, and the recent ratio of row cost to systematic
cost. The chosen share remains stable for the announced block.

## Implementation map

- `internal/code/band.go`: bounded incremental band decoder.
- `internal/flow/sliding.go`: sliding sender and shared orchestration.
- `internal/flow/sliding_receiver.go`: receiver, feedback, and epoch isolation.
- `internal/flow/recovery_policy.go`: continuous epoch allocation.
- `internal/flow/targeted_repair.go`: missing-driven exact repair.
- `internal/flow/epoch_repair.go`: fixed-block sender policy.
- `internal/flow/coded_symbol.go`: exact metadata and compact equations.
- `internal/flow/capacity_control.go`: source-first capacity controls.
- `internal/wire/wire.go`: version-1 serialization.
