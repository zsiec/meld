# Coding model

Meld uses systematic linear erasure coding over GF(2⁸). It treats a missing or
failed-authentication datagram as an erasure; it does not attempt to correct
unknown bit errors.

## Algebraic source

Each application chunk becomes a fixed-width coded value containing a
`SymbolSize` application region followed by its uint32 exact length and int64
deadline. The application region is zero-padded only inside the coding layer.
This representation lets any decoder return the original byte length and apply
the source's own deadline after recovery.

Systematic packets send exact source bytes. Dense and sparse equations may use a
compact wire representation that removes a guaranteed-zero application suffix
while retaining the coded metadata. Expansion happens before GF arithmetic and
must reproduce the full equation byte-for-byte.

## Coefficients

A repair packet carries its source set and a 16-bit `RepairKey`; it does not
carry an explicit coefficient vector. Encoder and decoder call the same
deterministic coefficient generator.

- Keys with both high bits set select bounded Cauchy-MDS rows. A block is capped
  so its systematic and parity evaluation points remain disjoint in GF(256).
- Other keys select deterministic seeded RLNC coefficients and are guaranteed
  not to produce an all-zero vector.

The same `(source set, RepairKey)` pair must always regenerate the same vector.

## Sliding RLNC

The default single-path flow emits repair over an elastic trailing band. Adjacent
equations may cover different, overlapping source sets. The band decoder uses
incremental reduced-row elimination and releases a source as soon as it is
solved in order. `CodingWindow` caps both recovery span and quadratic band work.

Moving-window equations remain fungible across unresolved sources in their
span. Feedback supplies rank deficits and loss observations, but proactive work
does not wait for feedback.

## Bounded MDS blocks

Generation flows and automatic 16-source epochs use systematic Cauchy blocks.
For one stable source set, any block-width collection of systematic and distinct
admissible parity rows is full rank.

Generation mode partitions the stream into bounded groups and is currently used
by the multipath host. Epoch repair is an actuator inside the sliding flow: all
sources announce one stable 16-source range, epoch rows enter a separate decoder,
and recovered values are exchanged with the ordinary sliding decoder. Block
state is bounded and retired when the ordered cursor passes the block.

## Sparse and exact repair

Sparse repair lists up to 64 source ids explicitly and derives coefficients in
that order. It protects selected dependency neighborhoods without coupling every
intervening source into the equation.

Unit repair carries the exact bytes for one retained source. It is admitted and
paced as repair but inserted into the decoder as a known systematic value. The
sender limits it to covered unresolved ids, the reported rank deficit, retained
history, available byte headroom, and deadlines. For clustered loss, a
deadline-aware crossover sends the named value at its final useful dispatch when
measured repair headroom can fund it; isolated holes retain one persistence
interval for already-in-flight coding to close.

## Decoder contract

An equation is admitted only when its declared geometry and payload shape are
valid. Each independent row increases rank by one. A decoder emits a source only
when the linear system determines that source exactly; it never guesses or emits
unsupported rank.

The implementation is tested for:

- exact coefficient agreement;
- full-rank bounded Cauchy blocks;
- correct compact-equation expansion;
- exact recovered lengths and deadlines;
- no duplicate or out-of-order delivery;
- no delivery after deadline;
- bounded state under malformed geometry; and
- scalar/SIMD byte equality for GF multiply-add.

## Implementation map

- `internal/gf`: GF(2⁸) arithmetic and architecture-specific multiply-add.
- `internal/code/code.go`: coefficient generation and bounded block encoder/
  decoder.
- `internal/code/band.go`: incremental sliding-band decoder.
- `internal/flow/coded_symbol.go`: coded metadata and compact serialization.
- `internal/flow`: deadline-aware recovery policy and wire admission.
