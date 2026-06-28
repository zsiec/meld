# Publishable Benchmark Battery

This document defines the benchmark battery we should use before publishing Meld
claims against SRT and RIST. The goal is not to find one flattering cell. The goal
is a defensible envelope: where Meld wins, where it ties, where it loses, and
whether each result is explained by transport behavior, source dependency
structure, or a physical latency ceiling.

## Principles

- Compare one deployable Meld arm: `meld-auto`.
- Compare against real SRT and real RIST tools, not in-process approximations.
- Keep the encoded source identical across transports unless the experiment is
  explicitly measuring Meld's encoder-cadence actuator.
- Include oracle rows: source ceiling and ideal transport deadline ceiling.
- Report media outcomes, transport overhead, observed relay bytes, runtime, and
  seed variance.
- Publish CSV and charts generated from the same run directory.
- Run more than one source structure before making a general protocol claim.
- Treat `scratchpad/` output as raw data. Commit only curated summaries and
  decision notes.

The external reference points are SRT's latency-window ARQ model, RIST's RTP/RTCP
repair profile model, and RTP monitoring practice around loss, burst/gap,
jitter, delay, and media repair outcomes.

References:

- SRT protocol draft: https://haivision.github.io/srt-rfc/draft-sharabayko-srt.html
- VSF RIST technical recommendations: https://vsf.tv/technical-recommendations/
- RTP Control Protocol Extended Reports, RFC 3611: https://www.rfc-editor.org/rfc/rfc3611
- RTP Metrics for Monitoring, RFC 8888: https://www.rfc-editor.org/rfc/rfc8888
- RLC FEC scheme, RFC 8681: https://www.rfc-editor.org/rfc/rfc8681

## Measurement Points

`glassbench -publishsuite` covers these publication axes:

| Measurement point | Why it matters | Artifact |
|---|---|---|
| Zero-loss sanity | Proves the source, chunker, shims, and ffprobe arbiter can reach the source ceiling. | `frontier_rows.csv`, oracle rows |
| Latency budget vs RTT | Separates below-RTT, one-RTT, and generous-buffer regimes. | `frontier_gaps.csv`, heatmap |
| RTT scaling | Shows whether recovery depends on a round trip. | heatmap by RTT |
| Iid erasure tolerance | Primary low-latency coded-transport frontier. | delta chart, gap table |
| Burst/tail erasure | Tests long loss runs and dependency-island damage. | failure report |
| Reorder/jitter | Tests whether reorder is mistaken for loss. | jitter-tagged cases |
| Media decodability | Uses `ffprobe` frames and frame/keyframe completeness, not packet delivery alone. | rows and charts |
| Continuity | Records the decodable prefix in frames, so a result can distinguish isolated loss from early stream breakage. | `frontier_rows.csv` |
| Repair overhead | Shows Meld source, repair, reactive, throttled, and repair/source ratio alongside frame gains. | `frontier_rows.csv`, cost/gain chart |
| Observed wire bytes | Counts relay-observed forward and reverse datagrams/bytes for the benchmark path. | `frontier_rows.csv`, `frontier_gaps.csv` |
| Runtime and process cost | Records wall-clock runtime for every arm and external-process CPU/RSS for SRT/RIST where the OS exposes rusage. | `frontier_rows.csv` |
| Oracle ceilings | Identifies source ceilings and physical deadline ceilings. | oracle rows |
| Fairness guards | Flags missing arms, source packet/byte mismatch, oracle-source misses, and conservative-region regressions. | `FAIRNESS.md`, `fairness.csv` |
| Per-seed attribution | Shows which dependency island failed and whether repair existed in time. | `failure_report.*` |

## Suites

### Smoke

Fast end-to-end check. Use before a long run or after changing the harness.

```sh
go run ./cmd/glassbench \
  -publishsuite smoke \
  -buf 0 \
  -reps 1 \
  -reportdir scratchpad/glassbench-results/publish-smoke
```

### Iid Frontier

Primary claim-confirmation suite. This is the suite for the current Meld thesis:
iid or tail-erasure loss under tight latency, especially below one RTT.

Matrix:

- losses: 0%, 1%, 3%, 5%, 10%
- burst: iid only
- RTTs: 50, 100, 200, 400 ms
- budgets: 0.5x, 0.75x, 1.0x, 1.25x, 1.5x RTT

```sh
go run ./cmd/glassbench \
  -publishsuite iid-frontier \
  -buf 0 \
  -reps 8 \
  -reportdir scratchpad/glassbench-results/publish-iid-frontier
```

To run more than one AVC source fixture:

```sh
go run ./cmd/glassbench \
  -publishsuite iid-frontier \
  -buf 0 \
  -reps 8 \
  -publishclips internal/shape/testdata/bbb_bframes.h264,internal/shape/testdata/bbb.h264 \
  -reportdir scratchpad/glassbench-results/publish-iid-frontier-sources
```

That creates one subdirectory per source and a top-level `SOURCES.md`.

### Bursty Frontier

Discovery suite for long bursts. This should be used to decide whether burst
claims are defensible, not to tune packet placement locally.

```sh
go run ./cmd/glassbench \
  -publishsuite bursty-frontier \
  -buf 0 \
  -reps 8 \
  -reportdir scratchpad/glassbench-results/publish-bursty-frontier
```

### Fallback Check

Conservative-region guard. Use this to verify that `meld-auto` does not create
stable regressions where ARQ has enough slack.

```sh
go run ./cmd/glassbench \
  -publishsuite fallback-check \
  -buf 0 \
  -reps 8 \
  -reportdir scratchpad/glassbench-results/publish-fallback-check
```

### Publish Core

Broad map across no-loss, iid loss, burst loss, RTT scaling, and latency-budget
scaling. This is large and should be run overnight or on a dedicated runner.

```sh
go run ./cmd/glassbench \
  -publishsuite publish-core \
  -buf 0 \
  -reps 8 \
  -reportdir scratchpad/glassbench-results/publish-core
```

## Artifacts

Each suite run writes:

- `PUBLISH.md`: suite definition and measurement points.
- `environment.json`: command line, git revision/dirty state, OS/arch, Go
  version, source identity, source ceiling, and external tool versions.
- `frontier_rows.csv`: aggregate case/arm results.
- `frontier_gaps.csv`: deployable Meld versus best ARQ row.
- `FRONTIER.md`: sorted frontier call and gap tables.
- `FAIRNESS.md` and `fairness.csv`: source equality, arm-presence, oracle, and
  conservative-regression checks.
- `failure_report.csv` and `failure_report.md`: per-seed first-failure
  attribution.
- `charts/delta-bars.svg`: highest-signal Meld-vs-ARQ deltas.
- `charts/frontier-heatmap.svg`: latency/RTT frontier heatmap.
- `charts/arm-frames.svg`: decoded-frame means by arm for high-signal cases.
- `charts/cost-gain.svg`: frame delta versus observed relay-byte delta, with
  marker size tied to Meld repair overhead.
- `SOURCES.md`: only for `-publishclips` runs; links the per-source subreports.

## Publication Bar

A claim is publishable only if:

- the run directory includes `environment.json`;
- oracle-source reaches the expected source ceiling;
- SRT and RIST rows are present and successful for the claimed cells;
- Meld uses `meld-auto`;
- the result is stable across seeds: `abs(delta ff)` exceeds the combined
  per-arm ffprobe-frame standard deviation;
- source packet/byte counts are equal across transports unless the section is
  explicitly about encoder control;
- every headline win reports repair overhead and observed relay-byte delta;
- conservative/fallback regions do not show stable Meld regressions, or those
  regressions are explicitly scoped out of the claim;
- charts are generated from the same CSVs as the tables;
- failure reports are reviewed for any headline deficit or unexpected win.

CPU/RSS note: the current harness records wall-clock runtime for all arms and
external-process CPU/RSS for SRT/RIST subprocesses. Meld runs in the benchmark
process, so a publishable CPU/RSS claim for Meld needs an isolated runner or a
separate process wrapper.

## Known Current Thesis

The current defensible frontier is iid/tail-erasure loss at tight latency. Burst48
and Burst96 did not yet produce a stable Meld advantage under the current source
structure and fixed repair budget. That may change with a different source family,
but it should be reopened only by a macro suite result.

The current low-latency wins also carry high repair and observed wire-byte cost
in smoke runs. That does not invalidate the frontier, but it changes the next
optimization target: preserve the sub-RTT frame wins while reducing cost enough
to fit a deployable envelope.
