# Benchmarking

`cmd/glassbench` measures decoded media under controlled loss, delay, jitter, and
capacity. It compares the automatic Meld profile with SRT, RIST, source and ideal
oracles, and records transport cost alongside media outcome.

Generated run directories belong under ignored `scratchpad/`. The repository defines
the methodology and report format; a dated result is meaningful only with the source,
environment, revision, and command captured in that run directory.

## Comparison rules

- Use the same encoded source and chunk boundaries for every transport unless the run
  explicitly evaluates an encoder-control actuator.
- Set both `-wirembps` and `-maxmbps` to the same positive capacity for cost or quality
  comparisons. An unbounded relay is useful only for coding diagnostics.
- Use `-cc` to enable Meld's delay/ECN controller on Meld arms for an identical-grid
  A/B against the static-ceiling behavior.
- Include `oracle-source` and `oracle-ideal` so source and deadline ceilings are visible.
- Compare `meld-auto`, not a benchmark-only internal override, for deployable claims.
- Use matched deterministic seeds. Repetition `r` uses `7919*r + 13`.
- Judge decoded frames and frame dependency closure, not packet delivery alone.
- Report relay-observed bytes, repair attribution, runtime, and seed variance with every
  quality result.

Named suites cap media chunks at 1,284 bytes after the benchmark sequence header. They
also repeat short sources until the run spans at least four cycles of the largest RTT.

## Named suites

| Suite | Purpose |
|---|---|
| `smoke` | Fast end-to-end report-pipeline check. |
| `codec-gate` | Cross-codec selector check over iid and medium/deep loss memory. |
| `iid-frontier` | Random-loss latency/RTT envelope. |
| `bursty-frontier` | Gilbert-Elliott burst-loss envelope. |
| `fallback-check` | Generous-deadline regression guard. |
| `publish-core` | Broad no-loss, iid, burst, RTT, and deadline matrix. |
| `full-envelope` | Capacity-matched cross-codec publication matrix including jitter. |

The suite matrices and required arms are defined in `cmd/glassbench/publish.go`; the code
is authoritative if a value here becomes stale.

## Typical runs

Run a smoke test first:

```sh
go run ./cmd/glassbench \
  -publishsuite smoke \
  -buf 0 \
  -reps 1 \
  -reportdir scratchpad/glassbench-results/smoke
```

Run the required cross-codec gate with a shared capacity:

```sh
go run ./cmd/glassbench \
  -publishsuite codec-gate \
  -buf 0 \
  -reps 3 \
  -maxmbps 10.8 -wirembps 10.8 \
  -publishclips internal/shape/testdata/bbb_bframes.h264,internal/shape/testdata/bbb.h265,internal/shape/testdata/bbb.obu \
  -reportdir scratchpad/glassbench-results/codec-gate
```

Use at least eight matched repetitions for a promoted performance claim. The
`full-envelope` suite additionally requires `-buf 0`, a positive shared capacity,
matching `-maxmbps`, and the deadline arbiter.

Large suites can be split deterministically with `-frontiershards` and
`-frontiershard`, then audited and combined with `-mergefrontier`. A merge rejects
missing, overlapping, incompatible, or duplicate cells.

## Artifacts

A named suite writes:

- `PUBLISH.md` and `environment.json` for the suite, command, source, revision, toolchain,
  and external tool versions;
- `frontier_rows.json` and `frontier_rows.csv` for aggregate arm results;
- `frontier_gaps.csv` and `FRONTIER.md` for Meld-versus-comparator rows;
- `FAIRNESS.md` and `fairness.csv` for source, oracle, arm-presence, and regression guards;
- `failure_report.*` and selected `seed_trace_*.json` files for failure attribution;
- generated SVG charts for quality and cost;
- `SOURCES.md` for multi-source runs.

## Acceptance bar

A performance claim should be made only when:

- the environment and exact source identity were recorded;
- source and ideal oracles reached their expected ceilings;
- all required external arms completed successfully;
- compared arms used identical source bytes and matched seeds;
- at least three repetitions support an engineering gate and at least eight support a
  promoted claim;
- the quality difference exceeds the combined run-to-run variation;
- repair overhead and forward/reverse wire bytes accompany the media score;
- the same generated data backs tables, fairness checks, and charts;
- failures and unexpected wins were inspected in their retained traces.

The harness records wall time for all arms and process resource use for external tools.
Meld runs in the benchmark process, so a CPU or RSS comparison requires an isolated Meld
runner rather than the in-process numbers.
