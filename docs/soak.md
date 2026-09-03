# Real-path soak protocol

`cmd/soak` is the turnkey harness. This file defines the measurement protocol and
acceptance bars for real-path runs; existing benchmark evidence is based on the
loopback harness.

## Setup

Two hosts with UDP reachability, NTP-synced (the scoring is min-anchored and
tolerates a stable offset; drift within a run adds noise).

```
# far box (receiver)
go build ./cmd/soak            # or GOOS=linux GOARCH=amd64 go build ./cmd/soak
./soak -rx -listen :7601 -runs 16 -budget 150ms -out reports/

# near box (sender): the A/B protocol
./soak -tx -to FAR:7601 -arms default,headroom -reps 8 -mbps 8 -dur 30s -budget 150ms
```

Runs interleave arms (A,B,A,B,…) so path drift affects both equally; each run
gets a fresh Receiver (the sequence space restarts). The receiver writes one
JSON report per run; the sender prints a per-second timeline (proactive rate,
reactive rate, headroom tightens) and emits its own JSON. Collect both.

Scoring convention: per-chunk one-way latency is min-anchored within the run
(`lat − min(lat)`), and a chunk is IN TIME when the relative latency fits the
budget minus a 20 ms release guard — the same convention the loopback bench
applies to its ARQ anchors. Repeat every judgment at two budgets (150 ms and
3×RTT of the actual path) before believing it.

## What the soak evaluates

1. **HeadroomAwareSizing.** The simulation
   (true capacity) says large wins on saturating paths and inertness elsewhere;
   loopback glass (no true capacity) says false tightens on bursty cells. On a
   real path:
   - On an uncongested path: `headroom_tightens` ≈ 0 and the arms tie within
     A/A — the loopback false-positive story should NOT reproduce where real
     capacity exists but is not being hit.
   - Against a genuinely capacity-limited path (e.g. a shaped uplink below the
     offered source+repair rate): the headroom arm must deliver ≥ the default
     arm and never enter the boom/slam signature (proactive rate oscillating
     against `Throttled`/collapse in the timeline).
   - Bar for default-on reconsideration: both of the above across ≥8 paired
     runs on ≥2 distinct real paths, plus no regression at the loopback guard
     cells.
2. **Confirmed-clean floor decay + settled-clean detector.** On a
   real, mostly-clean path the sender timeline must show the decay arming
   (proactive/s collapsing to ~0 after the ~1.3 s confirm) and re-arming on real
   loss; the receiver report must show `in_time_pct` unharmed vs a `-red 0.15`
   pinned-floor control run. Real-path jitter/reorder is the settled walk's
   direct path test: the decay arming at all on a real path validates the
   settled-clean detector (the raw composite never arms outside a lab).
3. **General honesty check.** `dups` and `reorders` must be 0 in every report
   (the four-invariants surface of the public API); `wire_lost`/`recovered`
   ratios should be consistent with the path's observed loss.

## Anchors

SRT/RIST anchor adapters are outside this repository and its dependency set.
Run the Meld soak first, then run an independently managed anchor at the same
budget on the same path and compare `in_time_pct` at equal budget. A Meld-only
soak already adjudicates the three mechanisms above.
