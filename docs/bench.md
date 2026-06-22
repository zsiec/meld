# Benchmarks — not yet published

Meld is validated for **correctness** today, not for published **performance**.

The correctness bar is real and enforced in CI (`go test -race`):

- the **four delivery invariants** on every flow/multipath test — no duplicate
  delivered, in-order output, nothing delivered past its deadline, and completeness
  under recoverable loss;
- the decoder's agreement with an independent **rank oracle** — it may never claim
  recovery the window rank does not support;
- **glass-to-glass dependency resolution** confirmed against a real decoder
  (`ffprobe` decodes exactly the model's predicted decodable picture set).

Comparative **performance** numbers (delivery, latency, overhead against SRT/RIST/
Zixi or ST 2022-7) are **deliberately withheld**. The figures produced during
development are single-host loopback, run against Go reimplementations rather than
the canonical C stacks, and have not been validated cross-host or on real networks.
Publishing them as claims would overstate what has actually been measured.

A reproducible, independently verifiable benchmark methodology — cross-host, over a
real impairment path, against the canonical baselines, at an equal latency budget —
is on the roadmap. This document will carry the numbers once they are defensible.
