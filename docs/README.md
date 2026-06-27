# Meld Documentation

This directory is the long-form documentation for Meld. The README is the entry
point; these files are the deeper references.

## Start Here

- [Protocol](protocol.md): how Meld sends, repairs, decodes, feeds back, paces,
  and adapts.
- [Integration](integration.md): how to use the Go API, choose config values,
  attach media descriptors, enable encryption, and read stats.
- [Benchmarking](bench.md): the macro frontier methodology and the current
  SRT/RIST comparison results.

## Protocol Components

- [Coding](coding.md): the RLNC code family, sliding-window band decoder,
  redundancy sizing, and rejected code-family alternatives.
- [Sliding Window](sliding-window.md): the default low-latency coder path.
- [Wire Format](wireformat.md): symbol, repair, feedback, descriptor, and control
  packet encoding.
- [Substrate](substrate.md): UDP host and caller-provided datagram substrate.
- [Encryption](encryption.md): hybrid handshake, record placement, and
  encrypt-then-code model.

## Media Awareness

- [Media Awareness](media-awareness.md): the generic frame descriptor and how the
  transport protects dependency-critical media.
- [AVC](media/avc.md)
- [HEVC](media/hevc.md)
- [AV1](media/av1.md)
- [JPEG XS](media/jpegxs.md)

## Decision Notes

Decision notes preserve benchmark outcomes and cleanup rationale. They are not a
replacement for the protocol docs, but they explain why certain branches are no
longer active.

- [Refresh-island sparse repair](decisions/2026-06-27-refresh-island-repair.md)
- [Burst48 source thesis](decisions/2026-06-27-burst48-source-thesis.md)
- [Macro frontier discovery](decisions/2026-06-27-macro-frontier-discovery.md)
- [Cleanup after frontier discovery](decisions/2026-06-27-cleanup-after-frontier.md)

## Generated Output

Benchmark output should be written under `scratchpad/`. That directory is ignored
by git. Commit only curated summaries, decision notes, and docs updates.
