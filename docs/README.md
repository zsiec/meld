# Meld Documentation

This directory is the long-form documentation for Meld. The README is the entry
point; these files are the deeper references.

## Start Here

- [Protocol](protocol.md): how Meld sends, repairs, decodes, feeds back, paces,
  and adapts.
- [Integration](integration.md): how to use the Go API, choose config values,
  attach media descriptors, enable encryption, and read stats.
- [Benchmarking](bench.md): benchmark suites, fairness rules, artifacts, and
  publication criteria.
- [Specification](spec/README.md): LaTeX working specification for the Meld
  protocol and wire version 1.

## Protocol Components

- [Coding](coding.md): sliding RLNC, bounded generation MDS, and redundancy sizing.
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

## Generated Output

Benchmark output should be written under `scratchpad/`. That directory is ignored
by git. Commit only curated summaries and documentation updates.
