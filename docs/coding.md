# Coding — the erasure-code family

Meld's recovery substrate is network coding. "Network coding" is a family, not a
single code, so the design originally left room to run *several* codes at once — a
low-latency sliding-window code for the steady media and a near-optimal block code
for the tiny, fatal-on-loss top class. **Two in-tree decision gates (§2) have since
settled it: sliding-window RLNC holds every class at Meld's operating points, and the
two alternative families were measured and declined.** This note is the engine, the
gate results, and the resulting assignment.

All of these are **erasure** codes, not error-correcting: they recover *lost*
(dropped / failed-checksum) packets, not corrupted bits. That pairs naturally with
UDP/RTP, where a dropped or bad-CRC datagram is simply "missing" — exactly Meld's
substrate.

---

## 1. The shipping engine: sliding-window RLNC

Random linear combinations over GF(2⁸) of an **elastic window** of source symbols
(RFC 8681 RLC, Tetrys, Steinwurf-style RLNC). Recovery needs *any k-of-n* symbols in
the window; repair is rateless within the window; feedback **tightens** the repair
rate rather than gating it.

- **Why it wins:** lowest latency — a symbol is useful the moment it arrives; no
  block barrier. Recoding at relays. The window slides with the deadline. Over GF(2⁸)
  it behaves as a near-ideal MDS code (linear-dependence ~1/255), so decode failure is
  governed by the erasure *count*, the property the redundancy sizer keys on.
- **Cost:** GF math per symbol. The hot path is the GF(2⁸) AXPY (`dst[i] ^= c·src[i]`,
  the random-linear-combination inner loop); `internal/gf.MulAdd` ships SIMD
  acceleration for it — NEON on arm64 (always on) and AVX2 on amd64 (behind a CPUID
  check), both via the split-table technique (two 16-byte per-coefficient tables
  `c·lo`/`c·hi`, one NEON `TBL` / x86 `VPSHUFB` per nibble), with a scalar fallback on
  other arches. Every SIMD path is **byte-for-byte identical** to the scalar golden
  (`mulAddScalar`), enforced by an exhaustive-coefficient test and a fuzz target.
  The NEON and AVX2 paths are validated for correctness (the AVX2 path incl. under
  Rosetta); GFNI is deferred (the Go assembler lacks the mnemonic). Decode is
  on-the-fly Gaussian elimination; rank
  deficit is the feedback signal. Decode is O(b²)/symbol in the band-form
  (Caterpillar) decoder with O(1) window advance.
- **Meld use:** **every class.** The steady media path for all codecs; the fatal
  top class via **unequal protection** (a tightened decode-failure target δ on
  parameter sets / RAPs — `targetFailureForPriority`) rather than a different code,
  with plain **path duplication** for a single-chunk parameter set where coding has
  no gain (see the RaptorQ gate). The *only* viable engine for JPEG XS (its ~0.5 ms
  line budget forbids both ARQ and block gathering).

---

## 2. Code-family gates: two alternatives, measured and declined

The original plan reserved two more families. Rather than build either on faith, each
was evaluated by a **throwaway in-repo oracle** that scored the *ideal* form of the
candidate (an upper bound no real construction beats) against the shipped RLNC on the
candidate's own claimed advantage — so the decision was made for the cost of a test
file, before writing a codec. Both came back **shut**; the oracles have since been
removed (as the substrate A/B harness was — see [`docs/substrate.md`](substrate.md)),
with the method and numbers recorded below.

### Streaming / convolutional codes → SHUT

*What they are:* delay-optimal convolutional codes (Martinian–Sundberg / Khisti /
Fong), provably optimal for a combined **burst ⊕ scattered** loss model at a fixed
decoding delay, capacity `(T−N+1)/(T−N+B+1)`. Candidate construction: Tambur's
RS/Cauchy τ-frame kernel (NSDI'23).

*The gate:* at **equal bandwidth and equal delay** on a Gilbert-Elliott channel, the
residual decode-failure of the ideal streaming code vs burst-aware RLNC, with the
streaming code tuned `(B,N)` per channel (its strongest case).

*Result:* in the contribution-video regime (mean burst ≤ 6 symbols) the ideal
streaming code's optimal operating point **reduces to RLNC — no advantage.**
It pulls ahead only once the mean burst exceeds the per-window parity budget
`(1−R)(T+1)` — a deep-fade / satellite regime of 16–32-symbol bursts — and even there
the model is generous (ideal optimum; RLNC's online sizing and recoding-at-relays
ignored). The burst tail it chases on a realistic channel is already absorbed by the
N2 burst-aware sizer (`repairForGE`).

*When this would change:* a burst-dominated link with **predictable, near-deterministic**
burst length at a tight delay. The ship bar was a meaningful residual reduction in the
contribution regime — missed entirely — so it warrants re-running the oracle
only if such a link becomes a target.

### RaptorQ block fountain (RFC 6330) → SHUT (redundant)

*What it is:* a rateless block code (LT → Raptor → RaptorQ), **systematic** (zero
decode cost when no loss), near-optimal overhead `k(1+ε)` (ε ≪ 1%), linear-time
decode via permanent inactivation. Heritage: 3GPP MBMS, ATSC 3.0, FLUTE —
feedback-free push to many receivers.

*The gate, on RaptorQ's two claimed differentiators:*

- **Overhead vs duplication.** RaptorQ ≈ an ideal MDS rateless code; so is RLNC over
  GF(2⁸). Both achieve far lower overhead than 2022-7 duplication for the same
  reliability at `k ≥ 16`. But that win is delivered by the **shipped RLNC**, which
  RaptorQ can only *tie* (and a real RaptorQ is *worse* at small `k` via its precode).
  At `k = 1` (a single-chunk parameter set) coding has no diversity to pool — it *is*
  duplication — so RaptorQ's block machinery is pure waste there.
- **Linear-time decode at large block.** RaptorQ's only structural edge is O(k) decode
  vs RLNC's O(k²). But Meld **chunks** a fatal unit into small generations, so a large
  IDR decodes as ⌈chunks/GenSize⌉ cheap O(GenSize²) generations — total **linear in
  unit size** (a large unit chunked into small generations decodes markedly faster
  than one monolithic block), RaptorQ's headline without a second code family.

*Result:* **redundant** across Meld's actual fatal-class sizes — duplication at the
`k=1` tip, the shipped RLNC in the middle. The only regime RaptorQ wins — one very
large (`k ≫ 1000`) feedback-free fountain at a relaxed deadline — is one Meld's chunked
architecture never forms.

### Redundancy-sizing gate: AHECM overhead-optimizer → DECLINED

This gate is on the **sizing** axis (how much repair), not the code family. The
shipped controller sizes proactive repair to a decode-failure **target** δ (tightened
per class for UEP), carries the GE burst tail (`repairForGE`), and reduces overhead
two ways: **widening the generation** when the budget allows (`AdaptiveGenSize`, gated
by RTT headroom *and* a bitrate fill-time cap so a wide generation never raises
low-bitrate latency) and **offloading the variance margin** to the reactive tier by a
fixed, burst-guarded fraction (`ProactiveDecay`, on by default).

*The alternative:* AHECM (Tsai et al., *Multimedia Systems* 2011) instead sizes
proactive repair to **minimize expected coded overhead** (proactive + retransmission
bytes, Eqs 15-18) subject to recovery within the deadline's reactive rounds (Eq 4/7):
`(n,k,R) = arg min(overhead)` (Eq 19). Since that total is monotone in the proactive
count, it is the *smallest* proactive count whose residual after the reactive rounds
still meets δ — the overhead-optimal proactive/reactive split, vs the heuristics'
fixed discount.

*The gate:* a throwaway sizer arm — built, raced on cref (real librist/libsrt-class
glass-to-glass timing) against the shipped levers across bitrate × RTT × loss, then
removed (as the code-family oracles above were), with the method and numbers kept here.

*Result:* **dominated.** At the 50 Mbps contribution operating point `AdaptiveGenSize`
wins every cell — in most, AHECM is worse on *both* overhead and p99; where it does
reach lower overhead it pays a ~3× p99 blowup (e.g. p99 32 → 89 ms at RTT40/2%). All
arms held 100% delivery. **Cause:** AHECM's objective minimizes *bytes* and has **no
latency term**, so it offloads aggressively to reactive — but at ~30 generations/RTT
reactive is not cheaper (it re-sends what proactive shed) while costing a reactive RTT
on every offloaded generation, so p99 explodes. The width lever cuts the same overhead
with *zero* reactive round. The architecture also forbids the full optimizer: the
generation stride must be identical on both ends from packet 0 (sans-I/O), so block
size is config-time-static, not a freely-optimized variable.

*When this would change:* a regime where reactive *is* materially cheaper than
proactive (very low bitrate / few generations per RTT) with ample p99 headroom — the
corner where Meld's gentler `ProactiveDecay` already captures most of the win. The
ship bar was beating the width + margin levers at the deployment operating point;
missed. One borrowable idea not adopted: AHECM's ERE metric (Eq 23, recovered ÷
redundant) as a bench column for redundancy→recovery efficiency.

---

## 3. Class → code assignment (one engine)

| Protection class | Deadline budget | Loss tolerance | Code |
|---|---|---|---|
| Parameter sets / sequence header (often `k=1`) | longest (cached, repeated at each RAP) | **zero** (fatal on loss) | sliding-window RLNC at a tightened δ (UEP); a single-chunk set → **path duplication** (no coding gain at `k=1`) |
| IDR / IRAP / KEY-SWITCH anchors | long (random-access point) | near-zero | sliding-window RLNC, heavy repair (tightened δ) |
| Base-layer reference media | medium (playout buffer) | low | sliding-window RLNC (default δ) |
| Enhancement layers (high T/S), discardable units | short, droppable | high (graceful) | sliding-window RLNC at a low repair rate, or **uncoded** |
| JPEG XS (all classes) | **~0.5 ms** (lines) | varies by subband | sliding-window RLNC only — intra-epoch, slice-aligned; UEP by subband via the repair *rate* |

**One `Symbol` waist, one engine behind it.** The redundancy controller sets the
repair *rate* per class from the descriptor's `priority_class` and `deadline`; unequal
protection (not a different code family) is what steers a fixed budget up the
dependency spine.

---

## 3b. Overhead efficiency: the two shipping levers

On the operating point that matters (a single contribution stream) the binding cost is
not throughput, CPU, or latency — all healthy — it is **repair overhead**. Two levers
cut it, each gated to its safe regime so it never trades away completeness:

- **`ProactiveDecay`** (sender-side, **on by default**): the proactive code rate carries
  the mean expected loss plus a reactive-scaled fraction of the variance margin, and the
  reactive tier cleans up the generations that drew above-mean loss. A burst guard
  (`roundsEff = rounds·burstQ8One/burstQ8`) self-reverts it to full protection on a
  bursty channel, so it sheds only the i.i.d. variance tail. It is self-adapting (reads
  the measured loss/burst), needs no configuration, and is an exact no-op where reactive
  cannot land (RTT ≥ budget) — which is why it is safe to default on.

- **`AdaptiveGenSize`** (**opt-in, but the recommended setting for a known fixed-rate
  path**): widens the generation 16 → 64 when the budget clears a reactive round,
  amortizing the proactive margin over more symbols. Gated two ways so it never costs
  latency: it stays narrow below a reactive round (budget < RTT), and a **fill-time gate**
  (`NominalBitrateBps`) caps the width so a generation always fills within ~15 ms — full
  width at high bitrate, near `GenSize` at low bitrate.

**The evidence (a real-timing sweep, vs librist/libsrt at matched 4×RTT buffers):** across
bitrate {5, 25, 50 Mbps} × RTT {20, 100, 300 ms} × loss {2, 10 %} × {iid, bursty}, meld
delivers in ~one propagation time — **8–21× lower p50** than the C stacks at low/mid RTT,
2–4× at high-RTT/bursty — at 100 % delivery. `AdaptiveGenSize` lowers overhead in every
cell (often a halving, e.g. 50 Mbps/100 ms/10 %: 90 % → 57 %) **and is the only config that
stays complete in the worst corner** — 50 Mbps, bursty 10 % loss, high RTT — where the
fixed `GenSize`-16 default drops to 96–98 %; the wider window holds ~100 % there at ~3×
lower p50 and lower overhead. So it is not merely safe to enable: it is the configuration
that makes the transport general-purpose.

**`AutoGenSize` — zero-config, the recommended way to get this.** `AdaptiveGenSize` needs two
*static, shared* hints (`NominalRTTMicros`, `NominalBitrateBps`) set the same on both ends,
because the original feedback path computed the generation stride by fixed-stride arithmetic on
each end — a mismatch corrupts delivery, so a library default could not be made safe.
**`AutoGenSize` removes the burden entirely.** It rests on one change: the generation structure is
now **sender-authoritative**. The sender already stamps each symbol with its generation base
(`WindowBase`) and width (`N`); the receiver now *follows those stamps* for everything (it absorbs
into the stamped generation, and its deficit-feedback walk advances by the actual per-generation
width — `base += width` — rather than a shared constant). So the sender may pick *any* width, vary
it per generation, and the receiver tracks it with **no matching config**.

On top of that, the sender derives the width from its **own measurements** — RTT (from feedback)
and the per-generation fill rate (wall-clock span ÷ symbols, robust to bursty frame writes) — with
the same budget/RTT ramp and ~15 ms fill cap as the static form, but no hints. It bootstraps to
`GenSize` until both are measured, so the stream starts narrow-and-safe and widens once it has
*measured* that widening is safe; if the bitrate or RTT drifts mid-stream, the next generation
re-sizes. A real-timing bench confirms zero-config `AutoGenSize` reaches the **same overhead and
delivery** as the hand-tuned hint-based arm, and survives a mid-stream 10×-bitrate change a static
width cannot. It is **sender-side only** (the receiver needs nothing; setting it there is a no-op).

**`AutoGenSize` is ON BY DEFAULT.** Unlike `AdaptiveGenSize`, the structural blockers to defaulting
it are gone (it is inert-proof, needs no hints, and a mismatch is impossible since the receiver
follows stamps), and a real-timing regression sweep across bitrate × RTT × loss × {iid, burst}
shows it a **Pareto win over the fixed `GenSize`**: never worse on delivery or p99 in any cell,
overhead cut 15–40 points at 25–50 Mbps and neutral at low bitrate, and it *fixes* the fixed-width
delivery holes (e.g. 50 Mbps / bursty 10 % / 300 ms RTT: 95 % → 99 % delivery at roughly half the
p99). It no-ops where it cannot help, so it meets the same bar `ProactiveDecay` cleared. Set
`AutoGenSize = false` to pin a fixed `GenSize`.

`AdaptiveGenSize` (static hints) remains for explicit control; `Config.Check` (surfaced by
`NewSender`/`NewReceiver`) warns when it is enabled without its hints and points to `AutoGenSize`.

---

## 4. Why one code suffices (measured, not assumed)

- **A block fountain on the fatal class** buys nothing RLNC doesn't already have on
  overhead, and nothing Meld's chunking doesn't already have on scaling — the RaptorQ
  gate (§2) measured both.
- **A streaming code** beats well-tuned RLNC only in a deep-fade burst regime outside
  contribution video, and the realistic burst tail is already handled by the N2
  burst-aware sizer — the streaming gate (§2) measured it.
- **ARQ anywhere on JPEG XS** is dead on arrival (one RTT > the whole latency budget),
  so the engine there is RLNC, not retransmission.

RLNC's rateless / recoding / deadline-sliding / near-MDS-over-GF(256) properties cover
the field. The multi-code future was a hedge; the gates retired it.

---

**References (the prior art the gates evaluated against):** RFC 8681 (sliding-window
RLC FEC), RFC 9407 (Tetrys on-the-fly coding), RFC 5053 (Raptor/R10), RFC 6330
(RaptorQ), streaming/convolutional erasure codes (Martinian–Sundberg, Khisti, Fong,
Tong et al.), Tambur (Rudow et al., NSDI'23; MS patent US 11,489,620 B1), Steinwurf
Kodo (RLNC + recoding), AHECM (Tsai et al., *Multimedia Systems* 2011 — adaptive
hybrid FEC/ARQ overhead minimization, the redundancy-sizing gate). Cite the
spec/behavior in code, never a library file path (the ristgo rule).
