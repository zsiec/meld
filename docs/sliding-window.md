# Sliding-window coding — the band-form low-latency profile

> **Status (built, benched, shipping as a selectable profile).** The band-form
> sliding decoder (`internal/code.BandDecoder`) is wired into `internal/flow` as a
> selectable low-latency profile (`Config.Sliding`, `CodingWindow`) behind the same
> `coreSender`/`coreReceiver` host seam as the generation coder. It is **not** the
> default — the generation coder still owns the low-RTT / generous-budget point —
> but it **serves the tight-budget long-haul regime the generation coder cannot**.
> Three rounds of work got it there:
>
> 1. **Band-form decode** (commit `721aabc`, `b76…`) replaced the first, dense windowed decoder.
>    Rows are keyed by absolute pivot so a window advance is O(1) (no left-shift) and
>    elimination only touches the band, so decode is O(b²)/symbol regardless of the
>    in-flight window. This removed the decode-cost overhead that sank the first,
>    dense attempt and brought the sliding profile's CPU back into the same range as
>    the generation coder.
> 2. **Deadline-aware adaptive band sizing.** A fixed-wide band stalls at WAN: a
>    symbol's whole coding span (band × inter-write interval) + one-way propagation
>    must arrive before its deadline, or the span's trailing edge lands at the
>    deadline and the skip beats recovery. The sender now sizes the *effective* span
>    below the configured max to leave room for `rtt/2` + a guard — wide at
>    LAN-generous budgets (low overhead), narrow at WAN/tight (slack). This lifted
>    the WAN collapse a wide fixed band would otherwise hit.
> 3. **Operating-point characterization** (`MELD_LATENCY=1 go test -run
>    TestLatencyProfile ./internal/flow`, and `txbench -lowlat`) located where the
>    profile applies, below.
>
> **Where each coder applies.** At a low-RTT or generous-budget operating point the
> **generation coder is the low-latency default** and the sliding profile does not
> displace it — a wider band lowers overhead but raises recovery latency, and the
> generation coder (eager systematic delivery + concentrated per-generation repair +
> parallel reactive) already sits at the low-latency point when the budget has
> headroom. The **band-form sliding profile exists for the budget-below-RTT regime**:
> a playout budget *smaller than the RTT* — low-latency contribution over a long-haul
> lossy link — where ARQ cannot fit a retransmit round and the generation coder's
> reactive tier storms on generations that have already missed their deadline. There
> the sliding profile's continuous, RTT-independent proactive repair, with the
> adaptive band held small by the tight deadline, recovers within the budget where
> the generation coder cannot.
>
> **The honest trade-off.** The sliding profile does **not** win on latency at a
> generous budget — it trades latency for overhead efficiency on tight and
> long-haul links. Its edges are (a) **lower overhead** for equal protection (the
> binomial variance margin shrinks with block length, so coding over a wider
> continuous window is cheaper than over a small generation), and (b) **graceful
> budget-below-RTT behavior**. Pick it for tight-budget long-haul and
> bandwidth-constrained links; keep the generation coder for low-RTT and
> generous-budget contribution.
>
> The design notes below (the model, the wire format, the controller) describe the
> sliding code generally and still stand; the band-form decoder and adaptive sizing
> are the realization that made it viable.

---

# Sliding-window coding — the design (research-grounded)

Meld's current core is **generation-based** RLNC: the stream is partitioned into
fixed blocks, repair belongs to one block, and recovery is gated per-block. With
the parallel/loss-sized/persistent reactive controller + idle-flush, this already
performs well at LAN (see [`bench.md`](bench.md)). But it has two
structural limits a **sliding window** removes:

1. **Per-generation serialization residual.** A repair only heals losses *within
   its block*; an isolated loss near a boundary, cross-block correlation, or a
   burst exceeding one block's budget leaves residual, and you pay head-of-line
   blocking waiting for the block to close. RFC 8681 §1.2: *"an isolated lost
   source packet is quickly recovered with the following repair packet [sliding
   window]. On the opposite, with a block code, recovering an isolated lost source
   packet always requires waiting for the first repair packet to arrive after the
   end of the block."*
2. **RTT-bound recovery at long-haul.** Our reactive repair needs a feedback round
   trip; when RTT > the playout budget (300 ms RTT vs 200 ms buffer in the bench),
   reactive can't help and delivery falls to the proactive-only level. Sliding-
   window recovery is **RTT-independent** — a loss is healed by the *next* repair,
   not a retransmit round trip (Tetrys: *"recovers lost packets below one RTT …
   does not depend on the RTT"*) — which lifts the long-haul numbers too.

This note is the implementation-ready design, synthesized from an adversarially
verified literature pass (sources at the end). It is the planned successor to the
generation core, behind the same `wire.Symbol`/`Feedback` waist and the same four
invariants.

## The model

One **elastic encoding window** `W` = the ordered set of source symbols not yet
known-delivered/decoded at the receiver. Append each new source symbol; evict the
oldest when `|W| > ewMax` or when feedback says it's received/decoded. A repair is a
random linear combination over **all** current window symbols (GF(2⁸)), generated
on demand. Because each repair is one degree of freedom usable against *any* hole
in the window, recovery is gated only by the global rank inequality over the whole
window — never per-block. That is what dissolves the residual.

## Wire format — adopt RFC 8681 RLC (seeded coefficients)

Carry a seed + window descriptor; both ends regenerate the coefficient vector
(extends Meld's existing `GenCoeffs` approach). Per the RFC 8681 64-bit Repair FEC
Payload ID:

- `Repair_Key` (16b) — PRNG seed (we already do this).
- `DT` (4b) — density threshold; P(nonzero coeff) = (DT+1)/16. Dense (DT=15) for
  small windows, sparser for large (cheaper decode).
- `NSS` (12b) — number of source symbols in the window for this repair.
- `FSS_ESI` (32b) — id of the first source symbol in that window.

Source symbols carry a 32-bit id (Meld already has `SrcIndex`). `(FSS_ESI, NSS)`
name the window; the seed names the coefficients — the receiver rebuilds the
equation from ~8 header bytes. (Tetrys/RFC 9407's explicit encoding-vector format
is the alternative; the seeded form is tighter and matches our `code` package.)

## Receiver — incremental in-order decoder, bounded cost

Keep a coefficient matrix in shifted reduced row-echelon form (one column per
window source symbol, one row per innovative arrival) — extend the online
Gauss-Jordan already in `internal/code`. **Deliver the contiguous decoded prefix**
(the maximal run of fully-solved columns from the window base) and advance the
delivery low-edge; **skip on deadline** (the only residual). Use the **"seen"
frontier** (Sundararajan: seen = RREF pivot column) for in-order tracking: a symbol
can be *seen* (pivot exists) before *decoded* (fully solved) — deliver on decode,
acknowledge on seen.

Bound complexity with the **band form** (Caterpillar RLNC): right-to-left pivot /
left-to-right elimination so at most one row is flushed per window advance →
**O(W²)** per symbol, matrix ≤ W×W, not O(N³) over the stream. Size `W ≈ deadline /
symbol-time`, capped by the CPU budget (a repair from further back can't arrive
before playout anyway). RFC 8681 reports 745 Mbps–2.8 Gbps decode at W≈18–23 on an
ARM A15 — feasible.

## Feedback — seen-edge + SACK

Report a cumulative **seen/decoded low-edge** (drives window pruning + in-order
progress) plus a **SACK bitmap of holes above it** (drives targeted repair).
Idempotent and loss-tolerant (a lost report is superseded by the next — Meld's
feedback already is). Prune a window symbol when feedback says *received OR
decoded*.

## Scheduler — AC-RLNC proactive + reactive (we already do the shape of this)

Two tiers (Cohen/Médard AC-RLNC):
- **A-priori (proactive)**, sized to the mean + variance: our `repairForTarget`
  (exact binomial to target δ) is exactly this, and is *stronger* than AC-RLNC's
  `m = ⌈p·k⌉` (mean only).
- **A-posteriori (reactive)**: send extra repair when the channel rate doesn't
  exceed demand — the rule is `r − d > th` with `r = 1−p`, `d =` DoF-needed /
  DoF-added, and threshold **`th = √v_e`** (the loss *variability*, not just the
  mean — reserves burst margin). Our parallel/loss-sized reactive repair is the
  generation-based analog; over a sliding window it becomes a single DoF-gap
  decision over the whole window (no per-generation loop).

## Why this gets long-haul too

Proactive redundancy sized to loss+variance heals most losses with **zero feedback
RTT**, so even at 300 ms RTT > 200 ms budget the window recovers within the deadline
where the generation core's reactive tier cannot. The `R > p` margin (Tetrys
Theorem 1: finite recovery delay iff code rate `R >` loss `p`, tail `∝ 1/(R−p)`)
sizes the proactive rate; the second-moment delay (`E(S)+k·√Var(S)`) budgets the
deadline against the tail, not the mean.

## Build checklist (maps to the existing packages)

- `internal/code` — add an elastic-window encoder (append/evict/prune) and a
  band-form incremental in-order decoder (extend the current RREF decoder); keep
  the seeded-coefficient generation.
- `internal/wire` — `Symbol` gains the RLC repair descriptor `(Repair_Key, DT,
  NSS, FSS_ESI)`; `Feedback` gains seen-edge + SACK (supersedes the per-gen
  `Deficits` vector).
- `internal/flow` — replace generation management with window management; the
  scheduler keeps `repairForTarget` (proactive) + a DoF-gap reactive over the
  window; deadline-skip unchanged.
- Tests — fuzz the coefficient round-trip; the four invariants with the `R > p`
  boundary as a property (recovery delay must diverge as `R ↓ p`); golden-vector
  the RFC 8681 repair formula.

## Sources (adversarially verified)

RFC 9407 (Tetrys) and arXiv:0904.4202 (Theorem 1, RTT-independence); RFC 8681 (RLC
wire format, on-the-fly repair, §6.2 linear-system decode, §8 complexity);
Karzand/Leith/Médard arXiv:1509.00167 (`E(S)=(l−1)ε(1−ε)^{l−1}/(1−lε)`, finite iff
`lε<1`); Cloud/Leith/Médard arXiv:1408.1440 (in-order delay, ARQ ≥1 RTT);
Cohen/Médard AC-RLNC arXiv:1905.02870 (proactive+reactive, `r−d>th`, `th=√v_e`,
zero error / >90% capacity); Sundararajan arXiv:0809.5022 (seen = RREF pivot);
Caterpillar RLNC IEEE Access 2017/2018 (band-form O(W²) decode).
