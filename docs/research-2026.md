# Meld — Architecture Review & Next-Build Decision Report

> **Provenance.** A fresh-eyes review of the Meld codebase paired with a frontier
> research sweep (network coding, QUIC/IETF substrate, congestion control,
> coded multipath, media-aware UEP, adaptive control, and adversarial/security
> lenses), 2026-06-21. Five subsystem deep-reads established ground truth; seven
> research axes plus two critic-surfaced gaps (DoS/resource-exhaustion safety;
> inter-path loss *correlation*) produced 58 candidate techniques, each
> adversarially verified for reality, novelty-vs-Meld, and fit with Meld's cost
> structure before scoring. Where researcher enthusiasm and the adversarial
> verdict diverged, **the verdict wins**.
>
> Calibrated to the two invariants that define Meld: the **determinism moat**
> (the sans-I/O, rank-oracle-scoreable core) and the **deliberate
> bandwidth-for-latency trade**. No recommendation here spends the redundancy
> lever Meld already pulls.

---

## 0. Post-review reconciliation (2026-06-21, after independent re-validation)

This review was written in parallel with two bug-fix commits and was then
independently re-researched (three literature deep-reads on burst-aware sizing,
CC for coded transports, and streaming-code SOTA). Read §1–§7 below as the original
review; this section is the corrected, adopted version. Where they disagree, **this
section wins**. The adopted items became the near-term build track N1–N5, and **all
of N1–N5 have since shipped** — the per-recommendation status flags in §2–§3 and the
build-plan progress in §7 record exactly what landed in the code.

**Corrections (claims now stale or wrong):**
- **No "reactive storm."** The budget&lt;RTT cpu/alloc "pathology" (lines 31, 33, 233)
  was *not* a reactive storm — the reactive tier was confirmed idle. It was two real bugs,
  both now **fixed**: (1) the erasure estimate counted a generation's deadline-skipped tail
  as channel-loss → substantially over-provisioned *proactive* repair; (2) a shared
  per-generation deadline dropped a generation's already-received tail when the cursor
  stalled. Switched to **per-symbol deadlines**.
- **The earlier budget&lt;RTT "collapse" was overstated**, and **WAN delivery at a generous
  budget improved materially**. The earlier "fundamental floor" framing was too pessimistic —
  about half that loss was the fixable shared-deadline amplification, not the block
  structure. The residual gap to the sliding profile is real but smaller.
- Consequently the case for "**BandDecoder becomes the default**" (line 31) is weaker:
  the generation coder wins LAN + generous-WAN on latency *and* (now) delivery. Make the
  band form **resource-safe via the N1 admission caps** (the `ls_max_size` /
  live-decoder bounds), not by demoting the default; "default" remains an unpinned
  product decision tied to whether Meld centers on budget&lt;RTT.

**Refinements (the re-research changed the top recommendations):**
- **A1 led with the risky half.** The 2-state GE forward-DP sizer is the *easy* part; the
  **online GE estimator is the real risk** (trigram moment solves go out-of-range on
  short windows; EM/Baum-Welch is non-convex and unfit for the core). The better first
  build is **sizing directly against the loss-run-length histogram** the receiver's
  gap-walk already produces — no GE identification, integer, oracle-scoreable — with the
  GE-DP as a higher-fidelity follow-on **in fixed-point** (the DP is *not* bit-reproducible
  in IEEE-754). Became the keystone N2 — now **shipped**: `repairForGE` is the integer
  (Q30 fixed-point) forward DP in `internal/flow/flow.go`, fed by the receiver's
  gap-walk burstiness estimate (`internal/flow/receiver.go`), with the GE-tail oracle
  and "money test" passing in `internal/flow/ge_test.go`.
- **A2/A3 confirmed and sharpened into one architecture:** **CC owns the total send-rate
  budget; the FEC sizer allocates a fraction within it** (`media_rate = budget −
  repair_rate`, never repair on top). Primary signal **delay (Copa)**; **ECN/L4S** a
  path-validated accelerator (CE survives FEC, but end-to-end validation is rare enough
  that it is never load-bearing); the **honest pre-recovery loss counter** ships first. RFC 9265's "treat
  recovered as lost" is non-normative and carved out for known-lossy paths. Became N1
  (honest loss counter + the RFC 8083/8681 resource caps) and N3 (the Copa CC with the
  L4S/DCTCP ECN response) — both now **shipped**: the loss accountant and admission caps
  are in `internal/flow/receiver.go`/`sender.go`, the controller in
  `internal/flow/congestion.go`. The one carve-out: the socket read of *real* ECN off
  the IP header is **deferred** (Linux-only — `golang.org/x/net/ipv4` exposes no TOS
  control message on darwin), so the CE path is core-complete but exercised by simulated
  marks.
- **WP7 streaming codes → gated spike.** Theory is closed but the win over burst-aware
  RLNC is narrow (burst + tight-delay + no-retransmit) and tuned to an assumed burst
  length `B`. Build the GE bench; ship only if it clears a meaningful margin over adaptive
  RLNC there.
  Candidate = **Tambur's deterministic kernel** (open, GF(2⁸); MS patent — clearance
  check), not a from-scratch construction. Borrow **now**: Tetrys ACK-driven window
  trimming + Tambur per-frame UEP. Still the frontier (§4 watch table); the GE sizer (N2)
  it depends on now exists, so the gating spike is unblocked but not yet taken.

**Resolved since this paragraph was first written** (it named the four strategic-core
gaps as open; all four are now closed in code):
- The **i.i.d.-on-a-bursty-channel controller exposure** — Meld's #1 correctness gap —
  is closed by the N2 GE sizer (`repairForGE`), which sizes the real burst tail; the
  money test in `ge_test.go` is the falsifiable proof the i.i.d. binomial sizer fails
  where the GE sizer holds.
- The **no-CC deployability gap** is closed by the N3 Copa controller
  (`internal/flow/congestion.go`): CC owns the send-rate budget and the FEC sizer
  allocates a fraction within it, so redundancy is no longer self-congesting on a shared
  link.
- The **cross-host clock** is closed by N4: an NTP-style offset handshake in
  `internal/session/clocksync.go` plus a per-symbol `SendTimestamp` wire field, with the
  offset passed as *data* into the deterministic core — so deadline comparison is correct
  off loopback.
- The **`n_src`-unbounded DoS hole** is closed by N1's RFC 8681 `ls_max_size` admission
  caps + live-decoder bound in `internal/flow/receiver.go` (covered by
  `internal/flow/safety_test.go`).

---

## 1. Verdict: are we using the right tech?

**Meld is genuinely ahead on the coding primitive and the determinism moat, and
*was* genuinely exposed on the channel model, congestion safety, and the cross-host
clock.** The coder is right; at the time of writing the controller was modeling the
wrong channel and the host couldn't leave the loopback. **The three exposures named
here have since been closed** (the GE sizer N2, the Copa CC N3, the cross-host clock
N4 — see §0 and §7); the table's *judgments* are the original verdict, annotated below
with what was acted on.

| Component | Judgment | One-line why |
|---|---|---|
| **GF(2⁸) coder (poly 0x11D, full mulTbl, MulAdd AXPY)** | **Keep** _(SIMD AXPY DONE)_ | Correct, minimum-viable field; the only real debt was a scalar AXPY where SIMD is a free win. Closed: byte-exact NEON (arm64) + AVX2 (amd64) `MulAdd` in `internal/gf`, differential-tested against the scalar golden. |
| **Seeded (base,n,key) repair encoding** | **Keep — wire format now PINNED** | RFC 8681 RLC design, real bandwidth win. Closed: `internal/wire` carries a version nibble in the leading byte (`Version=1`, `ErrVersion` on mismatch), so the header is no longer unversioned lock-in risk. |
| **Two decoders (Decoder / BandDecoder)** | **Evolve** | The Caterpillar band form is the structurally bounded one; the generation default carried a feared reactive-storm/unbounded-retention pathology — since shown (§0) to be two now-fixed bugs, not a storm, and bounded by the N1 caps. "Default" remains an unpinned product decision. (The first, dense `WinDecoder` was removed as superseded by the band form.) |
| **Generation core + variance-aware feed-forward controller** | **Evolve _(GE sizer DONE)_** | The variance-aware *set-point* is correct. The i.i.d.-binomial-on-a-bursty-channel exposure — the single biggest correctness gap — is closed by the N2 GE sizer (`repairForGE`); the binomial path stays selectable for the i.i.d. regime and differential testing. |
| **Reactive repair tier (parallel, loss-sized, persistent)** | **Evolve / cap _(token bucket DONE)_** | Closed: an aggregate emit-rate token bucket (`internal/flow/sender.go`, driven by the CC) caps source+repair bytes/sec and counts throttled repair; the receiver-side admission caps (N1) close the forged-feedback reflection vector. |
| **Band-form sliding decoder (selectable profile)** | **Keep, promote toward default** | Uniquely serves budget-below-RTT, the regime where the generation coder falls short. This is a strategic asset, not a side experiment. |
| **Rank oracle + four-invariant sans-I/O tests** | **Keep — this is the moat. Extend it, never compromise it.** | The independent rank oracle over 300 erasure patterns is the differentiator. Every new mechanism must generalize this, not erode it. |
| **UDP host (single mutex, I/O-outside-lock, 5ms tick)** | **Evolve _(clock + caps DONE)_** | Closed since: the cross-host clock (N4 offset handshake) and unbounded receiver state (N1 `ls_max_size` admission caps) are fixed. Still open: handshake/auth, PMTUD, GSO/GRO — deployability debt, not architectural error. |
| **Purist UDP vs QUIC** | **Fork RESOLVED — UDP for the shipping core, QUIC reachable behind the seam** | Closed: `internal/session/substrate.go` is an explicit `Substrate` interface (the datagram subset of `net.PacketConn`); a falsifiable A/B (since removed) showed QUIC-datagram double-controls the rate with its own CC for a ~3-orders-worse latency tail even on loopback, so the core stays UDP. See `docs/substrate.md`. |
| **Bench methodology (txbench)** | **Evolve — it currently flatters Meld** | The ARQ-wall-at-long-RTT result is real and load-bearing. The headline latency comparison and the "cost is only bandwidth" claim are **artifacts** of a shared-epoch single-process clock and zero competing traffic, not validated cross-host results. i.i.d. Bernoulli + fixed seed + few-rep median is the loss model that maximally favors block coding. |

**Where Meld is ahead:** the any-k-of-n collapse of ARQ+FEC+bonding into one
primitive is real and correct; the variance-aware feed-forward set-point is more
honest than the AC-RLNC mean-tracking the field defaults to; the sans-I/O
oracle-scoreable core is a testability advantage *no other transport has*.

**Where Meld *was* exposed (all four since closed — see §0/§7):** (1) the controller
modeled i.i.d. loss on a bursty channel and silently missed its own TargetFailure on
Gilbert-Elliott bursts — closed by the N2 GE sizer; (2) no congestion control +
coding-masks-loss was a congestion-collapse engine on a shared link — closed by the N3
Copa CC (CC owns the budget, FEC allocates within it); (3) the cross-host clock was
broken by design, making every headline latency number loopback-only — closed by the N4
offset handshake; (4) unbounded receiver state was trivially DoS-able and a reflection
amplifier — closed by the N1 admission caps + token bucket. **Still open:** the host has
no cryptographic handshake/auth, so the stateless-cookie + anti-amplification work (§4)
remains a real deployability gap.

---

## 2. Adopt now

The highest-confidence, verified, not-already-in-Meld moves. Ranked by leverage.

### A1. Gilbert-Elliott burst-aware redundancy sizing (the keystone) — **DONE (N2)**
> **Status:** Shipped as N2. `repairForGE` in `internal/flow/flow.go` is the O(N)
> forward DP over the 2-state Gilbert chain in **Q30 fixed-point** (bit-reproducible,
> unlike the IEEE-754 DP), fed by the receiver's gap-walk burstiness EWMA in
> `internal/flow/receiver.go`; `internal/flow/ge_test.go` carries the GE-tail oracle and
> the "money test" (the binomial sizer measurably violates TargetFailure on a bursty
> channel where the GE sizer holds it). The binomial `repairForTarget` stays selectable
> for the i.i.d. regime and differential testing.

- **Move:** Replace the i.i.d. binomial tail in `repairForTarget` with the
  per-generation erasure distribution of a 2-state GE channel — an O(N) forward
  DP over the Markov chain, sized to the same `delta` target. Pair with an
  **online GE estimator** (closed-form method-of-moments: marginal loss + lag-1
  autocorrelation + loss-after-loss counters, O(1)/symbol) extending
  `observeLoss`'s existing first-arrival-gap walk.
- **Benefit:** **survivability-at-loss×RTT** (the #1 protected win) and
  **overhead-efficiency** — at the same mean loss it places repair against the
  real burst tail and can allocate *less* during long good-state runs. It does
  NOT spend more bandwidth; it spends the same budget correctly. Fixes the
  documented fast-up/slow-0.8-down max-hold asymmetry that wastes bandwidth
  after a burst clears.
- **Effort:** M. **Risk:** 2 (the DP is a pure function mirroring
  `binomTailGreater`; oracle-scoreable like `controller_test.go`).
- **Seam:** `internal/flow/flow.go` (sizer), `internal/flow/receiver.go`
  (`observeLoss` → emit `(p, burstiness)`), one `wire.Feedback` field.
- **Citation:** Gilbert 1960 / Elliott 1963; LDMP-FEC 2025
  (doi:10.3390/electronics14030563) confirms live GE+Markov FEC sizing.
- **Why #1:** This is the prerequisite that unlocks the entire streaming-code
  frontier — none of those codes can set their burst parameter B without it —
  *and* it directly closes the WP3 GE-trace exit criterion the build currently
  fails. The adversarial verdict promoted this above the researcher's coder
  favorites for exactly this reason.

### A2. RFC 9265 loss-before-coding accountant — **DONE (N1)**
> **Status:** Shipped as part of N1. The receiver's forward-gap walk
> (`internal/flow/receiver.go`) accumulates a pre-recovery loss count that is **never
> decremented on a successful decode** and surfaces it as `wire.Feedback.CongestionLoss`
> — distinct from the smoothed `LossRate` the FEC sizer consumes. This is the honest
> congestion signal a CC loop consumes; the N3 controller now does.

- **Move:** Add a `congestionLoss` ledger in the sans-I/O core that counts
  pre-recovery wire loss (the receiver *already* measures this before the
  cursor/deadline gate at `receiver.go:87`) and **does not decrement on
  successful decode**. Surface it plus the already-carried-but-dead `EcnCE` as a
  congestion signal distinct from the residual deficit the FEC sizer consumes.
- **Benefit:** **determinism-preserving deployability precondition.** Meld's
  exact rank-deficit telemetry makes the RFC 9265 "recovered packet counts as
  lost to CC" rule *trivially correct* — a determinism advantage no generic FEC
  stack has. This is the cheapest step against the deepest objection (no CC =
  collapse loop).
- **Effort:** S. **Risk:** 2. Pure receiver-side accounting, zero clock reads,
  moat intact.
- **Seam:** `internal/flow/receiver.go`, `wire.Feedback` (two counters).
- **Citation:** RFC 9265 (IRTF NWCRG, 2022) — "FEC coding mechanisms should not
  hide congestion signals" (verbatim).
- **Note:** This builds the *honest signal*, not the CC loop. It's the
  precondition that makes the prototype CC candidates sound.

### A3. RFC 8083 circuit breaker + receiver linear-system cap (resource safety as provable invariants) — **caps + bucket DONE (N1); breaker open**
> **Status:** The load-bearing bounds shipped as part of N1. (a) **Receiver:** the RFC
> 8681 `ls_max_size` discipline is enforced in `FeedSymbol` (`internal/flow/receiver.go`)
> — a symbol whose declared window exceeds `maxGenSymbols`, sits beyond the bounded
> look-ahead horizon, or would push past the live-decoder cap is refused *before any
> decoder is allocated* (counted as `Rejected`); `internal/flow/safety_test.go` asserts a
> forged-symbol flood cannot exceed the cap. (b) **Sender:** an aggregate emit-rate token
> bucket over source+repair bytes (`internal/flow/sender.go`). Still open: the RFC 8083
> repair-not-helping circuit *breaker* (the explicit trip condition) was deferred — the
> *bound* landed, the breaker prototype did not.

- **Move:** Two deterministic bounds. (a) **Sender:** an aggregate token bucket
  over *all* emitted bytes/sec (source+repair) plus RFC 8083 trip conditions
  (repair-not-helping for N RTTs; feedback timeout). (b) **Receiver:** RFC 8681
  `ls_max_size` discipline — reject any symbol whose declared `n > maxGenSymbols`
  and cap `len(r.gens)` before allocation. Today the only gate is
  `base+n<=cursor`; `n` is used raw from a uint16 wire field, so `base=cursor+1`
  with huge N allocates unbounded state.
- **Benefit:** **determinism (the moat) + survivability.** Bounds the
  reactive-storm pathology and the forged-feedback reflection vector; turns
  worst-case receiver memory into `O(maxRetainedGens · GenSize · SymbolSize)`
  regardless of input — exactly the finite-state shape TLA+ can model-check.
  Costs honest paths *nothing* (an honest sender never declares n>GenSize).
- **Effort:** S (caps) / M (breaker). **Risk:** 1–2.
- **Seam:** `internal/flow/sender.go` (bucket+breaker),
  `internal/flow/receiver.go` `FeedSymbol`/`gen()` (pre-alloc predicates).
- **Citation:** RFC 8083 (2017); RFC 8681 (2020) `ls_max_size`; RFC 6363
  §Security.
- **Why adopt-now together:** A3a (caps) is the single load-bearing DoS
  primitive and the cheapest provable invariant; A3b (breaker) prototype-first
  if you prefer, but the *bound* should land now.

### A4. Cross-host clock-offset handshake (the latency numbers don't survive without it) — **DONE (N4)**
> **Status:** Shipped as N4. `internal/session/clocksync.go` runs the NTP-style 4-timestamp
> offset estimate (host-side, paced by a probe tick); the offset is passed as **data** into
> the deterministic core via `Receiver.coreNow()` (`internal/session/session.go`), so the
> flow core still reads no clock. The carrier is a per-symbol `SendTimestamp` wire field
> (`internal/wire/wire.go`). A clock seam injects an offset clock in tests so the handshake
> is exercised without two machines.

- **Move:** A minimal 2-message RIST-style timestamp exchange in
  `internal/session`, with the estimated offset passed as **data** into the
  sans-I/O core (no clock read in the flow core). Add a send-timestamp wire
  field. Optionally adopt the QUIC-TS one-way-delay-*variation* trick
  (offset-invariant by differencing) as the tracking estimator.
- **Benefit:** **latency-validity.** The bench audit names the shared-epoch clock
  "the single biggest methodological flatterer"; `clock.go:36-40` admits
  absolute-deadline comparison is loopback-only. Until this lands, every
  cross-host latency claim is fiction. Also feeds `effectiveBand`, which
  currently uses a polluted `rtt/2` one-way estimate.
- **Effort:** M. **Risk:** 2 (offset passed as data keeps the core
  deterministic; Manual-clock testable).
- **Seam:** `internal/session` (handshake), `internal/wire` (send-ts field +
  version nibble), `internal/clock`.
- **Citation:** RFC 9000 §connection-migration (2021); draft-huitema-quic-ts
  (OWD-variation-is-offset-invariant; note: -08 is Aug 2022, now inactive);
  RIST/SRT offset estimation (the author's own prior art).
- **Why adopt-now:** It's a precondition for *any* latency claim to mean anything
  off loopback, and it forces the wire-format/version decision you must make
  before deployments freeze the header anyway.

### A5. Decodable-Frame-Rate as the objective and the bench metric — **DONE in-core (WP6 + N2)**
> **Status:** The objective and loss model are built. The receiver computes decodable-frame
> stats **parse-free** from the per-symbol frame descriptors (`flow.FrameStats`:
> `Frames`/`DecodableFrames`/keyframes, `internal/flow/receiver.go`, surfaced via
> `Receiver.FrameStats()`), proven glass-to-glass against ffprobe on real B-frame media; and
> the Gilbert-Elliott channel + tail oracle live in-core (`internal/flow/ge_test.go`). The
> per-stream txbench GE scenario + bootstrap-CI methodology fix lives in the separate
> `~/dev/txbench` lab and is tracked there, not in this repo.

- **Move:** Add decodable-frame-rate Q and time-to-first-decodable-frame to
  txbench, fed by a real elementary stream with GOP/IRAP structure, and add a
  Gilbert-Elliott loss model alongside i.i.d. Reframe the controller target from
  per-generation decode-failure to per-frame decodability.
- **Benefit:** **turns the WP6 thesis from slogan to measured claim** and
  **exposes the GE-burst weakness the byte metric hides** (a burst killing one
  base frame tanks Q far more than scattered i.i.d. loss). A byte-exact-recovery
  figure under heavy loss is a packet claim, not QoE.
- **Effort:** M. **Risk:** 1. Lives in `txbench`/`relay.go`, touches nothing in
  the core; reuses the deterministic decode-target oracle for seed-reproducible
  Q.
- **Citation:** decodable-frame-rate Q is standard UEP-FEC eval
  (arXiv:2402.04729; IEEE TCE 2024).
- **Why adopt-now:** It's the yardstick. Without it you cannot honestly evaluate
  A1, and you cannot defend the "protect the picture" pillar. Pairs naturally
  with the GE loss model the rest of the roadmap needs.

---

## 3. Prototype next

Promising-but-unproven bets worth a time-boxed spike.

### P1. Second-moment in-order-delay budgeting (E[D] + z·√Var[D])
- **Move:** Size the band/deadline so `E[recovery delay] + z·√Var[recovery
  delay] ≤ BufferMicros` for a target **per-stream** late-probability, replacing
  the per-generation `TargetFailure` (whose per-generation setting compounds into a
  much larger per-stream outage over the many generations in a stream — a knob that
  surprises operators).
- **Benefit:** **latency-tail honesty** — the delay variance is exactly what the
  user feels as freeze/jitter and exactly what release-on-decode is selling.
  Fixes the surprising per-generation semantics. Coding *shrinks* the variance
  term, so this also tells you where coding earns its keep.
- **Effort:** M. **Risk:** 2. Pure function; testable via the existing
  `latency_experiment_test` delay-line harness.
- **Seam:** `internal/flow/sliding.go` (`effectiveBand`), `flow.go` (target
  semantics).
- **Citation:** Cloud/Leith/Medard arXiv:1408.1440 (derives E[D] and Var[D] for
  on-the-fly coding); arXiv:1509.00167.
- **Settles it:** Does z·√Var sizing hit a target *per-stream* late-probability
  on the GE bench (A5) at equal-or-lower overhead than the per-generation
  set-point? Sequence after A1/A2 supply the erasure distribution.

### P2. Tetrys-style feedback-driven window trimming (retention bound)
- **Move:** Let cumulative feedback (`DecodedLowEdge`, already on the wire) evict
  from the *coding* window, not just the delivery buffer — a deterministic
  retention rule directly bounding the "unbounded retention when budget<RTT"
  pathology. Also evaluate the standardized GF(2⁴) option as a data point for the
  field-size fork.
- **Benefit:** **determinism + overhead** — caps retention with a pure rule and
  gives a standards-grade interop oracle. Both changes are sans-I/O.
- **Effort:** M. **Risk:** 2.
- **Seam:** `internal/code/band.go` / `window.go` (trim on ACK),
  `internal/flow/receiver.go`.
- **Citation:** RFC 9407 (Tetrys, IRTF NWCRG 2023) — elastic window, ACK-driven
  trimming, GF(2⁴)/GF(2⁸) selectable. (Authors: Detchart, Lochin, Lacan, Roca.)
- **Settles it:** Does ACK-driven trimming bound worst-case retention under
  budget<RTT without losing recoverable symbols on the GE bench?

### P3. GFNI / PSHUFB / NEON SIMD AXPY behind go:build — **DONE**
> **Status:** Shipped. `internal/gf` carries a NEON split-table `MulAdd` (arm64,
> `gf_arm64.s`/`gf_arm64.go`) and an AVX2 path (amd64, `gf_amd64.s`/`gf_amd64.go`), each
> gated by build tag with a scalar generic fallback (`gf_generic.go`). The result is
> **byte-for-byte identical** to the scalar golden (`mulAddScalar`) — the sub-vector tail
> falls back to it — and `gf_test.go` differential-tests SIMD == scalar across coefficients.
> (The 0x11D↔0x11B change-of-basis for GF2P8AFFINEQB was not needed; the split-table TBL/
> PSHUFB form keeps the field as-is.)

- **Move:** Hand-written Plan9 asm split-table (AVX2 PSHUFB / AVX-512
  GF2P8AFFINEQB / ARM64 NEON TBL), with a constant 0x11D↔0x11B change-of-basis so
  GFNI is usable without abandoning the field. Generic scalar fallback.
  Differential fuzz: `SIMD == scalar` for all coefficients.
- **Benefit:** **latency + overhead, second-order.** A faster AXPY directly shrinks
  release-on-decode time at the dense-whole-window decode that is the O(W²) wall
  — *and* lets the operator afford a larger BandDecoder `b`, which widens the
  coding span, which shrinks the binomial variance margin = **lower overhead for
  equal protection.** Also makes per-symbol homomorphic-MAC affordable later.
- **Effort:** M. **Risk:** 2 (function contract byte-identical; oracle and moat
  untouched). Only friction is asm vs stdlib-only ethos — mitigated by fallback.
- **Seam:** `internal/gf` (`MulAdd`/`MulSlice` behind build tags).
- **Citation:** Plank FAST'13; klauspost/reedsolomon; ISA-L; GF2P8AFFINEQB
  hardwired to 0x11B (Intel SDM / WikiChip), isomorphism trick standard.
- **Settles it:** Measure decode-throughput and the largest affordable `b` across
  architectures; confirm the overhead-efficiency second-order win is real, not
  just raw Mops.

### P4. TLA+/Apalache spec of the sans-I/O state machine + record/replay
- **Move:** Model-check the four invariants **plus liveness** (every symbol
  delivered or deadline-evicted) over *all* interleavings of
  loss/feedback/deadline/tick events — not the 300 sampled patterns. Keep the
  spec and Go in lockstep (CCF "smart casual verification" style). Add
  deterministic record/replay: a captured `(symbol, feedback, timestamp)` trace
  replays byte-identically.
- **Benefit:** **extends the moat itself** (the explicitly protected asset).
  Exhaustive interleaving + liveness is exactly where the reactive-storm and
  unbounded-retention pathologies live — termination bugs property tests miss.
  Record/replay turns every impair/txbench failure into a minimizable artifact.
- **Effort:** M (spec-writing skill, not code churn). **Risk:** 1. Zero
  bandwidth, zero runtime cost.
- **Seam:** new `tla/` spec; `internal/flow` replay harness.
- **Citation:** Howard et al., "smart casual verification," arXiv:2406.17455
  (NSDI'25, CCF); TLA+/TLC, Apalache.
- **Settles it:** Does the checker confirm the A3 caps make the receiver a
  bounded finite-state machine, and does it surface any liveness gap in the
  reactive tier? Promote above the crypto/streaming favorites — this is the
  cheapest way to convert "tests pass on 300 patterns" into "invariant holds
  always."

### P5. Loss-proportional coded multipath (schedule degrees-of-freedom, not packets) + cross-path interleaving — **DONE (N5), generalized 2→N paths**
> **Status:** Shipped as N5 and since **generalized from 2 paths to N** (`Config.Paths`
> up to `maxPaths = 8`). The dof-balancing scheduler meters innovation per path by a
> weighted round-robin on delivered-innovation weight (`internal/flow/multipath.go`); the
> decoder is unchanged (union-k). The correlation-aware **joint-tail sizer is now N-path**:
> `repairForJointTailN` convolves the receiver-measured per-slot erasure-count histogram
> over aligned N-path slots (the 2-path `repairForJointTail` is a thin entry that builds the
> 3-bucket histogram and defers to it). The receiver measures per-path marginals + the count
> histogram (`coLossEstimator`) and reports them as a **variable wire section** — `nPaths`
> byte, then `PathLoss[]`, then `SlotDist[]` (`wire.Feedback`, length- and bound-gated by
> `feedbackMaxPaths` so a forged `nPaths` cannot over-allocate). The 3-path four-invariants
> and a 3-path joint-tail "money test" pass. (The contrarian correlation-gated concentration
> arm remains an open research direction, not shipped.)

- **Move:** A sans-I/O path-set abstraction and a dof-balancing scheduler: meter
  innovative symbols to each path proportional to its delivered-innovation rate,
  deadline-clustered. The decoder needs **zero change**; `repairForTarget` is
  reused over a union-k. Add a **correlation-aware joint-tail sizer** (size r
  against the joint co-loss tail, not the product of marginals) fed by an
  **aligned-window co-loss estimator** (2×2 contingency over aligned SrcIndex
  windows → `rho_co`). Add **cross-path coding-window interleaving** (pure
  `pathOf(kind, srcIndex, offset, paths)`) to actively de-correlate single-path
  bursts into scattered erasures the band decoder already tolerates — at **zero
  extra bandwidth**.
- **Benefit:** **overhead-efficiency + survivability** — realizes pillar #2
  (diversity not duplication: combine lossy paths into one survivable aggregate
  near unit overhead). Interleaving and joint-tail sizing both *lower* the r needed
  for equal TargetFailure; the contrarian correlation-gated concentration arm can
  *lower total bytes* versus both Meld-today and stream-duplication schemes in the
  worst shared-congestion regime.
- **Effort:** L. **Risk:** 2–3.
- **Seam:** `internal/flow` (path-set + scheduler), `wire.Symbol` (`path_id` —
  consumes the last reserved byte, so **pin the version nibble first**),
  `receiver.go` (per-path/co-loss estimators).
- **Citation:** arXiv:1507.08499 (Garcia-Saavedra/Karzand/Leith, IEEE TMC 2017);
  arXiv:1609.00424 (Cloud/Medard, GLOBECOM 2016); Kurant arXiv:0901.1479 (Spread
  interleaving); rural dual-5G correlation study.
- **Settles it:** On the **correlated-GE two-path bench** (build this first — see
  §7), does coded-union + interleaving improve on stream-duplication on overhead at
  equal delivery across the rho sweep, and does the joint-tail sizer hold
  TargetFailure as rho→1 where the i.i.d. union silently fails?

---

## 4. Watch / research-frontier

| Item | Trigger that promotes it |
|---|---|
| **Tambur-style streaming codes (per-frame burst-aware UEP parity)** | **Prerequisites now met** (A1/N2 burst estimator shipped; the WP6 shaper maps variable-size frames) — promotable to a gated prototype spike. It's the WP7 blueprint; strip the learned predictor to a deterministic EWMA/table. Ship only if it clears a meaningful margin over adaptive RLNC on the burst regime (§0). |
| **Delay-optimal (B,W,N) streaming convolutional codes** | Promote when you decide to replace online-GE-over-a-band with a true streaming code behind the waist (WP7). Needs A1's burst estimate to set B. The rank oracle generalizes to a deadline-recovery oracle — keep that property. |
| **METTLE peeling/SC-LDGM (GF(2) XOR, GE-tuned)** | Promote to a time-boxed spike *only* to settle the GF(2)-vs-GF(2⁸) and dense-vs-sparse forks. Single un-peer-reviewed 2026 preprint, patent-filed — do not commit production until independent replication exists. |
| **QUIC-datagram substrate + MPQUIC + accurate-ECN/L4S CC** | **Fork resolved *toward UDP*** for the shipping core (§1; `docs/substrate.md`) — QUIC stays reachable behind the `Substrate` seam but is not the chosen host, so this row is no longer the promotion path. MPQUIC's path lifecycle would be a fresh decision against the now-shipped N5 coded scheduler. The L4S-keys-CC-on-surviving-marks idea **shipped independently** in N3 (next row). |
| **Copa-style delay-based CC (non-ECN fallback)** | **DONE (N3).** `internal/flow/congestion.go` is the Copa controller (min-RTT/standing-RTT filters, additive 1/(δ·d_q) target, slow-start + min-RTT re-baseline) layered with the L4S/DCTCP α-EWMA multiplicative decrease on the CE-marked fraction. Structurally immune to loss-masking; pure-arithmetic, fits the moat. The send-timestamp echo it needed (A4/N4) is also shipped. |
| **CC competitive-mode fairness (vs. a real competing flow)** | **OPEN RESEARCH** — investigated, *not* shipped (the spike was reverted). Meld's loss-agnostic CC + min-RTT re-baseline don't fit the classic delay-vs-loss starvation framing, so the obvious "competitive mode" knob has no falsifiable target in the i.i.d. loopback bench. Settling it needs the `~/dev/impair`/`~/dev/txbench` lab driving a real competing TCP/BBR/Copa flow on a shared bottleneck. Promote when that scenario exists. |
| **Sliding-window recoding at relays (CRLNC) + homomorphic-MAC** | Promote when WP5 multipath is scoped *and* an untrusted-relay/CDN topology is committed. Recoding forces abandoning seeded coefficients for explicit vectors on recoded symbols — the largest wire lock-in; settle the format before any deployment freezes it. Homomorphic-MAC (SpaceMac/Agrawal-Boneh) is the *only* auth that survives recoding, but it's gated on a basic AEAD+handshake existing first. |
| **RaptorQ for the fatal+long-deadline class** | Promote when the shaper can *name* the fatal class (parameter sets / IDR / JPEG XS LL band). Must never become default — the block barrier kills the low-latency win. Verify licensing. |
| **Stateless cookie handshake + 3× anti-amplification** | Promote alongside A4 (the handshake is the natural carrier for both the cookie *and* the clock epoch). Precondition that makes the A3 caps trustworthy against spoofing. |
| **Expanding-window / nested-window RLNC as native UEP** | **Gate now met** — the WP6 shaper supplies the importance ordering (exact per-codec dependency; UEP-by-tier already lands keyframes ahead of B-frames, `internal/flow/uep_test.go`). The *nested-window* construction itself (importance-nested windows reusing `RepairWindow(key,ew)` for zero extra bandwidth, zero decoder change) is the remaining promotable step. |
| **Vidaptive repair-as-backlog-filler; AC-RLNC dof-gap unification; NVC-CC media-weighted CC** | **Gates now met** — the primary CC (N3 Copa/L4S) and the WP6 descriptor/shaper both exist, so these are revisitable. Each is a refinement of the now-shipped controller rather than a controller that doesn't exist. |
| **Kernel I/O offload (GSO/GRO/sendmmsg/SO_TXTIME)** | Promote when you need the pacing *enforcement arm* for A2 (SO_TXTIME makes redundancy safe on a shared link) or when GRO-fixable phantom kernel-drop loss starts flattering measured survivability. Linux build-tagged, below the waist. |

---

## 5. Reject (and why)

State these explicitly so they don't get re-proposed.

- **"Just add more redundancy" in any form.** This is the lever Meld *already
  pulls* (it already runs at high redundancy in heavy-loss LAN regimes). Every
  recommendation here must spend the *same* budget better (burst-aware placement,
  interleaving, joint-tail sizing) or *less* (concentration under correlation).
  Reject any technique whose only effect is more repair bytes.
- **Neural JSCC / DeepJSCC (GRACE-style) in the data path.** Optimized for the
  wireless PHY (continuous SNR, soft symbols); over a hard-decision
  packet-erasure UDP channel the graceful-degradation gains largely evaporate.
  Worse, it **destroys the determinism moat** (non-deterministic across
  versions/hardware/float, not oracle-scoreable) and couples transport to a
  specific codec, fighting the narrow waist. Salvageable kernel
  (importance-weighted protection) is already the WP6 UEP plan. *Reject for
  core/waist; at most the shaper borrows importance-as-scalar with no NN in the
  data path.*
- **Live RL/contextual-bandit redundancy control with the model in the flow
  path.** Breaks replayability. The *only* admissible form is offline-train →
  freeze → ship a quantized pure-function lookup, and even that is unproven over
  the analytic GE controller (A1) and needs trace infrastructure that doesn't
  exist. Watch the frozen-policy *packaging*, reject the live loop.
- **AC-RLNC mean-tracking control law.** Meld already evaluated and rejected this
  — mean-tracking leaks the variance tail at equilibrium, which is the exact
  thing Meld's feed-forward set-point gets right. Adopting its a-posteriori
  retransmit gate also reintroduces the RTT-feedback coupling the sliding thesis
  escapes. (The multipath water-filling allocation is separately worth mining
  for P5; the control law is not.)
- **Pairing-based homomorphic signatures for line-rate symbol auth.**
  Near-vaporware at real-time rates. If/when recoding auth is needed, favor
  symmetric homomorphic-MAC / null-space schemes and *measure before committing
  wire bytes*.
- **RaptorQ as a general/default engine.** The block barrier is antithetical to
  the entire low-latency thesis. Reserved, narrow, long-deadline only — never
  the steady path.
- **Quoting the headline latency comparison as a result.** It is a shared-epoch
  single-process-clock + release-on-decode artifact (relay-one-way + decode time,
  never met real jitter), not a validated cross-host result. Keep the *qualitative*
  claim (no TSBPD playout hold; releases on decode) and stop quoting any multiple
  until measured cross-host with the N4 offset estimation. Same for "the cost is only
  bandwidth" — that holds *only* because the bench has no shared bottleneck; with cross
  traffic, the redundancy Meld runs is loss-inducing self-congestion. The N3 Copa/L4S CC
  now exists to make that safe (CC owns the budget, FEC allocates within it), but
  the claim still must be re-measured *with* a competing flow before it can be
  quoted (see the competitive-mode open-research item in §4).

---

## 6. The single highest-leverage move — **DONE (N2)**

**Build Gilbert-Elliott burst-aware redundancy sizing + its online estimator
(A1).** *This was the recommendation; it has shipped — the sketch below records the
build that landed.*

**Why:** It is simultaneously (a) the fix for Meld's #1 correctness exposure —
the controller silently misses its own advertised TargetFailure by orders of
magnitude on the bursty channels that are the *actual* contribution-video loss
profile; (b) the keystone that unlocks the entire streaming-code frontier, none
of which can set their burst parameter B without it; (c) the closure of the WP3
GE-trace exit criterion the build currently fails against; and (d) an
**overhead-efficiency** win, not a bandwidth spend — it reallocates the same
budget to the real failure mode and can spend *less* during good-state runs. It
respects the cost structure exactly, preserves the moat (pure-function DP,
oracle-scoreable), and is medium effort / low risk.

**What shipped** (the sketch, as built):
1. In `internal/flow/receiver.go`, the forward-gap walk accumulates the
   pre-recovery loss count and loss-run lengths over its window, from which a
   smoothed **burstiness EWMA** (mean burst length) is derived in ppm/fixed-point —
   no matrix inversion, no floats on the wire.
2. The marginal loss (`LossRate`) and the burstiness estimate ride `wire.Feedback`,
   carried under the **version nibble** (`Version=1`) the same change pinned — the
   forcing function for header versioning, now done.
3. `internal/flow/flow.go` carries `repairForGE`: an O(N) forward DP over the
   2-state Gilbert chain in **Q30 fixed-point** (bit-reproducible, unlike the
   IEEE-754 DP) giving `P[erasures > r | GE]`, sized to the same `delta`. The
   binomial `repairForTarget` stays selectable for the i.i.d. regime and for
   differential testing.
4. **Invariant/bench:** `internal/flow/ge_test.go` carries a GE-tail oracle
   (independent dense occupancy distribution) confirming the integer Q30 DP agrees
   with the exact float DP, plus the "money test" asserting the binomial sizer
   measurably violates TargetFailure on a bursty channel where `repairForGE` holds
   it. The per-stream correlated-GE txbench scenario lives in the separate
   `~/dev/txbench` lab.

---

## 7. A concrete next-build proposal — **Steps 0–4 SHIPPED (N0–N5)**

An ordered, testable plan turning the top adopt/prototype items into Meld's
near-term build track, sans-I/O determinism preserved, each step gated by an
invariant or bench it must prove. **This plan has been executed: Steps 0–4 are all
in the code as N0–N5.** Each step below keeps its original gate and records what
landed (and the few sub-items deliberately deferred).

### Step 0 — Pin the wire format (blocking, do before anything touches the header) — **DONE**
Add a **version nibble** to the leading byte of every message and gate optional
tail fields, before A1, A4, and P5 each independently demand a new field and
collide.
- **What shipped:** `internal/wire/wire.go` carries `Version = 1` in the high nibble
  of byte 0 (`lead`/`splitLead`); an unknown version returns `ErrVersion` before any
  parse. Additive fields append under the same version, gated by a Symbol flags bit
  (e.g. `SendTimestamp`) or a feedback length check (the multipath section). The
  fuzz round-trips and `ErrVersion`-on-mismatch hold (`internal/wire/fuzz_test.go`);
  `docs/wireformat.md` is the pinned spec.

### Step 1 — Burst-aware control + the GE bench (the keystone, A1 + A5 loss model) — **DONE (N2)**
Build the GE estimator (gap-walk extension), the GE-tail DP sizer, and the
correlated-GE scenario with the decodable-frame-rate metric.
- **What shipped:** the burstiness EWMA in `internal/flow/receiver.go`, the Q30
  fixed-point `repairForGE` in `internal/flow/flow.go`, and the GE-tail oracle +
  "money test" in `internal/flow/ge_test.go` (the binomial sizer measurably violates
  TargetFailure where the GE sizer holds it). The decodable-frame metric is
  `flow.FrameStats` (WP6). The per-stream correlated-GE txbench scenario + bootstrap
  CI lives in the `~/dev/txbench` lab.

### Step 2 — Honest signal + provable bounds (A2 + A3) — **DONE (N1), except the RFC 8083 breaker**
Add the RFC 9265 loss ledger, the sender aggregate token bucket + RFC 8083 circuit
breaker, and the receiver `ls_max_size` admission caps.
- **What shipped:** the pre-recovery loss ledger (never decremented on decode →
  `Feedback.CongestionLoss`) and the `ls_max_size` + live-decoder admission caps in
  `internal/flow/receiver.go`; the aggregate emit-rate token bucket in
  `internal/flow/sender.go`. `internal/flow/safety_test.go` asserts a forged-symbol
  flood cannot exceed the cap. **Still open:** the RFC 8083 repair-not-helping
  circuit *breaker* and the TLA+/Apalache spec (P4) — the *bounds* landed; the
  breaker trip and the model-checked liveness proof did not.

### Step 3 — Make the latency claim real (A4) — **DONE (N4)**
The cross-host offset handshake in `internal/session`; offset passed as data into
the core; send-timestamp wire field (reserved in Step 0).
- **What shipped:** `internal/session/clocksync.go` (NTP-style 4-timestamp offset),
  `Receiver.coreNow()` adding the offset so the core compares sender-stamped
  deadlines correctly, and the per-symbol `SendTimestamp` wire field. A clock seam
  exercises the handshake in-test without two machines. The cross-host latency
  bench wiring lives in the lab.

### Step 4 — Coded multipath, honestly (P5) — **DONE (N5), generalized 2→N paths**
Path-set abstraction + dof-balancing scheduler (decoder unchanged, union-k);
aligned-window co-loss estimator; correlation-aware joint-tail sizer; cross-path
interleaving.
- **What shipped:** the weighted-round-robin dof scheduler, the `coLossEstimator`
  (per-path marginals + the per-slot erasure-count histogram over aligned N-path
  slots), and the joint-tail sizer **generalized to N paths** as
  `repairForJointTailN` (histogram convolution; the 2-path `repairForJointTail` is a
  thin entry over it) — all in `internal/flow/multipath.go`, with the per-path
  feedback a bound-gated variable wire section (`nPaths`/`PathLoss[]`/`SlotDist[]`).
  `Config.Paths` ranges to `maxPaths = 8`; the 3-path four-invariants and a 3-path
  joint-tail "money test" pass. **Still open:** the contrarian correlation-gated
  concentration arm.

**Sequencing rationale (as executed):** Step 0 unblocked every header change. Step 1
is the keystone and the in-core oracle that makes everything else falsifiable. Step 2
makes Meld defensibly deployable (bounded state, honest congestion signal) — the
model-checked liveness proof remains the open extension of the moat. Step 3 makes the
headline latency claim real instead of a loopback artifact. Step 4 realizes pillar #2
— and the joint-tail/interleaving machinery generalized cleanly from 2 to N paths.
Every step spent the determinism moat as an asset (the rank/GE/joint-tail oracles) and
never spent the bandwidth lever Meld already pulls.

---

## Appendix A — Verified candidate ledger

58 candidates across 9 axes; 11 adopt-now, 22 prototype, 17 watch, 1 reject,
3 already-in-Meld. Scores are 1–5 (benefit, fit, risk); effort S/M/L/XL. Every
row passed adversarial verification (reality + citation check + not-already-in-Meld).

### Adopt now

| Candidate | Axis | Ben | Fit | Eff | Risk | Key source |
|---|---|---|---|---|---|---|
| GE/run-length burst-aware sizing (keystone) | coding | 5 | 5 | M | 2 | Gilbert'60/Elliott'63 |
| GE 2-state Markov-tail sizer (vs i.i.d. binomial) | adaptive | 5 | 5 | M | 2 | LDMP-FEC 2025, doi:10.3390/electronics14030563 |
| Online GE estimation (MoM / recursive Baum-Welch) | adaptive | 4 | 4 | M | 3 | textbook GE estimators |
| RFC 9265 loss-before-coding accountant | wildcard/substrate | 5 | 5 | S | 1–2 | RFC 9265 (NWCRG 2022) |
| RFC 8083 congestion circuit breaker | congestion | 4 | 5 | S | 2 | RFC 8083 (2017) |
| Receiver linear-system cap (`ls_max_size`) | dos-safety | 5 | 5 | S | 1 | RFC 8681; RFC 6363 §Sec |
| Cross-host clock-offset handshake | substrate | 5 | 4 | M | 2 | RFC 9000; RIST/SRT prior art |
| Decodable-frame-rate objective + bench metric | media-uep | 4 | 5 | M | 1 | arXiv:2402.04729; IEEE TCE 2024 |
| GFNI/PSHUFB/NEON SIMD AXPY (0x11D↔0x11B) | wildcard | 5 | 5 | M | 2 | Plank FAST'13; ISA-L; klauspost/reedsolomon |
| Correlated-GE two-path bench scenario | multipath-corr | 4 | 4 | M | 2 | arXiv:2604.03160; rural dual-5G study |

### Prototype next

| Candidate | Axis | Ben | Fit | Eff | Risk | Key source |
|---|---|---|---|---|---|---|
| Loss-proportional coded multipath (DoF scheduling) | multipath | 5 | 5 | L | 2 | arXiv:1507.08499; arXiv:1609.00424 |
| Cross-path coding-window interleaving (de-correlation) | multipath-corr | 5 | 5 | M | 2 | Kurant arXiv:0901.1479 |
| Correlation-aware joint-tail sizer | multipath-corr | 4 | 5 | M | 2 | arXiv:2604.03160 |
| Aligned-window co-loss estimator (ρ_co) | multipath-corr | 4 | 5 | M | 2 | RFC 8382 (SBD) |
| ECN/L4S-primary congestion signal | congestion | 5 | 5 | L | 3 | RFC 9330/9331/9332 (2023) |
| Copa-style delay-based CC (non-ECN fallback) | congestion | 5 | 5 | L | 3 | Arun/Balakrishnan NSDI'18 |
| Second-moment delay budgeting (E[D]+z√Var) | adaptive | 4 | 5 | M | 2 | arXiv:1408.1440 |
| AV1 Dependency-Descriptor chains/DTI oracle | media-uep | 5 | 5 | L | 2 | av1-rtp-spec (AOM) |
| Expanding/nested-window RLNC as native UEP | media-uep | 4 | 5 | M | 2 | Sejdinovic/Vukobratovic 2009; RFC 8681 |
| Delay-optimal (B,W,N) streaming convolutional codes | coding | 5 | 4 | L | 3 | Fong-Khisti-Médard, IEEE-IT 65(7) 2019 |
| METTLE SC-LDGM peeling streaming code | coding | 5 | 4 | L | 4 | arXiv:2602.10020 (2026, unreplicated) |
| Tambur-style per-frame burst-aware UEP parity | media-uep / adaptive | 5 | 3–4 | L | 3 | Tambur, NSDI'23 |
| TLA+/Apalache spec + record/replay | wildcard | 4 | 5 | M | 1 | arXiv:2406.17455 (NSDI'25) |
| QUIC DATAGRAM substrate (RFC 9221) | substrate | 5 | 3 | XL | 4 | RFC 9221; RoQ/MoQ drafts |
| QUIC-TS one-way-delay-variation clock estimator | substrate | 4 | 4 | M | 3 | draft-huitema-quic-ts (-08, 2022) |
| Sliding-window recoding at relays (CRLNC) | multipath | 4 | 4 | L | 3 | arXiv:2306.10135 (LANMAN'23) |
| Aggregate repair token bucket + 8083 trip | dos-safety | 4 | 4 | M | 2 | RFC 8083 |
| Stateless cookie handshake + 3× anti-amplification | dos-safety | 4 | 4 | M | 2 | RFC 9000 §8.1; DTLS 1.3 RFC 9147 |
| Correlation-gated path concentration (contrarian) | multipath-corr | 4 | 4 | M | 3 | rural dual-5G study |
| Accurate-ECN as primary signal (L4S/ECT(1)) | substrate | 4 | 3 | L | 3 | RFC 9330/9331; IMC'23 ECN-in-the-wild |
| Kernel I/O offload (GSO/GRO/sendmmsg/SO_TXTIME) | wildcard | 3 | 4 | M | 2 | de Bruijn LPC'18; quic-go |
| Buffer-pool + bounded delivery channel w/ backpressure | dos-safety | 3 | 4 | M | 2 | sync.Pool; bounded RT queues |
| Homomorphic-MAC symbol auth (recoding-safe) | multipath | 3 | 3 | L | 4 | Agrawal-Boneh ACNS'09; SpaceMac |
| AC-RLNC a-priori+a-posteriori dof-gap tracker | adaptive | 3 | 3 | L | 4 | arXiv:1905.02870 (IEEE TComm 2020) |

## Appendix B — Adversarial-verification catches

The skeptic layer demoted or corrected several researcher claims (verdict trusted
over enthusiasm):
- **METTLE** (SC-LDGM, arXiv:2602.10020) — single un-peer-reviewed, patent-filed
  2026 preprint → **watch**, not adopt, until independent replication.
- **draft-huitema-quic-ts** — researcher said "-08 (2024) with running code";
  datatracker shows -08 is **Aug 2022 and inactive/expired**. Date inflation, not
  fabrication; technique still sound.
- **AC-RLNC** (arXiv:1905.02870) — venue mis-cited as ToN; it is **IEEE Trans.
  Communications 2020**.
- **RFC 9407 (Tetrys)** — author byline misattributed (correct: Detchart,
  Lochin, Lacan, Roca).
- **Inter-path correlation** — citation numbers garbled and one study's late-loss
  figure materially overstated relative to the verified source value at ≥800ms.
  Substance survived.
- **draft-irtf-nwcrg-coding-and-congestion** — actual position is "do not reduce
  cwnd *fully* on recovered packets / ECN survives FEC," slightly weaker than the
  researcher's "recovered MUST be treated as lost." RFC 9265 substance holds.
