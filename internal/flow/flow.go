// Package flow is Meld's deterministic, sans-I/O core: the Sender and Receiver
// state machines for coded transport. It never reads a clock, opens a socket, or
// spawns a goroutine — time enters only as explicit clock.Timestamp arguments and
// effects leave as drained datagram/delivery queues. The host (internal/session)
// owns the clock, the timer cadence, and the wire.
//
// # Coding model (first cut: generation-based RLNC)
//
// The source stream is partitioned into fixed-size generations of GenSize source
// symbols. Within a generation the Sender emits each source symbol as a
// SYSTEMATIC coded symbol immediately, then, when the generation closes (fills,
// or is flushed at end of stream), emits repairCount REPAIR symbols — random
// linear combinations over the whole generation (internal/code). The Receiver
// recovers any generation that loses at most its repair count, delivers source
// symbols in strict order, and evicts a generation whose deadline passes
// (delivering the in-order prefix it has, skipping the rest). This is a
// systematic block code with RLNC repair; a truly sliding / streaming window
// (lower tail latency, coding across boundaries) is a later refinement behind the
// same wire and the same four invariants.
package flow

import (
	"math"

	"github.com/zsiec/meld/internal/clock"
)

// Config parameterizes a Sender/Receiver pair. Both ends must agree on Flow,
// SymbolSize, and GenSize; Redundancy, TargetFailure, and BufferMicros are
// sender-side policy.
type Config struct {
	// Flow is the flow identifier stamped into every symbol and feedback report.
	Flow uint32
	// SymbolSize is the fixed coded-symbol size in bytes (one source media chunk).
	SymbolSize int
	// GenSize is the number of source symbols per generation (the coding window).
	GenSize int
	// Redundancy is the FLOOR proactive code rate (repair per source symbol): the
	// minimum protection even at ~0 estimated loss, covering the lag before the
	// loss estimate catches a sudden onset. The redundancy controller raises the
	// rate ABOVE this floor as the measured loss requires (see TargetFailure).
	Redundancy float64
	// TargetFailure is the per-generation decode-failure probability the redundancy
	// controller sizes the proactive code rate to — the QoS knob. The controller
	// provisions enough repair that, at the estimated erasure rate, a generation
	// fails to decode with at most this probability (mean loss plus the binomial
	// variance). 0 selects the default (1e-3).
	TargetFailure float64
	// BufferMicros is the playout/deadline budget: a generation must be delivered
	// within this of its start, or its still-missing symbols are declared lost.
	BufferMicros int64
	// Sliding selects the band-form sliding-window coder instead of the default
	// generation coder: a repair is fungible across a coding window of CodingWindow
	// symbols, delivered on decode (no per-generation close), at O(CodingWindow²)
	// decode cost. Use it when the latency budget is TIGHT relative to the RTT —
	// especially budget < RTT (long-haul low-latency contribution), where its
	// continuous RTT-independent proactive repair recovers within a deadline an ARQ
	// round trip and the generation coder's reactive tier cannot — and on
	// bandwidth-constrained links (lower repair overhead). At a generous budget with
	// a low RTT the generation coder has lower latency, so leave this off there.
	Sliding bool
	// CodingWindow is the MAX sliding band width in source symbols — the recovery span
	// and O(window²) decode-cost cap. The sender adapts the effective span below it to
	// the deadline budget (see SlidingSender.effectiveBand), so this is a ceiling, not
	// a fixed width. 0 selects the default. Ignored unless Sliding.
	CodingWindow int
	// MaxGenSymbols caps a symbol's declared window N (RFC 8681 ls_max_size): the
	// receiver refuses any symbol with N greater than this, bounding decoder
	// allocation against a forged wide window (the raw uint16 N would otherwise size a
	// decoder up to 65535 symbols). 0 selects the default. (N1 resource safety.)
	MaxGenSymbols int
	// MaxRetainedGens caps both the number of live generation decoders and the
	// look-ahead horizon (in generations) of an admissible window, bounding the
	// receiver's worst-case state to O(MaxRetainedGens·GenSize·SymbolSize) regardless
	// of input — the finite-state shape a model checker can verify. 0 ⇒ default.
	MaxRetainedGens int
	// MaxBitrate caps the sender's aggregate emitted rate in bits/sec via a token
	// bucket: media (systematic) is never dropped, but REPAIR is throttled to keep the
	// total under this ceiling — bounding a forged-feedback reactive-amplification /
	// reflection storm (N1). It is also the ceiling the congestion controller reduces
	// below. 0 selects the default (100 Mbps).
	MaxBitrate int64
	// CongestionControl enables the delay-based (Copa-style) controller (N3): it sizes
	// the send-rate budget from the standing-queue delay and drives the token bucket,
	// so repair is the first thing sacrificed under congestion (media is preserved) and
	// the budget is surfaced for the host to pace the media source. Off ⇒ the bucket
	// holds the static MaxBitrate ceiling. Loss-agnostic by design (coding masks loss).
	CongestionControl bool
	// Pace enables the HOST transmit pacer (internal/session): it smooths the core's
	// emitted datagrams onto the wire at a rate slaved to RateBudgetBitsPerSec (the
	// budget the core computes, never a second controller) and backpressures Write when
	// the queue would exceed the deadline. It is a host concern — the deterministic core
	// ignores this field — but lives here because flow.Config is the transport-wide knob
	// set the host reads. Off ⇒ the host transmits each emit immediately (microbursts and
	// no source backpressure). On by default via DefaultConfig.
	Pace bool
	// PaceBurstMicros is the pacer's smoothing granularity: the leaky bucket hoards at
	// most this many microseconds of budget as burst credit. It MUST stay small (a few ms)
	// and SEPARATE from the queue-time limit — a large value lets the bucket dump a whole
	// quiet-gap's worth of credit on the next keyframe, recreating the microburst. 0 ⇒
	// default (5 ms). Ignored unless Pace.
	PaceBurstMicros int64
	// PaceQueueLimitMicros is the standing-queue-time limit at which Write backpressures
	// the source: a datagram that would wait longer than this is doomed past its deadline,
	// so the source is slowed instead of bloating an invisible buffer. It should be the
	// deadline minus expected downstream latency. 0 ⇒ default (3/4 of BufferMicros).
	// Ignored unless Pace.
	PaceQueueLimitMicros int64
	// ProbeMTU enables host-side per-path DPLPMTUD (RFC 8899): the sender probes the path
	// MTU with padded, Don't-Fragment datagrams and detects black holes (a path that
	// silently drops oversized datagrams — invisible to FEC and the loss-agnostic CC). A
	// host concern (the core ignores this field). Phase 1: discovers and reports the PLPMTU
	// per path; it does NOT yet resize symbols. Off by default (experimental).
	ProbeMTU bool
	// MaxProbeMTU is the largest UDP-payload size DPLPMTUD probes for (the interface MTU /
	// deployment ceiling). 0 selects a safe default. Ignored unless ProbeMTU.
	MaxProbeMTU int
	// Paths is the number of network paths a generation is spread across (coding-native
	// multipath, N5). 0 or 1 ⇒ single path (the default; unchanged sizing and PathID 0).
	// ≥ 2 enables the correlation-aware joint-tail sizer (repairForJointTailN) and the path
	// scheduler — systematic symbols round-robin across the paths, repair is metered toward
	// the better-delivering paths, and the receiver decodes from the union, so lossy paths
	// add diversity rather than the N× cost of duplication. The receiver measures the
	// per-slot erasure-count histogram across the paths so the sizer provisions for their
	// cross-path correlation. Clamped to maxPaths.
	Paths int
	// EvictBrokenFrames turns on media-aware early eviction (WP6): when a frame the
	// receiver tracks is known UNDECODABLE — one of its own source ids was lost, or a
	// reference's whole sub-tree is dead — its remaining ids are dropped IMMEDIATELY
	// rather than waiting out each one's deadline, so the next independently-decodable
	// frame (the next keyframe) delivers sooner and the cursor advances past the dead
	// sub-tree. Because the sender retires every generation below the reported cursor
	// (DecodedLowEdge), that advance is also the implicit "stop repairing this GOP"
	// signal — reactive repair budget is reclaimed for live generations, no extra wire
	// field. Requires frame descriptors (Sender.WriteFrame); a no-op for plain byte
	// streams. OFF by default: it trades byte-completeness (a doomed-but-recoverable id
	// is dropped) for picture-completeness (every DECODABLE frame is still delivered
	// whole, in order, never late), so only media flows that consume whole frames want it.
	EvictBrokenFrames bool
}

// maxPaths bounds the multipath arity (matching wire.feedbackMaxPaths): the per-path
// feedback section and the joint-tail histogram stay small and a forged path count cannot
// allocate unboundedly.
const maxPaths = 8

// multipath reports whether the sender spreads a generation across more than one path.
func (c Config) multipath() bool { return c.Paths >= 2 }

// paths returns the effective path count, clamped to [1, maxPaths].
func (c Config) paths() int {
	if c.Paths < 1 {
		return 1
	}
	if c.Paths > maxPaths {
		return maxPaths
	}
	return c.Paths
}

// maxBitrate returns the sender's aggregate rate ceiling in bits/sec, or the default.
func (c Config) maxBitrate() int64 {
	if c.MaxBitrate > 0 {
		return c.MaxBitrate
	}
	return defaultMaxBitrate
}

// maxGenSymbols returns the admissible window cap, or a generous default keyed to
// the generation size (honest symbols never declare N > GenSize).
func (c Config) maxGenSymbols() int {
	if c.MaxGenSymbols > 0 {
		return c.MaxGenSymbols
	}
	if m := 4 * c.GenSize; m > defaultMaxGenSymbols {
		return m
	}
	return defaultMaxGenSymbols
}

// maxRetainedGens returns the live-decoder / look-ahead-horizon cap, or the default.
func (c Config) maxRetainedGens() int {
	if c.MaxRetainedGens > 0 {
		return c.MaxRetainedGens
	}
	return defaultMaxRetainedGens
}

// codingWindow returns the sliding band width, or the default.
func (c Config) codingWindow() int {
	if c.CodingWindow > 0 {
		return c.CodingWindow
	}
	return defaultCodingWindow
}

// DefaultConfig returns a reasonable starting configuration for a 1316-byte
// media chunk (the bench/RTP payload size).
func DefaultConfig() Config {
	return Config{
		SymbolSize:    1316,
		GenSize:       16,
		Redundancy:    0.15, // floor; the controller adapts above it
		TargetFailure: 1e-3,
		BufferMicros:  200_000, // 200 ms
		Pace:          true,    // host pacer on: smooth to the budget, backpressure the source
	}
}

// repairFloor returns the floor repair count for a generation of n source symbols
// at the configured baseline redundancy (rounded to nearest).
func (c Config) repairFloor(n int) int {
	r := int(float64(n)*c.Redundancy + 0.5)
	if r < 0 {
		return 0
	}
	return r
}

// targetFailure returns the configured decode-failure target, or the default.
func (c Config) targetFailure() float64 {
	if c.TargetFailure > 0 && c.TargetFailure < 1 {
		return c.TargetFailure
	}
	return 1e-3
}

// uepCenterTier is the protection tier (wire.Symbol.Priority) that maps to the
// configured TargetFailure; tiers above it tighten the target, below it loosen it
// (WP6 unequal protection). It is the media shaper's base/reference tier
// (internal/shape.ClassBase) by convention — the core stays codec-blind, acting only
// on the generic priority byte.
const uepCenterTier = 2

// targetFailureForPriority returns the per-generation decode-failure target for a
// protection tier: the configured baseline scaled by 10^(uepCenterTier − tier), so each
// tier above the reference provisions ~10× lower failure (more repair) and each below
// ~10× higher (less). This is the whole unequal-protection policy — a pure function of
// the generic priority byte the shaper assigns; the core never sees the codec. Clamped
// to a sane probability range so an extreme tier cannot underflow or exceed certainty.
func targetFailureForPriority(pri uint8, base float64) float64 {
	d := base
	for exp := uepCenterTier - int(pri); exp > 0; exp-- {
		d *= 10
	}
	for exp := uepCenterTier - int(pri); exp < 0; exp++ {
		d *= 0.1
	}
	if d < 1e-9 {
		d = 1e-9
	}
	if d > 0.5 {
		d = 0.5
	}
	return d
}

// feedbackIntervalMicros is how often the receiver emits a cumulative feedback
// report while a flow is active.
const feedbackIntervalMicros = 20_000 // 20 ms

// Reactive-repair (WP3) pacing constants. The sender retains closed generations
// and, on a feedback rank deficit, sends extra repair for the blocking
// generation — debounced by an estimated RTT so a batch can arrive and update the
// deficit before the next is sent (HARQ incremental redundancy).
const (
	defaultRTTMicros          = 50_000  // RTT estimate before the first sample
	minReactiveIntervalMicros = 8_000   // floor on the per-generation reactive cadence
	maxReactiveIntervalMicros = 500_000 // ceiling on it
)

// Redundancy controller (PLAN §3.5). The proactive code rate is sized FEED-FORWARD
// to the target per-generation decode-failure probability (Config.TargetFailure)
// from the receiver's reported channel erasure rate (wire.Feedback.LossRate),
// covering the mean loss AND the binomial variance — the term a mean-tracking AIMD
// omits, which is why AIMD leaks decode failures even at equilibrium (Cohen/Médard
// AC-RLNC; Rudow/Rashmi Tambur). The feedback rank deficit then trims only a thin
// reactive residual. Loss is estimated and sized for the i.i.d. case; a bursty
// (Gilbert-Elliott) channel has higher per-generation variance, so sizing to a
// burst-inflated rate (from the loss-run correlation) is a refinement that this
// exact-binomial form is the correct base for.
const (
	// maxRepairFactor caps the proactive code rate at this multiple of the
	// generation size, bounding overhead under an extreme or mis-estimated loss.
	maxRepairFactor = 3
	// lossWindowMin is the smallest source-id span the receiver averages channel
	// loss over before reporting; a wider span lowers estimator variance.
	lossWindowMin = 64
	// lossEWMAShift sets the receiver loss EWMA weight (1 / 2^shift per window).
	lossEWMAShift = 2
	// burstQ8One is mean loss-run length 1 (an i.i.d. channel) in Q8 fixed-point —
	// the floor and initial value of the receiver's smoothed burstiness estimate.
	burstQ8One = 256
	// burstEWMAShift sets the burstiness EWMA weight (1 / 2^shift per loss run).
	burstEWMAShift = 2
	// burstSampleCap bounds a single loss run's contribution to the burst EWMA (in
	// symbols), so one long outage cannot dominate the mean-burst estimate.
	burstSampleCap = 64
	// defaultMaxGenSymbols / defaultMaxRetainedGens are the resource-safety floors
	// (N1): a forged symbol cannot allocate a decoder wider than the former or push
	// the live-decoder count / look-ahead horizon past the latter.
	defaultMaxGenSymbols   = 256
	defaultMaxRetainedGens = 512
	// defaultMaxBitrate is the sender's aggregate rate ceiling (100 Mbps, matching the
	// libRIST recovery_maxbitrate default ethos): generous enough that legit media +
	// repair pass untouched, low enough to clip a reactive-amplification storm.
	defaultMaxBitrate = 100_000_000
	// symHeaderBytes is the on-wire symbol header length (wire format v1) — used only
	// to size the congestion controller's per-packet MSS, not for encoding.
	symHeaderBytes = 30
	// lossHoldDecay slows the decay of the conservative max-hold loss estimate
	// across windows, so a recent burst keeps protection up for a few windows.
	lossHoldDecay = 0.8
	// defaultCodingWindow is the MAX sliding band width when CodingWindow is unset:
	// the decode-cost cap (O(b²)/symbol) and the widest recovery span. The sender
	// adapts the EFFECTIVE span below it to fit the deadline budget after propagation
	// (SlidingSender.effectiveBand), so this is a ceiling, not a fixed width — 64
	// balances low overhead (a wider band needs less variance margin) against the
	// generous-WAN case (where a narrower span recovers more) and bounds decode cost.
	defaultCodingWindow = 64
	// slidingMaxWin is the sliding decoder's delivery-window cap (cursor lag). It is
	// large (memory only; decode cost is governed by the band, not this) and the
	// deadline-skip bounds the actual occupancy.
	slidingMaxWin = 2048
	// flushIdleMicros closes a partially filled generation when no new source has
	// arrived for this long, so a tail/idle generation gets its repair (and any
	// reactive repair) in flight well before its deadline instead of waiting for a
	// Flush at stream end. During continuous streaming the gap between writes is far
	// smaller, so it never fires.
	flushIdleMicros = 10_000 // 10 ms
	// maxIntervalMicros caps the receiver's per-symbol deadline-interval EWMA. A
	// legitimate inter-symbol interval is sub-millisecond at live bitrates; a value past
	// this is a forged/garbage Deadline stamp, so capping it keeps the deadline
	// extrapolation (id-refID)*intervalUs from overflowing int64 (a wire-reachable
	// invariant break — a wrapped deadline drops in-time symbols or delivers late ones).
	maxIntervalMicros = 1_000_000 // 1 s
)

// repairForTarget returns the smallest repair count r such that a generation of k
// source symbols plus r repair, over an i.i.d. erasure channel of probability p,
// fails to decode with probability at most delta — the EXACT binomial tail
// P[Binomial(k+r, p) > r]. This is the feed-forward set-point: unlike a
// mean-tracking controller (which provisions ~k*p and chronically misses the
// variance tail), it covers the binomial spread to hit the target outage. Over
// GF(256) RLNC behaves as an ideal MDS code (linear-dependence probability ~1/255),
// so decode failure is governed by the erasure count, exactly this tail. r is
// capped at maxRepairFactor*k.
func repairForTarget(k int, p, delta float64) int {
	if k <= 0 || p <= 0 {
		return 0
	}
	maxR := k * maxRepairFactor
	if p >= 1 {
		return maxR
	}
	for r := 0; r < maxR; r++ {
		if binomTailGreater(k+r, p, r) <= delta {
			return r
		}
	}
	return maxR
}

// binomTailGreater returns P[X > r] for X ~ Binomial(n, p), computed iteratively
// (P[X=j+1] = P[X=j] * (n-j)/(j+1) * p/(1-p)) so it neither overflows nor needs
// factorials. n is small (a generation plus its repair).
func binomTailGreater(n int, p float64, r int) float64 {
	if r >= n {
		return 0
	}
	// Degenerate probabilities: at p>=1 every trial succeeds (X=n, so P[X>r]=1 for r<n); at
	// p<=0 none do (X=0, so P[X>r]=0 for r>=0). Handling them keeps the loop's p/(1-p) term
	// finite — a NaN here would silently saturate symbolsForDeficit's sizing.
	if p >= 1 {
		return 1
	}
	if p <= 0 {
		return 0
	}
	q := 1 - p
	term := math.Pow(q, float64(n)) // P[X=0]
	cdf := term
	for j := 0; j < r; j++ {
		term *= float64(n-j) / float64(j+1) * p / q
		cdf += term
	}
	if tail := 1 - cdf; tail > 0 {
		return tail
	}
	return 0
}

// symbolsForDeficit returns the number of fresh repair symbols to send so that, over
// an erasure channel of probability p, at least `deficit` of them ARRIVE (clearing
// the deficit) with probability at least 1-delta. Each repair is an independent
// random combination, so arrivals ~ Binomial(r, 1-p); this sizes a reactive batch
// to clear the deficit in ONE round despite the loss of the repair symbols
// themselves — instead of a fixed margin that stalls convergence under heavy loss.
func symbolsForDeficit(deficit int, p, delta float64) int {
	if deficit <= 0 {
		return 0
	}
	maxR := deficit*maxRepairFactor + 4
	q := 1 - p // per-symbol arrival probability
	if q <= 0 {
		return maxR // total loss: no finite batch clears it; saturate
	}
	if q >= 1 {
		return deficit // lossless: every repair arrives, so exactly `deficit` clear it
	}
	want := 1 - delta
	for r := deficit; r < maxR; r++ {
		// P[arrivals >= deficit] = P[Binomial(r, q) > deficit-1].
		if binomTailGreater(r, q, deficit-1) >= want {
			return r
		}
	}
	return maxR
}

// geScale is the fixed-point unit (Q30) for the Gilbert-Elliott DP: probabilities
// are integers in [0, geScale]. Products stay under 2^60 (< int64 max), so the whole
// computation is exact integer arithmetic — bit-reproducible across architectures,
// unlike the IEEE-754 binomial path (the determinism-moat requirement for the
// burst-aware sizer).
const geScale = 1 << 30

// repairForGE returns the smallest repair count r such that a generation of k source
// symbols plus r repair, over a 2-state Gilbert-Elliott channel, fails to decode with
// probability at most delta — the burst-aware analog of repairForTarget. The channel
// is the robust two-parameter Gilbert (good state lossless, bad state total loss)
// recovered from the receiver's marginal loss pMeanPPM (parts per million) and mean
// loss-run length meanBurstQ8 (Q8): pBG = 1/meanBurst, pGB = p·pBG/(1−p). At mean
// burst 1 it tracks the binomial sizer; as bursts lengthen it provisions for the
// concentration of erasures within one generation that the binomial tail assigns near
// zero probability. r is capped at maxRepairFactor·k.
func repairForGE(k, pMeanPPM, meanBurstQ8 int, delta float64) int {
	if k <= 0 || pMeanPPM <= 0 {
		return 0
	}
	maxR := k * maxRepairFactor
	// pBG = geScale·(1/meanBurst) = geScale·256/meanBurstQ8 (meanBurstQ8 ≥ 256 ⇒ ≤ geScale).
	if meanBurstQ8 < burstQ8One {
		meanBurstQ8 = burstQ8One
	}
	pBG := int64(geScale) * burstQ8One / int64(meanBurstQ8)
	pQ := int64(pMeanPPM) * geScale / 1_000_000
	if pQ >= geScale {
		return maxR
	}
	// pGB = p·pBG/(1−p), clamped to a valid probability.
	pGB := pQ * pBG / (geScale - pQ)
	if pGB > geScale {
		pGB = geScale
	}
	// Steady-state bad fraction πB = pGB/(pGB+pBG).
	piB := pGB * geScale / (pGB + pBG)
	deltaQ := int64(delta * geScale)
	for r := 0; r < maxR; r++ {
		if geTailGreater(k+r, pGB, pBG, piB, r) <= deltaQ {
			return r
		}
	}
	return maxR
}

// geTailGreater returns P[erasures in n sent symbols > r] (in Q30) for the two-state
// Gilbert channel with transitions pGB/pBG and steady-state bad fraction piB. It is
// the forward HMM recursion specialized to count erasures (good ⇒ received, bad ⇒
// erased), tracking the joint (state, erasure-count) mass with the count capped at r:
// any path that exceeds r erasures escapes the tracked set (erasure count is
// monotonic), so Σ tracked mass is exactly P[≤ r] and the tail is geScale − that.
// O(n·r), integer multiply-add with rounded Q30 rescaling.
func geTailGreater(n int, pGB, pBG, piB int64, r int) int64 {
	const half = geScale / 2
	// k ranges 0..r. g[k]/b[k] = P(k erasures so far, channel in good/bad state).
	g := make([]int64, r+1)
	b := make([]int64, r+1)
	g[0] = geScale - piB
	b[0] = piB
	ng := make([]int64, r+1)
	nb := make([]int64, r+1)
	for step := 0; step < n; step++ {
		for k := 0; k <= r; k++ {
			// Good at this step ⇒ symbol received, erasure count unchanged.
			ng[k] = ((geScale-pGB)*g[k] + pBG*b[k] + half) >> 30
			// Bad at this step ⇒ symbol erased, count k came from k-1.
			if k == 0 {
				nb[k] = 0
			} else {
				nb[k] = (pGB*g[k-1] + (geScale-pBG)*b[k-1] + half) >> 30
			}
		}
		g, ng = ng, g
		b, nb = nb, b
	}
	var cdf int64
	for k := 0; k <= r; k++ {
		cdf += g[k] + b[k]
	}
	if tail := geScale - cdf; tail > 0 {
		return tail
	}
	return 0
}

// tokenBucket is a deterministic byte-rate limiter (time enters via the explicit
// clock, integer math so it is bit-reproducible). It refills at bytesPerSec and
// admits a take only while tokens remain; a non-droppable take (media) always
// proceeds and may drive tokens negative, so only the droppable surplus (repair) is
// clipped. The N1 aggregate-emit ceiling against reactive amplification.
type tokenBucket struct {
	bytesPerSec int64
	tokens      int64
	burst       int64
	last        clock.Timestamp
	primed      bool
}

func newTokenBucket(bitsPerSec int64) tokenBucket {
	if bitsPerSec <= 0 {
		return tokenBucket{}
	}
	bps := bitsPerSec / 8
	burst := bps / 5 // ~200 ms of slack
	if burst < 1<<16 {
		burst = 1 << 16
	}
	return tokenBucket{bytesPerSec: bps, tokens: burst, burst: burst}
}

// setRate updates the bucket's fill rate (bytes/sec) — how the congestion controller
// hands the bucket a dynamic budget. The burst cap tracks the rate (≈200 ms of it,
// with a small floor) and the token level is clamped to it, so a rate CUT tightens the
// allowance immediately rather than leaving a stale reservoir of credit.
func (tb *tokenBucket) setRate(bytesPerSec int64) {
	if bytesPerSec < 1 {
		bytesPerSec = 1
	}
	tb.bytesPerSec = bytesPerSec
	burst := bytesPerSec / 5
	if burst < 1<<12 {
		burst = 1 << 12 // floor so even a tiny budget still admits the odd packet
	}
	tb.burst = burst
	if tb.tokens > burst {
		tb.tokens = burst
	}
}

// refill advances the bucket to now.
func (tb *tokenBucket) refill(now clock.Timestamp) {
	if !tb.primed {
		tb.primed, tb.last = true, now
		return
	}
	if dt := now.Sub(tb.last); dt > 0 {
		tb.tokens += tb.bytesPerSec * dt / 1_000_000
		if tb.tokens > tb.burst {
			tb.tokens = tb.burst
		}
		tb.last = now
	}
}

// allow refills to now and reports whether n bytes may be emitted. A droppable emit
// is refused when tokens are exhausted; a non-droppable emit always proceeds.
func (tb *tokenBucket) allow(now clock.Timestamp, n int, droppable bool) bool {
	if tb.bytesPerSec <= 0 {
		return true // disabled
	}
	tb.refill(now)
	if droppable && tb.tokens < int64(n) {
		return false
	}
	tb.tokens -= int64(n)
	return true
}

// allowRepair is the priority-aware repair admission: repair AT OR ABOVE the baseline
// tier (uepCenterTier) is admitted whenever any tokens remain (the unchanged N1
// behavior — never pre-throttled, so it does not mask the congestion signal). Repair
// BELOW the baseline must clear a CUSHION that grows as the tier falls, so under budget
// pressure the bucket sheds DISPOSABLE repair first, then enhancement — the
// unequal-protection drop order (docs/media-awareness.md §4) — preserving the
// dependency spine. Media uses allow (non-droppable) and is never shed.
func (tb *tokenBucket) allowRepair(now clock.Timestamp, n int, pri uint8) bool {
	if tb.bytesPerSec <= 0 {
		return true
	}
	tb.refill(now)
	var cushion int64
	if int(pri) < uepCenterTier {
		cushion = tb.burst * int64(uepCenterTier-int(pri)) / int64(uepCenterTier+1)
	}
	if tb.tokens-int64(n) < cushion {
		return false
	}
	tb.tokens -= int64(n)
	return true
}

// p65535ToPPM converts a wire loss field (parts per 65535) to parts per million —
// the unit the joint-tail sizer keys on. The inverse of receiver.ppmToP65535.
func p65535ToPPM(v uint16) int { return int(int64(v) * 1_000_000 / 65535) }

// clampDeadline bounds a peer-stamped deadline to a sane window around now, so a forged
// far-future / far-past Deadline field cannot drive the int64 deadline arithmetic
// (symDeadline extrapolation, the monotonic maxDeadline backstop) toward overflow. An honest
// deadline is ≈ write_time + budget and is observed at write_time + one-way delay, so it sits
// within ±(a small multiple of the budget) of now — well inside this window — making the clamp
// a no-op for legitimate traffic and a guard only against forged stamps.
func clampDeadline(now, dl clock.Timestamp, budget int64) clock.Timestamp {
	slack := 16 * budget
	if slack < 1_000_000 {
		slack = 1_000_000 // floor so an unset/tiny budget still bounds the value
	}
	if hi := now.Add(slack); dl.After(hi) {
		return hi
	}
	if lo := now.Add(-slack); dl.Before(lo) {
		return lo
	}
	return dl
}

// genBaseOf returns the generation base (aligned to genSize) for a source id.
func genBaseOf(id uint32, genSize int) uint32 {
	if genSize <= 0 {
		return id
	}
	g := uint32(genSize)
	return id / g * g
}
