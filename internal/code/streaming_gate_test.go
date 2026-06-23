package code

// Streaming-code vs sliding-band overhead gate (re-run of the docs/coding.md §2
// streaming gate, as a checked-in, reproducible artifact rather than a throwaway).
//
// Question: a window/block code must hedge against a burst landing ANYWHERE in its
// window, so it over-provisions; a streaming (convolutional) code aligns each
// symbol's protection to its own deadline and is burst-position-agnostic by
// construction, achieving the rate-delay capacity C = (T-N+1)/(T-N+B+1) over the
// Badr/Fong sliding-window burst-erasure channel C(W=T+1, B, N) (1903.07434 eq. 4).
// Does that structural edge let it carry a burst at LOWER overhead than Meld's
// sliding-band RLNC, at equal delivery and delay?
//
// Method (deliberately generous to the streaming code, mirroring the original gate):
//   - STREAMING side: the IDEAL bound. A source symbol is recovered at delay T iff
//     its decode window of W=T+1 channel slots is C(W,B,N)-admissible (a single
//     burst of span <=B, or <=N scattered erasures). Overhead is then exactly
//     B/(T-N+1), fixed by construction. No real construction beats this; and we tune
//     (B,N) per channel to MINIMIZE its overhead at the target residual.
//   - BAND side: the REAL code.BandDecoder, driven with proactive random-linear
//     repairs over a trailing band, per-symbol erasures, and a deadline-T skip.
//     Pure proactive (no reactive/feedback, no recoding) -- the structural
//     comparison, and itself a handicap to the band.
//   - SAME Gilbert-Elliott erasure realization feeds both; T (delay, in channel
//     slots) is held equal. We sweep the channel's mean burst length and, for each
//     scheme, find the minimum overhead that holds a target residual loss.
//
// If even the ideal streaming bound cannot undercut the real band in the
// contribution-video regime (mean burst <= ~6), the SHUT verdict stands.

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
)

// geTrace returns a per-slot erasure realization of the two-state Gilbert channel
// (good = lossless, bad = total loss) used throughout the flow sizer tests: mean
// loss p, mean burst length meanBurst (= 1/pBG), started in steady state. Same
// channel model as flow.geErasures, so the two test suites agree on "the channel".
func geTrace(rng *rand.Rand, n int, p, meanBurst float64) []bool {
	pBG := 1.0 / meanBurst
	pGB := p * pBG / (1 - p)
	piB := pGB / (pGB + pBG)
	er := make([]bool, n)
	bad := rng.Float64() < piB
	for i := 0; i < n; i++ {
		if bad {
			er[i] = true
			if rng.Float64() < pBG {
				bad = false
			}
		} else if rng.Float64() < pGB {
			bad = true
			er[i] = true // the transition takes effect this slot (erased)
		}
	}
	return er
}

// windowAdmissible reports whether the erasures in a W-slot window satisfy the
// C(W,B,N) promise: at most N scattered erasures, OR all erasures confined to a
// single span of at most B consecutive slots (the standard burst notion; span<=B
// is generous to the streaming code -- it need not be a solid run).
func windowAdmissible(er []bool, lo, hi, B, N int) bool {
	cnt, first, last := 0, -1, -1
	for j := lo; j < hi; j++ {
		if er[j] {
			cnt++
			if first < 0 {
				first = j
			}
			last = j
		}
	}
	if cnt <= N {
		return true
	}
	return last-first+1 <= B
}

// streamingResidual is the ideal streaming code's residual loss on trace er for
// parameters (B,N) at delay T: the fraction of source slots whose decode window
// [i, i+T] (W=T+1 slots) is NOT C(W,B,N)-admissible. Schedule-free: admissibility
// of [i,i+T] does not depend on which slots carry parity, and the channel is
// stationary, so this is the per-source residual. Overhead is B/(T-N+1).
func streamingResidual(er []bool, T, B, N int) float64 {
	W := T + 1
	bad, total := 0, 0
	for i := 0; i+W <= len(er); i++ {
		total++
		if !windowAdmissible(er, i, i+W, B, N) {
			bad++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(bad) / float64(total)
}

// streamingMinOverhead finds the lowest-overhead (B,N) whose ideal residual holds
// the target, searching N in [1,nMax] and B in [N, T-1]. Overhead = B/(T-N+1).
// Returns +Inf if no admissible (B,N) within range meets the target.
func streamingMinOverhead(er []bool, T, nMax int, target float64) (ovh float64, bestB, bestN int) {
	ovh = math.Inf(1)
	for N := 1; N <= nMax; N++ {
		k := T - N + 1
		if k <= 0 {
			continue
		}
		for B := N; B < T; B++ {
			if streamingResidual(er, T, B, N) > target {
				continue
			}
			if o := float64(B) / float64(k); o < ovh {
				ovh, bestB, bestN = o, B, N
			}
			break // residual is monotone non-increasing in B, so the first hit is the cheapest for this N
		}
	}
	return
}

// schedState is what a repair-scheduling policy sees after each source emit. cursor
// and backlog are derived from the sender's (possibly feedback-delayed) view of the
// receiver's delivered frontier; gapOpen is the sender's stall signal -- the backlog
// has grown beyond the feedback pipeline, so a loss is probably outstanding.
type schedState struct {
	src     int
	cursor  int
	backlog int
	gapOpen bool
}

// repairPolicy returns how many proactive repairs to emit right after a source symbol.
// It is stateful (carries a rate accumulator) and called once per source emit.
type repairPolicy func(schedState) int

// uniformPolicy is the SHIPPING schedule: a steady proactive rate, blind to loss.
func uniformPolicy(rho float64) repairPolicy {
	acc := 0.0
	return func(schedState) int {
		acc += rho
		n := 0
		for acc >= 1 {
			n++
			acc--
		}
		return n
	}
}

// clusteredPolicy is a BLIND front-loaded schedule: the same average rate, but emitted
// in bunches of `clump` every clump/rho symbols instead of spread evenly. Tests whether
// merely reshaping the proactive pattern (a "diagonal" clumping), with no extra
// information, can close the burst gap. For a uniformly-random burst phase it should
// not -- by stationarity, where you put the clumps is independent of where the burst
// lands -- so a tie here is the informative (negative) result.
func clusteredPolicy(rho float64, clump int) repairPolicy {
	acc := 0.0
	return func(schedState) int {
		acc += rho
		if acc >= float64(clump) {
			acc -= float64(clump)
			return clump
		}
		return 0
	}
}

// onGapPolicy is the STALL-AWARE schedule: a low (often zero) baseline rate, switching
// to rhoGap while the sender's delayed cursor signals an open gap. It concentrates
// repair on the symbols that are actually missing -- but the signal is the cumulative
// cursor, delayed by feedback latency, so its benefit is RTT-gated. This is the
// scheduling form of reactive repair; the cursorDelay sweep shows the latency wall.
func onGapPolicy(rhoBase, rhoGap float64) repairPolicy {
	acc := 0.0
	return func(st schedState) int {
		r := rhoBase
		if st.gapOpen {
			r = rhoGap
		}
		acc += r
		n := 0
		for acc >= 1 {
			n++
			acc--
		}
		return n
	}
}

// bandSim drives the REAL BandDecoder over trace er with a pluggable repair-scheduling
// policy. Source symbols are emitted one per slot in id order; after each source the
// policy decides how many proactive repairs to emit over the cumulative-ACK window;
// a channel slot carries one symbol, dropped iff er[slot]. Each source must be
// delivered within T slots of its emission slot, else it is skipped (lost). The policy
// sees the cursor delayed by cursorDelay slots (a feedback-latency model affecting the
// SCHEDULING DECISION only; repair windowing always uses the true delivered frontier,
// so the data plane is idealized and the experiment isolates schedule quality vs RTT).
// Returns residual loss and realized overhead (repairs sent / source sent).
func bandSim(t *testing.T, er []bool, nSrc, T, b, symSize, cursorDelay int, policy repairPolicy) (residual, overhead float64) {
	t.Helper()
	enc := NewEncoderAt(symSize, 0)
	dec := NewBandDecoder(symSize, b, 3*T+b+64)

	tSrc := make([]int, nSrc) // emission slot of each source id, for its deadline
	chist := make([]int, 0, len(er))
	var slot, src, repairs, lost, delivered int
	var key uint16

	feedReady := func() {
		for {
			r, ok := dec.Deliver()
			if !ok {
				return
			}
			delivered++
			if len(r.Data) >= 4 && binary.BigEndian.Uint32(r.Data) != r.ID {
				t.Fatalf("false recovery: id %d carried %d", r.ID, binary.BigEndian.Uint32(r.Data))
			}
		}
	}
	// enforceDeadlines skips every head-of-line source whose deadline has passed by slot `now`.
	enforceDeadlines := func(now int) {
		for {
			feedReady()
			c := int(dec.Cursor())
			if c >= src || tSrc[c]+T > now {
				return
			}
			if dec.Skip() {
				lost++
			} else {
				return
			}
		}
	}
	recordSlot := func() { chist = append(chist, int(dec.Cursor())) }

	emitSource := func() {
		buf := make([]byte, symSize)
		binary.BigEndian.PutUint32(buf, uint32(src))
		id := enc.Add(buf)
		tSrc[src] = slot
		if !er[slot] {
			dec.AddSystematic(id, buf)
		}
		src++
		slot++
		enforceDeadlines(slot)
		recordSlot()
	}
	emitRepair := func() {
		// Cumulative-ACK windowing: slide the encoder to the receiver's delivered
		// frontier so the proactive repair covers exactly the still-in-flight region
		// [cursor, src). The BandDecoder rejects repairs whose base is below its cursor.
		enc.SlideTo(dec.Cursor())
		base, n, payload := enc.RepairWindow(key, b)
		if n > 0 && !er[slot] {
			dec.AddRepair(base, n, key, payload)
		}
		key++
		repairs++
		slot++
		enforceDeadlines(slot)
		recordSlot()
	}

	for src < nSrc && slot+2 < len(er) {
		emitSource()
		// The sender's delayed view of the receiver cursor; gapOpen = backlog beyond
		// the feedback pipeline (cursorDelay) signals a probable outstanding loss.
		dc := int(dec.Cursor())
		if cursorDelay > 0 && len(chist) > cursorDelay {
			dc = chist[len(chist)-1-cursorDelay]
		}
		st := schedState{src: src, cursor: dc, backlog: src - dc, gapOpen: src-dc > cursorDelay+2}
		for n := policy(st); n > 0 && slot+2 < len(er); n-- {
			emitRepair()
		}
	}
	// Drain: advance the deadline clock past every remaining source symbol.
	enforceDeadlines(slot + T + 1)
	for int(dec.Cursor()) < src {
		if !dec.Skip() {
			break
		}
		lost++
		feedReady()
	}
	feedReady()

	if src == 0 {
		return 0, 0
	}
	return float64(lost) / float64(src), float64(repairs) / float64(src)
}

// bandResidual is the shipping uniform schedule (the baseline used by the gates above).
func bandResidual(t *testing.T, er []bool, nSrc, T, b int, repairRate float64, symSize int) (residual, overhead float64) {
	return bandSim(t, er, nSrc, T, b, symSize, 0, uniformPolicy(repairRate))
}

// bandMinOverhead grid-searches the proactive repair rate for the lowest realized
// overhead whose band residual holds the target.
func bandMinOverhead(t *testing.T, er []bool, nSrc, T, b, symSize int, target float64) (ovh float64, rate float64) {
	t.Helper()
	ovh = math.Inf(1)
	// Residual is monotone non-increasing in the repair rate, so the first (ascending)
	// rate that holds the target is the minimum-overhead operating point.
	for _, r := range []float64{
		0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20,
		0.23, 0.27, 0.31, 0.36, 0.42, 0.50, 0.60, 0.72, 0.85, 1.0, 1.2, 1.45, 1.75,
	} {
		res, o := bandResidual(t, er, nSrc, T, b, r, symSize)
		if res <= target {
			return o, r
		}
	}
	return
}

// streamingResidualBudget returns the ideal streaming code's lowest residual whose
// overhead B/(T-N+1) stays within budget, searching N in [1,nMax]. For fixed N the
// residual is non-increasing in B while overhead increases, so the best B within
// budget is the largest one affordable; we then take the best N.
func streamingResidualBudget(er []bool, T, nMax int, budget float64) (res float64, B, N int) {
	res = 1.0
	for n := 1; n <= nMax; n++ {
		k := T - n + 1
		if k <= 0 {
			continue
		}
		bMax := int(budget * float64(k))
		if bMax > T-1 {
			bMax = T - 1
		}
		if bMax < n {
			continue
		}
		if r := streamingResidual(er, T, bMax, n); r < res {
			res, B, N = r, bMax, n
		}
	}
	return
}

// TestStreamingVsBandGE is the head-to-head on the channel Meld actually faces. For a
// fixed delay T and a Gilbert-Elliott channel (geometric, heavy-tailed bursts), at a
// few equal OVERHEAD budgets, it sweeps the mean burst length and prints the residual
// loss of the IDEAL streaming code (tuned (B,N) per channel) vs the REAL sliding band
// at the same overhead and same delay. Lower residual wins; the crossover is where
// the streaming code's structural edge finally beats the band on a stochastic channel.
func TestStreamingVsBandGE(t *testing.T) {
	if testing.Short() {
		t.Skip("overhead sweep is slow; run without -short")
	}
	const (
		symSize = 16
		pMean   = 0.05
		nMax    = 4 // streaming tuned over N in [1,4]
		seeds   = 2
	)
	for _, T := range []int{24, 48} {
		b := T // band width in source ids: generous (covers the whole deadline)
		nSrc := 16000
		traceLen := nSrc*3 + 4*T + 64
		for _, budget := range []float64{0.15, 0.30, 0.50} {
			t.Logf("=== T=%d, GE mean-loss %.0f%%, equal overhead budget %.0f%% ===", T, pMean*100, budget*100)
			t.Logf(" meanBurst | streaming residual (B,N) | band residual (ovh) | winner")
			for _, mb := range []float64{2, 4, 6, 8, 12, 16, 24} {
				var sRes, bRes, bOvh float64
				var sB, sN int
				for s := 0; s < seeds; s++ {
					rng := rand.New(rand.NewSource(int64(s)*1009 + int64(mb)*31 + int64(T)))
					er := geTrace(rng, traceLen, pMean, mb)
					sr, bb, nn := streamingResidualBudget(er, T, nMax, budget)
					br, o := bandResidual(t, er, nSrc, T, b, budget, symSize)
					sRes += sr
					bRes += br
					bOvh += o
					sB, sN = bb, nn
				}
				sRes /= seeds
				bRes /= seeds
				bOvh /= seeds
				win := "band"
				if sRes < bRes*0.9 { // 10% margin so a tie reads as the band's (it is the real, shipping code)
					win = "STREAMING"
				} else if sRes < bRes {
					win = "~tie"
				}
				t.Logf(" %8.0f | %8.2e (B=%d,N=%d) | %8.2e (%.0f%%) | %s",
					mb, sRes, sB, sN, bRes, 100*bOvh, win)

				// Regression guard for the SHUT verdict: in the contribution-video
				// regime (short bursts, modest overhead) the real band must beat the
				// IDEAL streaming code at equal overhead+delay. The gap there is 3-7x,
				// so this is a stable assertion, not a flaky one.
				if mb <= 6 && budget <= 0.30 && bRes >= sRes {
					t.Errorf("contribution regime T=%d ovh=%.0f%% mb=%.0f: band residual %.2e should beat ideal streaming %.2e",
						T, budget*100, mb, bRes, sRes)
				}
			}
		}
	}
}

// TestStreamingVsBandDeterministic contrasts the GE (geometric, heavy-tailed) result
// with a DETERMINISTIC burst channel: a solid burst of exactly L_b slots every
// period P, nothing else. This is the streaming code's home turf (a known, bounded
// burst), so it should match its capacity overhead L_b/(T-L_b+1)... wait, with N=1
// the streaming overhead is B/T at B=L_b; the band needs ~L_b/T too. The gap here is
// the cleanest read on the pure structural advantage, free of the geometric tail.
func TestStreamingVsBandDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("overhead sweep is slow; run without -short")
	}
	const (
		symSize = 16
		target  = 1e-3
		nMax    = 2
		T       = 32
	)
	b := T
	nSrc := 16000
	traceLen := nSrc*3 + 4*T + 64
	t.Logf("=== DETERMINISTIC burst, delay T=%d, target %.0e ===", T, target)
	t.Logf(" burstLen | period | streaming overhead (B,N) | band overhead (rate)")
	for _, lb := range []int{2, 4, 8, 12} {
		// Guard >= W so every W=T+1 window holds at most one burst -- the C(W,B,N)
		// model's home turf (isolated, bounded bursts), where the streaming code is
		// at its capacity and the geometric tail is absent.
		period := lb + T + 1
		er := make([]bool, traceLen)
		for i := 0; i < traceLen; i++ {
			if i%period < lb {
				er[i] = true
			}
		}
		so, bB, bN := streamingMinOverhead(er, T, nMax, target)
		bo, br := bandMinOverhead(t, er, nSrc, T, b, symSize, target)
		t.Logf(" %8d | %6d | %7.1f%% (B=%d,N=%d) | %7.1f%% (r=%.2f)",
			lb, period, 100*so, bB, bN, 100*bo, br)
	}
}

// schedRates is the ascending intensity grid the scheduling search walks; realized
// overhead is measured, not assumed (onGap spends only on gaps, so its realized
// overhead is far below its rhoGap).
var schedRates = []float64{
	0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15, 0.18, 0.22, 0.27, 0.33,
	0.40, 0.50, 0.62, 0.75, 0.90, 1.1, 1.3, 1.6, 2.0, 2.5, 3.0,
}

// searchMinOverhead walks the intensity grid for a policy factory and returns the
// lowest realized overhead whose residual holds target (first hit; residual is
// monotone non-increasing in intensity).
func searchMinOverhead(t *testing.T, er []bool, nSrc, T, b, symSize, cursorDelay int,
	makePolicy func(float64) repairPolicy, target float64) (ovh, res float64) {
	t.Helper()
	for _, rho := range schedRates {
		r, o := bandSim(t, er, nSrc, T, b, symSize, cursorDelay, makePolicy(rho))
		if r <= target {
			return o, r
		}
	}
	return math.Inf(1), 1
}

// TestRepairSchedulingDeterministic asks the decisive question on the streaming code's
// home turf (isolated bounded bursts): can a smarter repair SCHEDULE, with the same
// RLNC code and decoder, close the band's B/(T-B) overhead toward the streaming code's
// B/T? It pits the shipping UNIFORM schedule against a BLIND clustered schedule (same
// rate, front-loaded -- tests for a free, feedback-free win) and the STALL-AWARE onGap
// schedule with instant feedback (tests whether the gap is really a feedback effect).
func TestRepairSchedulingDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("scheduling sweep is slow; run without -short")
	}
	const (
		symSize = 16
		T       = 32
		eps     = 5e-4 // "full recovery"
	)
	b := T
	nSrc := 16000
	traceLen := nSrc*3 + 4*T + 64
	t.Logf("=== DETERMINISTIC burst, T=%d: min overhead for full recovery, by schedule ===", T)
	t.Logf(" burstLen | streaming~B/T | uniform | clustered(blind) | onGap(instant fb)")
	for _, lb := range []int{2, 4, 8, 12} {
		period := lb + T + 1 // one burst per window
		er := make([]bool, traceLen)
		for i := 0; i < traceLen; i++ {
			if i%period < lb {
				er[i] = true
			}
		}
		uni, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0, uniformPolicy, eps)
		clu, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0,
			func(r float64) repairPolicy { return clusteredPolicy(r, 4) }, eps)
		gap, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0,
			func(r float64) repairPolicy { return onGapPolicy(0, r) }, eps)
		t.Logf(" %8d | %12.1f%% | %6.1f%% | %14.1f%% | %14.1f%%",
			lb, 100*float64(lb)/float64(T), 100*uni, 100*clu, 100*gap)

		// The findings, as guards: blind reshaping yields no free win (clustered never
		// beats uniform), and the only schedule that closes the gap is stall-aware
		// concentration with feedback -- which matches the streaming optimum B/T, i.e.
		// the band's reactive ceiling already equals the ideal streaming code.
		if clu < uni*0.95 {
			t.Errorf("lb=%d: blind clustered %.3f unexpectedly beat uniform %.3f -- a free proactive win?", lb, clu, uni)
		}
		if gap > uni+1e-9 {
			t.Errorf("lb=%d: stall-aware onGap %.3f should be <= uniform %.3f", lb, gap, uni)
		}
		if bt := float64(lb) / float64(T); gap > bt*1.25 {
			t.Errorf("lb=%d: stall-aware onGap %.3f should approach streaming B/T %.3f", lb, gap, bt)
		}
	}
}

// TestRepairSchedulingGE runs the same three schedules on the GE channel Meld faces,
// and sweeps the stall-aware schedule's feedback delay to expose the latency wall: the
// lower its min overhead at delay 0, the more of that win evaporates as the cursor
// signal is delayed by RTT. If onGap collapses to uniform once the delay approaches T,
// the "scheduling win" is really a reactive/feedback win, RTT-gated -- not a free
// proactive lever.
func TestRepairSchedulingGE(t *testing.T) {
	if testing.Short() {
		t.Skip("scheduling sweep is slow; run without -short")
	}
	const (
		symSize = 16
		T       = 32
		pMean   = 0.05
		target  = 1e-2
		seeds   = 2
	)
	b := T
	nSrc := 16000
	traceLen := nSrc*3 + 4*T + 64
	t.Logf("=== GE mean-loss %.0f%%, T=%d: min overhead for residual<=%.0e, by schedule ===", pMean*100, T, target)
	t.Logf(" meanBurst | uniform | clustered | onGap d=0 | onGap d=T/2 | onGap d=T | onGap d=2T")
	for _, mb := range []float64{2, 4, 6, 8, 12, 16} {
		var uni, clu, g0, g1, g2, g3 float64
		for s := 0; s < seeds; s++ {
			rng := rand.New(rand.NewSource(int64(s)*1009 + int64(mb)*31))
			er := geTrace(rng, traceLen, pMean, mb)
			u, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0, uniformPolicy, target)
			c, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0,
				func(r float64) repairPolicy { return clusteredPolicy(r, 4) }, target)
			mk := func(r float64) repairPolicy { return onGapPolicy(0, r) }
			a0, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 0, mk, target)
			a1, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, T/2, mk, target)
			a2, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, T, mk, target)
			a3, _ := searchMinOverhead(t, er, nSrc, T, b, symSize, 2*T, mk, target)
			uni += u
			clu += c
			g0 += a0
			g1 += a1
			g2 += a2
			g3 += a3
		}
		n := float64(seeds)
		t.Logf(" %8.0f | %6.1f%% | %7.1f%% | %7.1f%% | %9.1f%% | %7.1f%% | %7.1f%%",
			mb, 100*uni/n, 100*clu/n, 100*g0/n, 100*g1/n, 100*g2/n, 100*g3/n)
	}
}
