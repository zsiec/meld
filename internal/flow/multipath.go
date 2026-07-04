package flow

import "math/bits"

// This file is N5's correlation-aware multipath sizing: when a generation is split
// across N paths and the receiver decodes from the union (RLNC combines symbols from
// all of them), the failure probability is governed by the TOTAL erasure count — and
// when the paths' losses are correlated (they go bad together), that tail is far
// heavier than the product of the marginals an i.i.d.-union sizer assumes. So size
// against the JOINT tail, fed by the receiver's measured per-path marginals and the
// per-slot erasure-count histogram. The decoder needs no change; this is purely how
// much repair to provision.

// pathScheduler places each outgoing symbol on one of the paths (PLAN §3.6). The
// decoder is path-agnostic — RLNC decodes from the union — so placement only shapes
// the per-path load and thus the diversity, never correctness. Systematic symbols
// round-robin for an even spread across paths; repair is metered toward each path in
// proportion to its delivered-innovation rate (1−loss) by a weighted-round-robin
// accumulator, so the better path carries more of the redundancy and fewer repair
// bytes are spent only to be dropped. Pure and deterministic; quality enters as data.
//
// Failover: when the receiver reports a path DEAD (Feedback.DeadPaths — a per-path
// outage beyond the recovery horizon), systematic placement shifts each symbol to
// the next live path (source stops feeding the outage) and repair runs its WRR over
// the live set, except that every probeRepairEvery-th repair is sent to a dead path
// as a droppable PROBE — the revival signal (any arrival clears the receiver's bit)
// that doubles as free rank if the path comes back mid-flight. A report that would
// mark every path dead is ignored: with no live path, round-robin is as good as
// anything.
type pathScheduler struct {
	paths            int
	weight           []int // per-path delivered-innovation weight (∝ 1−loss)
	acc              []int // weighted-round-robin accumulators for repair placement
	dead             uint8 // receiver-reported outage bitmap (bit i = path i)
	probeCount       int   // repair emissions since the last dead-path probe
	probeRR          int   // rotates probes across multiple dead paths
	probeArmed       bool  // backstop: force the next repair to probe (see probeOwed)
	closesSinceProbe int   // generation closes since the last probe (backstop cadence)
	failoverOff      bool  // test seam: pin the pre-failover behavior for paired A/B runs
}

// probeRepairEvery is the dead-path probe cadence: one repair in this many goes to a
// dead path. Cheap (droppable redundancy), frequent enough that revival is observed
// within a few feedback intervals at media rates. probeGenEvery is the BACKSTOP
// cadence in generation closes: revival must never depend on repair volume — on a
// clean surviving path the sizer (rightly) decays repair to zero, and with zero
// repair the every-16th-repair diversion never fires, latching the dead path dead
// forever. One dedicated probe repair every probeGenEvery closes costs at most
// 1/(2·GenSize) overhead while a path is dead, and only then.
const (
	probeRepairEvery = 16
	probeGenEvery    = 2
)

// newPathScheduler returns a scheduler over n paths with equal initial weights.
func newPathScheduler(n int) *pathScheduler {
	if n < 1 {
		n = 1
	}
	s := &pathScheduler{paths: n, weight: make([]int, n), acc: make([]int, n)}
	for i := range s.weight {
		s.weight[i] = 1
	}
	return s
}

// setQuality updates the per-path repair weighting from each path's delivered fraction
// (1−loss) in parts per million; a path that delivers nothing still keeps a weight of
// 1 so it is probed, not abandoned.
func (s *pathScheduler) setQuality(deliveredPpm []int) {
	for i := 0; i < s.paths && i < len(deliveredPpm); i++ {
		w := deliveredPpm[i] / 1000 // permille weight (0..1000)
		if w < 1 {
			w = 1
		}
		s.weight[i] = w
	}
}

// setDead adopts the receiver's outage bitmap. All-dead (or an out-of-range mask) is
// clamped to none-dead: failover needs a live path to fail over TO.
func (s *pathScheduler) setDead(mask uint8) {
	if s.failoverOff {
		return
	}
	mask &= uint8(1)<<s.paths - 1
	if bits.OnesCount8(mask) >= s.paths {
		mask = 0
	}
	s.dead = mask
}

// anyDead reports whether failover placement is active.
func (s *pathScheduler) anyDead() bool { return s.dead != 0 }

// systematicPath returns the path for the systematic symbol carrying source id:
// id mod paths — the exact mapping the receiver mirrors for co-loss attribution —
// shifted forward to the next live path while failover is active. Placement is
// DERIVED FROM THE ID, never from a free-running cursor: a cursor that advanced per
// skipped dead path drifted out of phase with the receiver's id-mod-paths model
// after an odd-length failover, so every post-revival stamp mismatched and the
// layout kill-switch permanently disabled co-loss estimation. Deriving from the id
// makes revival restore the exact pre-failover placement immediately, whatever the
// failover's length.
func (s *pathScheduler) systematicPath(id uint32) int {
	for i := uint32(0); i < uint32(s.paths); i++ {
		p := int((id + i) % uint32(s.paths))
		if s.dead&(uint8(1)<<p) == 0 {
			return p
		}
	}
	return int(id % uint32(s.paths)) // unreachable while setDead clamps all-dead to none-dead
}

// repairPath returns the path for the next repair symbol: the weighted round-robin
// over live paths, with every probeRepairEvery-th repair (or an armed backstop
// probe) diverted to a dead path.
func (s *pathScheduler) repairPath() int {
	if s.dead != 0 {
		s.probeCount++
		if s.probeArmed || s.probeCount >= probeRepairEvery {
			return s.probePath()
		}
	}
	total, best := 0, -1
	for i := 0; i < s.paths; i++ {
		if s.dead&(uint8(1)<<i) != 0 {
			continue
		}
		s.acc[i] += s.weight[i]
		total += s.weight[i]
		if best < 0 || s.acc[i] > s.acc[best] {
			best = i
		}
	}
	if best < 0 {
		return 0 // unreachable while setDead clamps all-dead to none-dead
	}
	s.acc[best] -= total
	return best
}

// probePath returns the next dead path to probe (rotating across multiple) and
// resets both probe cadences. Called only with dead != 0.
func (s *pathScheduler) probePath() int {
	s.probeArmed, s.probeCount, s.closesSinceProbe = false, 0, 0
	for i := 0; i < s.paths; i++ {
		p := s.probeRR % s.paths
		s.probeRR++
		if s.dead&(uint8(1)<<p) != 0 {
			return p
		}
	}
	return 0 // unreachable: callers check dead != 0
}

// probeOwed advances the backstop cadence by one generation close and reports
// whether a dedicated probe repair is owed: dead paths exist and no probe (WRR
// diversion or backstop) has gone out for probeGenEvery closes. The caller answers
// true by emitting one ordinary repair — the armed flag makes repairPath route it
// to the dead path. This keeps revival independent of repair VOLUME: with the
// surviving path clean the sizer decays repair to zero, and without the backstop
// the dead path would never be probed again (revival is observed only through an
// arrival on the dead path, so no probe means dead forever).
func (s *pathScheduler) probeOwed() bool {
	if s.dead == 0 || s.failoverOff {
		return false
	}
	s.closesSinceProbe++
	if s.closesSinceProbe < probeGenEvery {
		return false
	}
	s.probeArmed = true
	return true
}

// coLossWindow is the number of aligned N-path slots the co-loss estimator averages
// before folding a sample into its smoothed rates; coLossEWMAShift is the EWMA weight.
const (
	coLossWindow    = 64
	coLossEWMAShift = 2
)

// coLossEstimator measures, over aligned windows of N paths, the per-path marginal loss
// (for the scheduler) and the per-slot erasure-COUNT histogram (for the joint-tail sizer,
// N5). The receiver feeds it one aligned slot at a time — a length-N vector of which paths
// lost their symbol; a window's per-path loss fractions and count histogram are folded into
// smoothed parts-per-million rates by EWMA. The histogram's high-count tail beyond the
// independent product is the correlation the joint-tail sizer needs and the i.i.d.-union
// sizer misses. Integer arithmetic, deterministic.
type coLossEstimator struct {
	paths   int
	slots   int
	lost    []int // current-window per-path lost counts (len paths)
	hist    []int // current-window erasure-count histogram (len paths+1)
	margPpm []int // smoothed per-path marginal loss (len paths)
	histPpm []int // smoothed erasure-count histogram (len paths+1)
	primed  bool
}

// newCoLossEstimator returns an estimator over n paths.
func newCoLossEstimator(n int) *coLossEstimator {
	if n < 1 {
		n = 1
	}
	return &coLossEstimator{
		paths: n, lost: make([]int, n), hist: make([]int, n+1),
		margPpm: make([]int, n), histPpm: make([]int, n+1),
	}
}

// observe folds one aligned slot: lost[i] is whether path i lost its symbol in this slot.
func (e *coLossEstimator) observe(lost []bool) {
	e.slots++
	cnt := 0
	for i := 0; i < e.paths && i < len(lost); i++ {
		if lost[i] {
			e.lost[i]++
			cnt++
		}
	}
	e.hist[cnt]++
	if e.slots < coLossWindow {
		return
	}
	for i := 0; i < e.paths; i++ {
		p := e.lost[i] * 1_000_000 / e.slots
		if !e.primed {
			e.margPpm[i] = p
		} else {
			e.margPpm[i] += (p - e.margPpm[i]) >> coLossEWMAShift
		}
	}
	for j := 0; j <= e.paths; j++ {
		p := e.hist[j] * 1_000_000 / e.slots
		if !e.primed {
			e.histPpm[j] = p
		} else {
			e.histPpm[j] += (p - e.histPpm[j]) >> coLossEWMAShift
		}
	}
	e.primed = true
	e.slots = 0
	for i := range e.lost {
		e.lost[i] = 0
	}
	for j := range e.hist {
		e.hist[j] = 0
	}
}

// marginals returns the smoothed per-path marginal loss rates (ppm).
func (e *coLossEstimator) marginals() []int { return e.margPpm }

// slotDist returns the smoothed per-slot erasure-count histogram (ppm, len paths+1).
func (e *coLossEstimator) slotDist() []int { return e.histPpm }

// repairForJointTailN returns the smallest total repair count r such that a generation of
// k source symbols plus r repair, split round-robin across N paths and decoded from the
// union, fails with probability at most delta. slotDistPpm[j] is the probability (parts per
// million) that j of the N paths erase their symbol in one aligned slot — the per-slot
// erasure-COUNT distribution (len N+1). Union-decode failure depends only on the TOTAL
// erasure count, so this histogram is the exact sufficient statistic: it embeds the
// cross-path correlation (a correlated channel puts more mass at the high-j tail than the
// independent product would) without enumerating which paths failed. The total erasure
// distribution is the slots-fold convolution of slotDistPpm; r is the smallest count whose
// upper tail is ≤ delta. Fixed-point Q30 throughout (bit-reproducible). The final partial
// slot (≤ N−1 tail symbols) is folded in as a full slot — negligible at k ≫ N.
func repairForJointTailN(k int, slotDistPpm []int, delta float64, maxFactor int) int {
	n := len(slotDistPpm) - 1 // paths per slot
	if k <= 0 || n < 1 {
		return 0
	}
	slotDist := make([]int64, n+1)
	var sum int64
	for j := 0; j <= n; j++ {
		v := int64(slotDistPpm[j]) * geScale / 1_000_000
		if v < 0 {
			v = 0
		}
		slotDist[j] = v
		sum += v
	}
	if sum <= 0 {
		return 0 // no measured loss
	}
	for j := range slotDist { // renormalize so the histogram sums to exactly geScale
		slotDist[j] = slotDist[j] * geScale / sum
	}
	maxR := k * maxFactor
	deltaQ := int64(delta * geScale)
	for r := 0; r < maxR; r++ {
		slots := (k + r) / n // N symbols per aligned slot; the partial tail slot is folded in below
		if rem := (k + r) % n; rem != 0 {
			slots++
		}
		if jointTailGreaterN(slots, slotDist, r) <= deltaQ {
			return r
		}
	}
	return maxR
}

// repairForJointTail is the 2-path entry: it builds the per-slot erasure-count histogram
// (pNone, pOne, pTwo) from the per-path marginals paPpm/pbPpm and the joint both-erased rate
// pBothPpm — clamping pBoth to the Fréchet bounds [max(0, pa+pb−1), min(pa, pb)] so the cell
// probabilities stay valid — and defers to repairForJointTailN. The i.i.d.-union sizer is the
// special case pBothPpm = pa·pb (independence); a positive co-loss makes the tail heavier.
func repairForJointTail(k, paPpm, pbPpm, pBothPpm int, delta float64, maxFactor int) int {
	if k <= 0 || (paPpm <= 0 && pbPpm <= 0) {
		return 0
	}
	const scale = 1_000_000
	pBoth := pBothPpm
	if lo := paPpm + pbPpm - scale; pBoth < lo {
		pBoth = lo
	}
	if pBoth < 0 {
		pBoth = 0
	}
	if pBoth > paPpm {
		pBoth = paPpm
	}
	if pBoth > pbPpm {
		pBoth = pbPpm
	}
	return repairForJointTailN(k, []int{scale - paPpm - pbPpm + pBoth, paPpm + pbPpm - 2*pBoth, pBoth}, delta, maxFactor)
}

// jointTailGreaterN returns P[total erasures over `slots` aligned N-path slots > r] in Q30,
// where each slot independently contributes m ∈ [0,N] erasures with probability slotDist[m].
// It is a convolution DP over the slots, the erasure count capped at r (mass escaping past r
// is the tail), integer multiply-add with rounded Q30 rescaling — the N-path generalization
// of the two-path 0/1/2 step.
func jointTailGreaterN(slots int, slotDist []int64, r int) int64 {
	const half = geScale / 2
	n := len(slotDist) - 1
	dist := make([]int64, r+1)
	dist[0] = geScale
	nd := make([]int64, r+1)
	for s := 0; s < slots; s++ {
		for j := 0; j <= r; j++ {
			var v int64
			for m := 0; m <= n && m <= j; m++ {
				if slotDist[m] != 0 {
					v += dist[j-m] * slotDist[m]
				}
			}
			nd[j] = (v + half) >> 30
		}
		dist, nd = nd, dist
	}
	var cdf int64
	for j := 0; j <= r; j++ {
		cdf += dist[j]
	}
	if tail := geScale - cdf; tail > 0 {
		return tail
	}
	return 0
}
