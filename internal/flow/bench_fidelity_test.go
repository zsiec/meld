package flow

import (
	"os"
	"sort"
	"testing"
)

// TestReactiveBreakdown isolates WHERE the real-timing over-send lives — proactive set-point vs
// reactive top-up — by printing the raw sender counters with and without jitter. The intervention
// must target whichever tier inflates.
func TestReactiveBreakdown(t *testing.T) {
	if os.Getenv("AUTORED_SWEEP") == "" {
		t.Skip("set AUTORED_SWEEP=1")
	}
	const (
		owd       = 50_000
		srcMicros = 526
		n         = 1500
		seeds     = 6
		loss      = 0.01
		burst     = 2.0
		mult      = 4.0
	)
	cfg := Config{
		Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
		BufferMicros: int64(mult * 100_000), AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
	}
	for _, jit := range []int64{0, 80_000} {
		var src, pro, react, thr, pest, bq8 float64
		for s := int64(1); s <= seeds; s++ {
			sl := simLink{
				cfg: cfg, owdMicros: owd, srcMicros: srcMicros, n: n, drop: geDrop(s, loss, burst),
				paceBytesPerSec: 12_500_000, timingJitterMicros: jit,
			}
			if jit > 0 {
				sl.timingSeed = s*7919 + 1
			}
			res := sl.run()
			r := res.sstats
			src += float64(r.Source)
			pro += float64(r.Repair)
			react += float64(r.ReactiveRepair)
			thr += float64(r.Throttled)
			pest += res.finalPEst / seeds
			bq8 += float64(res.finalBurstQ8) / seeds
		}
		t.Logf("jitter=%2dms: src=%.0f proactive=%.0f (%.0f%%) reactive=%.0f | sender pEst=%.3f burstQ8=%.0f (i.i.d.=256)",
			jit/1000, src/seeds, pro/seeds, 100*pro/src, react/seeds, pest, bq8)
	}
}

// TestAutoReorderHoldoffScreen pre-registers the bar for making the reorder window default-able: the
// MEASURED holdoff must (a) capture the overhead win under reorder, matching the hand-tuned fixed
// window, AND (b) self-disable on a clean-but-lossy link — gaps from genuine loss must NOT grow the
// window, so loss-onset responsiveness (and thus delivery) is unharmed where there is no reorder.
func TestAutoReorderHoldoffScreen(t *testing.T) {
	if os.Getenv("AUTORED_SWEEP") == "" {
		t.Skip("set AUTORED_SWEEP=1")
	}
	const (
		owd       = 50_000
		srcMicros = 526
		n         = 1500
		seeds     = 8
		pace      = 12_500_000
	)
	mk := func(fixed int64, auto bool, mult float64) Config {
		return Config{
			Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: int64(mult * 100_000), AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
			ReorderHoldoffMicros: fixed, AutoReorderHoldoff: auto,
		}
	}
	agg := func(fixed int64, auto bool, mult, loss float64, jit int64) (deliv, ovh float64) {
		for s := int64(1); s <= seeds; s++ {
			sl := simLink{
				cfg: mk(fixed, auto, mult), owdMicros: owd, srcMicros: srcMicros, n: n,
				drop: geDrop(s, loss, 2), paceBytesPerSec: pace, timingJitterMicros: jit,
			}
			if jit > 0 {
				sl.timingSeed = s*7919 + 1
			}
			res := sl.run()
			deliv += 100 * float64(res.delivered) / float64(n) / seeds
			ovh += 100 * res.overhead() / seeds
		}
		return deliv, ovh
	}
	t.Logf("AutoReorderHoldoff screen, 4×RTT, %d seeds | arm: deliv%% / ovhd%%", seeds)
	t.Logf("REORDER regime (1%% loss, 80ms jitter) — auto must cut overhead like fixed:")
	for _, a := range []struct {
		name  string
		fixed int64
		auto  bool
	}{{"off", 0, false}, {"fixed 80ms", 80_000, false}, {"AUTO", 0, true}} {
		d, o := agg(a.fixed, a.auto, 4, 0.01, 80_000)
		t.Logf("   %-11s | %5.1f / %5.0f", a.name, d, o)
	}
	t.Logf("CLEAN-LOSSY regime (5%% loss, NO jitter) — auto must self-disable (≈ off):")
	for _, a := range []struct {
		name  string
		fixed int64
		auto  bool
	}{{"off", 0, false}, {"fixed 80ms", 80_000, false}, {"AUTO", 0, true}} {
		d, o := agg(a.fixed, a.auto, 4, 0.05, 0)
		t.Logf("   %-11s | %5.1f / %5.0f", a.name, d, o)
	}
}

// TestReorderHoldoffScreen evaluates the loss-estimate reorder window. Real-timing
// oversend is proactive: reorder can look like high loss and burstiness to the
// receiver, inflating the repair set point. Several holdoff values are swept to
// find the smallest that reduces this overhead without harming delivery or p99.
func TestReorderHoldoffScreen(t *testing.T) {
	if os.Getenv("AUTORED_SWEEP") == "" {
		t.Skip("set AUTORED_SWEEP=1 to run the reorder-holdoff screen")
	}
	const (
		owd       = 50_000
		srcMicros = 526
		n         = 1500
		seeds     = 10
		loss      = 0.01
		burst     = 2.0
		pace      = 12_500_000
		jit       = 80_000
	)
	cfg := func(holdoff int64, mult float64) Config {
		return Config{
			Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: int64(mult * 100_000), AutoGenSize: true, ProactiveDecay: true,
			RepairWithinBudget: true, ReorderHoldoffMicros: holdoff,
		}
	}
	agg := func(holdoff int64, mult float64) (dmean, dlo, dhi, p99, ovh float64) {
		var ds []float64
		for s := int64(1); s <= seeds; s++ {
			sl := simLink{
				cfg: cfg(holdoff, mult), owdMicros: owd, srcMicros: srcMicros, n: n,
				drop: geDrop(s, loss, burst), paceBytesPerSec: pace, timingJitterMicros: jit, timingSeed: s*7919 + 1,
			}
			res := sl.run()
			d := 100 * float64(res.delivered) / float64(n)
			ds = append(ds, d)
			dmean += d / seeds
			p99 += float64(pctlMicros(res.latencyMicros, 0.99)) / 1000 / seeds
			ovh += 100 * res.overhead() / seeds
		}
		sort.Float64s(ds)
		return dmean, ds[0], ds[len(ds)-1], p99, ovh
	}
	t.Logf("reorder holdoff · 20Mbps 1%% burst2, jittered sim (80ms), %d seeds", seeds)
	for _, mult := range []float64{2, 4} {
		t.Logf("--- %.0f×RTT --- holdoff | deliv[min-max] | p99ms | ovhd%%", mult)
		for _, h := range []int64{0, 40_000, 80_000, 120_000} {
			dm, dlo, dhi, p9, ov := agg(h, mult)
			t.Logf("           %3dms | %5.1f[%5.1f-%5.1f] | %5.0f | %5.0f", h/1000, dm, dlo, dhi, p9, ov)
		}
	}
}

// TestBenchMapDefault uses the now-paced+jittered sim to MAP where default meld actually has headroom
// across the budget axis — delivery, p99, and overhead — and to check the bench reproduces cref's
// real-timing overhead explosion (the unpaced sim shows ~23% repair at 1%/2×RTT; cref shows ~220%,
// the reactive controller thrashing under jitter). If jitter explodes the sim's overhead toward cref,
// the bench is predictive for overhead AND the destination is named: reactive over-send under jitter.
// Env-gated (measurement).
func TestBenchMapDefault(t *testing.T) {
	if os.Getenv("AUTORED_SWEEP") == "" {
		t.Skip("set AUTORED_SWEEP=1 to run the headroom map")
	}
	const (
		owd       = 50_000
		srcMicros = 526
		n         = 1500
		seeds     = 8
		loss      = 0.01
		burst     = 2.0
	)
	cfg := func(mult float64) Config {
		return Config{
			Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3,
			BufferMicros: int64(mult * 100_000), AutoGenSize: true, ProactiveDecay: true, RepairWithinBudget: true,
		}
	}
	run := func(mult, paceBps float64, jit, seed int64) (deliv, p99, ovh float64) {
		sl := simLink{
			cfg: cfg(mult), owdMicros: owd, srcMicros: srcMicros, n: n,
			drop: geDrop(seed, loss, burst), paceBytesPerSec: int64(paceBps), timingJitterMicros: jit,
		}
		if jit > 0 {
			sl.timingSeed = seed*7919 + 1
		}
		res := sl.run()
		return 100 * float64(res.delivered) / float64(n), float64(pctlMicros(res.latencyMicros, 0.99)) / 1000, 100 * res.overhead()
	}
	type cond struct {
		name    string
		paceBps float64
		jit     int64
	}
	conds := []cond{{"no jitter (orig sim)", 1e12, 0}, {"pacer100M + jit 80ms", 12_500_000, 80_000}}
	t.Logf("default meld @ 20Mbps 1%% burst2, %d seeds (cref ref: 2×RTT ovhd~220%%, 4×RTT ovhd~140%%, deliv unstable@2× ~100@4×)", seeds)
	for _, c := range conds {
		t.Logf("=== %s ===  budget | deliv%% [min-max] | p99ms | ovhd%%", c.name)
		for _, mult := range []float64{2, 3, 4, 6} {
			var ds, ps, os_ []float64
			for s := int64(1); s <= seeds; s++ {
				d, p, o := run(mult, c.paceBps, c.jit, s)
				ds, ps, os_ = append(ds, d), append(ps, p), append(os_, o)
			}
			sort.Float64s(ds)
			var dm, pm, om float64
			for i := range ds {
				dm += ds[i]
				pm += ps[i]
				om += os_[i]
			}
			t.Logf("              %.0f×RTT | %5.1f [%5.1f-%5.1f] | %5.0f | %5.0f", mult, dm/seeds, ds[0], ds[len(ds)-1], pm/seeds, om/seeds)
		}
	}
}
