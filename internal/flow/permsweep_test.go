package flow

// Config-permutation performance sweep: paired, paced (1 MiB/s), timing-jittered (2 ms) simLink
// cells across budget × loss × RTT, comparing knob permutations against the
// default sliding profile. Reports per-cell delivery, overhead, p99 and the
// PAIRED per-seed delivery delta vs default; the A/A arm (default under disjoint
// timing seeds) measures the noise floor every judgment must clear.
//
// Env-gated: MELD_PERMSWEEP=1 go test -run TestPermutationSweep -v ./internal/flow

import (
	"fmt"
	"os"
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

type sweepCell struct {
	name        string
	owd         int64
	budget      int64
	loss, burst float64 // burst 0 => iid; loss 0 => clean
}

type sweepArm struct {
	name string
	mod  func(*Config)
}

func sweepDefaultCfg(budget int64) Config {
	return Config{
		Flow: 1, SymbolSize: 256, Sliding: true, CodingWindow: 64,
		Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: budget,
		OutageAware: true, AutoReorderHoldoff: true, RepairWithinBudget: true,
		ProtectedRepairPhasing: true,
	}
}

// sweepArms is the arm set under comparison.
func sweepArms() []sweepArm {
	return []sweepArm{
		{"default", func(c *Config) {}},
		{"AA", func(c *Config) {}}, // same config, disjoint timing seed: the noise floor
		// Fixed holdoff is retained as the comparison against automatic/default
		// reorder handling.
		{"hold8", func(c *Config) { c.ReorderHoldoffMicros = 8_000 }},
	}
}

func sweepDrop(cell sweepCell, seed int64) func(wire.Symbol) bool {
	switch {
	case cell.loss == 0:
		return func(wire.Symbol) bool { return false }
	case cell.burst == 0:
		return uniformDrop(uint64(seed)*0x9E3779B9+11, cell.loss)
	default:
		return geDrop(seed*7919+13, cell.loss, cell.burst)
	}
}

func TestPermutationSweep(t *testing.T) {
	if os.Getenv("MELD_PERMSWEEP") == "" {
		t.Skip("set MELD_PERMSWEEP=1 to run the permutation performance sweep")
	}
	const (
		n     = 6_000
		src   = 500
		pace  = 1 << 20
		tjit  = 2_000
		seeds = 4
	)
	var cells []sweepCell
	for _, rtt := range []int64{60_000, 200_000} {
		for _, mult := range []float64{0.75, 1, 1.5, 2.5, 4} {
			for _, ch := range []struct {
				tag         string
				loss, burst float64
			}{{"clean", 0, 0}, {"iid3", 0.03, 0}, {"iid8", 0.08, 0}, {"ge12", 0.10, 12}, {"ge48", 0.10, 48}} {
				cells = append(cells, sweepCell{
					name:   fmt.Sprintf("rtt%d-b%.2gx-%s", rtt/1000, mult, ch.tag),
					owd:    rtt / 2,
					budget: int64(mult * float64(rtt)),
					loss:   ch.loss, burst: ch.burst,
				})
			}
		}
	}
	arms := sweepArms()
	for _, cell := range cells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			t.Parallel()
			type armAgg struct {
				deliv, ovh, p99 [seeds]float64
			}
			results := map[string]*armAgg{}
			for _, arm := range arms {
				agg := &armAgg{}
				results[arm.name] = agg
				for seed := int64(0); seed < seeds; seed++ {
					cfg := sweepDefaultCfg(cell.budget)
					arm.mod(&cfg)
					tseed := seed*211 + 3
					if arm.name == "AA" {
						tseed = seed*577 + 41 // disjoint timing draw, same channel
					}
					sl := simLink{
						cfg: cfg, owdMicros: cell.owd, srcMicros: src,
						n: n, sliding: cfg.Sliding, drop: sweepDrop(cell, seed),
						paceBytesPerSec: pace, timingJitterMicros: tjit, timingSeed: tseed,
					}
					var res simResult
					if cfg.Sliding {
						res = sl.runCores(NewSlidingSender(cfg), NewSlidingReceiver(cfg))
					} else {
						res = sl.runCores(NewSender(cfg), NewReceiver(cfg))
					}
					agg.deliv[seed] = float64(res.deliveredInTime) / float64(n) * 100 // ARBITERED (the honest live metric; raw counts late backlog)
					agg.ovh[seed] = res.overhead() * 100
					agg.p99[seed] = float64(pctlMicros(res.latencyMicros, 0.99)) / 1000
				}
			}
			def := results["default"]
			for _, arm := range arms {
				a := results[arm.name]
				var dm, om, pm, dd, wd float64
				wd = 1e9
				for s := 0; s < seeds; s++ {
					dm += a.deliv[s] / seeds
					om += a.ovh[s] / seeds
					pm += a.p99[s] / seeds
					d := a.deliv[s] - def.deliv[s]
					dd += d / seeds
					if d < wd {
						wd = d
					}
				}
				t.Logf("SWEEP|%s|%s|deliv=%.2f|ovh=%.1f|p99=%.0f|dDeliv=%+.2f|worstD=%+.2f",
					cell.name, arm.name, dm, om, pm, dd, wd)
			}
		})
	}
}
