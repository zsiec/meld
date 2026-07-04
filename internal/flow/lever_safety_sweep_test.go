package flow

import (
	"fmt"
	"os"
	"testing"
)

// TestLeverSafetySweep is the falsifiable default-on proof: across a regime matrix (RTT ×
// budget × loss × burst) it runs the default sizer against ProactiveDecay (self-adapting, no
// hint), AdaptiveGenSize (correct RTT hint), and both, and asserts (1) the four delivery
// invariants hold in EVERY cell, and (2) neither lever regresses delivery vs the default by
// more than 1 point where it claims to be safe. A regression or invariant break fails the test
// — so this both proves the safety case and guards it. Overhead is reported per cell so the win
// is visible alongside the safety result.
//
// Env-gated (it is a few hundred sim runs): MELD_LEVER_SWEEP=1 go test -run TestLeverSafetySweep ./internal/flow
func TestLeverSafetySweep(t *testing.T) {
	if os.Getenv("MELD_LEVER_SWEEP") == "" {
		t.Skip("set MELD_LEVER_SWEEP=1 to run the lever default-on safety sweep")
	}
	const n = 1600 // large enough that the finite-stream tail is a small fraction of delivery
	type cell struct {
		owd, budget int64
		loss, burst float64
	}
	var cells []cell
	for _, owd := range []int64{0, 20_000, 40_000, 75_000} { // RTT 0/40/80/150 ms
		for _, budget := range []int64{100_000, 200_000} {
			for _, loss := range []float64{0.02, 0.05, 0.10} {
				for _, burst := range []float64{1, 6} {
					cells = append(cells, cell{owd, budget, loss, burst})
				}
			}
		}
	}

	base := func(c cell) Config {
		return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.05, TargetFailure: 1e-3, BufferMicros: c.budget}
	}
	run := func(cfg Config, c cell) simResult {
		return simLink{cfg: cfg, owdMicros: c.owd, srcMicros: 500, n: n, drop: geDrop(99, c.loss, c.burst)}.run()
	}

	variants := []struct {
		name      string
		make      func(c cell) Config
		safeClaim bool // claims DEFAULT-ON safety (asserted: never regress delivery vs default)
	}{
		// default + ProactiveDecay are the default-on candidates: asserted regression-free.
		{"default", func(c cell) Config { return base(c) }, true},
		{"decay", func(c cell) Config { x := base(c); x.ProactiveDecay = true; return x }, true},
		// AdaptiveGenSize is OPT-IN: its static width cannot react to measured loss, so it trades a
		// point or two of delivery for overhead in the tight-budget + heavy-bursty corner. Reported,
		// not asserted — the operator enables it knowing their link's loss profile / budget headroom.
		{"adaptive", func(c cell) Config {
			x := base(c)
			x.AdaptiveGenSize, x.NominalRTTMicros = true, 2*c.owd // correct hint == actual RTT
			return x
		}, false},
		{"both", func(c cell) Config {
			x := base(c)
			x.ProactiveDecay, x.AdaptiveGenSize, x.NominalRTTMicros = true, true, 2*c.owd
			return x
		}, false},
	}

	worstRegression := 0.0
	var worstCell string
	t.Logf("%-26s | %-10s | %-10s | %-10s | %-10s", "RTT/buf/loss/burst", "default", "decay", "adaptive", "both")
	for _, c := range cells {
		baseRes := run(base(c), c)
		baseFrac := float64(baseRes.delivered) / float64(n)
		row := fmt.Sprintf("R%-3d b%-3d l%-2.0f%% k%-2.0f", c.owd/500, c.budget/1000, c.loss*100, c.burst)
		var fracs, ovhs []string
		for _, v := range variants {
			res := run(v.make(c), c)
			// Invariant 1-4 must hold unconditionally, every cell.
			if res.lateDeliv {
				t.Fatalf("[%s/%s] delivered past deadline", row, v.name)
			}
			if res.corrupt {
				t.Fatalf("[%s/%s] false recovery (wrong bytes)", row, v.name)
			}
			for i := 1; i < len(res.deliveredIDs); i++ {
				if res.deliveredIDs[i] <= res.deliveredIDs[i-1] {
					t.Fatalf("[%s/%s] out-of-order / duplicate at %d", row, v.name, i)
				}
			}
			frac := float64(res.delivered) / float64(n)
			fracs = append(fracs, fmt.Sprintf("%.1f%%", 100*frac))
			ovhs = append(ovhs, fmt.Sprintf("%.0f%%", 100*res.overhead()))
			// Safety: a self-adapting / correctly-configured lever must not regress delivery.
			if v.safeClaim {
				reg := baseFrac - frac
				if reg > worstRegression {
					worstRegression, worstCell = reg, row+"/"+v.name
				}
				if reg > 0.01 {
					t.Errorf("[%s/%s] DELIVERY REGRESSION: %.1f%% vs default %.1f%% (%.1f pt)",
						row, v.name, 100*frac, 100*baseFrac, 100*reg)
				}
			}
		}
		t.Logf("%-26s | d=%-8s | d=%-8s | d=%-8s | d=%-8s  ovh[%s]", row, fracs[0], fracs[1], fracs[2], fracs[3],
			fmt.Sprintf("%s/%s/%s/%s", ovhs[0], ovhs[1], ovhs[2], ovhs[3]))
	}
	t.Logf("worst delivery regression among safe-claim configs: %.2f pt (%s)", 100*worstRegression, worstCell)
}

// TestAdaptiveWrongHintFailureMode demonstrates WHY AdaptiveGenSize needs a correct RTT hint to
// be safe (the reason it is not blindly default-on): an optimistic hint (claims a low RTT, so it
// widens) on a path whose real budget is below the real RTT loses materially more than the
// correctly-narrow default — the budget<RTT completeness cliff. This is reported, not asserted as
// a failure: it is the documented hazard the hint exists to avoid.
func TestAdaptiveWrongHintFailureMode(t *testing.T) {
	t.Parallel()
	const n = 800
	// Real path: RTT 150 ms, budget 100 ms ⇒ budget < RTT (all-proactive). A wrong hint claiming
	// RTT 40 ms makes genWidth widen to 64 — exactly where wide hurts.
	owd := int64(75_000)
	cfg := func(adaptive bool, hintRTT int64) Config {
		c := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.05, TargetFailure: 1e-3, BufferMicros: 100_000}
		if adaptive {
			c.AdaptiveGenSize, c.NominalRTTMicros = true, hintRTT
		}
		return c
	}
	run := func(c Config) float64 {
		r := simLink{cfg: c, owdMicros: owd, srcMicros: 500, n: n, drop: geDrop(99, 0.05, 6)}.run()
		return float64(r.delivered) / float64(n)
	}
	def := run(cfg(false, 0))
	correct := run(cfg(true, 2*owd)) // hint == actual 150 ms ⇒ genWidth stays narrow ⇒ safe
	wrong := run(cfg(true, 40_000))  // hint claims 40 ms ⇒ widens to 64 ⇒ the cliff
	t.Logf("budget<RTT: default=%.1f%%  adaptive(correct hint)=%.1f%%  adaptive(WRONG low hint)=%.1f%%",
		100*def, 100*correct, 100*wrong)
	if correct < def-0.01 {
		t.Errorf("correct hint should match default here (both stay narrow): correct=%.1f%% default=%.1f%%", 100*correct, 100*def)
	}
	if wrong >= def-0.005 {
		t.Logf("note: wrong hint did not visibly regress in this seed — the hazard is seed/loss dependent")
	}
}
