package flow

import (
	"fmt"
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// This file proves the confidence-gated proactive-floor decay (effectiveFloor) is STRICTLY
// positive: it reclaims the floor's clean-link waste while never reducing protection that was
// load-bearing. Two properties, both on the deterministic simLink with the four invariants:
//   1. on a durably clean link the floor decays to zero, so the run's overhead falls well below the
//      static floor — at full delivery (the waste recovered nothing, so dropping it costs nothing);
//   2. across a clean→loss onset, at EVERY RTT, delivery is preserved — the reactive tier covers a
//      decayed floor where it can land in time, and the floor stays full (gate closed) where it
//      cannot, so neither regime under-covers the onset.

// perfBudget is the playout/deadline budget these tests size against (200 ms, as the bench).
const perfBudget = 200_000

// perfCfg is the shared generation-coder config for the floor-decay tests: the bench's symbol/gen
// size at the default 15% redundancy floor.
func perfCfg() Config {
	return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: perfBudget}
}

// onsetDrop forwards the first cleanCalls symbols cleanly (to arm the floor-decay confidence), then
// switches to i.i.d. p loss — the clean→degradation transient the decay must not break.
func onsetDrop(cleanCalls int, seed uint64, p float64) func(wire.Symbol) bool {
	base := uniformDrop(seed, p)
	n := 0
	return func(sym wire.Symbol) bool {
		n++
		if n <= cleanCalls {
			return false
		}
		return base(sym)
	}
}

// TestFloorDecayReclaimsCleanWaste — property (1).
func TestFloorDecayReclaimsCleanWaste(t *testing.T) {
	const n = 6000 // long enough to build cleanRun past cleanFloorConfirm, then run decayed
	res := simLink{
		cfg: perfCfg(), owdMicros: 10_000, srcMicros: 1_000, n: n,
		drop: func(wire.Symbol) bool { return false }, // durably clean
	}.run()
	assertCoreInvariants(t, res, n, "long clean link")
	if res.delivered != res.n {
		t.Fatalf("clean link must deliver all %d, got %d", res.n, res.delivered)
	}
	if res.stats.Recovered != 0 {
		t.Fatalf("clean link recovered %d — there was nothing to recover", res.stats.Recovered)
	}
	ovhd := res.overhead()
	floor := perfCfg().Redundancy
	t.Logf("long clean link: delivered=%d/%d recovered=0 overhead=%.1f%% (static floor=%.0f%%)",
		res.delivered, res.n, 100*ovhd, 100*floor)
	// After the floor decays the steady state spends ~no proactive repair, so the whole-run overhead
	// (a brief full-floor warmup + a long decayed tail) falls well under the static floor.
	if ovhd >= floor {
		t.Fatalf("floor did not decay on a durably clean link: overhead %.1f%% still at/above the %.0f%% floor",
			100*ovhd, 100*floor)
	}
}

// TestFloorDecayOnsetSafeAcrossRTT — property (2). A clean prefix (to arm the decay) then i.i.d.
// loss, swept across RTTs spanning the reactive-backstop boundary. Delivery must hold at every RTT.
func TestFloorDecayOnsetSafeAcrossRTT(t *testing.T) {
	const n = 6000
	for _, owd := range []int64{10_000, 25_000, 50_000, 75_000} { // RTT 20, 50, 100, 150 ms
		res := simLink{
			cfg: perfCfg(), owdMicros: owd, srcMicros: 1_000, n: n,
			drop: onsetDrop(n/2, 0xC0FFEE, 0.15), // clean half (arm decay), then 15% loss
		}.run()
		assertCoreInvariants(t, res, n, fmt.Sprintf("onset rtt=%dms", owd/500))
		d := float64(res.delivered) / float64(res.n)
		t.Logf("onset rtt=%3dms: delivered=%d/%d (%.1f%%) recovered=%d overhead=%.0f%%",
			owd/500, res.delivered, res.n, 100*d, res.stats.Recovered, 100*res.overhead())
		if d < 0.97 {
			t.Fatalf("onset rtt=%dms: delivery %.1f%% — the floor decay left the onset under-covered",
				owd/500, 100*d)
		}
	}
}

// TestFloorDecayFlickerStaysFull — a flickering link (loss never durably clears) must keep resetting
// cleanRun so the floor never decays: the gate only ever opens on a *durably* clean link, so an
// intermittently-lossy link is protected exactly as before (a pure no-op there).
func TestFloorDecayFlickerStaysFull(t *testing.T) {
	const n = 6000
	// Alternating ~clean and lossy stretches so cleanRun cannot reach cleanFloorConfirm.
	flick := func() func(wire.Symbol) bool {
		base := uniformDrop(0x5151, 0.10)
		i := 0
		return func(s wire.Symbol) bool {
			i++
			if (i/200)%2 == 0 {
				return false // clean stretch (shorter than the confirm window)
			}
			return base(s)
		}
	}()
	res := simLink{cfg: perfCfg(), owdMicros: 75_000, srcMicros: 1_000, n: n, drop: flick}.run()
	assertCoreInvariants(t, res, n, "flicker")
	d := float64(res.delivered) / float64(res.n)
	t.Logf("flicker (RTT 150ms, 10%% bursts): delivered=%d/%d (%.1f%%) overhead=%.0f%%",
		res.delivered, res.n, 100*d, 100*res.overhead())
	if d < 0.97 {
		t.Fatalf("flicker delivery %.1f%% — the floor must stay full on a non-durably-clean link", 100*d)
	}
}
