package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestCleanLinkBurstyDeliversAll pins a delivery invariant the deadline machinery must
// honor: on a ZERO-LOSS link whose one-way delay is well within the budget, EVERY source
// symbol is delivered in time — none is evicted as "late" — even when media is written in
// bursts (a whole access unit at one instant) rather than at a uniform cadence.
//
// Regression for the clean-link deadline cliff: the receiver extrapolated each id's
// deadline as refDL+(id-refID)*interval, assuming uniformly-spaced write times. A burst of
// B chunks written together shares one write time (one deadline), so the backward
// extrapolation handed the burst's leading ids deadlines up to (B-1)*interval in the past;
// when a burst spanned more than budget/interval, those ids were dropped the instant they
// arrived — a fictitious loss on a perfect link (~35% at a 30 ms budget over a real camera
// feed) that also inflated the channel-loss estimate. The fix uses a directly-received
// symbol's own stamped deadline and extrapolates only for ids that never arrived.
func TestCleanLinkBurstyDeliversAll(t *testing.T) {
	noLoss := func(wire.Symbol) bool { return false }
	// GenSize is 16 below; each case's burst*bursts is a multiple of it so the final
	// generation fills exactly — isolating the mid-stream deadline cliff from the orthogonal
	// end-of-stream artifact where the last partial generation is padded to GenSize on the
	// wire and the receiver times out the phantom tail it can't know never existed.
	for _, tc := range []struct {
		name         string
		bufferMicros int64
		burst        int
		bursts       int
		srcMicros    int64
	}{
		// Budget smaller than the burst's wall-clock span — the regime the cliff lived in.
		{"tight-budget-full-gen-burst", 30_000, 16, 50, 56_000},
		{"very-tight-budget", 15_000, 16, 50, 56_000},
		// A generous budget must of course also stay clean.
		{"loose-budget", 120_000, 16, 50, 56_000},
		// A generation spans MULTIPLE bursts (8<16) — the cursor reaches an id mid-generation
		// before its burst has arrived; the old global backstop evicted it on the inter-burst gap.
		{"two-bursts-per-gen", 20_000, 8, 40, 12_000},
		// A non-aligned burst (6 ∤ 16): generation and burst boundaries interleave; 6*48=288=18*16.
		{"unaligned-burst", 20_000, 6, 48, 21_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.SymbolSize = 64
			cfg.GenSize = 16
			cfg.BufferMicros = tc.bufferMicros
			res := simLink{
				cfg:       cfg,
				owdMicros: 1_000, // ~clean LAN: 1 ms each way, far inside any budget here
				srcMicros: tc.srcMicros,
				burst:     tc.burst,
				n:         tc.burst * tc.bursts,
				drop:      noLoss,
			}.run()

			if res.corrupt {
				t.Fatal("delivered payload did not match its id (false recovery)")
			}
			if res.lateDeliv {
				t.Fatal("a symbol was delivered after its true write+budget deadline")
			}
			// The link drops nothing, so every source symbol must be delivered in order.
			// delivered == n is the strong check: delivery is strictly in order and nothing
			// is recovered here, so a single mid-stream eviction (the cliff) would leave
			// delivered < n. This is exactly what regressed to delivered=6/300 before the fix.
			if res.delivered != res.n {
				t.Fatalf("clean link must deliver every source symbol: delivered=%d/%d lost=%d wireLost=%d recovered=%d duplicates=%d",
					res.delivered, res.n, res.stats.Lost, res.stats.WireLost, res.stats.Recovered, res.stats.Duplicates)
			}
			// wireLost is the honest channel-loss signal; on a zero-loss link it must read 0.
			// The cliff used to leak deadline-evictions into it (a fictitious ~35% that tripled
			// proactive repair); it must stay 0 now.
			if res.stats.WireLost != 0 {
				t.Fatalf("zero-loss link reported wireLost=%d (deadline-loss leaking into the channel estimate)", res.stats.WireLost)
			}
			// The only remaining loss is the end-of-stream tail: the final generation declares
			// GenSize ids on the wire, so the receiver times out the phantom tail past the true
			// stream end it cannot know never existed. It is bounded by one generation and is
			// NOT the mid-stream cliff (which delivered==n already rules out).
			if res.stats.Lost > uint64(cfg.GenSize) {
				t.Fatalf("loss beyond the end-of-stream tail: lost=%d (> GenSize=%d) — mid-stream eviction regressed",
					res.stats.Lost, cfg.GenSize)
			}
		})
	}
}
