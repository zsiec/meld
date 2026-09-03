package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// uniformDrop returns a deterministic per-symbol i.i.d. loss predicate at rate p, keyed on
// the symbol's IDENTITY (not draw order) so the loss pattern a systematic id sees is
// independent of how much repair the sender emits — letting two runs at different code rates
// be compared on the same channel realization. Shared by the cliff and overhead tests.
func uniformDrop(seed uint64, p float64) func(wire.Symbol) bool {
	return func(sym wire.Symbol) bool {
		var key uint32
		if sym.Kind == wire.Systematic {
			key = sym.SrcIndex
		} else {
			key = sym.WindowBase*100003 + uint32(sym.RepairKey) + (1 << 30)
		}
		return coinU(seed, uint32(sym.Kind), key) < p
	}
}

// TestDeadlineCliffResourceBound is the headline guard for the 600ms-RTT collapse. When the
// one-way propagation delay EXCEEDS the latency budget, every symbol physically arrives after
// its absolute (write-time + budget) deadline, so the receiver delivers ~nothing — a real
// operational cliff. The benchmark's astronomical cpu/GB and alloc/GB in that regime are
// per-GB-DELIVERED with ~0 delivery (a divide-by-near-zero artifact), NOT an unbounded spin.
// This test pins that distinction: delivery collapses, but the receiver's live-decoder set
// and the sender's retained set stay bounded by the admission and retirement caps. A regression
// that removed those caps — turning the cliff into a true memory runaway — fails here.
func TestDeadlineCliffResourceBound(t *testing.T) {
	const (
		budget    = 200_000
		srcMicros = 1_000
	)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: budget}
	res := simLink{
		cfg:       cfg,
		owdMicros: 300_000, // 300 ms one-way > 200 ms budget: nothing can arrive in time
		srcMicros: srcMicros,
		n:         320,
		drop:      uniformDrop(0xC11FF, 0.10),
	}.run()

	t.Logf("cliff: delivered=%d/%d lost=%d peakGens=%d peakRetained=%d overhead=%.0f%%",
		res.delivered, res.n, res.stats.Lost, res.peakGens, res.peakRetained, 100*res.overhead())

	// The cliff: with one-way delay past the budget, delivery is ~0.
	if res.delivered > res.n/20 {
		t.Fatalf("expected near-zero delivery past the budget cliff, got %d/%d", res.delivered, res.n)
	}
	// ...but NOT a runaway: state stays bounded by the resource-safety caps.
	if res.peakGens > cfg.maxRetainedGens() {
		t.Fatalf("receiver live-decoder set %d exceeded the admit cap %d (resource runaway)", res.peakGens, cfg.maxRetainedGens())
	}
	if res.peakRetained > cfg.maxRetainedGens() {
		t.Fatalf("sender retained set %d exceeded the cap %d (resource runaway)", res.peakRetained, cfg.maxRetainedGens())
	}
	// The caps should hold the working set to the in-flight window (≈ a couple dozen
	// generations here), not merely under the hard 512 cap — a leak stalled at the cap would
	// also be a bug. The bound is generous to stay non-flaky while still catching a leak.
	const smallWorkingSet = 128
	if res.peakGens > smallWorkingSet {
		t.Fatalf("receiver live-decoder set %d is far above the in-flight working set — leaking gens", res.peakGens)
	}
	if res.peakRetained > smallWorkingSet {
		t.Fatalf("sender retained set %d is far above the in-flight working set — leaking retained gens", res.peakRetained)
	}
}

// TestDeadlineCliffControl is the contrast that proves the cliff is specifically about
// one-way-delay > budget, not the harness: with the SAME loss but a one-way delay comfortably
// UNDER the budget, proactive repair recovers and delivery is high.
func TestDeadlineCliffControl(t *testing.T) {
	const budget = 200_000
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: budget}
	res := simLink{
		cfg:       cfg,
		owdMicros: 80_000, // 80 ms one-way < 200 ms budget: recovery fits
		srcMicros: 1_000,
		n:         320,
		drop:      uniformDrop(0xC11FF, 0.10),
	}.run()
	if res.lateDeliv {
		t.Fatal("a symbol was delivered past its deadline")
	}
	if frac := float64(res.delivered) / float64(res.n); frac < 0.90 {
		t.Fatalf("one-way under budget should deliver almost everything, got %.1f%% (%d/%d)", 100*frac, res.delivered, res.n)
	}
}

// TestDeadlineCliffShedsOverhead guards budget-aware repair shedding. Once the sender's RTT
// estimate implies the one-way delay exceeds the budget, NOTHING it sends can be delivered —
// so spending proactive + reactive repair on that flow is pure waste (the bench shows 47–180%
// overhead at 0% delivery). When the path RTT is configured, the sender can know this before
// startup and should shed repair toward zero instead of compounding the cliff.
func TestDeadlineCliffShedsOverhead(t *testing.T) {
	const budget = 200_000
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: budget, NominalRTTMicros: 600_000}
	res := simLink{
		cfg:       cfg,
		owdMicros: 300_000,
		srcMicros: 1_000,
		n:         2000,
		drop:      uniformDrop(0xC11FF, 0.10),
	}.run()
	t.Logf("unsatisfiable-budget overhead = %.0f%% at %d/%d delivered (cliff=%v, want ~0%%)",
		100*res.overhead(), res.delivered, res.n, res.finalCliff)
	if res.overhead() > 0.05 {
		t.Fatalf("sender spent %.0f%% repair overhead on an undeliverable flow", 100*res.overhead())
	}
}
