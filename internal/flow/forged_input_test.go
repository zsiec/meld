package flow

import (
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestSlidingReceiverRejectsForgedFarIndex pins the fix for the band-decoder DoS: a single
// forged sliding-profile datagram whose source id sits ~2^32 beyond the cursor used to make
// BandDecoder.grow() advance the frontier one id at a time across the whole gap — ~4.29
// billion iterations, a multi-second one-packet receiver hang. The receiver must now reject it
// in O(1) via the admission horizon. The timeout guard fails loudly on a regression.
func TestSlidingReceiverRejectsForgedFarIndex(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 64, GenSize: 16, Sliding: true, CodingWindow: 32, BufferMicros: 200_000}
	r := NewSlidingReceiver(cfg)
	feed := func(s wire.Symbol) {
		t.Helper()
		done := make(chan struct{})
		go func() { r.FeedSymbol(0, wire.EncodeSymbol(nil, s)); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("FeedSymbol hung on a forged far id — band-decoder grow() DoS regressed")
		}
	}
	// Forged systematic ~2^32 beyond the cursor.
	feed(wire.Symbol{Flow: 1, Kind: wire.Systematic, WindowBase: 0xFFFFFFF0, SrcIndex: 0xFFFFFFF0, N: 1, Deadline: 1000, Payload: make([]byte, 64)})
	// Forged repair with a far window base.
	feed(wire.Symbol{Flow: 1, Kind: wire.Repair, WindowBase: 0xFFFFFF00, N: 8, RepairKey: 3, Deadline: 1000, Payload: make([]byte, 64)})
	// Forged repair whose window WRAPS the id space.
	feed(wire.Symbol{Flow: 1, Kind: wire.Repair, WindowBase: 0xFFFFFFFE, N: 8, RepairKey: 4, Deadline: 1000, Payload: make([]byte, 64)})

	if r.Stats().Rejected < 2 {
		t.Fatalf("forged far ids were not rejected (Rejected=%d)", r.Stats().Rejected)
	}
	if c := r.dec.Cursor(); c != 0 {
		t.Fatalf("forged id blew out the frontier: cursor advanced to %d", c)
	}
	// A subsequent honest symbol still decodes (the receiver is not wedged).
	r.FeedSymbol(0, wire.EncodeSymbol(nil, wire.Symbol{Flow: 1, Kind: wire.Systematic, SrcIndex: 0, N: 1, Deadline: 1000, Payload: make([]byte, 64)}))
	if _, _, ok := r.PollDeliver(); !ok {
		t.Fatal("receiver wedged: an honest symbol after the forgeries did not deliver")
	}
}

// TestClampDeadlineBounds is a unit check on the deadline clamp that backs the overflow fix.
func TestClampDeadlineBounds(t *testing.T) {
	now := clock.Timestamp(1_000_000)
	const budget = 200_000
	// An honest deadline (≈ now + budget) passes through unchanged.
	if got := clampDeadline(now, now.Add(budget), budget); got != now.Add(budget) {
		t.Fatalf("honest deadline was clamped: %d", got)
	}
	// A forged far-future deadline is bounded.
	if got := clampDeadline(now, clock.Timestamp(1)<<61, budget); got.Sub(now) > 16*budget {
		t.Fatalf("far-future deadline not clamped: %d (now+%d)", got, got.Sub(now))
	}
	// A forged far-past deadline is bounded.
	if got := clampDeadline(now, clock.Timestamp(-(int64(1) << 61)), budget); now.Sub(got) > 16*budget {
		t.Fatalf("far-past deadline not clamped: %d", got)
	}
}

// TestForgedDeadlineNoOverflow pins the deadline-extrapolation overflow fix: forged Deadline
// stamps used to ratchet the inter-symbol-interval EWMA unbounded, so (id-refID)*intervalUs
// overflowed int64 in symDeadline and produced a wrapped (e.g. negative/past) deadline — which
// breaks the "nothing past deadline" / "completeness" invariants by dropping in-time symbols or
// delivering late ones. With the deadline clamp + the intervalUs cap, the extrapolated deadline
// stays finite and sane.
func TestForgedDeadlineNoOverflow(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: 64, GenSize: 16, BufferMicros: 200_000}
	r := NewReceiver(cfg)
	now := clock.Timestamp(1_000_000)
	for i := uint32(0); i < 6; i++ {
		s := wire.Symbol{
			Flow: 1, Kind: wire.Systematic, WindowBase: genBaseOf(i, 16), SrcIndex: i, N: 16,
			Deadline: int64(1) << 61, Payload: make([]byte, 64),
		} // absurd forged deadline
		r.FeedSymbol(now, wire.EncodeSymbol(nil, s))
		now = now.Add(1000)
	}
	if r.intervalUs > maxIntervalMicros {
		t.Fatalf("intervalUs ratcheted past the cap: %d", r.intervalUs)
	}
	dl, ok := r.symDeadline(5)
	if ok && (dl < 0 || dl.Sub(now) > 100*cfg.BufferMicros) {
		t.Fatalf("symDeadline overflowed/wrapped: %d at now=%d", dl, now)
	}
}
