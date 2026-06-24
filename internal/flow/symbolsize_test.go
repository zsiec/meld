package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestRejectSymbolSizeMismatch: a coded symbol whose payload length differs from the receiver's
// SymbolSize is rejected, not zero-padded/truncated into the GF math where it would silently
// corrupt the recovered bytes. SymbolSize is configured per-end and not negotiated, so a peer
// that disagrees must fail loud (a Rejected count, zero delivery) rather than corrupt — the same
// genBaseOf "both ends must agree" class as the epoch/path fixes. Covers both receiver profiles.
func TestRejectSymbolSizeMismatch(t *testing.T) {
	const sym = 256
	cfg := Config{Flow: 1, SymbolSize: sym, GenSize: 8, Redundancy: 0, BufferMicros: 100_000}
	mk := func(srcIndex uint32, payLen int) []byte {
		return wire.EncodeSymbol(nil, wire.Symbol{
			Flow: 1, Kind: wire.Systematic, WindowBase: srcIndex, SrcIndex: srcIndex, N: 8,
			Payload: make([]byte, payLen),
		})
	}

	t.Run("generation", func(t *testing.T) {
		r := NewReceiver(cfg)
		r.FeedSymbol(clock.Timestamp(0), mk(0, sym-8)) // too short
		r.FeedSymbol(clock.Timestamp(0), mk(1, sym+8)) // too long
		if got := r.Stats().Rejected; got != 2 {
			t.Fatalf("wrong-size symbols rejected=%d, want 2", got)
		}
		if _, _, ok := r.PollDeliver(); ok {
			t.Fatal("a wrong-size symbol must not be delivered")
		}
		r.FeedSymbol(clock.Timestamp(0), mk(0, sym)) // correct size: admitted
		if got := r.Stats().Rejected; got != 2 {
			t.Fatalf("a correct-size symbol must not be rejected (rejected=%d)", got)
		}
	})

	t.Run("sliding", func(t *testing.T) {
		scfg := cfg
		scfg.Sliding = true
		r := NewSlidingReceiver(scfg)
		r.FeedSymbol(clock.Timestamp(0), mk(0, sym-8))
		r.FeedSymbol(clock.Timestamp(0), mk(1, sym+8))
		if got := r.Stats().Rejected; got != 2 {
			t.Fatalf("wrong-size symbols rejected=%d, want 2", got)
		}
		if _, _, ok := r.PollDeliver(); ok {
			t.Fatal("a wrong-size symbol must not be delivered")
		}
		r.FeedSymbol(clock.Timestamp(0), mk(0, sym)) // correct size: admitted
		if got := r.Stats().Rejected; got != 2 {
			t.Fatalf("a correct-size symbol must not be rejected (rejected=%d)", got)
		}
	})
}
