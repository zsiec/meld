package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
)

// Allocation gates for the flow warm path. These measure per-symbol warm-path allocation by
// differencing two stream lengths, canceling fixed per-run construction cost and isolating
// steady-state behavior.

// captureSymbols runs a sender over n source writes with no loss and returns every datagram it
// emitted, so the receiver-side gate can replay a real wire stream.
func captureSymbols(cfg Config, n int) [][]byte {
	s := NewSender(cfg)
	now := clock.Timestamp(0)
	var out [][]byte
	flush := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				return
			}
			out = append(out, append([]byte(nil), d...))
		}
	}
	for i := 0; i < n; i++ {
		s.Write(now, simChunk(cfg.SymbolSize, uint32(i)))
		now = now.Add(1_000)
		s.Tick(now)
		flush()
	}
	s.Flush(now)
	flush()
	return out
}

func allocCfg() Config {
	return Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: 200_000}
}

// TestSenderWriteAllocPerSymbol gates the encode path. The sender allocates a symbol-sized
// buffer per source symbol (code.Encoder.Add) plus the encoded datagram; this asserts the
// per-symbol allocation count is bounded and, by differencing, free of hidden per-symbol
// growth.
func TestSenderWriteAllocPerSymbol(t *testing.T) {
	cfg := allocCfg()
	write := func(n int) float64 {
		return testing.AllocsPerRun(20, func() {
			s := NewSender(cfg)
			now := clock.Timestamp(0)
			for i := 0; i < n; i++ {
				s.Write(now, simChunk(cfg.SymbolSize, uint32(i)))
				now = now.Add(1_000)
				for {
					if _, ok := s.PollSend(); !ok {
						break
					}
				}
			}
		})
	}
	a1 := write(cfg.GenSize * 4)
	a2 := write(cfg.GenSize * 8)
	perSym := (a2 - a1) / float64(cfg.GenSize*4)
	t.Logf("sender encode: %.1f allocs over 4 gens, %.1f over 8 gens ⇒ %.2f allocs/symbol", a1, a2, perSym)
	if perSym > 8 {
		t.Fatalf("sender allocates %.2f/symbol on the warm path (gate: <= 8)", perSym)
	}
}

// TestReceiverFeedAllocPerSymbol gates the decode path. FeedSymbol decodes the datagram and,
// on delivery, copies the payload; this asserts the per-datagram warm-path allocation is
// bounded and free of hidden growth.
func TestReceiverFeedAllocPerSymbol(t *testing.T) {
	cfg := allocCfg()
	data := captureSymbols(cfg, cfg.GenSize*8)
	feed := func(m int) float64 {
		return testing.AllocsPerRun(20, func() {
			r := NewReceiver(cfg)
			now := clock.Timestamp(0)
			for i := 0; i < m; i++ {
				r.FeedSymbol(now, data[i])
				now = now.Add(500)
				r.Tick(now)
				for {
					if _, _, ok := r.PollDeliver(); !ok {
						break
					}
				}
			}
		})
	}
	half := len(data) / 2
	a1 := feed(half)
	a2 := feed(len(data))
	perDatagram := (a2 - a1) / float64(len(data)-half)
	t.Logf("receiver decode: %.1f allocs over %d datagrams, %.1f over %d ⇒ %.2f allocs/datagram",
		a1, half, a2, len(data), perDatagram)
	if perDatagram > 10 {
		t.Fatalf("receiver allocates %.2f/datagram on the warm path (gate: <= 10)", perDatagram)
	}
}
