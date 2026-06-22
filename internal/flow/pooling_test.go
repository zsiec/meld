package flow

import (
	"runtime"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// drivePair streams n source chunks through s -> [10% i.i.d. loss] -> r with feedback looped
// back, on a fixed clock (no propagation — this exercises the coders, not the deadline). It is
// the steady-state allocation harness: generations close, retire, and recycle their buffers.
func drivePair(s *Sender, r *Receiver, symSize, n int) {
	drop := uniformDrop(0xA110C, 0.10)
	now := clock.Timestamp(0)
	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			if sym, err := wire.DecodeSymbol(d); err == nil && !drop(sym) {
				r.FeedSymbol(now, d)
			}
		}
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
		for {
			if _, _, ok := r.PollDeliver(); !ok {
				break
			}
		}
	}
	for i := 0; i < n; i++ {
		s.Write(now, simChunk(symSize, uint32(i)))
		pump()
		now = now.Add(500)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
}

// TestBufferPoolingReducesAllocation pins the symbol-buffer pooling win: recycling the per-
// symbol payload buffers across generations (code.Pool) materially cuts steady-state
// allocation versus allocating fresh each generation. Measured as bytes allocated per source
// symbol over a real send→receive→feedback loop; the pool must save a clear margin.
func TestBufferPoolingReducesAllocation(t *testing.T) {
	const (
		symSize = 1316
		n       = 4000
	)
	cfg := Config{Flow: 1, SymbolSize: symSize, GenSize: 32, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 200_000}
	measure := func(disablePool bool) float64 {
		run := func() {
			s, r := NewSender(cfg), NewReceiver(cfg)
			if disablePool {
				s.pool, r.pool = nil, nil
				s.live.SetPool(nil)
			}
			drivePair(s, r, symSize, n)
		}
		run() // warm
		var a, b runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&a)
		run()
		runtime.ReadMemStats(&b)
		return float64(b.TotalAlloc-a.TotalAlloc) / float64(n)
	}
	pooled, unpooled := measure(false), measure(true)
	t.Logf("steady-state allocation: pooled=%.0f B/symbol (%.1f× payload), unpooled=%.0f B/symbol (%.1f×) — saved %.0f%%",
		pooled, pooled/symSize, unpooled, unpooled/symSize, 100*(1-pooled/unpooled))
	if pooled > 0.75*unpooled {
		t.Fatalf("pooling saved too little: %.0f vs %.0f B/symbol — recycling regressed", pooled, unpooled)
	}
}
