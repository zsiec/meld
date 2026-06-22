package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestCCBacksOffOnECN: at a steady RTT, a flow that the network CE-marks settles to a
// strictly lower rate budget than an unmarked flow — the L4S/DCTCP multiplicative decrease
// layered on Copa. With no marks the controller is unchanged (pure delay-based Copa).
func TestCCBacksOffOnECN(t *testing.T) {
	const mss = 1316 + 29
	drive := func(ceFrac float64) int64 {
		cc := newCongestionController(ccDefaultDelta, mss, 100_000_000)
		now := clock.Timestamp(0)
		const rtt = 20_000 // 20 ms, stable (no queue feedback in this unit test)
		var rate int64
		for i := 0; i < 600; i++ {
			cc.onSample(now, rtt, ceFrac)
			rate = cc.rateBudgetBytesPerSec()
			now = now.Add(5_000) // 5 ms feedback cadence
		}
		return rate
	}
	noMark := drive(0)
	marked := drive(0.2) // 20% of symbols CE-marked
	t.Logf("rate budget: no-mark %d B/s vs 20%%-marked %d B/s", noMark, marked)
	if noMark <= 0 {
		t.Fatal("no-mark run produced no budget")
	}
	if marked >= noMark {
		t.Fatalf("ECN did not reduce the rate: 20%%-marked %d >= no-mark %d B/s", marked, noMark)
	}
	// A heavier mark fraction backs off harder still.
	if heavier := drive(0.5); heavier >= marked {
		t.Fatalf("heavier marking did not reduce further: 50%%-marked %d >= 20%%-marked %d", heavier, marked)
	}
}

// TestReceiverReportsECN: the receiver echoes the CE-marked fraction of admitted symbols in
// Feedback.EcnCE (parts per 65535) — the wire half of the L4S loop. All-marked reports ≈ full
// scale; unmarked reports zero.
func TestReceiverReportsECN(t *testing.T) {
	cfg := Config{Flow: 3, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 200_000}

	// feed streams one generation of systematic symbols into a fresh receiver, marking each
	// with the given codepoint, then forces a feedback report and returns its EcnCE.
	feed := func(ecn ECN) uint16 {
		s := NewSender(cfg)
		r := NewReceiver(cfg)
		now := clock.Timestamp(0)
		for i := 0; i < cfg.GenSize; i++ {
			s.Write(now, makeChunkN(uint32(i)))
		}
		s.Flush(now)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			if sym, err := wire.DecodeSymbol(d); err == nil && sym.Kind == wire.Systematic {
				r.FeedSymbolECN(now, d, ecn)
			}
		}
		now = now.Add(feedbackIntervalMicros + 1)
		r.Tick(now)
		var last wire.Feedback
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				last = f
			}
		}
		return last.EcnCE
	}

	if got := feed(NotECT); got != 0 {
		t.Fatalf("unmarked stream reported EcnCE %d, want 0", got)
	}
	got := feed(CE)
	if got < 60000 { // every admitted symbol marked ⇒ near full-scale 65535
		t.Fatalf("all-CE stream reported EcnCE %d, want ≈65535", got)
	}
}
