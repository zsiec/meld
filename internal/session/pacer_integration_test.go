package session

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/flow"
)

// TestPacerSeamOverPipe is TestSeamOverPipe with the host pacer ENABLED: it proves the
// paced transmit path delivers a media stream byte-exact and in order end to end (the
// goroutine pacer, backpressure, and flush-on-close all on the real-clock pipe).
func TestPacerSeamOverPipe(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 256, GenSize: 16, Redundancy: 0.25, BufferMicros: 200_000, Pace: true}
	txEnd, rxEnd := newPipe("tx", "rx", 1<<16)
	rx, err := newReceiver(rxEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer rx.Close()
	tx, _ := newSender(txEnd, cfg, clock.NewRealClock(), nil)
	defer tx.Close()

	const n = 200
	want := make([][]byte, n)
	for i := 0; i < n; i++ {
		chunk := make([]byte, cfg.SymbolSize)
		for j := range chunk {
			chunk[j] = byte(i*31 + j)
		}
		want[i] = chunk
		if _, err := tx.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	tx.Flush()

	rx.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, cfg.SymbolSize)
	for i := 0; i < n; i++ {
		m, err := rx.Read(buf)
		if err != nil {
			t.Fatalf("read %d: %v (delivered %d/%d, paced)", i, err, i, n)
		}
		if !bytes.Equal(buf[:m], want[i]) {
			t.Fatalf("chunk %d mismatch (paced)", i)
		}
	}
	if st := rx.Stats(); st.Delivered < n {
		t.Fatalf("delivered %d, want >= %d", st.Delivered, n)
	}
}

// sendRec is one timestamped wire send captured by captureSubstrate.
type sendRec struct {
	at   time.Duration
	size int
}

// captureSubstrate records the time and size of every WriteTo (the wire emission schedule)
// and otherwise discards. ReadFrom blocks until close (no inbound feedback), so the sender
// runs on its static budget — exactly the open-loop case where pacing's effect on the
// emission schedule is isolated.
type captureSubstrate struct {
	mu    sync.Mutex
	base  time.Time
	sends []sendRec
	done  chan struct{}
	once  sync.Once
}

func newCaptureSubstrate() *captureSubstrate {
	return &captureSubstrate{base: time.Now(), done: make(chan struct{})}
}

func (c *captureSubstrate) ReadFrom(buf []byte) (int, net.Addr, error) {
	<-c.done
	return 0, nil, net.ErrClosed
}

func (c *captureSubstrate) WriteTo(b []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	c.sends = append(c.sends, sendRec{at: time.Since(c.base), size: len(b)})
	c.mu.Unlock()
	return len(b), nil
}

func (c *captureSubstrate) LocalAddr() net.Addr { return pipeAddr("capture") }
func (c *captureSubstrate) Close() error        { c.once.Do(func() { close(c.done) }); return nil }

// maxInWindow returns the most datagrams emitted in any window of w (the microburst peak).
func (c *captureSubstrate) maxInWindow(w time.Duration) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) == 0 {
		return 0
	}
	best := 0
	for i := range c.sends {
		count := 0
		for j := i; j < len(c.sends) && c.sends[j].at-c.sends[i].at < w; j++ {
			count++
		}
		if count > best {
			best = count
		}
	}
	return best
}

func (c *captureSubstrate) span() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sends) == 0 {
		return 0
	}
	return c.sends[len(c.sends)-1].at - c.sends[0].at
}

// TestPacerSmoothsBurstAtSession is the session-level analogue of the probe's Exp1: an
// encoder dumps a large burst of chunks; with the pacer OFF the host blasts them onto the
// wire as a microburst, with it ON they are spread at the budget. We measure the peak
// datagrams-per-5ms at the substrate for both and assert the pacer flattens it.
func TestPacerSmoothsBurstAtSession(t *testing.T) {
	run := func(pace bool) (peak int, span time.Duration) {
		cfg := flow.Config{
			Flow: 9, SymbolSize: 1316, GenSize: 16, Redundancy: 0.1,
			BufferMicros: 200_000, MaxBitrate: 10_000_000, // 10 Mbps budget ⇒ ~1.25 MB/s
			Pace: pace,
		}
		cap := newCaptureSubstrate()
		tx, _ := newSender(cap, cfg, clock.NewRealClock(), nil)
		// Dump a ~260 KB burst (200 chunks) as fast as the host accepts it. With the pacer
		// on, Write backpressures and this loop is paced; off, it returns immediately.
		const n = 200
		chunk := make([]byte, cfg.SymbolSize)
		for i := 0; i < n; i++ {
			if _, err := tx.Write(chunk); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		// Let the pacer drain the backlog at the budget before closing, so we measure
		// STEADY-STATE pacing, not Close()'s deliberate end-of-stream flush (which blasts the
		// residual tail to get the last repair out before its deadline).
		if pace {
			time.Sleep(500 * time.Millisecond)
		}
		peak, span = cap.maxInWindow(5*time.Millisecond), cap.span()
		tx.Close()
		return peak, span
	}

	offPeak, offSpan := run(false)
	onPeak, onSpan := run(true)
	t.Logf("pacer OFF: peak %d datagrams / 5ms, emitted over %v", offPeak, offSpan)
	t.Logf("pacer ON : peak %d datagrams / 5ms, emitted over %v", onPeak, onSpan)

	if onPeak >= offPeak {
		t.Errorf("pacer did not flatten the burst: on-peak %d >= off-peak %d per 5ms", onPeak, offPeak)
	}
	// At 1.25 MB/s a 5 ms window holds ≈ 4-5 MTU datagrams; allow the initial full-bucket
	// burst plus slack. The off case dumps the whole ~220-datagram burst near-instantly.
	if onPeak > 24 {
		t.Errorf("paced peak %d/5ms too high — not smoothed to the budget", onPeak)
	}
	if onSpan < 80*time.Millisecond {
		t.Errorf("paced emission span %v too short — burst not spread across the budget", onSpan)
	}
}
