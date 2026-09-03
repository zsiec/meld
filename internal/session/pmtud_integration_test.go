package session

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/flow"
)

// mtuPipe is an in-memory datagram pipe whose forward direction silently DROPS any
// datagram larger than maxSize — a size-based path black hole, the thing DPLPMTUD must
// discover. maxSize is atomic so a test can shrink/grow the path mid-run.
type mtuPipe struct {
	local   pipeAddr
	in      chan []byte
	peer    *mtuPipe
	maxSize atomic.Int32 // forward-direction MTU; datagrams above it are dropped (0 = unlimited)
	done    chan struct{}
	once    sync.Once
}

func newMTUPipe(aName, bName string, depth int) (*mtuPipe, *mtuPipe) {
	a := &mtuPipe{local: pipeAddr(aName), in: make(chan []byte, depth), done: make(chan struct{})}
	b := &mtuPipe{local: pipeAddr(bName), in: make(chan []byte, depth), done: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (p *mtuPipe) ReadFrom(buf []byte) (int, net.Addr, error) {
	select {
	case d := <-p.in:
		return copy(buf, d), p.peer.local, nil
	case <-p.done:
		return 0, nil, net.ErrClosed
	}
}

func (p *mtuPipe) WriteTo(b []byte, _ net.Addr) (int, error) {
	if m := p.maxSize.Load(); m > 0 && len(b) > int(m) {
		return len(b), nil // black hole: silently drop the oversized datagram
	}
	d := append([]byte(nil), b...)
	select {
	case <-p.done:
		return 0, net.ErrClosed
	case p.peer.in <- d:
		return len(b), nil
	default:
		return len(b), nil // peer buffer full: drop like UDP
	}
}

func (p *mtuPipe) LocalAddr() net.Addr { return p.local }
func (p *mtuPipe) Close() error        { p.once.Do(func() { close(p.done) }); return nil }

// fastProbeCfg drives the state machine quickly so the end-to-end test runs in well under a
// second of wall time (the unit tests already cover the slow-timer logic).
func fastProbeCfg() pmtudConfig {
	return pmtudConfig{
		Base: 1200, Max: 1500, Granularity: 8, MaxProbes: 3,
		ProbeTimeoutUs: 10_000, RaiseIntervalUs: 10_000_000, ConfirmEveryUs: 100_000,
	}
}

// waitFor polls cond up to d, returning whether it held. Used for the asynchronous,
// goroutine-driven probing loop (real clock).
func waitFor(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

// TestPMTUD_EndToEndDiscoveryAndBlackHole exercises the full wiring over the real
// Sender/Receiver goroutines + substrate: probes go out, the receiver acks them, the state
// machine discovers the path MTU, and when the path silently shrinks the confirmation probe
// trips a black hole and re-discovers the smaller MTU — the size loss that FEC and the CC
// are blind to is now both detected and reported.
func TestPMTUD_EndToEndDiscoveryAndBlackHole(t *testing.T) {
	cfg := flow.Config{
		Flow: 7, SymbolSize: 1316, GenSize: 16, Redundancy: 0.25, BufferMicros: 200_000,
		ProbeMTU: true, MaxProbeMTU: 1500,
	}
	txEnd, rxEnd := newMTUPipe("tx", "rx", 1<<12)
	txEnd.maxSize.Store(1500) // forward path carries up to 1500 to start

	rx, err := newReceiver(rxEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := newSenderCfg(txEnd, cfg, clock.NewRealClock(), nil, fastProbeCfg())
	if err != nil {
		t.Fatalf("newSenderCfg: %v", err)
	}
	defer func() { _ = tx.Close() }()

	// 1) Discover the full ceiling.
	if !waitFor(2*time.Second, func() bool { return tx.PathMTU() == 1500 }) {
		t.Fatalf("did not discover PLPMTU 1500 end to end; got %d", tx.PathMTU())
	}
	t.Logf("discovered PLPMTU = %d over the real probe/ack loop", tx.PathMTU())

	// 2) The path silently shrinks to 1300 (a black hole for the 1500 it was using).
	const after = 1300
	txEnd.maxSize.Store(after)
	ok := waitFor(3*time.Second, func() bool {
		m := tx.PathMTU()
		return tx.PathMTUBlackHoles() >= 1 && m <= after && m >= after-probeGran
	})
	if !ok {
		t.Fatalf("did not detect black hole + re-discover ≈%d; PLPMTU=%d blackHoles=%d",
			after, tx.PathMTU(), tx.PathMTUBlackHoles())
	}
	t.Logf("black hole detected (%d events); re-discovered PLPMTU = %d", tx.PathMTUBlackHoles(), tx.PathMTU())
}

// probeGran mirrors the test probe granularity for the bound check above.
const probeGran = 8
