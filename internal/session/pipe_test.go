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

// pipeAddr is a fake datagram address for the in-memory substrate.
type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

// pipeSubstrate is an in-memory, point-to-point datagram pipe implementing Substrate, so a
// session host can be driven without a real socket. It is the proof the seam works: the
// host pumps the SAME coded flow over UDP or this fake without changing a line. WriteTo
// ignores the address (point-to-point) and drops when the peer's buffer is full (UDP-like
// rather than blocking the host); the test sizes the buffer so no drop occurs.
type pipeSubstrate struct {
	local pipeAddr
	in    chan []byte
	peer  *pipeSubstrate
	done  chan struct{}
	once  sync.Once
}

// newPipe returns two cross-linked endpoints of an in-memory datagram pipe.
func newPipe(aName, bName string, depth int) (*pipeSubstrate, *pipeSubstrate) {
	a := &pipeSubstrate{local: pipeAddr(aName), in: make(chan []byte, depth), done: make(chan struct{})}
	b := &pipeSubstrate{local: pipeAddr(bName), in: make(chan []byte, depth), done: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (p *pipeSubstrate) ReadFrom(buf []byte) (int, net.Addr, error) {
	select {
	case d := <-p.in:
		return copy(buf, d), p.peer.local, nil
	case <-p.done:
		return 0, nil, net.ErrClosed
	}
}

func (p *pipeSubstrate) WriteTo(b []byte, _ net.Addr) (int, error) {
	d := append([]byte(nil), b...)
	select {
	case <-p.done:
		return 0, net.ErrClosed
	case p.peer.in <- d:
		return len(b), nil
	default:
		return len(b), nil // peer buffer full: drop, like UDP
	}
}

func (p *pipeSubstrate) LocalAddr() net.Addr { return p.local }

func (p *pipeSubstrate) Close() error {
	p.once.Do(func() { close(p.done) })
	return nil
}

// TestSeamOverPipe drives the real session hosts over the in-memory pipe substrate — no
// sockets — and confirms a media stream is delivered byte-exact in order. This is the
// substrate seam's load-bearing test: the host and the coded core are unchanged; only the
// datagram service underneath them is swapped, exactly as a QUIC-datagram adapter would.
func TestSeamOverPipe(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 256, GenSize: 16, Redundancy: 0.25, BufferMicros: 200_000}
	txEnd, rxEnd := newPipe("tx", "rx", 1<<16)
	rx, err := newReceiver(rxEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer rx.Close()
	tx, _ := newSender(txEnd, cfg, clock.NewRealClock(), nil)
	defer tx.Close()

	// The coder's unit is a fixed-size symbol, so each chunk is exactly SymbolSize bytes
	// with a per-chunk pattern; delivery must be byte-exact and in order.
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
			t.Fatalf("read %d: %v (delivered %d/%d over the pipe substrate)", i, err, i, n)
		}
		if !bytes.Equal(buf[:m], want[i]) {
			t.Fatalf("chunk %d mismatch over the pipe substrate", i)
		}
	}
	st := rx.Stats()
	if st.Delivered < n {
		t.Fatalf("delivered %d, want >= %d", st.Delivered, n)
	}
}
