package session

import (
	"bytes"
	"errors"
	"io"
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

type shortWriteSubstrate struct {
	*pipeSubstrate
}

func (s shortWriteSubstrate) WriteTo(b []byte, addr net.Addr) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	return len(b) - 1, nil
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
	defer func() { _ = rx.Close() }()
	tx, _ := newSender(txEnd, cfg, clock.NewRealClock(), nil)
	defer func() { _ = tx.Close() }()

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

	if err := rx.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
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

func TestSeamOverPipeLargeSymbolNotTruncated(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 4096, GenSize: 1, Redundancy: 0, BufferMicros: 200_000}
	txEnd, rxEnd := newPipe("tx", "rx", 16)
	rx, err := newReceiver(rxEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := newSender(txEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	want := make([]byte, cfg.SymbolSize)
	for i := range want {
		want[i] = byte(i)
	}
	if _, err := tx.Write(want); err != nil {
		t.Fatalf("write: %v", err)
	}
	tx.Flush()

	if err := rx.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	got := make([]byte, cfg.SymbolSize)
	n, err := rx.Read(got)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got[:n], want) {
		t.Fatalf("large symbol truncated or corrupted: got %d bytes", n)
	}
}

func TestCleartextOversizedWriteRejected(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 64, GenSize: 4, Redundancy: 0.25, BufferMicros: 200_000}
	txEnd, _ := newPipe("tx", "rx", 16)
	tx, err := newSender(txEnd, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	tooLarge := make([]byte, cfg.SymbolSize+1)
	for name, write := range map[string]func([]byte) (int, error){
		"Write":      tx.Write,
		"WriteUnit":  func(p []byte) (int, error) { return tx.WriteUnit(p, 2) },
		"WriteFrame": func(p []byte) (int, error) { return tx.WriteFrame(p, flow.FrameDesc{Priority: 2}) },
	} {
		n, err := write(tooLarge)
		if !errors.Is(err, ErrChunkTooLarge) {
			t.Fatalf("%s oversized error = %v, want ErrChunkTooLarge", name, err)
		}
		if n != 0 {
			t.Fatalf("%s oversized wrote %d bytes, want 0", name, n)
		}
	}
}

func TestNewOverRejectsNilSubstrate(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 64, GenSize: 4, Redundancy: 0.25, BufferMicros: 200_000}
	if _, err := newSender(nil, cfg, clock.NewRealClock(), nil); !errors.Is(err, ErrNilSubstrate) {
		t.Fatalf("newSender(nil) error = %v, want ErrNilSubstrate", err)
	}
	if _, err := newReceiver(nil, cfg, clock.NewRealClock(), nil); !errors.Is(err, ErrNilSubstrate) {
		t.Fatalf("newReceiver(nil) error = %v, want ErrNilSubstrate", err)
	}
	var typedNil *pipeSubstrate
	if _, err := newSender(typedNil, cfg, clock.NewRealClock(), nil); !errors.Is(err, ErrNilSubstrate) {
		t.Fatalf("newSender(typed nil) error = %v, want ErrNilSubstrate", err)
	}
}

func TestShortWriteSubstrateFailsWrite(t *testing.T) {
	cfg := flow.Config{Flow: 7, SymbolSize: 64, GenSize: 4, Redundancy: 0, BufferMicros: 200_000}
	txEnd, _ := newPipe("tx", "rx", 16)
	tx, err := newSender(shortWriteSubstrate{txEnd}, cfg, clock.NewRealClock(), nil)
	if err != nil {
		t.Fatalf("newSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	payload := make([]byte, cfg.SymbolSize)
	n, err := tx.Write(payload)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Write error = %v, want io.ErrShortWrite", err)
	}
	if n != len(payload) {
		t.Fatalf("Write returned n=%d, want %d", n, cfg.SymbolSize)
	}
}
