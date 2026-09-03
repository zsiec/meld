package session

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/flow"
)

func mpTestCfg() flow.Config {
	c := testCfg()
	c.Paths = 2
	c.Redundancy = 0.15
	c.BufferMicros = 300_000 // generous budget so the per-path estimate can ramp + reactive-repair
	return c
}

func TestMultipathRejectsSlidingProfile(t *testing.T) {
	cfg := mpTestCfg()
	cfg.Sliding = true
	if _, err := NewMultipathSender(nil, cfg, nil); !errors.Is(err, ErrSlidingMultipathUnsupported) {
		t.Fatalf("NewMultipathSender sliding error = %v, want ErrSlidingMultipathUnsupported", err)
	}
	if _, err := newMultipathReceiver(nil, cfg, clock.NewRealClock(), nil, nil); !errors.Is(err, ErrSlidingMultipathUnsupported) {
		t.Fatalf("newMultipathReceiver sliding error = %v, want ErrSlidingMultipathUnsupported", err)
	}
}

func TestMultipathRejectsPathCountMismatch(t *testing.T) {
	cfg := mpTestCfg()
	cfg.Paths = 1
	if _, err := NewMultipathSender([]string{"127.0.0.1:1", "127.0.0.1:2"}, cfg, nil); !errors.Is(err, ErrPathCountMismatch) {
		t.Fatalf("NewMultipathSender mismatch error = %v, want ErrPathCountMismatch", err)
	}
	a, b := newPipe("a", "b", 1)
	if _, err := newMultipathReceiver([]Substrate{a, b}, cfg, clock.NewRealClock(), nil, nil); !errors.Is(err, ErrPathCountMismatch) {
		t.Fatalf("newMultipathReceiver mismatch error = %v, want ErrPathCountMismatch", err)
	}
}

func TestMultipathCleartextFeedbackDuplicateWindow(t *testing.T) {
	s := &MultipathSender{}
	now := clock.Timestamp(10_000)
	fb := []byte{1, 2, 3, 4}
	s.rememberCleartextFeedback(now, fb)
	if !s.duplicateCleartextFeedback(now.Add(feedbackDuplicateWindowMicros-1), append([]byte(nil), fb...)) {
		t.Fatal("identical cleartext feedback inside the interval was not treated as duplicate")
	}
	if s.duplicateCleartextFeedback(now.Add(feedbackDuplicateWindowMicros+1), fb) {
		t.Fatal("identical feedback after the interval must remain processable as fresh state")
	}
	if s.duplicateCleartextFeedback(now.Add(1), []byte{4, 3, 2, 1}) {
		t.Fatal("different feedback must not be treated as duplicate")
	}
}

func TestMultipathPMTUDDiscoversEachPath(t *testing.T) {
	cfg := mpTestCfg()
	cfg.ProbeMTU = true
	cfg.MaxProbeMTU = 1280
	rx, err := NewMultipathReceiver([]string{"127.0.0.1:0", "127.0.0.1:0"}, cfg, nil)
	if err != nil {
		t.Fatalf("NewMultipathReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()

	tx, err := NewMultipathSender(rx.LocalAddrs(), cfg, nil)
	if err != nil {
		t.Fatalf("NewMultipathSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mtus := tx.PathMTUs()
		if len(mtus) == 2 && mtus[0] == cfg.MaxProbeMTU && mtus[1] == cfg.MaxProbeMTU && tx.PathMTU() == cfg.MaxProbeMTU {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("multipath PMTUD did not discover both paths: mtus=%v min=%d", tx.PathMTUs(), tx.PathMTU())
}

// streamAndCollect writes n id-stamped chunks through tx at ~1 ms cadence and reads
// delivered chunks off rx until n arrive or the deadline passes. Returns the set of
// delivered ids (which must be a strict in-order prefix) and any out-of-order/byte
// mismatch flag.
func streamAndCollect(t *testing.T, tx multipathWriter, rx *MultipathReceiver, n, sym int) (got map[uint32]bool, ordered, byteExact bool) {
	t.Helper()
	got = map[uint32]bool{}
	ordered, byteExact = true, true
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			msg := make([]byte, sym)
			binary.BigEndian.PutUint32(msg, uint32(i))
			rand.New(rand.NewSource(int64(i))).Read(msg[4:])
			if _, err := tx.Write(msg); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			time.Sleep(time.Millisecond)
		}
		tx.Flush()
	}()

	var lastID int64 = -1
	deadline := time.Now().Add(8 * time.Second)
	buf := make([]byte, sym)
	for len(got) < n && time.Now().Before(deadline) {
		if err := rx.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		nn, err := rx.Read(buf)
		if err != nil || nn < 4 {
			continue
		}
		id := binary.BigEndian.Uint32(buf[:4])
		if int64(id) <= lastID {
			ordered = false
		}
		lastID = int64(id)
		want := make([]byte, sym)
		binary.BigEndian.PutUint32(want, id)
		rand.New(rand.NewSource(int64(id))).Read(want[4:])
		for k := 0; k < sym; k++ {
			if buf[k] != want[k] {
				byteExact = false
				break
			}
		}
		got[id] = true
	}
	wg.Wait()
	return got, ordered, byteExact
}

type multipathWriter interface {
	Write([]byte) (int, error)
	Flush()
}

// TestMultipathCleanDelivery: over two real UDP sockets with no loss, a 2-path flow
// delivers every chunk byte-exact and in order, AND both path sockets actually carry
// traffic (the scheduler spread it and the host routed by PathID).
func TestMultipathCleanDelivery(t *testing.T) {
	cfg := mpTestCfg()
	var perPath [2]int
	var pmu sync.Mutex
	conns, err := bindAll([]string{"127.0.0.1:0", "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	rx, err := newMultipathReceiver(conns, cfg, clock.NewRealClock(), func(path int, _ []byte) bool {
		pmu.Lock()
		perPath[path]++
		pmu.Unlock()
		return false // count only, drop nothing
	}, nil)
	if err != nil {
		t.Fatalf("newMultipathReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()

	tx, err := NewMultipathSender(rx.LocalAddrs(), cfg, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = tx.Close() }()

	const n = 500
	got, ordered, byteExact := streamAndCollect(t, tx, rx, n, cfg.SymbolSize)
	if !ordered {
		t.Fatal("delivery not in order")
	}
	if !byteExact {
		t.Fatal("a delivered chunk did not match what was sent")
	}
	if len(got) != n {
		t.Fatalf("delivered %d/%d over a clean 2-path link", len(got), n)
	}
	pmu.Lock()
	defer pmu.Unlock()
	if perPath[0] == 0 || perPath[1] == 0 {
		t.Fatalf("traffic not spread across both paths: %v", perPath)
	}
	t.Logf("clean 2-path: delivered %d/%d, symbols per path %v", len(got), n, perPath)
}

// TestMultipathSurvivesBadPath: with one path dropping ~70% of its symbols, the 2-path
// flow still delivers essentially everything — the coding recovers the bad path's
// losses from repair the scheduler steers onto the healthy path (diversity, not the N×
// cost of duplication). Demonstrates the host + core composing under a real lossy path.
func TestMultipathSurvivesBadPath(t *testing.T) {
	cfg := mpTestCfg()
	// Path 1 is badly lossy (70%); path 0 is clean. Deterministic per-datagram coin.
	var n1 uint64
	var cmu sync.Mutex
	rng := rand.New(rand.NewSource(1))
	conns, err := bindAll([]string{"127.0.0.1:0", "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	rx, err := newMultipathReceiver(conns, cfg, clock.NewRealClock(), func(path int, _ []byte) bool {
		if path != 1 {
			return false
		}
		cmu.Lock()
		defer cmu.Unlock()
		n1++
		return rng.Float64() < 0.70
	}, nil)
	if err != nil {
		t.Fatalf("newMultipathReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()

	tx, err := NewMultipathSender(rx.LocalAddrs(), cfg, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = tx.Close() }()

	const n = 800
	got, ordered, byteExact := streamAndCollect(t, tx, rx, n, cfg.SymbolSize)
	if !ordered {
		t.Fatal("delivery not in order under a bad path")
	}
	if !byteExact {
		t.Fatal("a delivered chunk did not match what was sent")
	}
	// recvLoop keeps writing n1 (under cmu) until rx.Close(), so snapshot it under the lock before
	// reading — a bare read here races the loss-coin closure.
	cmu.Lock()
	seen1 := n1
	cmu.Unlock()
	// Allow a small residual for the ramp-up before the per-path estimate converges.
	if len(got) < n*97/100 {
		t.Fatalf("only %d/%d delivered with one 70%%-loss path (path-1 symbols seen=%d)", len(got), n, seen1)
	}
	st := rx.Stats()
	t.Logf("bad-path survival: delivered %d/%d (recovered=%d, path-1 symbols=%d)", len(got), n, st.Recovered, seen1)
}
