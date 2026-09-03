package meld_test

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zsiec/meld"
)

const (
	e2eChunk = 1316
	e2eN     = 320 // chunks (multiple of GenSize), ~421 KB
)

// makeChunk encodes id in the first 4 bytes so a delivered chunk reveals its
// source id; the rest is seeded-random so byte-correctness is meaningful.
func makeChunk(rng *rand.Rand, id uint32) []byte {
	b := make([]byte, e2eChunk)
	binary.BigEndian.PutUint32(b, id)
	rng.Read(b[4:])
	return b
}

// streamSource writes n id-encoded chunks to tx, lightly paced, then flushes.
func streamSource(tx *meld.Sender, rng *rand.Rand, n int) (map[uint32][]byte, error) {
	src := make(map[uint32][]byte, n)
	for i := 0; i < n; i++ {
		c := makeChunk(rng, uint32(i))
		src[uint32(i)] = c
		if _, err := tx.Write(c); err != nil {
			return nil, err
		}
		if i%8 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	tx.Flush()
	return src, nil
}

// readStream reads delivered chunks until a read idles out (stream ended),
// returning the delivered ids in order and a map id->chunk.
func readStream(t *testing.T, rx interface {
	Read([]byte) (int, error)
	SetReadDeadline(time.Time) error
}, idle time.Duration,
) ([]uint32, map[uint32][]byte) {
	t.Helper()
	var ids []uint32
	byID := map[uint32][]byte{}
	buf := make([]byte, 4096)
	for {
		if err := rx.SetReadDeadline(time.Now().Add(idle)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		n, err := rx.Read(buf)
		if err != nil {
			break
		}
		if n >= 4 {
			id := binary.BigEndian.Uint32(buf[:4])
			ids = append(ids, id)
			byID[id] = append([]byte(nil), buf[:n]...)
		}
	}
	return ids, byID
}

func assertOrderedCorrect(t *testing.T, ids []uint32, byID, src map[uint32][]byte) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("delivery not strictly increasing at %d: %d then %d", i, ids[i-1], ids[i])
		}
	}
	for _, id := range ids {
		if !bytes.Equal(byID[id], src[id]) {
			t.Fatalf("delivered chunk %d does not match source", id)
		}
	}
}

func cleanConfig() meld.Config {
	cfg := meld.DefaultConfig()
	cfg.SymbolSize = e2eChunk
	cfg.GenSize = 16
	cfg.Redundancy = 0.25
	cfg.BufferMicros = 1_000_000 // 1 s
	return cfg
}

// TestE2ELoopbackClean: Sender -> Receiver over UDP loopback, no loss. Every
// chunk is delivered in order, byte-exact — the end-to-end pipeline works.
func TestE2ELoopbackClean(t *testing.T) {
	cfg := cleanConfig()
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := meld.NewSender(rx.LocalAddr(), cfg)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(1))
	writeDone := make(chan error, 1)
	go func() {
		_, err := streamSource(tx, rng, e2eN)
		writeDone <- err
	}()

	ids, byID := readStream(t, rx, 3*time.Second)
	if err := <-writeDone; err != nil {
		t.Fatalf("stream source: %v", err)
	}
	assertOrderedCorrect(t, ids, byID, mustSrc(rng, e2eN))
	if len(ids) != e2eN {
		st := rx.Stats()
		t.Fatalf("delivered %d/%d (lost=%d recovered=%d)", len(ids), e2eN, st.Lost, st.Recovered)
	}
}

// TestE2EHeavyLossSurvival: Sender -> 25%-loss proxy -> Receiver. Proactive RLNC
// repair recovers the bulk with ZERO retransmits; everything delivered is in
// order and byte-exact, and the residual loss is far below the raw 25%.
func TestE2EHeavyLossSurvival(t *testing.T) {
	cfg := cleanConfig()

	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()

	proxy := startLossyProxy(t, 0.25, 7, rx.LocalAddr())
	defer func() { _ = proxy.Close() }()

	tx, err := meld.NewSender(proxy.addr(), cfg)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(2))
	src, err := streamSourceCollect(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}

	ids, byID := readStream(t, rx, 4*time.Second)
	assertOrderedCorrect(t, ids, byID, src)

	st := rx.Stats()
	sst := tx.Stats()
	residual := float64(e2eN-len(ids)) / float64(e2eN)
	overhead := float64(sst.Repair) / float64(sst.Source)
	t.Logf("25%% loss: proxy dropped %d/%d, delivered %d/%d (%.1f%% residual), recovered %d, repair overhead %.0f%% (%d reactive), retransmits 0",
		proxy.dropped(), proxy.dropped()+proxy.forwarded(), len(ids), e2eN, residual*100, st.Recovered, overhead*100, sst.ReactiveRepair)

	if proxy.dropped() == 0 {
		t.Fatal("proxy dropped nothing — loss path not exercised")
	}
	if st.Recovered == 0 {
		t.Fatal("no symbols recovered — the coding path was not exercised")
	}
	if sst.ReactiveRepair == 0 {
		t.Log("no reactive repair sent; proactive/adaptive repair covered this loss draw before a deficit was reported")
	}
	// Coding recovers 25% loss byte-exact at LAN RTT; a small margin tolerates rare
	// variance at the deadline edge.
	if residual > 0.02 {
		t.Fatalf("residual loss %.1f%% too high (reactive repair should clear 25%% at LAN RTT)", residual*100)
	}
}

// mustSrc regenerates the deterministic source map for a seed (the writer above
// uses the same rng sequence). Used by the clean test, which needs source bytes
// to compare against but streams from a goroutine.
func mustSrc(_ *rand.Rand, n int) map[uint32][]byte {
	rng := rand.New(rand.NewSource(1))
	src := make(map[uint32][]byte, n)
	for i := 0; i < n; i++ {
		src[uint32(i)] = makeChunk(rng, uint32(i))
	}
	return src
}

// streamSourceCollect streams and returns the exact source map (for the loss
// test, which runs the writer inline so timing overlaps the reader).
func streamSourceCollect(tx *meld.Sender, rng *rand.Rand, n int) (map[uint32][]byte, error) {
	src := make(map[uint32][]byte, n)
	for i := 0; i < n; i++ {
		c := makeChunk(rng, uint32(i))
		src[uint32(i)] = c
		if _, err := tx.Write(c); err != nil {
			return nil, err
		}
		if i%8 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	tx.Flush()
	return src, nil
}

// streamSourcePaced streams n chunks at ~0.5 ms/symbol (1 ms every 2 chunks),
// modeling a real media cadence so the sliding band's recovery window spans many
// feedback round-trips. Returns the exact source map.
func streamSourcePaced(tx *meld.Sender, rng *rand.Rand, n int) (map[uint32][]byte, error) {
	src := make(map[uint32][]byte, n)
	for i := 0; i < n; i++ {
		c := makeChunk(rng, uint32(i))
		src[uint32(i)] = c
		if _, err := tx.Write(c); err != nil {
			return nil, err
		}
		if i%2 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	tx.Flush()
	return src, nil
}

// lossyProxy forwards sender->receiver datagrams with random loss and relays
// receiver->sender feedback losslessly, over real UDP.
type lossyProxy struct {
	conn    *net.UDPConn
	rxAddr  *net.UDPAddr
	sender  *net.UDPAddr
	rng     *rand.Rand
	lossPct float64
	drop    int64
	fwd     int64
}

func startLossyProxy(t *testing.T, lossPct float64, seed int64, rxAddr string) *lossyProxy {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	raddr, err := net.ResolveUDPAddr("udp", rxAddr)
	if err != nil {
		t.Fatalf("proxy resolve: %v", err)
	}
	p := &lossyProxy{conn: pc, rxAddr: raddr, rng: rand.New(rand.NewSource(seed)), lossPct: lossPct}
	go p.run()
	return p
}

func (p *lossyProxy) addr() string     { return p.conn.LocalAddr().String() }
func (p *lossyProxy) dropped() int64   { return atomic.LoadInt64(&p.drop) }
func (p *lossyProxy) forwarded() int64 { return atomic.LoadInt64(&p.fwd) }
func (p *lossyProxy) Close() error     { return p.conn.Close() }

func (p *lossyProxy) run() {
	buf := make([]byte, 2048)
	for {
		n, src, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if src.Port == p.rxAddr.Port && src.IP.Equal(p.rxAddr.IP) {
			if p.sender != nil { // feedback receiver -> sender, lossless
				_, _ = p.conn.WriteToUDP(buf[:n], p.sender)
			}
			continue
		}
		p.sender = src // symbol sender -> receiver
		if p.rng.Float64() < p.lossPct {
			atomic.AddInt64(&p.drop, 1)
			continue
		}
		atomic.AddInt64(&p.fwd, 1)
		_, _ = p.conn.WriteToUDP(buf[:n], p.rxAddr)
	}
}

// TestE2ESlidingHeavyLoss exercises the band-form sliding profile end-to-end over a
// 25%-loss proxy: continuous fungible repair must recover byte-exact in order.
func TestE2ESlidingHeavyLoss(t *testing.T) {
	cfg := cleanConfig()
	cfg.Sliding = true
	cfg.CodingWindow = 96
	// Floor at the adaptive set-point for 25% loss over a 96-symbol band
	// (repairForTarget(96, .25, 1e-3) ≈ 0.40), so the first band is proactively
	// protected before the loss estimate ramps; reactive still fires on variance.
	cfg.Redundancy = 0.4

	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	proxy := startLossyProxy(t, 0.25, 11, rx.LocalAddr())
	defer func() { _ = proxy.Close() }()
	tx, err := meld.NewSender(proxy.addr(), cfg)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(3))
	// Pace the write to ~0.5 ms/symbol: the 96-symbol band then spans ~48 ms,
	// comfortably more than the 20 ms feedback round-trip, so reactive repair lands
	// while a lost symbol is still inside the recovery window (a burst write would
	// overrun the band before feedback returns — that is a rate/window mismatch, not
	// a coding failure).
	src, err := streamSourcePaced(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}
	ids, byID := readStream(t, rx, 4*time.Second)
	assertOrderedCorrect(t, ids, byID, src)

	st := rx.Stats()
	residual := float64(e2eN-len(ids)) / float64(e2eN)
	t.Logf("sliding 25%% loss: delivered %d/%d (%.1f%% residual), recovered %d, dropped %d",
		len(ids), e2eN, residual*100, st.Recovered, proxy.dropped())
	if st.Recovered == 0 {
		t.Fatal("no symbols recovered — coding path not exercised")
	}
	if residual > 0.02 {
		t.Fatalf("residual %.1f%% too high for sliding profile at 25%% LAN loss", residual*100)
	}
}

// --- encryption (slice 3): the full stack end to end over real UDP ---

// e2eEncChunk is the largest chunk an encrypted Sender accepts: SymbolSize minus the
// 16-byte AEAD tag, so the sealed ciphertext is exactly SymbolSize (no coder padding).
const e2eEncChunk = e2eChunk - 16

func encConfig() meld.Config {
	cfg := cleanConfig()
	cfg.Passphrase = "studio-to-truck contribution passphrase"
	return cfg
}

func makeEncChunk(rng *rand.Rand, id uint32) []byte {
	b := make([]byte, e2eEncChunk)
	binary.BigEndian.PutUint32(b, id)
	rng.Read(b[4:])
	return b
}

// streamEncrypted writes n id-encoded (SymbolSize-16)-byte chunks and flushes.
func streamEncrypted(tx interface {
	Write([]byte) (int, error)
	Flush()
}, rng *rand.Rand, n int,
) (map[uint32][]byte, error) {
	src := make(map[uint32][]byte, n)
	for i := 0; i < n; i++ {
		c := makeEncChunk(rng, uint32(i))
		src[uint32(i)] = c
		if _, err := tx.Write(c); err != nil {
			return nil, err
		}
		if i%8 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	tx.Flush()
	return src, nil
}

// TestE2EEncryptedClean: the full encryption stack over UDP loopback — the X25519 +
// ML-KEM-768 hybrid handshake establishes, then every chunk is AEAD-sealed, coded,
// decoded, and opened byte-exact. If encryption were broken the receiver could not open
// a single symbol, so a complete byte-exact delivery is the proof.
func TestE2EEncryptedClean(t *testing.T) {
	cfg := encConfig()
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := meld.NewSender(rx.LocalAddr(), cfg) // blocks until the handshake completes
	if err != nil {
		t.Fatalf("NewSender (handshake): %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(5))
	src, err := streamEncrypted(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}
	ids, byID := readStream(t, rx, 3*time.Second)
	assertOrderedCorrect(t, ids, byID, src)
	if len(ids) != e2eN {
		st := rx.Stats()
		t.Fatalf("encrypted clean: delivered %d/%d (lost=%d)", len(ids), e2eN, st.Lost)
	}
}

// TestE2EEncryptedLossSurvival: encryption composes with the coding. At 25% loss the
// coder recovers the missing CIPHERTEXT symbols (encrypt-then-code: it codes over the
// sealed bytes, so a relay could recombine them with no key), and every recovered symbol
// still opens byte-exact. The handshake itself survives message-1 loss via retransmission.
func TestE2EEncryptedLossSurvival(t *testing.T) {
	cfg := encConfig()
	cfg.Redundancy = 0.5
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	proxy := startLossyProxy(t, 0.25, 13, rx.LocalAddr())
	defer func() { _ = proxy.Close() }()
	tx, err := meld.NewSender(proxy.addr(), cfg)
	if err != nil {
		t.Fatalf("NewSender (handshake through loss): %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(6))
	src, err := streamEncrypted(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}
	ids, byID := readStream(t, rx, 4*time.Second)
	assertOrderedCorrect(t, ids, byID, src)

	st := rx.Stats()
	residual := float64(e2eN-len(ids)) / float64(e2eN)
	t.Logf("encrypted 25%% loss: delivered %d/%d (%.1f%% residual), recovered %d, dropped %d",
		len(ids), e2eN, residual*100, st.Recovered, proxy.dropped())
	if st.Recovered == 0 {
		t.Fatal("no symbols recovered — coding-over-ciphertext not exercised")
	}
	if residual > 0.02 {
		t.Fatalf("encrypted residual %.1f%% too high (coding should recover 25%% loss byte-exact)", residual*100)
	}
}

// TestE2EEncryptedSliding confirms encryption composes with the band-form sliding coder
// too: the seal sits above the coder (at the Write boundary), so it is coder-agnostic.
func TestE2EEncryptedSliding(t *testing.T) {
	cfg := encConfig()
	cfg.Sliding = true
	cfg.CodingWindow = 64
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := meld.NewSender(rx.LocalAddr(), cfg)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(8))
	src, err := streamEncrypted(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}
	ids, byID := readStream(t, rx, 3*time.Second)
	assertOrderedCorrect(t, ids, byID, src)
	if len(ids) != e2eN {
		t.Fatalf("encrypted sliding: delivered %d/%d", len(ids), e2eN)
	}
}

// TestE2EEncryptedMultipath: encryption across two paths. The handshake rides path 0, the
// per-symbol seal is path-agnostic, and the receiver decodes from the union and opens
// byte-exact — so the coding-native multipath diversity and the crypto compose.
func TestE2EEncryptedMultipath(t *testing.T) {
	cfg := encConfig()
	rx, err := meld.NewMultipathReceiver([]string{"127.0.0.1:0", "127.0.0.1:0"}, cfg)
	if err != nil {
		t.Fatalf("NewMultipathReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := meld.NewMultipathSender(rx.LocalAddrs(), cfg)
	if err != nil {
		t.Fatalf("NewMultipathSender (handshake): %v", err)
	}
	defer func() { _ = tx.Close() }()

	rng := rand.New(rand.NewSource(9))
	src, err := streamEncrypted(tx, rng, e2eN)
	if err != nil {
		t.Fatalf("stream source: %v", err)
	}
	ids, byID := readStream(t, rx, 3*time.Second)
	assertOrderedCorrect(t, ids, byID, src)
	if len(ids) != e2eN {
		st := rx.Stats()
		t.Fatalf("encrypted multipath: delivered %d/%d (lost=%d)", len(ids), e2eN, st.Lost)
	}
}
