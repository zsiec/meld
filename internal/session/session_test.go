package session

import (
	"bytes"
	"encoding/binary"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/flow"
	"github.com/zsiec/meld/internal/wire"
)

// lightArgon2 keeps the handshake password-stretch cheap in tests.
var lightArgon2 = crypto.Argon2idParams{Time: 1, Memory: 8, Threads: 1}

// TestEncryptedEpochRotation drives the encrypted single-path host across several key
// epochs (EpochSize 64 — the configurable minimum — and 200 symbols) and asserts every
// chunk still delivers byte-exact, proving the sender's ratcheting send key and the
// receiver's ratcheting receive key stay in lockstep as the epoch (= src_index/EpochSize)
// advances.
func TestEncryptedEpochRotation(t *testing.T) {
	cfg := testCfg()
	sec := &SecurityConfig{Passphrase: []byte("rotation passphrase"), EpochSize: 64, Params: lightArgon2}
	rx, err := NewReceiver("127.0.0.1:0", cfg, sec)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := NewSender(rx.LocalAddr(), cfg, sec)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	defer func() { _ = tx.Close() }()

	const n = 200
	chunkLen := cfg.SymbolSize - 16
	want := make([][]byte, n)
	for i := range want {
		b := make([]byte, chunkLen)
		binary.BigEndian.PutUint32(b, uint32(i))
		for j := 4; j < chunkLen; j++ {
			b[j] = byte(i*7 + j)
		}
		want[i] = b
	}
	go func() {
		for i := 0; i < n; i++ {
			if _, err := tx.Write(want[i]); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		tx.Flush()
	}()

	got, buf := 0, make([]byte, 512)
	for got < n {
		if err := rx.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		m, err := rx.Read(buf)
		if err != nil {
			break
		}
		id := binary.BigEndian.Uint32(buf[:4])
		if int(id) >= n || !bytes.Equal(buf[:m], want[id]) {
			t.Fatalf("chunk %d mismatch across epoch rotation", id)
		}
		got++
	}
	if got != n {
		t.Fatalf("epoch rotation: delivered %d/%d (epochs of %d symbols)", got, n, sec.EpochSize)
	}
}

// TestRespondInitCommitAfterConfirm drives the responder's message-1 handler directly to
// assert the commit-after-confirm contract: a first message 1 establishes; a retransmit
// resends the cached reply to the established PEER (anti-reflection); a NEW handshake stages a
// pending session WITHOUT disturbing the live one (it is promoted only later, by a symbol
// that opens under it); and garbage elicits nothing.
func TestRespondInitCommitAfterConfirm(t *testing.T) {
	const flowID = 0xABCD
	sec := &SecurityConfig{Passphrase: []byte("rehandshake pw"), Params: lightArgon2}
	o, err := newOpenState(sec, flowID)
	if err != nil {
		t.Fatalf("newOpenState: %v", err)
	}
	pid := []byte("198.51.100.7")
	mk := func() []byte { // a fresh initiator's message 1 (distinct ephemeral keys each call)
		init, err := crypto.NewInitiator(o.psk, beU32(flowID), 1<<14)
		if err != nil {
			t.Fatalf("NewInitiator: %v", err)
		}
		m1, err := init.WriteMessage1()
		if err != nil {
			t.Fatalf("WriteMessage1: %v", err)
		}
		return m1
	}

	// First message 1 establishes the live session and replies to the source (not the peer).
	m1 := mk()
	if res := o.respondInit(m1, pid); res.send == nil || res.toPeer {
		t.Fatalf("first init: want establish reply to source, got %+v", res)
	}
	if !o.established {
		t.Fatal("not established after the first message 1")
	}
	liveSecret := append([]byte(nil), o.recvSecret...)

	// A retransmit of the SAME message 1 resends the cached reply ONLY to the established peer
	// (anti-reflection), with no KEM and no state change.
	if res := o.respondInit(m1, pid); res.send == nil || !res.toPeer {
		t.Fatalf("retransmit: want resend to the peer, got %+v", res)
	}

	// A DIFFERENT (new) message 1 stages a PENDING session and replies to the source, but must
	// NOT disturb the live session — no reset happens until a symbol authenticates under it.
	if res := o.respondInit(mk(), pid); res.send == nil || res.toPeer {
		t.Fatalf("re-handshake: want pending reply to source, got %+v", res)
	}
	if o.pending == nil {
		t.Fatal("re-handshake did not stage a pending session")
	}
	if !bytes.Equal(o.recvSecret, liveSecret) {
		t.Fatal("re-handshake displaced the live session's keys before confirmation")
	}

	// Garbage (too short to carry the keys) elicits nothing.
	if res := o.respondInit([]byte{0x00, 0x01}, pid); res.send != nil {
		t.Fatalf("garbage init: want drop, got %+v", res)
	}
}

// TestTryPromoteBoundedAndStateless asserts the trial-decrypt is bounded and non-corrupting:
// a forged/old-epoch systematic with a huge SrcIndex neither promotes, nor spins an unbounded
// ratchet, nor advances the pending keyer — so a genuine epoch-0 confirmation can still happen.
func TestTryPromoteBoundedAndStateless(t *testing.T) {
	const flowID = 0xABCD
	sec := &SecurityConfig{Passphrase: []byte("promote pw"), Params: lightArgon2}
	o, err := newOpenState(sec, flowID)
	if err != nil {
		t.Fatalf("newOpenState: %v", err)
	}
	o.established = true
	o.hsKeys = []byte("live-keys-placeholder")
	// any secret (trials must fail-closed); a real epochSize so the bound below is the epoch-distance
	// check, not the degenerate epochSize==0 short-circuit.
	o.pending = &pendingState{keyer: epochKeyer{recvSecret: o.psk}, epochSize: sec.epochSize()}

	// A forged systematic with an enormous SrcIndex (epoch ≈ 2^32/epochSize) must be rejected
	// cheaply: its epoch is far beyond maxTrialEpochs, so trialOpen returns false WITHOUT
	// ratcheting (the bound is the epoch-distance check, not a loop over the wire epoch).
	huge, err := wire.DecodeSymbol(wire.EncodeSymbol(nil, wire.Symbol{Flow: flowID, Kind: wire.Systematic, SrcIndex: 0xFFFFFFFF, Payload: make([]byte, 32)}))
	if err != nil {
		t.Fatalf("decode huge: %v", err)
	}
	if o.tryPromote(huge) {
		t.Fatal("a huge-SrcIndex forged symbol must not promote")
	}
	if o.pending == nil {
		t.Fatal("a failed trial must not clear the pending")
	}
	// An epoch-0 systematic that does not open under the pending keys also fails closed and
	// leaves the pending intact (so the real sender's symbol can still promote it).
	forged0, err := wire.DecodeSymbol(wire.EncodeSymbol(nil, wire.Symbol{Flow: flowID, Kind: wire.Systematic, SrcIndex: 0, Payload: make([]byte, 32)}))
	if err != nil {
		t.Fatalf("decode forged0: %v", err)
	}
	if o.tryPromote(forged0) {
		t.Fatal("a forged epoch-0 symbol must not promote")
	}
	if o.pending == nil {
		t.Fatal("a failed epoch-0 trial must not clear the pending")
	}
}

// TestEncryptedReHandshakeDelivers proves a restarted sender re-establishes AND delivers
// end to end: a second NewSender to the same receiver stages a pending re-handshake (fresh
// ephemeral keys), which the receiver promotes — rebuilding the core — when the restarted
// sender's first symbol opens under the new keys (commit-after-confirm). The restarted
// sender's source ids begin again at 0 — below the first session's delivery cursor — so
// without the core rebuild they would all be dropped as duplicates. Receiving the second
// phase's chunks is the proof the promotion reset the core, not just the crypto keys.
func TestEncryptedReHandshakeDelivers(t *testing.T) {
	cfg := testCfg()
	sec := &SecurityConfig{Passphrase: []byte("re-handshake pw"), Params: lightArgon2}
	rx, err := NewReceiver("127.0.0.1:0", cfg, sec)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	chunkLen := cfg.SymbolSize - 16
	const n = 16

	send := func(tag byte) { // a full sender lifetime: handshake, stream n chunks, close
		tx, err := NewSender(rx.LocalAddr(), cfg, sec) // the 2nd call re-handshakes (fresh keys → pending → promote)
		if err != nil {
			t.Fatalf("NewSender(%q): %v", tag, err)
		}
		for i := 0; i < n; i++ {
			b := make([]byte, chunkLen)
			b[0] = tag
			binary.BigEndian.PutUint32(b[1:], uint32(i))
			if _, err := tx.Write(b); err != nil {
				t.Fatalf("Write(%q,%d): %v", tag, i, err)
			}
			time.Sleep(time.Millisecond)
		}
		tx.Flush()
		time.Sleep(30 * time.Millisecond) // let the tail drain before the socket closes
		_ = tx.Close()
	}
	count := func(tag byte, want int) int {
		buf := make([]byte, 512)
		got := 0
		for got < want {
			if err := rx.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatalf("set read deadline: %v", err)
			}
			m, err := rx.Read(buf)
			if err != nil {
				break
			}
			if m > 0 && buf[0] == tag {
				got++
			}
		}
		return got
	}

	send('A')
	if got := count('A', n); got != n {
		t.Fatalf("phase A (initial handshake): delivered %d/%d", got, n)
	}
	send('B') // restarted sender → re-handshake → core reset
	if got := count('B', n); got != n {
		t.Fatalf("phase B (after re-handshake): delivered %d/%d — core not reset, ids<cursor dropped", got, n)
	}
}

// TestRespondInitUnauthedDoesNotChargeCookieGate asserts a forged/garbage message 1 (failing
// mac1) does not advance the cookie attempt counter (#8), so an unauthenticated flood cannot
// trip the under-load cookie gate against legitimate senders.
func TestRespondInitUnauthedDoesNotChargeCookieGate(t *testing.T) {
	const flowID = 0xABCD
	sec := &SecurityConfig{Passphrase: []byte("gate pw"), Params: lightArgon2, CookieThreshold: 1}
	o, err := newOpenState(sec, flowID)
	if err != nil {
		t.Fatalf("newOpenState: %v", err)
	}
	pid := []byte("198.51.100.7")
	// A real message 1 with a flipped mac1 byte: valid length, but mac1 fails.
	init, _ := crypto.NewInitiator(o.psk, beU32(flowID), 1<<14)
	m1, _ := init.WriteMessage1()
	bad := append([]byte(nil), m1...)
	bad[len(bad)-20] ^= 0x40 // inside the mac1 region (before the zero mac2 trailer)
	for i := 0; i < 10; i++ {
		if res := o.respondInit(bad, pid); res.send != nil {
			t.Fatal("a forged (bad mac1) init elicited a reply")
		}
	}
	if o.attempts != 0 {
		t.Fatalf("attempts=%d after a forged flood, want 0 (unauthenticated must not charge the gate)", o.attempts)
	}
}

func testCfg() flow.Config {
	return flow.Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0.15, BufferMicros: 200_000}
}

// TestReceiverClockOffsetConverges: a receiver whose clock is offset from the
// sender's by a large constant recovers that offset over real UDP via the handshake,
// so coreNow lands in the sender's frame (offset estimate ≈ −Δ).
func TestReceiverClockOffsetConverges(t *testing.T) {
	const deltaMicros = 2_000_000 // receiver clock 2 s ahead of the sender

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	rx, err := newReceiver(conn, testCfg(), clock.NewOffset(clock.NewRealClock(), deltaMicros), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()

	tx, err := NewSender(rx.LocalAddr(), testCfg(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = tx.Close() }()

	// Stream a little media so the receiver learns the peer and the probe loop runs;
	// then let several probe rounds (≈200 ms apart) complete.
	go func() {
		msg := make([]byte, 256)
		for i := 0; i < 400; i++ {
			if _, err := tx.Write(msg); err != nil {
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	var off int64
	for time.Now().Before(deadline) {
		off = rx.cs.offsetMicros()
		if off != 0 && abs(off-(-deltaMicros)) < 50_000 {
			return // converged within 50 ms of the true −Δ
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("offset estimate %d did not converge to ≈ %d", off, -deltaMicros)
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

// TestCrossHostLatencyUnderOffset: with the receiver's clock offset 2 s from the
// sender's, the clock handshake corrects the deadline frame so media is delivered
// normally — and the (real-time) latency is within budget. Without the correction the
// receiver would judge every symbol ~2 s past its deadline and deliver nothing.
func TestCrossHostLatencyUnderOffset(t *testing.T) {
	const deltaMicros = 2_000_000

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg := testCfg() // 200 ms buffer
	rx, err := newReceiver(conn, cfg, clock.NewOffset(clock.NewRealClock(), deltaMicros), nil)
	if err != nil {
		t.Fatalf("newReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := NewSender(rx.LocalAddr(), cfg, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = tx.Close() }()

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			msg := make([]byte, cfg.SymbolSize)
			binary.BigEndian.PutUint64(msg[0:8], uint64(time.Now().UnixMicro()))
			if _, err := tx.Write(msg); err != nil {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	defer close(stop)

	// Wait for the offset handshake to converge (estimate ≈ −Δ).
	converged := false
	for d := time.Now().Add(3 * time.Second); time.Now().Before(d); {
		if off := rx.cs.offsetMicros(); off != 0 && abs(off+deltaMicros) < 50_000 {
			converged = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !converged {
		t.Fatalf("clock offset did not converge (got %d, want ≈ %d)", rx.cs.offsetMicros(), -deltaMicros)
	}

	// Measure the steady state: count deliveries sent after this point and their latency.
	base := time.Now().UnixMicro()
	var lats []int64
	for d := time.Now().Add(1500 * time.Millisecond); time.Now().Before(d); {
		if err := rx.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		buf := make([]byte, cfg.SymbolSize)
		n, err := rx.Read(buf)
		if err != nil || n < 8 {
			continue
		}
		sendT := int64(binary.BigEndian.Uint64(buf[0:8]))
		if sendT < base {
			continue // a straggler from before the measurement window
		}
		lats = append(lats, time.Now().UnixMicro()-sendT)
	}

	if len(lats) < 100 {
		t.Fatalf("only %d deliveries in the 1.5 s window — the clock offset broke delivery", len(lats))
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p50 := lats[len(lats)/2]
	t.Logf("cross-host (+2 s offset): %d delivered, p50 latency %d µs (budget %d)", len(lats), p50, cfg.BufferMicros)
	if p50 > cfg.BufferMicros {
		t.Fatalf("p50 latency %d µs exceeds the %d µs budget", p50, cfg.BufferMicros)
	}
}

// TestEncryptedCookieUnderLoad forces the mac2 anti-amplification path (CookieThreshold 1
// ⇒ the responder demands a cookie on every handshake): the sender's first message gets a
// cookie reply, it retries with the cookie-derived mac2, and only then does the handshake
// complete. NewSender returning without timeout — plus byte-exact delivery — proves the
// cookie round trip works end to end.
func TestEncryptedCookieUnderLoad(t *testing.T) {
	cfg := testCfg()
	sec := &SecurityConfig{Passphrase: []byte("cookie pw"), Params: lightArgon2, CookieThreshold: 1}
	rx, err := NewReceiver("127.0.0.1:0", cfg, sec)
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	defer func() { _ = rx.Close() }()
	tx, err := NewSender(rx.LocalAddr(), cfg, sec) // completes only via the cookie round trip
	if err != nil {
		t.Fatalf("NewSender (cookie handshake): %v", err)
	}
	defer func() { _ = tx.Close() }()

	const n = 16
	chunkLen := cfg.SymbolSize - 16
	want := make([][]byte, n)
	for i := range want {
		b := make([]byte, chunkLen)
		binary.BigEndian.PutUint32(b, uint32(i))
		for j := 4; j < chunkLen; j++ {
			b[j] = byte(i*5 + j)
		}
		want[i] = b
	}
	go func() {
		for i := 0; i < n; i++ {
			if _, err := tx.Write(want[i]); err != nil {
				t.Errorf("write %d: %v", i, err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		tx.Flush()
	}()

	got, buf := 0, make([]byte, 512)
	for got < n {
		if err := rx.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		m, err := rx.Read(buf)
		if err != nil {
			break
		}
		id := binary.BigEndian.Uint32(buf[:4])
		if int(id) >= n || !bytes.Equal(buf[:m], want[id]) {
			t.Fatalf("chunk %d mismatch", id)
		}
		got++
	}
	if got != n {
		t.Fatalf("under-load cookie handshake: delivered %d/%d", got, n)
	}
}

// TestPromoteCommitsAtBaseEpoch proves a re-handshake confirmed by a symbol at epoch E>0 — the
// restarted sender's epoch-0 burst was lost/reordered, so the first symbol the receiver opens
// lands several epochs in — commits the live receive keyer at its BASE epoch 0, NOT advanced to
// E. The rebuilt core redelivers ids from 0, so the keyer must ratchet forward in id order:
// committing the advanced copy would leave every id below E*EpochSize underivable (forward
// secrecy) and silently dropped. The proof is two-fold: openEpoch is 0 after promotion, and an
// epoch-0 id then opens. Driven at the openState/sealState layer so the confirming epoch is
// exact (a socket test cannot force which epoch survives loss).
func TestPromoteCommitsAtBaseEpoch(t *testing.T) {
	const flowID = 0x1234
	const epochSize = 64
	const E = 3 // the confirming symbol lands at epoch 3 (epoch-0..2 bursts "lost")
	cfg := testCfg()
	sec := &SecurityConfig{Passphrase: []byte("base-epoch pw"), EpochSize: epochSize, Params: lightArgon2}
	o, err := newOpenState(sec, flowID)
	if err != nil {
		t.Fatalf("newOpenState: %v", err)
	}
	mkInit := func() *crypto.Initiator {
		// The sender is authoritative for epochSize: it negotiates the same value it seals with
		// (sec.epochSize() == epochSize), so the receiver's trial keys the confirming symbol at the
		// same epoch the sender sealed it (192/64 == 3), not at a stale receiver-config epoch.
		init, err := crypto.NewInitiator(o.psk, beU32(flowID), epochSize)
		if err != nil {
			t.Fatalf("NewInitiator: %v", err)
		}
		return init
	}
	pid := []byte("203.0.113.9")

	// Establish a first live session (initiator A) so B's handshake is a re-handshake (pending).
	a := mkInit()
	m1a, err := a.WriteMessage1()
	if err != nil {
		t.Fatalf("A WriteMessage1: %v", err)
	}
	if res := o.respondInit(m1a, pid); res.send == nil || !o.established {
		t.Fatal("first handshake did not establish the live session")
	}

	// Restarted sender B re-handshakes: respondInit stages a pending keyed under B's session.
	b := mkInit()
	m1b, err := b.WriteMessage1()
	if err != nil {
		t.Fatalf("B WriteMessage1: %v", err)
	}
	resB := o.respondInit(m1b, pid)
	if resB.send == nil || o.pending == nil {
		t.Fatal("re-handshake did not stage a pending session")
	}
	// Recover B's send traffic secret (== the pending keyer's receive secret) by completing the
	// handshake on the initiator side, so the test can seal genuine confirming symbols.
	payloadB, err := wire.DecodeHandshake(resB.send)
	if err != nil {
		t.Fatalf("decode B's message 2: %v", err)
	}
	sessB, err := b.ReadMessage2(payloadB)
	if err != nil {
		t.Fatalf("B ReadMessage2: %v", err)
	}

	// Seal B's first ARRIVING systematic at epoch E>0 (its epoch-0..E-1 burst was lost). A
	// sealState fast-forwarded to nextSrc = E*epochSize derives the same epoch-E key the pending
	// keyer's trial will, so it genuinely opens and promotes.
	sealAt := func(srcIndex uint32, chunk []byte) wire.Symbol {
		ss := newSealState(sec, cfg.SymbolSize, flowID)
		ss.sendSecret = sessB.SendTrafficSecret()
		ss.nextSrc = srcIndex
		ct, err := ss.seal(chunk)
		if err != nil {
			t.Fatalf("seal at %d: %v", srcIndex, err)
		}
		return wire.Symbol{Flow: flowID, Kind: wire.Systematic, SrcIndex: srcIndex, Payload: append([]byte(nil), ct...)}
	}
	confirm := sealAt(E*epochSize, []byte("confirming symbol at epoch E"))
	if !o.tryPromote(confirm) {
		t.Fatal("the genuine confirming symbol did not promote the pending")
	}
	if o.pending != nil {
		t.Fatal("promotion did not clear the pending")
	}
	// THE FIX: the committed live keyer is at epoch 0 (the pending base), not advanced to E.
	if o.openEpoch != 0 {
		t.Fatalf("live keyer committed at epoch %d, want 0 (an epoch-E commit silently drops every id below E*EpochSize)", o.openEpoch)
	}

	// And it opens an epoch-0 id — exactly what the rebuilt core redelivers first, and exactly
	// what an epoch-E commit could no longer derive (forward secrecy).
	id0 := []byte("id-zero after restart")
	ct0 := sealAt(0, id0)
	out := o.openAll([]delivered{{id: 0, data: ct0.Payload}})
	if len(out) != 1 || !bytes.HasPrefix(out[0], id0) {
		t.Fatalf("epoch-0 id failed to open after promotion: got %d chunks (want the prefix %q)", len(out), id0)
	}
}

// TestStaleStragglerBound confirms the post-promotion straggler screen's documented boundary
// (finding #6): a near-epoch old-session systematic that does not open under the live keys is
// SCREENED, while one more than maxTrialEpochs from the live openEpoch is NOT cheaply judged
// here and is deferred to the core's admission window (which rejects the far-ahead id). With the
// epoch-0 commit (above) keeping the live openEpoch low after a restart, real old stragglers are
// high-id and land in this deferred-to-the-core band — so the screen never needs to ratchet far.
func TestStaleStragglerBound(t *testing.T) {
	const flowID = 0x55
	const epochSize = 64
	sec := &SecurityConfig{Passphrase: []byte("straggler pw"), EpochSize: epochSize, Params: lightArgon2}
	o, err := newOpenState(sec, flowID)
	if err != nil {
		t.Fatalf("newOpenState: %v", err)
	}
	// A live session at openEpoch 0 with an arbitrary receive secret; the stragglers below are
	// forged (random/zero payload), so they never open under it.
	o.established = true
	o.epochKeyer = epochKeyer{recvSecret: o.psk}

	forgedSystematic := func(srcIndex uint32) wire.Symbol {
		return wire.Symbol{Flow: flowID, Kind: wire.Systematic, SrcIndex: srcIndex, Payload: make([]byte, 32)}
	}
	// Near the live epoch (within maxTrialEpochs) and not opening ⇒ screened out of the core.
	near := forgedSystematic(2 * epochSize) // epoch 2 ≤ maxTrialEpochs
	if !o.staleStraggler(near) {
		t.Fatal("a near-epoch old-session straggler that does not open must be screened")
	}
	// Beyond maxTrialEpochs ⇒ not screened here (deferred to the core's admission window).
	far := forgedSystematic((maxTrialEpochs + 5) * epochSize)
	if o.staleStraggler(far) {
		t.Fatal("a straggler beyond maxTrialEpochs must be deferred to the core, not screened here")
	}
	// A repair (non-systematic) is never screened by this systematic-only path.
	if o.staleStraggler(wire.Symbol{Flow: flowID, Kind: wire.Repair, SrcIndex: 2 * epochSize}) {
		t.Fatal("a repair symbol must not be screened by staleStraggler")
	}
}
