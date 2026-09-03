package crypto

import (
	"bytes"
	"testing"
)

// testEpochSize is a deliberately NON-default epoch size, so a test that checks it propagated
// proves the value was negotiated from the initiator, not defaulted independently on each side.
const testEpochSize uint32 = 4096

// runHandshake drives a full exchange and returns both sessions (or a fatal error).
func runHandshake(t *testing.T, initPSK, respPSK, initPro, respPro []byte) (*Session, *Session, error) {
	t.Helper()
	init, err := NewInitiator(initPSK, initPro, testEpochSize)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	resp, err := NewResponder(respPSK, respPro)
	if err != nil {
		t.Fatalf("NewResponder: %v", err)
	}
	m1, err := init.WriteMessage1()
	if err != nil {
		t.Fatalf("WriteMessage1: %v", err)
	}
	if len(m1) != msg1Len {
		t.Fatalf("msg1 length %d, want %d", len(m1), msg1Len)
	}
	if err := resp.ReadMessage1(m1); err != nil {
		return nil, nil, err
	}
	m2, rsess, err := resp.WriteMessage2()
	if err != nil {
		return nil, nil, err
	}
	if len(m2) != msg2Len {
		t.Fatalf("msg2 length %d, want %d", len(m2), msg2Len)
	}
	isess, err := init.ReadMessage2(m2)
	if err != nil {
		return nil, nil, err
	}
	return isess, rsess, nil
}

// sendEpochKey / recvEpochKey mirror what the host (internal/session/secure.go) derives:
// the directional epoch-0 traffic secret expanded to a per-epoch AEAD key.
func sendEpochKey(s *Session, epoch uint16) []byte { return EpochKey(s.SendTrafficSecret(), epoch) }
func recvEpochKey(s *Session, epoch uint16) []byte { return EpochKey(s.RecvTrafficSecret(), epoch) }

func TestHandshakeSucceedsAndKeysLineUp(t *testing.T) {
	psk := testKey(0x42)
	pro := []byte("flow-0xF00D")
	isess, rsess, err := runHandshake(t, psk, psk, pro, pro)
	if err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	if !bytes.Equal(isess.Master, rsess.Master) {
		t.Fatal("the two ends derived different master secrets")
	}
	if !isess.Initiator || rsess.Initiator {
		t.Fatal("session roles are wrong")
	}
	// The initiator's (media sender's) epoch size is carried in message 1 and adopted by both
	// sessions — sender-authoritative, so the receiver never relies on its own configured value.
	if isess.EpochSize != testEpochSize || rsess.EpochSize != testEpochSize {
		t.Fatalf("EpochSize not negotiated: initiator=%d responder=%d want %d", isess.EpochSize, rsess.EpochSize, testEpochSize)
	}
	// One side's send key must equal the other's receive key, in both directions.
	if !bytes.Equal(sendEpochKey(isess, 7), recvEpochKey(rsess, 7)) {
		t.Fatal("initiator send key != responder recv key")
	}
	if !bytes.Equal(recvEpochKey(isess, 7), sendEpochKey(rsess, 7)) {
		t.Fatal("initiator recv key != responder send key")
	}
	// And the directions are distinct (no key reuse across directions).
	if bytes.Equal(sendEpochKey(isess, 7), recvEpochKey(isess, 7)) {
		t.Fatal("send and receive keys must differ")
	}

	// A media chunk sealed by the initiator opens for the responder, end to end.
	chunk := bytes.Repeat([]byte("frame"), 200)
	aad := AAD(0xF00D, 0, 99)
	s, _ := NewSealer(sendEpochKey(isess, 0), 0)
	ct, err := s.Seal(nil, chunk, aad, 99)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	o, _ := NewOpener(recvEpochKey(rsess, 0), 0)
	got, err := o.Open(nil, ct, aad, 99)
	if err != nil {
		t.Fatalf("Open across handshake: %v", err)
	}
	if !bytes.Equal(got, chunk) {
		t.Fatal("media chunk did not round-trip across the handshake keys")
	}
}

func TestHandshakeWrongPSKRejected(t *testing.T) {
	// A different PSK fails mac1 on the very first message (cheap rejection).
	if _, _, err := runHandshake(t, testKey(1), testKey(2), nil, nil); err != ErrBadHandshake {
		t.Fatalf("wrong PSK: got %v, want ErrBadHandshake", err)
	}
}

func TestHandshakeDifferentPrologueRejected(t *testing.T) {
	// Same PSK but a different binding context: mac1 passes, the KEM runs, but the
	// transcripts diverge so the key-confirmation tag fails (downgrade/context binding).
	psk := testKey(0x42)
	if _, _, err := runHandshake(t, psk, psk, []byte("flow-A"), []byte("flow-B")); err != ErrBadHandshake {
		t.Fatalf("different prologue: got %v, want ErrBadHandshake", err)
	}
}

func TestHandshakeTamperedMessagesRejected(t *testing.T) {
	psk := testKey(0x42)
	// Tamper message 1: responder rejects.
	init, _ := NewInitiator(psk, nil, testEpochSize)
	resp, _ := NewResponder(psk, nil)
	m1, _ := init.WriteMessage1()
	// Positions inside the mac1-covered region (pubs ‖ epochSize ‖ mac1); the mac2 trailer is
	// only checked under load (cookie.go), so it is deliberately not covered here. msg1PubsLen is
	// the first epochSize byte — tampering it must be rejected (mac1 + the transcript bind it).
	for _, pos := range []int{0, X25519KeySize, msg1PubsLen, msg1BodyLen + macSize - 1} {
		bad := append([]byte(nil), m1...)
		bad[pos] ^= 0x40
		if err := resp.ReadMessage1(bad); err != ErrBadHandshake {
			t.Fatalf("tampered msg1 at %d: got %v, want ErrBadHandshake", pos, err)
		}
		resp, _ = NewResponder(psk, nil) // fresh (ReadMessage1 is single-shot)
	}

	// Tamper message 2: initiator rejects.
	init2, _ := NewInitiator(psk, nil, testEpochSize)
	resp2, _ := NewResponder(psk, nil)
	m1b, _ := init2.WriteMessage1()
	if err := resp2.ReadMessage1(m1b); err != nil {
		t.Fatalf("ReadMessage1: %v", err)
	}
	m2, _, _ := resp2.WriteMessage2()
	bad := append([]byte(nil), m2...)
	bad[10] ^= 0x40
	if _, err := init2.ReadMessage2(bad); err != ErrBadHandshake {
		t.Fatalf("tampered msg2: got %v, want ErrBadHandshake", err)
	}
}

func TestHandshakeZeroEpochSizeRejected(t *testing.T) {
	// The host keys every symbol by id/epochSize, so a peer advertising epochSize 0 would divide
	// by zero downstream. ReadMessage1 must reject it as a bad handshake even though mac1 verifies
	// (the value is authenticated — this is a malformed, not a forged, message).
	psk := testKey(0x42)
	init, err := NewInitiator(psk, nil, 0)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	m1, err := init.WriteMessage1()
	if err != nil {
		t.Fatalf("WriteMessage1: %v", err)
	}
	resp, _ := NewResponder(psk, nil)
	if err := resp.ReadMessage1(m1); err != ErrBadHandshake {
		t.Fatalf("zero epochSize: got %v, want ErrBadHandshake", err)
	}
}

func TestHandshakeStateOrdering(t *testing.T) {
	psk := testKey(0x42)
	resp, _ := NewResponder(psk, nil)
	if _, _, err := resp.WriteMessage2(); err != ErrHandshakeState {
		t.Errorf("WriteMessage2 before ReadMessage1: got %v, want ErrHandshakeState", err)
	}
	init, _ := NewInitiator(psk, nil, testEpochSize)
	if _, err := init.ReadMessage2(make([]byte, msg2Len)); err != ErrHandshakeState {
		t.Errorf("ReadMessage2 before WriteMessage1: got %v, want ErrHandshakeState", err)
	}
	if _, err := init.WriteMessage1(); err != nil {
		t.Fatalf("WriteMessage1: %v", err)
	}
	if _, err := init.WriteMessage1(); err != ErrHandshakeState {
		t.Errorf("double WriteMessage1: got %v, want ErrHandshakeState", err)
	}
}

func TestHandshakeMalformedLengthRejected(t *testing.T) {
	psk := testKey(0x42)
	resp, _ := NewResponder(psk, nil)
	if err := resp.ReadMessage1(make([]byte, msg1Len-1)); err != ErrBadHandshake {
		t.Errorf("short msg1: got %v, want ErrBadHandshake", err)
	}
	init, _ := NewInitiator(psk, nil, testEpochSize)
	_, _ = init.WriteMessage1()
	if _, err := init.ReadMessage2(make([]byte, msg2Len+1)); err != ErrBadHandshake {
		t.Errorf("long msg2: got %v, want ErrBadHandshake", err)
	}
}

// FuzzResponderReadMessage1NoPanic: arbitrary first-message bytes must never panic.
func FuzzResponderReadMessage1NoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, msg1Len))
	psk := testKey(0x42)
	f.Fuzz(func(t *testing.T, msg1 []byte) {
		r, err := NewResponder(psk, nil)
		if err != nil {
			t.Fatalf("NewResponder: %v", err)
		}
		_ = r.ReadMessage1(msg1) // must not panic
	})
}

// FuzzInitiatorReadMessage2NoPanic: arbitrary reply bytes must never panic.
func FuzzInitiatorReadMessage2NoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, msg2Len))
	init, err := NewInitiator(testKey(0x42), nil, testEpochSize)
	if err != nil {
		f.Fatalf("NewInitiator: %v", err)
	}
	if _, err := init.WriteMessage1(); err != nil {
		f.Fatalf("WriteMessage1: %v", err)
	}
	f.Fuzz(func(t *testing.T, msg2 []byte) {
		_, _ = init.ReadMessage2(msg2) // must not panic
	})
}
