package crypto

import (
	"bytes"
	"testing"
)

// TestEndToEnd walks the entire package along the exact path the session host will
// drive: a shared passphrase is stretched to a PSK, a hybrid X25519 + ML-KEM-768
// exchange agrees a master secret on both ends, the master is split into a directional
// traffic secret and a per-epoch key, and a media chunk round-trips through the AEAD
// record layer. It is the in-package proof that the primitives compose into a working
// encrypt-then-code key path before the handshake protocol and host wiring (slice 2).
func TestEndToEnd(t *testing.T) {
	const (
		flow     = 0xF00DF00D
		epoch    = 9
		srcIndex = 12345
	)
	passphrase := []byte("studio-to-truck contribution key")
	salt := []byte("deployment salt")

	// 1. Both ends stretch the shared passphrase to the same long-term PSK (cached).
	psk := DerivePSK(passphrase, salt, fastArgon2)
	prologue := []byte("flow-context")

	// 2. The LIVE hybrid handshake agrees a master secret on both ends.
	init, err := NewInitiator(psk, prologue)
	if err != nil {
		t.Fatalf("NewInitiator: %v", err)
	}
	resp, err := NewResponder(psk, prologue)
	if err != nil {
		t.Fatalf("NewResponder: %v", err)
	}
	m1, err := init.WriteMessage1()
	if err != nil {
		t.Fatalf("WriteMessage1: %v", err)
	}
	if err := resp.ReadMessage1(m1); err != nil {
		t.Fatalf("ReadMessage1: %v", err)
	}
	m2, rsess, err := resp.WriteMessage2()
	if err != nil {
		t.Fatalf("WriteMessage2: %v", err)
	}
	isess, err := init.ReadMessage2(m2)
	if err != nil {
		t.Fatalf("ReadMessage2: %v", err)
	}
	if !bytes.Equal(isess.Master, rsess.Master) {
		t.Fatal("handshake did not agree a shared master secret")
	}

	// 3. Per-epoch keys via the Session helpers: the initiator (sender) seals on its send
	// direction, the responder (receiver) opens on the matching direction.
	sendKey := EpochKey(isess.SendTrafficSecret(), epoch)
	recvKey := EpochKey(rsess.RecvTrafficSecret(), epoch)

	// 4. Encrypt-then-code record layer: the sender seals a media chunk, the receiver
	// (after the coder would have recovered the ciphertext) opens it byte-exact.
	chunk := bytes.Repeat([]byte("media!"), 219) // ~1314 bytes, a realistic symbol
	aad := AAD(flow, epoch, srcIndex)

	sealer, err := NewSealer(sendKey, epoch)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	sealed, err := sealer.Seal(nil, chunk, aad, srcIndex)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	opener, err := NewOpener(recvKey, epoch)
	if err != nil {
		t.Fatalf("NewOpener: %v", err)
	}
	got, err := opener.Open(nil, sealed, aad, srcIndex)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, chunk) {
		t.Fatal("end-to-end media chunk did not round-trip")
	}

	// 5. A wrong passphrase cannot even complete the handshake: mac1 (keyed by the wrong
	// PSK) rejects message 1 outright, so no key is ever agreed.
	wrongResp, err := NewResponder(DerivePSK([]byte("wrong passphrase"), salt, fastArgon2), prologue)
	if err != nil {
		t.Fatalf("NewResponder(wrong): %v", err)
	}
	if err := wrongResp.ReadMessage1(m1); err != ErrBadHandshake {
		t.Fatalf("a wrong passphrase must reject message 1 with ErrBadHandshake, got %v", err)
	}
}
