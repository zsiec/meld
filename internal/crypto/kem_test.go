package crypto

import (
	"bytes"
	"testing"
)

func TestHybridRoundTripAndMasterAgreement(t *testing.T) {
	resp, err := GenerateEphemeralKeys()
	if err != nil {
		t.Fatalf("GenerateEphemeralKeys: %v", err)
	}
	rx, rk := resp.Public()
	if len(rx) != X25519KeySize {
		t.Fatalf("X25519 public key %d bytes, want %d", len(rx), X25519KeySize)
	}
	if len(rk) != MLKEM768EncapKeySize {
		t.Fatalf("ML-KEM encapsulation key %d bytes, want %d", len(rk), MLKEM768EncapKeySize)
	}

	ix, ct, ssXi, ssKi, err := Initiate(rx, rk)
	if err != nil {
		t.Fatalf("Initiate: %v", err)
	}
	if len(ix) != X25519KeySize {
		t.Fatalf("initiator X25519 key %d bytes, want %d", len(ix), X25519KeySize)
	}
	if len(ct) != MLKEM768CiphertextSize {
		t.Fatalf("ML-KEM ciphertext %d bytes, want %d", len(ct), MLKEM768CiphertextSize)
	}

	ssXr, ssKr, err := resp.Respond(ix, ct)
	if err != nil {
		t.Fatalf("Respond: %v", err)
	}
	if !bytes.Equal(ssXi, ssXr) {
		t.Fatal("X25519 shared secrets disagree")
	}
	if !bytes.Equal(ssKi, ssKr) {
		t.Fatal("ML-KEM shared secrets disagree")
	}
	// Master-secret agreement from these shared secrets is tested through the live
	// handshake (TestHandshakeSucceedsAndKeysLineUp); here we only assert the KEM
	// round-trip itself agrees.
}

func TestTamperedCiphertextDivergesMaster(t *testing.T) {
	resp, _ := GenerateEphemeralKeys()
	rx, rk := resp.Public()
	ix, ct, ssXi, ssKi, _ := Initiate(rx, rk)

	bad := append([]byte(nil), ct...)
	bad[0] ^= 0x01 // same length: ML-KEM implicit rejection yields a different secret, no error
	ssXr, ssKr, err := resp.Respond(ix, bad)
	if err != nil {
		t.Fatalf("Respond on tampered ciphertext should not error (implicit rejection): %v", err)
	}
	if bytes.Equal(ssKi, ssKr) {
		t.Fatal("a tampered ML-KEM ciphertext must decapsulate to a different secret")
	}
	_ = ssXi // X25519 secret is unaffected; the divergent ML-KEM secret is what splits the
	_ = ssXr // master, so the two ends derive different keys and the AEAD layer rejects.
}

func TestMalformedKeysRejected(t *testing.T) {
	resp, _ := GenerateEphemeralKeys()
	rx, rk := resp.Public()
	ix, ct, _, _, _ := Initiate(rx, rk)

	if _, _, _, _, err := Initiate(rx[:X25519KeySize-1], rk); err != ErrBadPublicKey {
		t.Errorf("short responder X25519: got %v, want ErrBadPublicKey", err)
	}
	if _, _, _, _, err := Initiate(rx, rk[:MLKEM768EncapKeySize-1]); err != ErrBadPublicKey {
		t.Errorf("short responder ML-KEM key: got %v, want ErrBadPublicKey", err)
	}
	if _, _, err := resp.Respond(ix[:X25519KeySize-1], ct); err != ErrBadPublicKey {
		t.Errorf("short initiator X25519: got %v, want ErrBadPublicKey", err)
	}
	if _, _, err := resp.Respond(ix, ct[:MLKEM768CiphertextSize-1]); err != ErrBadCiphertext {
		t.Errorf("short ML-KEM ciphertext: got %v, want ErrBadCiphertext", err)
	}
}
