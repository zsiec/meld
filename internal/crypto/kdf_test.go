package crypto

import (
	"bytes"
	"testing"
)

// fastArgon2 keeps the password-stretch cheap in tests; the determinism property the
// tests assert does not depend on the work factor (production uses DefaultArgon2idParams).
var fastArgon2 = Argon2idParams{Time: 1, Memory: 8, Threads: 1}

func TestDerivePSKDeterministicAndSalted(t *testing.T) {
	pass := []byte("a human-chosen contribution passphrase")
	salt1 := []byte("deployment-salt-1")
	salt2 := []byte("deployment-salt-2")

	a := DerivePSK(pass, salt1, fastArgon2)
	b := DerivePSK(pass, salt1, fastArgon2)
	if len(a) != KeySize {
		t.Fatalf("PSK length %d, want %d", len(a), KeySize)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("DerivePSK is not deterministic for equal inputs")
	}
	if bytes.Equal(a, DerivePSK(pass, salt2, fastArgon2)) {
		t.Fatal("a different salt must yield a different PSK")
	}
	if bytes.Equal(a, DerivePSK([]byte("other passphrase"), salt1, fastArgon2)) {
		t.Fatal("a different passphrase must yield a different PSK")
	}
}

func TestTrafficAndEpochKeysAreDistinct(t *testing.T) {
	master := bytes.Repeat([]byte{0xAB}, KeySize)
	s2r := TrafficSecret(master, SenderToReceiver)
	r2s := TrafficSecret(master, ReceiverToSender)
	if len(s2r) != KeySize {
		t.Fatalf("traffic secret length %d, want %d", len(s2r), KeySize)
	}
	if bytes.Equal(s2r, r2s) {
		t.Fatal("the two directions must derive independent traffic secrets")
	}
	if !bytes.Equal(s2r, TrafficSecret(master, SenderToReceiver)) {
		t.Fatal("TrafficSecret is not deterministic")
	}

	k0, k1 := EpochKey(s2r, 0), EpochKey(s2r, 1)
	if bytes.Equal(k0, k1) {
		t.Fatal("different epochs must derive different keys")
	}
	if bytes.Equal(k0, EpochKey(r2s, 0)) {
		t.Fatal("epoch keys of the two directions must differ")
	}
	if !bytes.Equal(k0, EpochKey(s2r, 0)) {
		t.Fatal("EpochKey is not deterministic")
	}
}

func TestRatchetAdvancesIrreversibly(t *testing.T) {
	s0 := TrafficSecret(bytes.Repeat([]byte{1}, KeySize), SenderToReceiver)
	s1 := RatchetTrafficSecret(s0)
	if bytes.Equal(s0, s1) {
		t.Fatal("ratchet must advance the secret")
	}
	if !bytes.Equal(s1, RatchetTrafficSecret(s0)) {
		t.Fatal("ratchet is not deterministic")
	}
	if bytes.Equal(EpochKey(s0, 0), EpochKey(s1, 0)) {
		t.Fatal("an advanced secret must derive different epoch keys")
	}
}

func TestNonceStructure(t *testing.T) {
	n := nonceFor(0x0102, 0x03040506)
	// epoch in the high two bytes, src_index in the low four, zeros between.
	want := [NonceSize]byte{0x01, 0x02, 0, 0, 0, 0, 0, 0, 0x03, 0x04, 0x05, 0x06}
	if n != want {
		t.Fatalf("nonceFor = % x, want % x", n[:], want[:])
	}
	// Distinct (epoch, src_index) must give distinct nonces.
	if nonceFor(1, 5) == nonceFor(2, 5) || nonceFor(1, 5) == nonceFor(1, 6) {
		t.Fatal("nonce collision across epoch or src_index")
	}
}
