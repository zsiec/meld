package crypto

import (
	"bytes"
	"testing"
)

func testKey(b byte) []byte { return bytes.Repeat([]byte{b}, KeySize) }

func TestSealOpenRoundTrip(t *testing.T) {
	key := testKey(0x11)
	const epoch, srcIndex = 7, 42
	aad := AAD(0xDEADBEEF, epoch, srcIndex)
	plain := []byte("one 1316-byte media chunk would go here")

	s, err := NewSealer(key, epoch)
	if err != nil {
		t.Fatalf("NewSealer: %v", err)
	}
	ct, err := s.Seal(nil, plain, aad, srcIndex)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if len(ct) != len(plain)+Overhead {
		t.Fatalf("ciphertext len %d, want %d", len(ct), len(plain)+Overhead)
	}
	if bytes.Contains(ct, plain) {
		t.Fatal("plaintext appears verbatim in the ciphertext")
	}

	o, err := NewOpener(key, epoch)
	if err != nil {
		t.Fatalf("NewOpener: %v", err)
	}
	got, err := o.Open(nil, ct, aad, srcIndex)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("round-trip mismatch: %q != %q", got, plain)
	}
}

func TestOpenRejects(t *testing.T) {
	const epoch, srcIndex = 7, 42
	key := testKey(0x11)
	aad := AAD(1, epoch, srcIndex)
	plain := []byte("payload")
	s, _ := NewSealer(key, epoch)
	ct, _ := s.Seal(nil, plain, aad, srcIndex)

	cases := []struct {
		name string
		open func() error
	}{
		{"tampered ciphertext", func() error {
			bad := append([]byte(nil), ct...)
			bad[0] ^= 0x80
			o, _ := NewOpener(key, epoch)
			_, err := o.Open(nil, bad, aad, srcIndex)
			return err
		}},
		{"wrong key (same epoch ⇒ key commitment)", func() error {
			o, _ := NewOpener(testKey(0x22), epoch)
			_, err := o.Open(nil, ct, aad, srcIndex)
			return err
		}},
		{"wrong epoch (nonce mismatch)", func() error {
			o, _ := NewOpener(key, epoch+1)
			_, err := o.Open(nil, ct, aad, srcIndex)
			return err
		}},
		{"wrong src_index (nonce mismatch)", func() error {
			o, _ := NewOpener(key, epoch)
			_, err := o.Open(nil, ct, aad, srcIndex+1)
			return err
		}},
		{"wrong aad (different flow)", func() error {
			o, _ := NewOpener(key, epoch)
			_, err := o.Open(nil, ct, AAD(2, epoch, srcIndex), srcIndex) // sealed under flow 1
			return err
		}},
		{"truncated ciphertext", func() error {
			o, _ := NewOpener(key, epoch)
			_, err := o.Open(nil, ct[:Overhead-1], aad, srcIndex)
			return err
		}},
	}
	for _, c := range cases {
		if err := c.open(); err != ErrOpen {
			t.Errorf("%s: got %v, want ErrOpen", c.name, err)
		}
	}
}

func TestSealRefusesNonceReuse(t *testing.T) {
	s, _ := NewSealer(testKey(1), 0)
	aad := AAD(1, 0, 0)
	if _, err := s.Seal(nil, []byte("x"), aad, 5); err != nil {
		t.Fatalf("first seal: %v", err)
	}
	if _, err := s.Seal(nil, []byte("x"), aad, 5); err != ErrNonceExhausted {
		t.Fatalf("repeated src_index: got %v, want ErrNonceExhausted", err)
	}
	if _, err := s.Seal(nil, []byte("x"), aad, 3); err != ErrNonceExhausted {
		t.Fatalf("lower src_index (wrap): got %v, want ErrNonceExhausted", err)
	}
	if _, err := s.Seal(nil, []byte("x"), aad, 6); err != nil {
		t.Fatalf("advancing src_index: %v", err)
	}
}

func TestShortKeyRejected(t *testing.T) {
	if _, err := NewSealer(make([]byte, KeySize-1), 0); err != ErrShortKey {
		t.Errorf("NewSealer short key: got %v, want ErrShortKey", err)
	}
	if _, err := NewOpener(make([]byte, KeySize+1), 0); err != ErrShortKey {
		t.Errorf("NewOpener long key: got %v, want ErrShortKey", err)
	}
}
