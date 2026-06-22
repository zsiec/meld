package crypto

import (
	"bytes"
	"testing"
)

// FuzzOpenNoPanic: arbitrary bytes to Open must never panic and must reject (the
// no-panic-in-library rule for any decoder/decryptor on hostile input).
func FuzzOpenNoPanic(f *testing.F) {
	f.Add([]byte{}, []byte{}, uint32(0))
	f.Add(bytes.Repeat([]byte{0xFF}, 64), []byte("aad"), uint32(1))

	o, err := NewOpener(testKey(0x5A), 3)
	if err != nil {
		f.Fatalf("NewOpener: %v", err)
	}
	f.Fuzz(func(t *testing.T, ciphertext, aad []byte, srcIndex uint32) {
		if _, err := o.Open(nil, ciphertext, aad, srcIndex); err == nil {
			t.Fatal("Open accepted arbitrary fuzz bytes as authentic")
		}
	})
}

// FuzzSealOpenRoundTrip: anything sealed opens back byte-exact under the matching key,
// epoch, src_index, and AAD.
func FuzzSealOpenRoundTrip(f *testing.F) {
	f.Add([]byte("media"), uint16(0), uint32(0))
	f.Add([]byte{}, uint16(65535), uint32(4294967295))

	key := testKey(0x33)
	f.Fuzz(func(t *testing.T, plaintext []byte, epoch uint16, srcIndex uint32) {
		s, err := NewSealer(key, epoch)
		if err != nil {
			t.Fatalf("NewSealer: %v", err)
		}
		aad := AAD(0xABCD1234, epoch, srcIndex)
		ct, err := s.Seal(nil, plaintext, aad, srcIndex)
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		o, _ := NewOpener(key, epoch)
		got, err := o.Open(nil, ct, aad, srcIndex)
		if err != nil {
			t.Fatalf("Open of own ciphertext failed: %v", err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("round-trip mismatch: % x != % x", got, plaintext)
		}
	})
}

// FuzzRespondNoPanic: arbitrary peer public key and ciphertext bytes to the responder's
// hybrid step must never panic — they error or decapsulate to a (useless) secret.
func FuzzRespondNoPanic(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, X25519KeySize), make([]byte, MLKEM768CiphertextSize))

	resp, err := GenerateEphemeralKeys()
	if err != nil {
		f.Fatalf("GenerateEphemeralKeys: %v", err)
	}
	f.Fuzz(func(t *testing.T, x25519Pub, ciphertext []byte) {
		_, _, _ = resp.Respond(x25519Pub, ciphertext) // must not panic
	})
}

// FuzzInitiateNoPanic: arbitrary peer X25519 public key and ML-KEM encapsulation key bytes
// to the encapsulating step (run by the responder against message-1 pubs) must never panic —
// a malformed key errors via the stdlib length checks rather than crashing.
func FuzzInitiateNoPanic(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, X25519KeySize), make([]byte, MLKEM768EncapKeySize))

	f.Fuzz(func(t *testing.T, x25519Pub, encapKey []byte) {
		_, _, _, _, _ = Initiate(x25519Pub, encapKey) // must not panic
	})
}

// FuzzControlMACOpenNoPanic: arbitrary control-plane datagram bytes to ControlMAC.Open must
// never panic. Open is the decoder an off-path attacker reaches by forging feedback/clock
// messages on an encrypted flow, so the no-panic-in-library rule applies to every length.
func FuzzControlMACOpenNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xAA}, controlSeqLen+controlTagLen))
	f.Add(bytes.Repeat([]byte{0x5C}, 200))

	c := NewControlMAC(testKey(0x11))
	f.Fuzz(func(t *testing.T, datagram []byte) {
		if _, _, ok := c.Open(datagram); ok {
			t.Fatal("ControlMAC.Open accepted arbitrary fuzz bytes as authentic")
		}
	})
}

// FuzzOpenCookieReplyNoPanic: arbitrary PSK and reply bytes to OpenCookieReply (the decoder
// for a cookie reply anyone can inject at the initiator) must never panic — a wrong length or
// a forged ciphertext errors rather than crashing.
func FuzzOpenCookieReplyNoPanic(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add(testKey(0x22), make([]byte, cookieReplyLen))

	f.Fuzz(func(t *testing.T, psk, reply []byte) {
		_, _ = OpenCookieReply(psk, reply) // must not panic
	})
}

// FuzzCookieValidNoPanic: arbitrary message-1 and peerID bytes to CookieChecker.Valid (which
// runs verifyMac2 on the attacker-supplied mac2 trailer under load) must never panic.
func FuzzCookieValidNoPanic(f *testing.F) {
	f.Add([]byte{}, []byte{})
	f.Add(make([]byte, msg1Len), []byte("198.51.100.7"))

	c, err := NewCookieChecker()
	if err != nil {
		f.Fatalf("NewCookieChecker: %v", err)
	}
	f.Fuzz(func(t *testing.T, msg1, peerID []byte) {
		if c.Valid(msg1, peerID) {
			t.Fatal("CookieChecker.Valid accepted arbitrary fuzz bytes as a valid cookie")
		}
	})
}
