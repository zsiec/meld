package crypto

import (
	"crypto/hmac"
	"crypto/rand"

	"golang.org/x/crypto/chacha20poly1305"
)

// This file is the mac2 anti-amplification cookie (docs/encryption.md §4.4; WireGuard's
// cookie, Donenfeld NDSS 2017). It is a SECOND, optional gate the responder raises only
// UNDER LOAD: it forces a return-routability round trip (the initiator must echo a cookie
// bound to its source address) before the responder commits handshake work, defeating a
// source-address-spoofing flood. mac1 (PSK knowledge, handshake.go) is the always-on
// gate; mac2 adds protection against an attacker who holds the PSK but spoofs addresses —
// a narrow threat for point-to-point contribution, hence load-gated.

const (
	cookieSize     = macSize
	cookieNonceLen = chacha20poly1305.NonceSizeX                             // 24 (XChaCha: random nonce safe)
	cookieReplyLen = cookieNonceLen + cookieSize + chacha20poly1305.Overhead // 24+16+16 = 56
)

// CookieChecker is the responder's cookie state. It keeps the CURRENT and the PREVIOUS
// secret so a cookie issued just before a rotation is still accepted for one rotation
// window — otherwise a rotation landing between the cookie reply and the initiator's
// retry would reject a legitimate retry and livelock the handshake (WireGuard keeps two
// likewise). Rotate periodically so an observed cookie expires within two windows. Not
// safe for concurrent use; the host serializes it under its mutex.
type CookieChecker struct {
	cur  [32]byte
	prev [32]byte
}

// NewCookieChecker returns a checker with a fresh random secret.
func NewCookieChecker() (*CookieChecker, error) {
	c := &CookieChecker{}
	if _, err := rand.Read(c.cur[:]); err != nil {
		return nil, err
	}
	c.prev = c.cur
	return c, nil
}

// Rotate shifts the current secret to previous and draws a fresh current. A cookie under
// the just-superseded secret stays valid for this one window. It draws the new secret into
// a temporary and commits both fields only on success, so a crypto/rand failure leaves the
// existing cur/prev intact rather than installing a half-written, low-entropy secret.
func (c *CookieChecker) Rotate() error {
	var next [32]byte
	if _, err := rand.Read(next[:]); err != nil {
		return err
	}
	c.prev, c.cur = c.cur, next
	return nil
}

// cookie is the responder's cookie for a peer identity (its source-address bytes) under a
// given secret: HMAC(secret, peerID), truncated. It is the same keyed-truncated-HMAC as the
// handshake's mac(); cookieSize == macSize ties the truncation, so it delegates rather than
// re-implement the construction.
func cookie(secret, peerID []byte) []byte {
	return mac(secret, peerID)
}

// Reply seals a cookie-reply payload for the initiator: the cookie for peerID, encrypted
// under a PSK-derived key (so only a PSK holder learns it) with a random nonce.
func (c *CookieChecker) Reply(psk, peerID []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(cookieReplyKey(psk))
	if err != nil {
		return nil, ErrShortKey
	}
	out := make([]byte, cookieNonceLen, cookieReplyLen)
	if _, err := rand.Read(out); err != nil {
		return nil, err
	}
	return aead.Seal(out, out[:cookieNonceLen], cookie(c.cur[:], peerID), nil), nil
}

// Valid reports whether msg1's mac2 matches the cookie for peerID under the current OR
// the previous secret — i.e. the initiator proved it received the cookie at its claimed
// source address, accepting one rotation window so a rotation between reply and retry
// does not reject a legitimate handshake.
func (c *CookieChecker) Valid(msg1, peerID []byte) bool {
	return verifyMac2(msg1, cookie(c.cur[:], peerID)) || verifyMac2(msg1, cookie(c.prev[:], peerID))
}

// OpenCookieReply decrypts a cookie reply under the PSK, returning the cookie the
// initiator must echo via mac2 (WriteMessage1WithCookie).
func OpenCookieReply(psk, reply []byte) ([]byte, error) {
	if len(reply) != cookieReplyLen {
		return nil, ErrOpen
	}
	aead, err := chacha20poly1305.NewX(cookieReplyKey(psk))
	if err != nil {
		return nil, ErrShortKey
	}
	ck, err := aead.Open(nil, reply[:cookieNonceLen], reply[cookieNonceLen:], nil)
	if err != nil {
		return nil, ErrOpen
	}
	return ck, nil
}

// cookieReplyKey derives the cookie-reply AEAD key from the PSK.
func cookieReplyKey(psk []byte) []byte { return expand(extract(psk, nil), label("cookie", 0)) }

// mac2Of computes the cookie MAC over the part of message 1 before mac2 (pubs ‖ mac1) — the
// same keyed-truncated-HMAC as the handshake's mac(), keyed by the cookie.
func mac2Of(cookie, body []byte) []byte {
	return mac(cookie, body)
}

// verifyMac2 checks message 1's trailing mac2 against the cookie.
func verifyMac2(msg1, cookie []byte) bool {
	if len(msg1) != msg1Len {
		return false
	}
	body, got := msg1[:msg1Len-macSize], msg1[msg1Len-macSize:]
	return hmac.Equal(got, mac2Of(cookie, body))
}
