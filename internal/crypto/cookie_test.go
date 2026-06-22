package crypto

import "testing"

// TestCookieRoundTrip exercises the mac2 anti-amplification path: the responder seals a
// cookie reply, the initiator opens it under the PSK and echoes it via mac2, and the
// responder accepts that mac2 only for the right source address — the return-routability
// proof. It also checks the negative cases the gate relies on.
func TestCookieRoundTrip(t *testing.T) {
	psk := testKey(0x42)
	peerID := []byte("203.0.113.7:51000")

	cc, err := NewCookieChecker()
	if err != nil {
		t.Fatalf("NewCookieChecker: %v", err)
	}

	reply, err := cc.Reply(psk, peerID)
	if err != nil {
		t.Fatalf("Reply: %v", err)
	}
	if len(reply) != cookieReplyLen {
		t.Fatalf("cookie reply length %d, want %d", len(reply), cookieReplyLen)
	}
	cookie, err := OpenCookieReply(psk, reply)
	if err != nil {
		t.Fatalf("OpenCookieReply: %v", err)
	}

	init, _ := NewInitiator(psk, nil)
	if _, err := init.WriteMessage1(); err != nil {
		t.Fatalf("WriteMessage1: %v", err)
	}
	m1, err := init.WriteMessage1WithCookie(cookie)
	if err != nil {
		t.Fatalf("WriteMessage1WithCookie: %v", err)
	}
	if !cc.Valid(m1, peerID) {
		t.Fatal("a valid cookie mac2 was rejected")
	}

	// Negative cases.
	if cc.Valid(m1, []byte("198.51.100.9:51000")) {
		t.Fatal("cookie accepted for the wrong source address (no return-routability)")
	}
	zero, _ := NewInitiator(psk, nil)
	m1zero, _ := zero.WriteMessage1() // mac2 = zeros
	if cc.Valid(m1zero, peerID) {
		t.Fatal("a zero mac2 was accepted under load")
	}
	if _, err := OpenCookieReply(testKey(0x99), reply); err != ErrOpen {
		t.Fatalf("wrong-PSK cookie open: got %v, want ErrOpen", err)
	}
	// A cookie survives ONE rotation (the grace window that keeps a retry valid across a
	// rotation boundary) but not two.
	if err := cc.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if !cc.Valid(m1, peerID) {
		t.Fatal("cookie should still be valid after one rotation (grace window)")
	}
	if err := cc.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if cc.Valid(m1, peerID) {
		t.Fatal("cookie must be invalid after two rotations")
	}
}
