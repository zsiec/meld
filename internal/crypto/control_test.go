package crypto

import (
	"bytes"
	"testing"
)

// TestControlMACRoundTrip checks the control-plane MAC frames a datagram, authenticates it
// under the keyed ControlMAC, and — critically — leaves the leading type byte at offset 0 so
// the host's wire.PeekType dispatch still works on a sealed datagram (the seq + tag are a
// trailer).
func TestControlMACRoundTrip(t *testing.T) {
	c := NewControlMAC(testKey(0x11))
	msg := []byte{0x42, 'f', 'e', 'e', 'd', 'b', 'a', 'c', 'k'} // first byte = a wire type tag

	sealed := c.Seal(7, msg)
	if len(sealed) != len(msg)+controlSeqLen+controlTagLen {
		t.Fatalf("sealed length %d, want %d", len(sealed), len(msg)+controlSeqLen+controlTagLen)
	}
	if sealed[0] != msg[0] {
		t.Fatalf("type byte moved: sealed[0]=%#x, want %#x (PeekType must still work)", sealed[0], msg[0])
	}

	seq, body, ok := c.Open(sealed)
	if !ok {
		t.Fatal("Open rejected a valid datagram")
	}
	if seq != 7 {
		t.Fatalf("recovered seq %d, want 7", seq)
	}
	if !bytes.Equal(body, msg) {
		t.Fatalf("recovered body %x, want %x", body, msg)
	}
}

// TestControlMACRejects covers the negative paths: a wrong key, a tampered body or tag, and
// a too-short datagram are all rejected.
func TestControlMACRejects(t *testing.T) {
	c := NewControlMAC(testKey(0x22))
	sealed := c.Seal(1, []byte("clock-echo"))

	if _, _, ok := NewControlMAC(testKey(0x23)).Open(sealed); ok {
		t.Fatal("Open accepted a datagram under the wrong key")
	}
	for _, pos := range []int{0, len(sealed) / 2, len(sealed) - 1} {
		bad := append([]byte(nil), sealed...)
		bad[pos] ^= 0x80
		if _, _, ok := c.Open(bad); ok {
			t.Fatalf("Open accepted a datagram tampered at %d", pos)
		}
	}
	for _, n := range []int{0, controlSeqLen, controlSeqLen + controlTagLen - 1} {
		if _, _, ok := c.Open(make([]byte, n)); ok {
			t.Fatalf("Open accepted a %d-byte (too short) datagram", n)
		}
	}
}

// TestReplayWindow exercises the anti-replay window: the first sequence is accepted, strictly
// increasing sequences advance the window, an exact replay is rejected, an out-of-order but
// in-window sequence is accepted once, and a sequence older than the window is rejected.
func TestReplayWindow(t *testing.T) {
	var w ReplayWindow

	if !w.Accept(100) {
		t.Fatal("first sequence rejected")
	}
	if w.Accept(100) {
		t.Fatal("exact replay accepted")
	}
	if !w.Accept(101) || !w.Accept(102) {
		t.Fatal("increasing sequences rejected")
	}
	// Out-of-order but within the window: accept once, then reject the replay.
	if !w.Accept(99) {
		t.Fatal("in-window out-of-order sequence rejected")
	}
	if w.Accept(99) {
		t.Fatal("in-window replay accepted")
	}
	// Jump far ahead, then a sequence older than the window is rejected.
	if !w.Accept(102 + ReplayWindowSize + 10) {
		t.Fatal("forward jump rejected")
	}
	if w.Accept(101) {
		t.Fatal("too-old sequence accepted after the window advanced past it")
	}

	// Reset clears the window so a re-keyed channel starts its sequence space afresh.
	w.Reset()
	if !w.Accept(0) {
		t.Fatal("Reset did not clear the window (a fresh sequence was rejected)")
	}
}

// TestControlKeysDirectional checks the control keys are directional and line up across the
// handshake: each side's send key equals the peer's receive key, the two directions differ
// (so a datagram cannot be reflected into the other direction), and a datagram sealed by one
// end opens on the other.
func TestControlKeysDirectional(t *testing.T) {
	psk := testKey(0x42)
	pro := []byte("flow")
	isess, rsess, err := runHandshake(t, psk, psk, pro, pro)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if !bytes.Equal(isess.SendControlKey(), rsess.RecvControlKey()) {
		t.Fatal("initiator send control key != responder recv control key")
	}
	if !bytes.Equal(isess.RecvControlKey(), rsess.SendControlKey()) {
		t.Fatal("initiator recv control key != responder send control key")
	}
	if bytes.Equal(isess.SendControlKey(), isess.RecvControlKey()) {
		t.Fatal("the two control directions must use distinct keys (anti-reflection)")
	}
	// A datagram the initiator seals opens under the responder's matching recv key, and a
	// datagram sealed for the wrong direction does not.
	sealed := NewControlMAC(isess.SendControlKey()).Seal(3, []byte{0x42, 'f', 'b'})
	if _, _, ok := NewControlMAC(rsess.RecvControlKey()).Open(sealed); !ok {
		t.Fatal("a control datagram sealed by the initiator did not open under the responder's recv key")
	}
	if _, _, ok := NewControlMAC(rsess.SendControlKey()).Open(sealed); ok {
		t.Fatal("a control datagram verified under the wrong-direction key (reflection)")
	}
}
