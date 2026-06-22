package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

// This file authenticates the flow's CONTROL plane — feedback reports and clock
// probe/echo messages — on an encrypted flow (docs/encryption.md §3). The handshake
// secures the media path, but feedback retires the sender's recovery state and clock
// messages steer the receiver's deadline frame; left unauthenticated, an off-path
// attacker who reads the cleartext flow id could forge them to DoS an "encrypted" flow.
//
// Each direction has its OWN key (derived from the handshake master by role) and a
// monotonic sequence number, so a captured datagram cannot be replayed (a sliding window
// rejects stale sequences) nor reflected into the opposite direction (the keys differ).
// Every control datagram is framed as a trailer so the leading wire type byte the host
// dispatches on stays at offset 0:  datagram ‖ Seq(8) ‖ HMAC-SHA256(key, datagram‖Seq)[:16].

// controlTagLen is the HMAC tag appended to an authenticated control datagram; controlSeqLen
// is the big-endian sequence-number suffix bound under that tag (anti-replay).
const (
	controlTagLen = macSize
	controlSeqLen = 8
)

// SendControlKey returns the key this side seals its outbound control datagrams under;
// RecvControlKey returns the key it verifies the peer's under. The two are distinct,
// directional keys (so one direction's datagram can never verify in the other), and one
// side's send key equals the other's receive key.
func (s *Session) SendControlKey() []byte {
	if s.Initiator {
		return expand(s.Master, label("control-i2r", 0))
	}
	return expand(s.Master, label("control-r2i", 0))
}

// RecvControlKey returns the key this side verifies the peer's control datagrams under.
func (s *Session) RecvControlKey() []byte {
	if s.Initiator {
		return expand(s.Master, label("control-r2i", 0))
	}
	return expand(s.Master, label("control-i2r", 0))
}

// ControlMAC authenticates one direction's control datagrams under a fixed key. It caches
// the keyed HMAC so the per-datagram warm path neither re-derives the key schedule nor
// allocates a new hash. Not safe for concurrent use; the host serializes it under its mutex.
type ControlMAC struct {
	h hash.Hash
}

// NewControlMAC returns a ControlMAC keyed by key (a directional control key).
func NewControlMAC(key []byte) *ControlMAC { return &ControlMAC{h: hmac.New(sha256.New, key)} }

// appendTag appends the truncated HMAC of body to dst using the cached hash.
func (c *ControlMAC) appendTag(dst, body []byte) []byte {
	var full [sha256.Size]byte
	c.h.Reset()
	c.h.Write(body)
	c.h.Sum(full[:0])
	return append(dst, full[:controlTagLen]...)
}

// Seal frames a control datagram: datagram ‖ seq ‖ tag(datagram‖seq). The caller supplies a
// strictly increasing seq so the peer can reject replays.
func (c *ControlMAC) Seal(seq uint64, datagram []byte) []byte {
	out := make([]byte, 0, len(datagram)+controlSeqLen+controlTagLen)
	out = append(out, datagram...)
	out = binary.BigEndian.AppendUint64(out, seq)
	return c.appendTag(out, out) // tag covers datagram‖seq (computed before the append lands)
}

// Open verifies the tag and returns the sequence number and inner datagram, or ok=false if
// the datagram is too short or the tag is wrong (the caller drops it). The caller MUST still
// feed seq through a ReplayWindow to reject replays. Constant time in the tag comparison.
func (c *ControlMAC) Open(datagram []byte) (seq uint64, body []byte, ok bool) {
	if len(datagram) < controlSeqLen+controlTagLen {
		return 0, nil, false
	}
	n := len(datagram) - controlTagLen // end of datagram‖seq
	var full [sha256.Size]byte
	c.h.Reset()
	c.h.Write(datagram[:n])
	c.h.Sum(full[:0])
	if !hmac.Equal(datagram[n:], full[:controlTagLen]) {
		return 0, nil, false
	}
	bodyEnd := n - controlSeqLen
	return binary.BigEndian.Uint64(datagram[bodyEnd:n]), datagram[:bodyEnd], true
}

// ReplayWindowSize is the span of the anti-replay sliding window: a sequence number more
// than this far below the highest accepted is rejected as too old.
const ReplayWindowSize = 64

// ReplayWindow rejects replayed and too-old control sequence numbers with a 64-slot sliding
// window (the IPsec/DTLS anti-replay algorithm, RFC 6479 §3). The zero value is ready to use
// — no sequence accepted yet — and Reset returns it to that state for a re-keyed session.
// Not safe for concurrent use; the host serializes it under its mutex.
type ReplayWindow struct {
	last   uint64 // highest sequence accepted so far
	bitmap uint64 // bit i (i>0) set ⇒ sequence (last-i) already seen
	seen   bool   // whether any sequence has been accepted yet
}

// Accept records seq and reports whether it is fresh — neither a replay of an
// already-seen sequence nor older than the window. A fresh sequence is marked seen.
func (w *ReplayWindow) Accept(seq uint64) bool {
	if !w.seen {
		w.seen, w.last, w.bitmap = true, seq, 1
		return true
	}
	if seq > w.last { // advance the window
		if shift := seq - w.last; shift >= ReplayWindowSize {
			w.bitmap = 1
		} else {
			w.bitmap = (w.bitmap << shift) | 1
		}
		w.last = seq
		return true
	}
	diff := w.last - seq
	if diff >= ReplayWindowSize {
		return false // too old to verify against the window
	}
	mask := uint64(1) << diff
	if w.bitmap&mask != 0 {
		return false // already seen — a replay
	}
	w.bitmap |= mask
	return true
}

// Reset clears the window so a re-keyed control channel starts its sequence space afresh.
func (w *ReplayWindow) Reset() { *w = ReplayWindow{} }
