package crypto

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"

	"golang.org/x/crypto/argon2"
)

// extract is HKDF-Extract-SHA256 (RFC 5869 §2.2): a salt and input keying material to
// a pseudorandom key. It is the Noise-style MixKey step when salt is the running
// chaining key. HMAC over any input never errors, so the documented error is discarded.
func extract(ikm, salt []byte) []byte {
	prk, _ := hkdf.Extract(sha256.New, ikm, salt)
	return prk
}

// expand is HKDF-Expand-SHA256 (RFC 5869 §2.3) to KeySize bytes. The length is fixed
// and far inside HKDF's 255·HashLen limit, so the documented error is impossible here
// and discarded.
func expand(prk []byte, info string) []byte {
	out, _ := hkdf.Expand(sha256.New, prk, info, KeySize)
	return out
}

// label builds an HKDF info string: a format-versioned tag plus a big-endian context
// number, so distinct derivations (directions, epochs) never share output.
func label(tag string, ctx uint32) string {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], ctx)
	return "meld-v1-" + tag + string(b[:])
}

// Direction names the two one-way traffic keys of a flow. The directions derive
// independent keys so they never share a (key, nonce) space.
type Direction uint8

const (
	// SenderToReceiver keys the media path.
	SenderToReceiver Direction = 0
	// ReceiverToSender keys the reverse path (feedback, when encrypted).
	ReceiverToSender Direction = 1
)

// Argon2idParams configures the memory-hard passphrase stretch (RFC 9106). Both
// endpoints MUST agree on the parameters and the salt to derive the same PSK. The
// defaults follow RFC 9106 §4's second recommended option; raise Memory on provisioned
// hardware — the PSK is a long-term value derived once per peer and cached, so a strong
// profile costs only one startup stretch.
type Argon2idParams struct {
	Time    uint32 // iterations (t)
	Memory  uint32 // memory in KiB (m)
	Threads uint8  // lanes (p)
}

// DefaultArgon2idParams returns the RFC 9106 §4 second-recommended profile
// (64 MiB, t=3, p=4).
func DefaultArgon2idParams() Argon2idParams {
	return Argon2idParams{Time: 3, Memory: 64 * 1024, Threads: 4}
}

// WithDefaults returns p with each zero field replaced by the DefaultArgon2idParams value,
// so a zero or partially-specified Params (e.g. only Memory set) is completed to a valid,
// in-range profile. DerivePSK applies it before calling argon2.IDKey, which PANICS on a
// zero Time or Threads rather than erroring — so the defaulting, not a length check, is what
// keeps a degenerate Params out of the library-no-panic surface. Both endpoints must still
// agree on the effective parameters to derive the same PSK; this only fills unset fields.
func (p Argon2idParams) WithDefaults() Argon2idParams {
	d := DefaultArgon2idParams()
	if p.Time == 0 {
		p.Time = d.Time
	}
	if p.Memory == 0 {
		p.Memory = d.Memory
	}
	if p.Threads == 0 {
		p.Threads = d.Threads
	}
	return p
}

// DerivePSK stretches a (possibly human-entropy) passphrase into a KeySize pre-shared
// key with Argon2id (RFC 9106), salted to defeat precomputation. It is the modern
// replacement for SRT/RIST's PBKDF2: memory-hard, so each offline guess costs memory ×
// time, not a cheap hash. The PSK is long-term — derive it once per peer and cache it;
// forward secrecy comes from the ephemeral hybrid handshake, not this value. A zero field
// in p is filled from DefaultArgon2idParams (WithDefaults) so argon2.IDKey never panics.
func DerivePSK(passphrase, salt []byte, p Argon2idParams) []byte {
	p = p.WithDefaults()
	return argon2.IDKey(passphrase, salt, p.Time, p.Memory, p.Threads, KeySize)
}

// TrafficSecret derives a directional traffic secret from the handshake master secret
// (HKDF-Expand, RFC 5869). The two directions are independent.
func TrafficSecret(masterSecret []byte, dir Direction) []byte {
	return expand(masterSecret, label("traffic", uint32(dir)))
}

// EpochKey derives the per-epoch AEAD key from a directional traffic secret and the
// wire epoch number — the key an epoch's symbols are sealed under (the epoch field is
// Meld's key-update counter). For intra-session forward secrecy, ratchet the traffic
// secret forward each epoch (RatchetTrafficSecret) and derive from the advanced secret,
// discarding the old one; without the ratchet, a leaked traffic secret exposes every
// epoch.
func EpochKey(trafficSecret []byte, epoch uint16) []byte {
	return expand(trafficSecret, label("epoch", uint32(epoch)))
}

// RatchetTrafficSecret advances a directional traffic secret to the next one and
// returns it. Discard the input afterward: a one-way HKDF step means a later compromise
// of the advanced secret cannot derive any earlier epoch's key (intra-session forward
// secrecy).
func RatchetTrafficSecret(trafficSecret []byte) []byte {
	return expand(trafficSecret, label("ratchet", 0))
}

// nonceFor builds the deterministic 96-bit record nonce for a source symbol: the epoch
// in the high two bytes, the source index in the low four. With a per-epoch key this is
// unique per (key, nonce) as long as src_index does not wrap within an epoch — the
// Sealer refuses before it does. The nonce never travels on the wire: the receiver
// reconstructs it from the delivered source id and the current epoch.
func nonceFor(epoch uint16, srcIndex uint32) [NonceSize]byte {
	var n [NonceSize]byte
	binary.BigEndian.PutUint16(n[0:], epoch)
	binary.BigEndian.PutUint32(n[NonceSize-4:], srcIndex)
	return n
}
