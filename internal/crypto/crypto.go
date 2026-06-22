// Package crypto is Meld's pure, deterministic cryptographic core: the password
// key-derivation, the HKDF key schedule, the AEAD record layer, and the hybrid
// post-quantum key encapsulation the session host drives. Like internal/flow it is
// sans-I/O — it never reads a clock, opens a socket, or spawns a goroutine; the only
// non-determinism is fresh key generation, which draws from crypto/rand. The design
// and the spec citations are in docs/encryption.md.
//
// The split mirrors the rest of Meld: this package is pure functions over byte slices
// and opaque key handles; the host (internal/session) owns the keys, sequences the
// handshake, and does the I/O. The deterministic core (internal/flow) never imports
// this package and never sees a key — media payloads are AEAD-sealed at the host's
// Write boundary before they enter the coder (encrypt-then-code, so a recoding relay
// recombines ciphertext with no key) and opened after the coder delivers them.
//
// Modern by design, closing the three gaps SRT and RIST share: Argon2id instead of
// PBKDF2-SHA1 (memory-hard password stretch), an X25519 + ML-KEM-768 hybrid handshake
// for forward secrecy AND post-quantum resistance, and AEAD everywhere (never the
// unauthenticated AES-CTR SRT shipped for years).
package crypto

import "errors"

// Sentinel errors. User-facing strings are prefixed "meld: " per the project
// convention; malformed input always returns an error rather than panicking (the
// no-panic-in-library rule — fuzz-enforced).
var (
	// ErrShortKey is returned when key material is the wrong length.
	ErrShortKey = errors.New("meld: crypto: key material is the wrong length")
	// ErrOpen is returned when an AEAD message fails authentication: a tampered
	// ciphertext, a polluted repair symbol that corrupted the decoder's linear solve,
	// the wrong epoch key, or a mismatched nonce/AAD.
	ErrOpen = errors.New("meld: crypto: message authentication failed")
	// ErrNonceExhausted is returned when a Sealer would reuse a (key, nonce) pair — the
	// per-epoch source-index space is spent and the host must rekey (bump the epoch).
	ErrNonceExhausted = errors.New("meld: crypto: epoch nonce space exhausted, rekey")
	// ErrBadPublicKey is returned for a malformed peer X25519 or ML-KEM public key.
	ErrBadPublicKey = errors.New("meld: crypto: malformed peer public key")
	// ErrBadCiphertext is returned for a malformed ML-KEM ciphertext.
	ErrBadCiphertext = errors.New("meld: crypto: malformed KEM ciphertext")
	// ErrBadHandshake is returned for a malformed or unauthenticated handshake message —
	// a wrong length, a failed mac1 (the peer did not prove PSK knowledge), or a failed
	// key confirmation. It is deliberately uniform so it leaks nothing about which check
	// tripped.
	ErrBadHandshake = errors.New("meld: crypto: malformed or unauthenticated handshake message")
	// ErrHandshakeState is returned when a handshake method is called out of order.
	ErrHandshakeState = errors.New("meld: crypto: handshake step called out of order")
)

const (
	// KeySize is the symmetric key length used throughout: 256-bit — the size of a
	// ChaCha20-Poly1305 / AES-256 key, an HKDF-SHA256 output, an ML-KEM shared key, and
	// the derived PSK.
	KeySize = 32
	// NonceSize is the AEAD nonce length: 96-bit (RFC 8439).
	NonceSize = 12
)
