package crypto

import (
	"crypto/ecdh"
	"crypto/mlkem"
	"crypto/rand"
)

const (
	// X25519KeySize is the X25519 public/private key length (RFC 7748).
	X25519KeySize = 32
	// MLKEM768EncapKeySize is the ML-KEM-768 encapsulation (public) key length (FIPS 203).
	MLKEM768EncapKeySize = 1184
	// MLKEM768CiphertextSize is the ML-KEM-768 ciphertext length (FIPS 203).
	MLKEM768CiphertextSize = 1088
)

// EphemeralKeys is one party's ephemeral hybrid key material for a handshake: an X25519
// keypair (the classical, forward-secret half) and an ML-KEM-768 keypair (the
// post-quantum half). The INITIATOR generates these (NewInitiator) and publishes the
// public parts in message 1; the responder runs the encapsulating step against those
// published pubs (the Initiate free function), and the initiator later recovers the shared
// secrets from the responder's reply with the private parts (the Respond method, decapsulate
// + ECDH). Fresh per handshake, so a compromise of one handshake's keys does not affect any
// other.
type EphemeralKeys struct {
	x25519 *ecdh.PrivateKey
	mlkem  *mlkem.DecapsulationKey768
}

// GenerateEphemeralKeys draws a fresh hybrid keypair from crypto/rand — the one source
// of non-determinism in this package.
func GenerateEphemeralKeys() (*EphemeralKeys, error) {
	xk, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	mk, err := mlkem.GenerateKey768()
	if err != nil {
		return nil, err
	}
	return &EphemeralKeys{x25519: xk, mlkem: mk}, nil
}

// Public returns this party's wire public material: the 32-byte X25519 public key
// (RFC 7748) and the 1184-byte ML-KEM-768 encapsulation key (FIPS 203).
func (k *EphemeralKeys) Public() (x25519Pub, mlkemEncapKey []byte) {
	return k.x25519.PublicKey().Bytes(), k.mlkem.EncapsulationKey().Bytes()
}

// Initiate performs the initiator's hybrid step against the responder's published
// public material: an X25519 ECDH (RFC 7748) and an ML-KEM-768 encapsulation (FIPS
// 203). It returns the initiator's X25519 public key and the ML-KEM ciphertext to send
// the responder, plus the two raw shared secrets to fold into the key schedule (the
// handshake mixes each into the running chaining key via mixKey, then finalize derives
// the master). A malformed responder key yields ErrBadPublicKey, never a panic.
func Initiate(respX25519Pub, respMLKEMEncapKey []byte) (initX25519Pub, mlkemCiphertext, ssX25519, ssMLKEM []byte, err error) {
	if len(respX25519Pub) != X25519KeySize {
		return nil, nil, nil, nil, ErrBadPublicKey
	}
	peerX, err := ecdh.X25519().NewPublicKey(respX25519Pub)
	if err != nil {
		return nil, nil, nil, nil, ErrBadPublicKey
	}
	ek, err := mlkem.NewEncapsulationKey768(respMLKEMEncapKey)
	if err != nil {
		return nil, nil, nil, nil, ErrBadPublicKey
	}
	myX, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	ssX, err := myX.ECDH(peerX)
	if err != nil {
		return nil, nil, nil, nil, ErrBadPublicKey
	}
	ssK, ct := ek.Encapsulate()
	return myX.PublicKey().Bytes(), ct, ssX, ssK, nil
}

// Respond performs the responder's matching step against the initiator's X25519 public
// key and ML-KEM ciphertext: an X25519 ECDH and an ML-KEM-768 decapsulation, yielding
// the same two shared secrets the initiator derived. ML-KEM applies implicit rejection
// (FIPS 203): a tampered ciphertext decapsulates to a pseudo-random secret rather than
// erroring, so the two sides simply derive different masters and the AEAD layer rejects
// — there is no decryption oracle. Malformed inputs yield an error, never a panic.
func (k *EphemeralKeys) Respond(initX25519Pub, mlkemCiphertext []byte) (ssX25519, ssMLKEM []byte, err error) {
	if len(initX25519Pub) != X25519KeySize {
		return nil, nil, ErrBadPublicKey
	}
	peerX, err := ecdh.X25519().NewPublicKey(initX25519Pub)
	if err != nil {
		return nil, nil, ErrBadPublicKey
	}
	ssX, err := k.x25519.ECDH(peerX)
	if err != nil {
		return nil, nil, ErrBadPublicKey
	}
	ssK, err := k.mlkem.Decapsulate(mlkemCiphertext)
	if err != nil {
		return nil, nil, ErrBadCiphertext
	}
	return ssX, ssK, nil
}

// The hybrid master-secret combiner is NOT a free function here: the live handshake
// derives the master incrementally via symmetricState.mixKey (the X25519 then ML-KEM-768
// shared secrets) and finalize (binding the transcript) in handshake.go — one
// implementation, no second copy to drift from. See docs/encryption.md §4.3.
