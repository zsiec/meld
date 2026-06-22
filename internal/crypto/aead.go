package crypto

import (
	"crypto/cipher"
	"encoding/binary"

	"golang.org/x/crypto/chacha20poly1305"
)

// Overhead is the per-source-symbol ciphertext expansion: the 16-byte Poly1305 tag
// (RFC 8439). The coded symbol is media_chunk + Overhead bytes; the tag is carried as
// coded payload and reconstructed by the decoder's linear solve like any other byte,
// so a forged repair symbol corrupts the recovered tag and Open rejects it end to end.
const Overhead = chacha20poly1305.Overhead

// Sealer encrypts the source symbols of one epoch under one key with ChaCha20-Poly1305
// (RFC 8439) and a deterministic (epoch, src_index) nonce — the send half of the
// encrypt-then-code record layer (docs/encryption.md §5). The host seals each media
// chunk here before it enters the coder. It requires strictly increasing source indices
// and refuses a repeat or wrap rather than reuse a (key, nonce) pair. Not safe for
// concurrent use; the host serializes the send path, as it does the core.
type Sealer struct {
	aead    cipher.AEAD
	epoch   uint16
	started bool
	last    uint32
}

// NewSealer returns a Sealer for one epoch under key (KeySize bytes). ChaCha20-Poly1305
// is the v1 record cipher: constant-time in pure Go with no AES hardware dependence.
func NewSealer(key []byte, epoch uint16) (*Sealer, error) {
	if len(key) != KeySize {
		return nil, ErrShortKey
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, ErrShortKey
	}
	return &Sealer{aead: aead, epoch: epoch}, nil
}

// Seal authenticates aad and encrypts plaintext for source index srcIndex, appending
// ciphertext‖tag (len(plaintext)+Overhead bytes) to dst. aad is the symbol's cleartext
// metadata and MUST bind the epoch (use AAD) so a ciphertext whose cleartext epoch was
// tampered fails authentication. That is symbol-IDENTITY binding, NOT key commitment:
// ChaCha20-Poly1305 is not a committing AEAD, and what actually stops an epoch's ciphertext
// from verifying under a different epoch is that each epoch has a distinct ratcheted key,
// not the AAD. It returns ErrNonceExhausted if srcIndex would not advance past the last
// sealed index (a repeat or a 2^32 wrap), forcing the host to rekey rather than reuse a
// nonce — the generalization of RIST's ErrIVExhausted discipline.
func (s *Sealer) Seal(dst, plaintext, aad []byte, srcIndex uint32) ([]byte, error) {
	if s.started && srcIndex <= s.last {
		return nil, ErrNonceExhausted
	}
	nonce := nonceFor(s.epoch, srcIndex)
	out := s.aead.Seal(dst, nonce[:], plaintext, aad)
	s.started, s.last = true, srcIndex
	return out, nil
}

// Opener authenticates and decrypts the source symbols of one epoch — the receive half
// of the record layer. The host opens each recovered (ciphertext‖tag) after the coder
// delivers it. Decryption order is unconstrained (the nonce is reconstructed per call),
// so it is safe to open out of order. Not safe for concurrent use.
type Opener struct {
	aead  cipher.AEAD
	epoch uint16
}

// NewOpener returns an Opener for one epoch under key (KeySize bytes).
func NewOpener(key []byte, epoch uint16) (*Opener, error) {
	if len(key) != KeySize {
		return nil, ErrShortKey
	}
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, ErrShortKey
	}
	return &Opener{aead: aead, epoch: epoch}, nil
}

// Open authenticates and decrypts one source symbol for srcIndex, appending the
// recovered media chunk to dst. It returns ErrOpen on any authentication failure — a
// tampered ciphertext, a polluted repair symbol that corrupted the linear solve, the
// wrong epoch key, or a mismatched nonce/AAD. The nonce is reconstructed from (epoch,
// srcIndex), so aad must equal what the Sealer used (including the bound epoch).
func (o *Opener) Open(dst, ciphertext, aad []byte, srcIndex uint32) ([]byte, error) {
	nonce := nonceFor(o.epoch, srcIndex)
	out, err := o.aead.Open(dst, nonce[:], ciphertext, aad)
	if err != nil {
		return nil, ErrOpen
	}
	return out, nil
}

// AAD builds the canonical additional-authenticated-data for a source symbol: the
// cleartext routing identity both ends know for ANY delivered symbol — flow, epoch, and
// source index — bound in so a tampered cleartext identity fails authentication (symbol-
// identity binding, not key commitment — see Seal). The same bytes go to Seal and Open.
// Only fields the RECEIVER can reconstruct for a recovered symbol (which carries no wire
// header) are bound; priority, deadline, and the frame descriptor are core-assigned and
// not bound — tampering them causes mis-delivery, not a confidentiality or integrity
// break — see docs/encryption.md §5.4.
func AAD(flow uint32, epoch uint16, srcIndex uint32) []byte {
	var b [AADSize]byte
	PutAAD(&b, flow, epoch, srcIndex)
	return b[:]
}

// AADSize is the length of the canonical additional-authenticated-data (flow ‖ epoch ‖
// source index).
const AADSize = 10

// PutAAD writes the canonical AAD (see AAD) into dst. Taking a fixed-size array pointer
// makes a too-short buffer a compile error rather than a runtime panic, so the warm path
// can reuse one scratch array instead of allocating per symbol.
func PutAAD(dst *[AADSize]byte, flow uint32, epoch uint16, srcIndex uint32) {
	binary.BigEndian.PutUint32(dst[0:], flow)
	binary.BigEndian.PutUint16(dst[4:], epoch)
	binary.BigEndian.PutUint32(dst[6:], srcIndex)
}
