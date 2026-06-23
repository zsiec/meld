package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// This file is the Meld handshake: a 2-message, PSK-authenticated, forward-secret,
// post-quantum-hybrid key agreement (docs/encryption.md §4). It is an NNpsk0-style
// Noise pattern (The Noise Protocol Framework) with the Diffie-Hellman exchange
// replaced by a hybrid X25519 + ML-KEM-768 KEM — the generic DH→KEM substitution proven
// to preserve confidentiality, authenticity, and forward secrecy (PQNoise, CCS 2022).
//
// The construction, as running transcript hash h and chaining key ck:
//   - initialize h = ck = SHA-256(protocol name); mix the prologue (the flow context).
//   - mixKey(psk) up front (psk0): every derived key requires the PSK, so a non-PSK
//     holder can neither read traffic nor forge a handshake.
//   - message 1 (initiator → responder): the initiator's ephemeral X25519 public key
//     and ML-KEM-768 encapsulation key, mixed into h, with a mac1 keyed by the PSK so
//     the responder rejects a non-PSK flood before any asymmetric work (the cheap
//     anti-amplification gate; the message is also larger than the reply, so the
//     amplification factor is <1). Freshness across a sender restart is NOT carried in
//     message 1: the host re-handshakes commit-after-confirm (a new handshake becomes a
//     PENDING session that is promoted only once traffic authenticates under it), so a
//     replayed message 1 can never displace a live session — see internal/session.
//   - message 2 (responder → initiator): the responder's ephemeral X25519 public key
//     and the ML-KEM ciphertext, mixing BOTH shared secrets into ck (HKDF-chaining,
//     the sound hybrid combiner), then a key-confirmation tag and a mac1.
//   - both ends fold the full transcript h into ck to produce the master secret; the
//     host derives directional traffic and per-epoch keys from it (kdf.go).
//
// mac2 (the WireGuard cookie for source-address return-routability under load) is a host
// policy layered on top; mac1 + the large first message already foreclose amplification
// from any party that does not hold the PSK, the dominant threat for a provisioned
// point-to-point contribution link. Pure and deterministic apart from ephemeral key
// generation; drive both halves with the message bytes — no I/O.

// handshakeName identifies the protocol; it seeds the transcript so a peer speaking a
// different version/suite derives an incompatible key and the handshake simply fails.
const handshakeName = "Meld_v1_NNpsk0_X25519+MLKEM768_ChaCha20Poly1305_SHA256"

const (
	macSize     = 16 // truncated HMAC-SHA256 mac1 / mac2 / confirmation tag length
	confirmSize = 16
	// msg1 = pubs ‖ epochSize ‖ mac1 (PSK proof) ‖ mac2 (cookie; zero unless the responder is
	// under load). The initiator (the media sender) carries its EpochSize in message 1 so the
	// responder (receiver) adopts it for opening rather than both ends deriving it from
	// independently-configured values — a mismatch otherwise silently mis-keys every symbol past
	// the first epoch (the sender-authoritative principle, like the coding-window width). mac1
	// covers pubs‖epochSize and the value is folded into the transcript hash, so a tampered
	// epochSize fails the handshake (the confirmation tag won't verify). mac2 covers
	// pubs‖epochSize‖mac1 (the cookie.go anti-amplification gate). msg2 = responder pubs ‖ ML-KEM
	// ciphertext ‖ key-confirmation ‖ mac1.
	msg1PubsLen  = X25519KeySize + MLKEM768EncapKeySize                           // 1216 (pubs only; HandshakeInitKeys)
	epochSizeLen = 4                                                              // big-endian uint32 EpochSize
	msg1BodyLen  = msg1PubsLen + epochSizeLen                                     // 1220 (mac1-covered, transcript-mixed)
	msg1Len      = msg1BodyLen + macSize + macSize                                // 1252
	msg2Len      = X25519KeySize + MLKEM768CiphertextSize + confirmSize + macSize // 1152
)

// symmetricState is the running transcript hash and chaining key of a handshake.
type symmetricState struct {
	h  [32]byte
	ck []byte
}

// newSymmetric seeds h and ck from the protocol name and mixes the prologue (the
// caller's binding context, e.g. the flow id).
func newSymmetric(prologue []byte) *symmetricState {
	sum := sha256.Sum256([]byte(handshakeName))
	s := &symmetricState{h: sum, ck: sum[:]}
	s.mixHash(prologue)
	return s
}

// mixHash folds public data into the transcript hash: h = SHA-256(h ‖ data).
func (s *symmetricState) mixHash(data []byte) {
	buf := make([]byte, 0, len(s.h)+len(data))
	buf = append(buf, s.h[:]...)
	buf = append(buf, data...)
	s.h = sha256.Sum256(buf)
}

// mixKey folds input keying material into the chaining key (HKDF-Extract, RFC 5869):
// the Noise MixKey step with ck as the salt.
func (s *symmetricState) mixKey(ikm []byte) {
	s.ck = extract(ikm, s.ck)
}

// finalize binds the full transcript into the chaining key and expands the master
// secret — so the keys depend on every public message (downgrade/unknown-keyshare
// resistance) as well as every secret.
func (s *symmetricState) finalize() []byte {
	ck := extract(s.h[:], s.ck)
	return expand(ck, label("master", 0))
}

// mac1Key derives the PSK-keyed MAC key that gates handshake processing.
func mac1Key(psk []byte) []byte { return expand(extract(psk, nil), label("mac1", 0)) }

// mac computes a truncated HMAC-SHA256 over msg.
func mac(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)[:macSize]
}

// confirmTag is the key-confirmation MAC over the final transcript, under a key derived
// from the master secret: it proves the sender completed the same handshake (knows the
// PSK and the KEM secrets), authenticating the responder to the initiator.
func confirmTag(master []byte, h [32]byte) []byte {
	return mac(expand(master, label("confirm", 0)), h[:])
}

// Session is a completed handshake: the master secret both ends agree, plus the role
// and transcript. The host derives the directional epoch-0 traffic secrets from it with
// SendTrafficSecret / RecvTrafficSecret (which map the role to the right direction so the
// two ends line up), then ratchets them per epoch and derives each epoch's AEAD key with
// EpochKey (kdf.go) — see internal/session/secure.go.
type Session struct {
	Master     []byte   // handshake master secret (KeySize bytes)
	Transcript [32]byte // final transcript hash (channel binding / records)
	Initiator  bool     // true on the side that sent message 1 (the media sender)
	EpochSize  uint32   // the media sender's epoch size, carried in message 1 (sender-authoritative; the receiver opens with this, not its own config)
}

// SendTrafficSecret / RecvTrafficSecret return the epoch-0 directional traffic secrets
// the host ratchets forward per epoch (RatchetTrafficSecret) for intra-session forward
// secrecy. One side's send secret equals the other's receive secret.
func (s *Session) SendTrafficSecret() []byte { return TrafficSecret(s.Master, s.sendDir()) }

// RecvTrafficSecret returns the receive-direction traffic secret.
func (s *Session) RecvTrafficSecret() []byte { return TrafficSecret(s.Master, flip(s.sendDir())) }

func (s *Session) sendDir() Direction {
	if s.Initiator {
		return SenderToReceiver
	}
	return ReceiverToSender
}

func flip(d Direction) Direction {
	if d == SenderToReceiver {
		return ReceiverToSender
	}
	return SenderToReceiver
}

// Initiator is the handshake half that speaks first (the media sender). Use is
// WriteMessage1 then ReadMessage2; it is single-shot and not safe for concurrent use.
type Initiator struct {
	psk       []byte
	keys      *EphemeralKeys
	sym       *symmetricState
	epochSize uint32 // the media sender's epoch size, stamped into message 1
	sent      bool
	body      []byte // cached pubs‖epochSize‖mac1, so a cookie retry only swaps the mac2 trailer
}

// NewInitiator starts an initiator handshake from the (Argon2id-derived) PSK and a
// prologue binding context, carrying the media sender's epochSize so the receiver adopts
// it (sender-authoritative). It generates the ephemeral hybrid keypair.
func NewInitiator(psk, prologue []byte, epochSize uint32) (*Initiator, error) {
	if len(psk) != KeySize {
		return nil, ErrShortKey
	}
	keys, err := GenerateEphemeralKeys()
	if err != nil {
		return nil, err
	}
	s := newSymmetric(prologue)
	s.mixKey(psk)
	return &Initiator{psk: psk, keys: keys, sym: s, epochSize: epochSize}, nil
}

// WriteMessage1 produces the first handshake message to send the responder. Its mac2
// trailer is zero; if the responder is under load it replies with a cookie and the
// initiator re-sends via WriteMessage1WithCookie.
func (i *Initiator) WriteMessage1() ([]byte, error) {
	if i.sent {
		return nil, ErrHandshakeState
	}
	xpub, ekey := i.keys.Public()
	body := make([]byte, 0, msg1BodyLen)
	body = append(body, xpub...)
	body = append(body, ekey...)
	body = binary.BigEndian.AppendUint32(body, i.epochSize) // pubs ‖ epochSize
	i.sym.mixHash(body)
	i.body = append(body, mac(mac1Key(i.psk), body)...) // pubs ‖ epochSize ‖ mac1
	i.sent = true
	return append(append([]byte(nil), i.body...), make([]byte, macSize)...), nil
}

// WriteMessage1WithCookie re-emits message 1 with the cookie-derived mac2 — the retry
// after a cookie reply, proving the initiator can receive at its source address. It
// reuses the same ephemeral keys and transcript (mac2 is not mixed into the handshake
// hash), so the established keys are identical whether or not a cookie was used.
func (i *Initiator) WriteMessage1WithCookie(cookie []byte) ([]byte, error) {
	if !i.sent {
		return nil, ErrHandshakeState
	}
	return append(append([]byte(nil), i.body...), mac2Of(cookie, i.body)...), nil
}

// ReadMessage2 verifies and processes the responder's reply, returning the established
// Session. It checks mac1 (PSK proof), completes the hybrid KEM, and verifies the
// responder's key-confirmation tag — any failure returns ErrBadHandshake, leaking
// nothing about which check tripped.
func (i *Initiator) ReadMessage2(msg2 []byte) (*Session, error) {
	if !i.sent {
		return nil, ErrHandshakeState
	}
	if len(msg2) != msg2Len {
		return nil, ErrBadHandshake
	}
	body, tag := msg2[:msg2Len-macSize], msg2[msg2Len-confirmSize-macSize:msg2Len-macSize]
	if !hmac.Equal(msg2[msg2Len-macSize:], mac(mac1Key(i.psk), body)) {
		return nil, ErrBadHandshake
	}
	respX := msg2[:X25519KeySize]
	ct := msg2[X25519KeySize : X25519KeySize+MLKEM768CiphertextSize]
	ssX, ssK, err := i.keys.Respond(respX, ct)
	if err != nil {
		return nil, ErrBadHandshake
	}
	// Derive into a COPY of the transcript so a message 2 that passes mac1 but fails the
	// key-confirmation check leaves i.sym pristine — ReadMessage2 stays idempotent and a
	// retransmitted message 2 is processed from clean state (symmetricState's h is an
	// array and mixKey/mixHash reassign ck/h rather than mutating shared storage, so the
	// shallow copy is fully isolated).
	sym := *i.sym
	sym.mixKey(ssX)
	sym.mixKey(ssK)
	sym.mixHash(msg2[:X25519KeySize+MLKEM768CiphertextSize])
	master := sym.finalize()
	if !hmac.Equal(tag, confirmTag(master, sym.h)) {
		return nil, ErrBadHandshake
	}
	*i.sym = sym // commit only after the confirmation tag verifies
	return &Session{Master: master, Transcript: sym.h, Initiator: true, EpochSize: i.epochSize}, nil
}

// HandshakeInitKeys returns the initiator's ephemeral public material (X25519 public key ‖
// ML-KEM-768 encapsulation key) carried in a decoded message-1 payload, without verifying it
// (mac1, checked in ReadMessage1, authenticates these bytes). The host compares it to the
// established/pending handshake's keys to tell a retransmit (identical keys ⇒ the reply was
// lost) from a new handshake by a restarted sender (fresh keys). ok=false if the payload is
// too short to carry the keys.
func HandshakeInitKeys(msg1 []byte) (keys []byte, ok bool) {
	if len(msg1) < msg1PubsLen {
		return nil, false
	}
	return msg1[:msg1PubsLen], true
}

// Responder is the handshake half that replies (the media receiver). Use is
// ReadMessage1 then WriteMessage2; single-shot, not safe for concurrent use.
type Responder struct {
	psk       []byte
	sym       *symmetricState
	initX     []byte
	initEKey  []byte
	epochSize uint32 // the initiator's (media sender's) epoch size, read from message 1
	got1      bool
}

// NewResponder starts a responder handshake from the PSK and the prologue.
func NewResponder(psk, prologue []byte) (*Responder, error) {
	if len(psk) != KeySize {
		return nil, ErrShortKey
	}
	s := newSymmetric(prologue)
	s.mixKey(psk)
	return &Responder{psk: psk, sym: s}, nil
}

// ReadMessage1 verifies the initiator's first message — the mac1 PSK proof is the cheap
// gate that rejects a non-PSK flood before any asymmetric work — and stores its public
// material. A bad length or mac1 returns ErrBadHandshake.
func (r *Responder) ReadMessage1(msg1 []byte) error {
	if r.got1 {
		return ErrHandshakeState
	}
	if len(msg1) != msg1Len {
		return ErrBadHandshake
	}
	body := msg1[:msg1BodyLen] // pubs ‖ epochSize, what mac1 covers and the transcript folds in
	mac1 := msg1[msg1BodyLen : msg1BodyLen+macSize]
	if !hmac.Equal(mac1, mac(mac1Key(r.psk), body)) {
		return ErrBadHandshake
	}
	pubs := body[:msg1PubsLen]
	r.initX = append([]byte(nil), pubs[:X25519KeySize]...)
	r.initEKey = append([]byte(nil), pubs[X25519KeySize:]...)
	r.epochSize = binary.BigEndian.Uint32(body[msg1PubsLen:]) // the sender's epoch size, authenticated by mac1 + transcript
	// A zero epoch size is malformed: the host keys every symbol by id/epochSize, so a peer that
	// sent 0 would divide by zero on the receive path. Reject it as a bad handshake (the only
	// constraint crypto enforces on the value; the session layer applies its own size policy).
	if r.epochSize == 0 {
		return ErrBadHandshake
	}
	r.sym.mixHash(body)
	r.got1 = true
	return nil
}

// WriteMessage2 produces the reply and returns the established Session: it completes the
// hybrid KEM against the initiator's keys, mixes both shared secrets, and appends a key
// confirmation and a mac1.
func (r *Responder) WriteMessage2() ([]byte, *Session, error) {
	if !r.got1 {
		return nil, nil, ErrHandshakeState
	}
	respX, ct, ssX, ssK, err := Initiate(r.initX, r.initEKey)
	if err != nil {
		return nil, nil, ErrBadHandshake
	}
	r.sym.mixKey(ssX)
	r.sym.mixKey(ssK)
	pubs := make([]byte, 0, X25519KeySize+MLKEM768CiphertextSize)
	pubs = append(append(pubs, respX...), ct...)
	r.sym.mixHash(pubs)
	master := r.sym.finalize()
	body := make([]byte, 0, msg2Len)
	body = append(body, respX...)
	body = append(body, ct...)
	body = append(body, confirmTag(master, r.sym.h)...)
	out := append(body, mac(mac1Key(r.psk), body)...)
	return out, &Session{Master: master, Transcript: r.sym.h, Initiator: false, EpochSize: r.epochSize}, nil
}
