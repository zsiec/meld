package session

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/wire"
)

// handshakeTimeout bounds how long a sender waits for the secure channel before giving
// up (a wrong passphrase or an unreachable peer never completes).
const handshakeTimeout = 10 * time.Second

var (
	// errHandshakeTimeout is returned when the encryption handshake does not complete.
	errHandshakeTimeout = errors.New("meld: session: encryption handshake timed out")
	// ErrChunkTooLarge is returned when an encrypted Write exceeds SymbolSize-Overhead (the
	// AEAD tag leaves 16 fewer bytes than the cleartext budget).
	ErrChunkTooLarge = errors.New("meld: session: encrypted chunk exceeds SymbolSize minus the AEAD tag")
	// ErrFlowExhausted is returned when a flow has sealed the entire 2^32 source-index
	// space; the host must re-handshake rather than wrap the nonce.
	ErrFlowExhausted = errors.New("meld: session: source-index space exhausted, re-handshake the flow")
	// errSmallSymbol is returned by validate when SymbolSize cannot hold the AEAD tag.
	errSmallSymbol = errors.New("meld: session: SymbolSize must exceed the AEAD tag (16 bytes) for encryption")
	// errEpochTooSmall is returned by validate when a non-zero EpochSize is below the floor.
	errEpochTooSmall = errors.New("meld: session: EpochSize is too small (min 64)")
)

// defaultEpochSize is the symbols-per-epoch when SecurityConfig.EpochSize is unset;
// minEpochSize is the smallest a caller may configure, so the per-epoch ratchet cost stays
// negligible relative to the symbols it protects.
const (
	defaultEpochSize = 1 << 14
	minEpochSize     = 1 << 6
)

// guardDurationMicros is how long after a re-handshake promotion inbound systematics are
// screened for old-session stragglers (one that fails to open under the new keys is dropped
// before it can poison an id in the freshly-rebuilt core). A wall-clock window — not a symbol
// count — so a flood of repairs/forgeries cannot exhaust it before the in-flight tail drains.
const guardDurationMicros = 500_000 // 500 ms

// maxTrialEpochs bounds how far a trial-decrypt ratchets a traffic secret forward from a
// keyer's current epoch, so an attacker-controlled wire SrcIndex can never drive an unbounded
// ratchet under the host lock. It comfortably spans the core's in-flight admission window in
// epochs for any sane EpochSize, so a genuine symbol within that window is still judged.
const maxTrialEpochs = 64

// SecurityConfig enables and parameterizes the encryption layer (docs/encryption.md).
// A nil *SecurityConfig leaves a flow in cleartext, identical to the pre-encryption
// host. When set, the host runs the X25519 + ML-KEM-768 hybrid handshake before any
// media and AEAD-seals every source symbol.
type SecurityConfig struct {
	// Passphrase is the shared secret both ends hold; it is stretched with Argon2id into
	// the handshake PSK. Required (a SecurityConfig with an empty Passphrase is cleartext).
	Passphrase []byte
	// Salt salts the Argon2id stretch; both ends must agree. A fixed default when empty.
	Salt []byte
	// Params tunes the Argon2id work factor; both ends must agree. DefaultArgon2idParams
	// when zero.
	Params crypto.Argon2idParams
	// EpochSize is the number of source symbols sealed under one epoch key before the key
	// ratchets forward (intra-session forward secrecy, docs/encryption.md §6). Both ends
	// must agree. 0 ⇒ a large default — a forward-secrecy cadence, not a correctness knob
	// (one epoch's nonce space holds 2^32 symbols).
	EpochSize uint32
	// CookieThreshold is the per-tick handshake-attempt count above which the responder
	// demands a mac2 return-routability cookie (docs/encryption.md §4.4). 0 ⇒ effectively
	// off (mac1 + the large first message already foreclose non-PSK amplification); set a
	// small value to require the cookie under a handshake flood.
	CookieThreshold uint32
}

// defaultCookieThreshold (when CookieThreshold is 0) is high enough that the cookie gate
// never fires in normal operation — it is opportunistic hardening, not a default cost.
const defaultCookieThreshold = 1 << 30

func (s *SecurityConfig) cookieThreshold() int {
	if s != nil && s.CookieThreshold > 0 {
		return int(s.CookieThreshold)
	}
	return defaultCookieThreshold
}

func (s *SecurityConfig) active() bool { return s != nil && len(s.Passphrase) > 0 }

func (s *SecurityConfig) salt() []byte {
	if len(s.Salt) > 0 {
		return s.Salt
	}
	return []byte("meld-v1-psk-salt")
}

// params returns the Argon2id parameters, filling any zero field with the default so a
// partially-specified Params (e.g. Memory set, Time/Threads left 0) never reaches
// argon2.IDKey with an out-of-range value (which would panic). The defaulting lives on the
// crypto type that owns it; DerivePSK applies the same WithDefaults, so this is belt-and-
// suspenders for callers that read params() directly.
func (s *SecurityConfig) params() crypto.Argon2idParams {
	return s.Params.WithDefaults()
}

// validate rejects a security config that cannot encrypt at the given symbol size, so the
// host fails construction with an error rather than panicking on the first Write. A nil /
// inactive config (cleartext) always validates.
func (s *SecurityConfig) validate(symSize int) error {
	if !s.active() {
		return nil
	}
	if symSize <= crypto.Overhead {
		return errSmallSymbol
	}
	// A tiny EpochSize would ratchet the traffic secret (one HKDF-Expand) almost every
	// symbol — a CPU cost paid under the host lock. EpochSize is a forward-secrecy cadence,
	// not a fine-grained knob, so require a sane floor.
	if s.EpochSize > 0 && s.EpochSize < minEpochSize {
		return errEpochTooSmall
	}
	return nil
}

// psk derives the long-term PSK from the passphrase (Argon2id). Cached by the caller.
func (s *SecurityConfig) psk() []byte {
	return crypto.DerivePSK(s.Passphrase, s.salt(), s.params())
}

func (s *SecurityConfig) epochSize() uint32 {
	if s != nil && s.EpochSize > 0 {
		return s.EpochSize
	}
	return defaultEpochSize
}

// beU32 renders a flow id as the handshake prologue (the binding context both ends mix).
func beU32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// controlState authenticates the flow's control plane (feedback / clock) on an encrypted
// flow: every outbound datagram is sealed with a monotonic sequence under the shared control
// key, and every inbound datagram is verified and run through a replay window. A cleartext
// flow (active=false) passes through untouched. An encrypted flow whose handshake has not yet
// keyed the channel (key nil) DROPS inbound control and SUPPRESSES outbound control, so the
// pre-establishment window is never an unauthenticated hole. Not safe for concurrent use; the
// host serializes it under its mutex.
type controlState struct {
	active  bool               // encrypted flow (sec.active())
	send    *crypto.ControlMAC // outbound direction's keyed MAC; nil until established
	recv    *crypto.ControlMAC // inbound direction's keyed MAC; nil until established
	sendSeq uint64             // next outbound sequence number
	recvWin crypto.ReplayWindow
}

// setKeys installs (or replaces, on a re-handshake) the directional control keys and resets
// the sequence space and replay window so the new session starts clean. The two keys are
// distinct per direction, so a datagram cannot be reflected into the opposite direction.
func (c *controlState) setKeys(sendKey, recvKey []byte) {
	c.send = crypto.NewControlMAC(sendKey)
	c.recv = crypto.NewControlMAC(recvKey)
	c.sendSeq = 0
	c.recvWin.Reset()
}

// seal frames an outbound control datagram. ok=false means the caller must NOT send it: an
// active flow whose channel is not yet keyed suppresses control rather than leaking it
// unauthenticated.
func (c *controlState) seal(d []byte) ([]byte, bool) {
	if !c.active {
		return d, true // cleartext flow
	}
	if c.send == nil {
		return nil, false // encrypted, not yet established
	}
	out := c.send.Seal(c.sendSeq, d)
	c.sendSeq++
	return out, true
}

// sealBatch seals a batch of outbound control datagrams (e.g. feedback), dropping any the
// channel cannot yet authenticate. The caller holds the host mutex (seal advances the
// sequence). A cleartext flow returns the datagrams unchanged. Each sealed datagram is a
// fresh slice: the batch is collected under the lock and transmitted after it is released, so
// the slices must stay independent — and the control plane is paced (feedback cadence + the
// occasional clock probe), not per media symbol, so the per-datagram allocation is negligible.
func (c *controlState) sealBatch(ds [][]byte) [][]byte {
	out := make([][]byte, 0, len(ds))
	for _, d := range ds {
		if sd, ok := c.seal(d); ok {
			out = append(out, sd)
		}
	}
	return out
}

// open verifies an inbound control datagram and rejects replays. ok=false means drop it.
func (c *controlState) open(d []byte) ([]byte, bool) {
	if !c.active {
		return d, true // cleartext flow
	}
	if c.recv == nil {
		return nil, false // encrypted, not yet established
	}
	seq, body, ok := c.recv.Open(d)
	if !ok || !c.recvWin.Accept(seq) {
		return nil, false
	}
	return body, true
}

// sealState is the sender-side encryption keying shared by the single-path and multipath
// hosts: a ratcheting per-epoch AEAD sealer driven by the source index. Nil sec ⇒
// cleartext (seal is a pass-through). Not safe for concurrent use; the host serializes
// the send path under its mutex.
type sealState struct {
	sec        *SecurityConfig
	psk        []byte // cached Argon2id output, so the handshake never re-stretches the passphrase
	symSize    int
	flowID     uint32
	epochSize  uint32
	sendSecret []byte         // current ratcheted send traffic secret (nil ⇒ not established)
	ctl        controlState   // control-plane (feedback/clock) authentication
	sealer     *crypto.Sealer // current epoch's sealer
	sealEpoch  uint32         // FULL epoch (no truncation) so the ratchet count is monotonic
	nextSrc    uint32         // mirrors the core's src_index assignment, for the nonce

	// Warm-path scratch, sized once: a chunk's zero-padded plaintext and the sealed-symbol
	// output, reused across seal calls so the encrypt path allocates nothing per symbol (the
	// core copies the ciphertext into its window, so it never retains these buffers).
	padBuf []byte
	ctBuf  []byte
	aadBuf [crypto.AADSize]byte
}

func newSealState(sec *SecurityConfig, symSize int, flowID uint32) sealState {
	ss := sealState{symSize: symSize, flowID: flowID, epochSize: sec.epochSize()}
	if sec.active() {
		ss.sec = sec
		ss.psk = sec.psk() // stretch the passphrase ONCE (Argon2id), cache it
		ss.ctl.active = true
		ss.padBuf = make([]byte, symSize-crypto.Overhead)
		ss.ctBuf = make([]byte, 0, symSize)
	}
	return ss
}

// seal AEAD-encrypts one media chunk for the next source index (the nonce input),
// advancing the mirror of the core's id assignment. A nil sec is a pass-through. The
// plaintext is padded to SymbolSize-Overhead so the ciphertext is exactly SymbolSize and
// the coder never zero-pads (which would corrupt the tag); a chunk larger than that is
// rejected with ErrChunkTooLarge rather than silently truncated. The FULL epoch (no
// uint16 truncation) drives the ratchet so its count is strictly monotonic across the
// whole flow — the per-epoch key never repeats, which is what prevents (key, nonce)
// reuse even though the nonce's 16-bit epoch field cycles; only the source id need stay
// unique, so the flow is capped at 2^32 symbols (ErrFlowExhausted) rather than wrapping.
func (s *sealState) seal(p []byte) ([]byte, error) {
	if s.sec == nil {
		return p, nil
	}
	if len(p) > s.symSize-crypto.Overhead {
		return nil, ErrChunkTooLarge
	}
	if s.nextSrc == math.MaxUint32 {
		return nil, ErrFlowExhausted // refuse the wrap that would reuse a (key, nonce)
	}
	epoch := s.nextSrc / s.epochSize // full epoch (uint32), no truncation
	if s.sealer == nil || epoch != s.sealEpoch {
		for s.sealEpoch < epoch {
			s.sendSecret = crypto.RatchetTrafficSecret(s.sendSecret)
			s.sealEpoch++
		}
		sl, err := crypto.NewSealer(crypto.EpochKey(s.sendSecret, uint16(epoch)), uint16(epoch))
		if err != nil {
			s.sealer = nil // do not leave a previous-epoch sealer cached under the new epoch
			return nil, err
		}
		s.sealer, s.sealEpoch = sl, epoch
	}
	// Pad into the reused scratch, clearing the tail so a short chunk never leaks the
	// previous chunk's bytes; seal into the reused output (the core copies it).
	copy(s.padBuf, p)
	clear(s.padBuf[len(p):])
	crypto.PutAAD(&s.aadBuf, s.flowID, uint16(epoch), s.nextSrc)
	ct, err := s.sealer.Seal(s.ctBuf[:0], s.padBuf, s.aadBuf[:], s.nextSrc)
	if err != nil {
		return nil, err
	}
	s.ctBuf = ct
	s.nextSrc++
	return ct, nil
}

// completeResp processes the responder's message 2 on the initiator (sender) side under the
// host mutex: it verifies the reply and installs the send traffic secret and control keys. It
// returns false when this was not the awaited reply (already established, or a
// tampered/duplicate message), so the caller does nothing — in particular it must not close
// its handshake-done channel twice.
func (s *sealState) completeResp(payload []byte, init *crypto.Initiator) bool {
	if s.sendSecret != nil || init == nil {
		return false
	}
	sess, err := init.ReadMessage2(payload)
	if err != nil {
		return false
	}
	s.sendSecret = sess.SendTrafficSecret()
	s.ctl.setKeys(sess.SendControlKey(), sess.RecvControlKey())
	return true
}

// epochKeyer derives and caches the per-epoch AEAD opener for one receive direction by
// ratcheting a traffic secret forward with the source id. It is shared by the live session
// (embedded in openState) and a pending re-handshake (pendingState), so the ratchet logic
// lives once.
type epochKeyer struct {
	recvSecret []byte         // current ratcheted receive traffic secret
	opener     *crypto.Opener // current epoch's opener
	openEpoch  uint32         // FULL epoch (no truncation), mirroring the sender's ratchet
}

// openerFor returns the AEAD opener for epoch, advancing the ratcheting receive secret
// forward to it (forward secrecy: past epochs' keys are unrecoverable). Delivery is strictly
// in source-id order, so the epoch is monotonic and only one opener is live.
func (k *epochKeyer) openerFor(epoch uint32) *crypto.Opener {
	if k.opener != nil && k.openEpoch == epoch {
		return k.opener
	}
	// Ratchet forward to the target epoch. The ratchet cannot fail; only NewOpener can. On a
	// build failure CLEAR the cached opener so the guard above cannot later hand back a
	// previous-epoch opener under the now-advanced epoch — a retry re-derives from the
	// (already-correct) ratcheted secret.
	for k.openEpoch < epoch {
		k.recvSecret = crypto.RatchetTrafficSecret(k.recvSecret)
		k.openEpoch++
	}
	op, err := crypto.NewOpener(crypto.EpochKey(k.recvSecret, uint16(epoch)), uint16(epoch))
	if err != nil {
		k.opener = nil
		return nil
	}
	k.opener = op
	return op
}

// pendingState is a re-handshake awaiting confirmation (docs/encryption.md §4): a new session
// the responder has keyed but NOT yet committed. The live session keeps running until an
// inbound symbol authenticates under these keys (tryPromote), so a replayed or forged
// message 1 — which can never produce such a symbol — cannot displace the live session.
type pendingState struct {
	keyer   epochKeyer
	ctlSend []byte // directional control keys, installed on promotion
	ctlRecv []byte
	hsKeys  []byte // the initiator's ephemeral pubs, to recognize a retransmit of THIS pending
	msg2    []byte // framed message 2, resent on a pending retransmit
}

// openState is the receiver-side encryption keying shared by both hosts: a ratcheting
// per-epoch AEAD opener driven by the delivered source id. Nil sec ⇒ cleartext
// (pass-through). Not safe for concurrent use; the host opens under its mutex.
type openState struct {
	sec       *SecurityConfig
	psk       []byte       // cached Argon2id output
	ctl       controlState // control-plane (feedback/clock) authentication
	flowID    uint32
	epochSize uint32
	aadBuf    [crypto.AADSize]byte

	// Live session.
	established bool
	epochKeyer         // active receive keyer (embedded: o.openerFor / o.recvSecret / …)
	hsKeys      []byte // the established initiator's ephemeral pubs (retransmit detection)
	activeMsg2  []byte // framed message 2 of the live session, resent on a retransmit

	// Pending re-handshake (commit-after-confirm). nil unless a re-handshake is in flight.
	pending *pendingState
	// guardUntilMicros is the host-clock deadline until which old-session stragglers are
	// screened out of the freshly-rebuilt core after a promotion (see staleStraggler).
	guardUntilMicros int64

	// mac2 anti-amplification cookie (responder side). attempts counts handshake inits in
	// the current tick window; under load (attempts > threshold) the responder demands a
	// cookie before committing handshake work.
	cookies   *crypto.CookieChecker
	threshold int
	attempts  int

	// ptPool recycles open-path plaintext buffers (set only for an encrypted session). openAll
	// decrypts into a pooled buffer instead of allocating per symbol; Read returns the buffer once
	// it has copied the plaintext out. Safe because in an encrypted session EVERY delivered buffer
	// comes from a decrypt (the cleartext passthrough in openAll is unreachable when sec != nil), so
	// Read can recycle unconditionally; a pointer so the embedding Receiver is copy-safe (go vet).
	ptPool *sync.Pool
}

func newOpenState(sec *SecurityConfig, flowID uint32) (openState, error) {
	os := openState{flowID: flowID, epochSize: sec.epochSize()}
	if sec.active() {
		os.sec = sec
		os.psk = sec.psk()
		os.ctl.active = true
		os.threshold = sec.cookieThreshold()
		cc, err := crypto.NewCookieChecker()
		if err != nil {
			return openState{}, err // surface the RNG failure rather than silently disable the gate
		}
		os.cookies = cc
		os.ptPool = &sync.Pool{}
	}
	return os, nil
}

// underLoad reports whether the responder should demand a return-routability cookie:
// once the handshake-attempt count this window reaches the threshold. The default
// threshold is effectively infinite (cookie off); a threshold of 1 demands a cookie on
// every handshake (the test/forced-on setting).
func (o *openState) underLoad() bool { return o.cookies != nil && o.attempts >= o.threshold }

// cookieReply seals a cookie reply for a peer (its source-address bytes).
func (o *openState) cookieReply(peerID []byte) []byte {
	if o.cookies == nil {
		return nil
	}
	reply, err := o.cookies.Reply(o.psk, peerID)
	if err != nil {
		return nil
	}
	return reply
}

// cookieValid reports whether msg1's mac2 proves return-routability for peerID.
func (o *openState) cookieValid(msg1, peerID []byte) bool {
	return o.cookies != nil && o.cookies.Valid(msg1, peerID)
}

// rotateCookie refreshes the cookie secret (call periodically so cookies expire) and
// resets the per-window attempt counter. If the RNG fails, Rotate leaves the existing
// secret intact (it is atomic); the attempt counter is reset regardless so the load window
// still advances, and the gate retries on the next rotation.
func (o *openState) rotateCookie() {
	if o.cookies != nil {
		_ = o.cookies.Rotate() // atomic: keeps the current secret on RNG failure
	}
	o.attempts = 0
}

// initResult is the action respondInit asks the host to take for a message 1. send is the
// framed datagram to write (a cookie reply or a message 2), or nil to drop the input; toPeer
// chooses the destination — true means the established peer (an anti-reflection retransmit of
// the LIVE session's reply), false means the message's own source address.
type initResult struct {
	send   []byte
	toPeer bool
}

// respondInit drives the responder side of message 1 under the host mutex, using
// commit-after-confirm rather than a clock so a replayed message 1 can never displace a live
// session:
//   - keys == the live session's → a retransmit whose reply was lost: resend the cached
//     message 2 to the ESTABLISHED peer only (never the spoofable source — no reflection);
//   - keys == an in-flight pending re-handshake's → resend that pending message 2 to the source;
//   - any other authenticated keys while established → a NEW handshake: build a PENDING session
//     (do NOT touch the live session) and reply; it is promoted only once a symbol opens under
//     it (tryPromote);
//   - first contact → establish the live session directly.
//
// mac1 is verified (cheap) before the cookie gate and the KEM; the cookie attempt counter is
// charged only for mac1-authenticated inits, so an unauthenticated flood cannot trip the gate.
func (o *openState) respondInit(payload, peerID []byte) initResult {
	keys, ok := crypto.HandshakeInitKeys(payload)
	if !ok {
		return initResult{} // too short to carry the keys — not a handshake init
	}
	if o.established {
		if bytes.Equal(keys, o.hsKeys) {
			return initResult{send: o.activeMsg2, toPeer: true} // retransmit of the live session
		}
		if o.pending != nil && bytes.Equal(keys, o.pending.hsKeys) {
			return initResult{send: o.pending.msg2} // retransmit of the in-flight re-handshake
		}
		// A different authenticated handshake → a re-handshake; fall through to build a pending.
	}
	resp, err := crypto.NewResponder(o.psk, beU32(o.flowID))
	if err != nil {
		return initResult{}
	}
	if err := resp.ReadMessage1(payload); err != nil {
		return initResult{} // mac1 failed: forged/non-PSK — no reply, no KEM, not counted
	}
	o.attempts++ // count only mac1-authenticated inits toward the cookie gate
	// Under a handshake flood, demand a return-routability cookie BEFORE the (expensive) KEM.
	if o.underLoad() && !o.cookieValid(payload, peerID) {
		if reply := o.cookieReply(peerID); reply != nil {
			return initResult{send: wire.EncodeHandshakeCookie(nil, reply)}
		}
		return initResult{}
	}
	raw, sess, err := resp.WriteMessage2()
	if err != nil {
		return initResult{}
	}
	framed := wire.EncodeHandshakeResp(nil, raw)
	if o.established {
		// Commit-after-confirm: stage the new session as PENDING; the live session keeps
		// running until a symbol authenticates under these keys (tryPromote). Build the epoch-0
		// opener once here so the per-symbol trial reuses it instead of re-deriving it.
		kp := epochKeyer{recvSecret: sess.RecvTrafficSecret()}
		kp.openerFor(0)
		o.pending = &pendingState{
			keyer:   kp,
			ctlSend: sess.SendControlKey(),
			ctlRecv: sess.RecvControlKey(),
			hsKeys:  append([]byte(nil), keys...),
			msg2:    framed,
		}
		return initResult{send: framed}
	}
	// First contact: establish the live session immediately.
	o.epochKeyer = epochKeyer{recvSecret: sess.RecvTrafficSecret()}
	o.ctl.setKeys(sess.SendControlKey(), sess.RecvControlKey())
	o.hsKeys = append([]byte(nil), keys...)
	o.activeMsg2 = framed
	o.established = true
	return initResult{send: framed}
}

// trialOpen reports whether sym (a systematic symbol) opens under base's session at the
// symbol's epoch, deriving the opener on a COPY of base so the live/pending keyer is never
// mutated, and refusing to ratchet backward or further than maxTrialEpochs forward — so an
// attacker-controlled wire SrcIndex can neither drive an unbounded ratchet nor be judged
// against a wrong-epoch key. base is the keyer to trial (the live keyer for a roam/straggler
// check, the pending keyer for a promotion). It only VERIFIES — it returns a bool and never
// hands back the advanced copy: a promotion commits the pending keyer at its BASE epoch
// (tryPromote), never the copy advanced to the confirming symbol's epoch, so the rebuilt core
// ratchets the receive secret forward in id order rather than starting mid-ratchet.
//
// Two invariants the trial silently rests on:
//   - The copy (k := base) shallow-copies base.recvSecret's backing array; the trial is
//     non-corrupting ONLY because RatchetTrafficSecret / EpochKey allocate fresh slices and
//     never write through their input. A future in-place ratchet would corrupt the live keyer.
//   - epoch is truncated to uint16 for the AAD and the opener (openerFor), exactly as the live
//     open path is, so the trial agrees with delivery byte-for-byte. Sound only while
//     maxTrialEpochs keeps the trialed epoch within uint16 of a low base; widening the cap past
//     65535 (or trialing a keyer already beyond epoch 65535) would alias epochs and mis-key.
func (o *openState) trialOpen(base epochKeyer, sym wire.Symbol) bool {
	if sym.Kind != wire.Systematic {
		return false
	}
	epoch := sym.SrcIndex / o.epochSize
	if epoch < base.openEpoch || epoch-base.openEpoch > maxTrialEpochs {
		return false
	}
	k := base // copy: openerFor mutates only this local (non-corrupting per the invariants above)
	op := k.openerFor(epoch)
	if op == nil {
		return false
	}
	crypto.PutAAD(&o.aadBuf, o.flowID, uint16(epoch), sym.SrcIndex)
	_, err := op.Open(nil, sym.Payload, o.aadBuf[:], sym.SrcIndex)
	return err == nil
}

// tryPromote promotes the pending re-handshake when sym opens under the pending keys: the
// pending session is then genuine (only the real restarted sender can produce that symbol), so
// it becomes the live session and the caller rebuilds the core. A replayed/forged message 1
// leaves a pending no symbol ever opens, so the live session is never displaced. A restart
// whose epoch-0 burst was lost can still confirm on a later symbol (within maxTrialEpochs of 0)
// rather than wedging.
//
// The confirming symbol only VERIFIES the pending; the committed live keyer is the pending
// keyer at its BASE epoch (0), NOT the copy advanced to the confirming symbol's epoch. The
// rebuilt core redelivers ids from 0, so openAll must ratchet the receive secret forward in id
// order from epoch 0 — committing an already-advanced keyer would leave every id below the
// confirming epoch underivable (forward secrecy makes the skipped epochs' keys unrecoverable)
// and silently dropped, losing the restart's whole prefix when its epoch-0 burst was reordered.
func (o *openState) tryPromote(sym wire.Symbol) bool {
	if o.pending == nil {
		return false
	}
	if !o.trialOpen(o.pending.keyer, sym) {
		return false
	}
	o.epochKeyer = o.pending.keyer // commit at the pending base epoch (0), never advanced to sym's epoch
	o.ctl.setKeys(o.pending.ctlSend, o.pending.ctlRecv)
	o.hsKeys = o.pending.hsKeys
	o.activeMsg2 = o.pending.msg2
	o.pending = nil
	return true
}

// staleStraggler reports whether sym is a systematic that does NOT open under the live keys at
// its epoch — an old-session symbol left in flight across a re-handshake that would otherwise
// be delivered by the freshly-rebuilt core, fail to open, and leave a permanent gap at that
// id. The host drops such symbols during the post-promotion guard window. The trial is bounded
// and non-mutating (trialOpen on a copy); a symbol below the cursor or beyond the bound is not
// screened (a duplicate, or already refused by the core's admission window).
func (o *openState) staleStraggler(sym wire.Symbol) bool {
	if o.sec == nil || sym.Kind != wire.Systematic {
		return false
	}
	epoch := sym.SrcIndex / o.epochSize
	if epoch < o.openEpoch || epoch-o.openEpoch > maxTrialEpochs {
		return false // not cheaply judgeable here — let the core handle it
	}
	return !o.trialOpen(o.epochKeyer, sym)
}

// authenticates reports whether sym opens under the LIVE receive keys at its epoch, without
// mutating the live keyer (trialOpen works on a copy). A cleartext flow has no keys, so any
// source is accepted. The host uses it to adopt a roamed sender's new address on authenticated
// receipt — never on an unauthenticated forgery.
func (o *openState) authenticates(sym wire.Symbol) bool {
	if o.sec == nil {
		return true
	}
	return o.trialOpen(o.epochKeyer, sym)
}

// openAll opens (or, when cleartext, passes through) a batch of delivered symbols. A
// symbol that fails authentication — a tampered ciphertext or a polluted repair that
// corrupted the linear solve — is dropped, never delivered.
func (o *openState) openAll(ds []delivered) [][]byte {
	out := make([][]byte, 0, len(ds))
	for _, d := range ds {
		if o.sec == nil {
			out = append(out, d.data)
			continue
		}
		epoch := d.id / o.epochSize // full epoch (uint32), no truncation
		op := o.openerFor(epoch)
		if op == nil {
			continue
		}
		crypto.PutAAD(&o.aadBuf, o.flowID, uint16(epoch), d.id)
		buf, _ := o.ptPool.Get().([]byte) // nil on a pool miss ⇒ Open allocates; recycled buffers self-size
		pt, err := op.Open(buf[:0], d.data, o.aadBuf[:], d.id)
		if err != nil {
			if buf != nil {
				o.ptPool.Put(buf[:0]) // auth failure: nothing was delivered, so return the buffer
			}
			continue
		}
		out = append(out, pt)
	}
	return out
}
