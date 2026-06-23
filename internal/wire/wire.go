// Package wire defines Meld's narrow waist: the normalized Symbol and Feedback
// types every path encodes/decodes through, plus their byte codec. The
// deterministic core (internal/flow) speaks only these types; coding-coefficient
// regeneration, codec specifics, and the per-codec media descriptor are erased on
// either side of this seam. New behavior is a new field here, never a special
// case inside the core.
//
// # Versioning (the forcing function)
//
// The leading byte packs a 4-bit format VERSION in its high nibble and the
// message-type tag in its low nibble: byte0 = (Version<<4) | type. A decoder that
// sees a version it does not understand returns ErrVersion rather than misparsing —
// so an additive field landing in a later revision never decodes as silent garbage
// in an older peer. The extension policy (docs/wireformat.md): a BASE-LAYOUT change
// bumps the version nibble; an ADDITIVE optional field appends to the tail under the
// same version, gated by a Symbol Flags bit (symbols) or a length check (feedback),
// so an older decoder reads the base it knows and ignores the rest. This is why N0
// lands before N1/N2/N4 each add a field — they fill reserved/tail space by policy
// instead of colliding.
//
// The codec never panics on malformed input (Meld's no-panic-in-library rule):
// short, mis-versioned, or corrupt buffers return an error. All multi-byte fields
// are big-endian.
package wire

import (
	"encoding/binary"
	"errors"
)

// Sentinel errors returned by the codec.
var (
	// ErrShort is returned when a buffer is too small to hold the declared
	// header or payload.
	ErrShort = errors.New("meld: wire: buffer too short")
	// ErrType is returned when the message-type nibble is unknown.
	ErrType = errors.New("meld: wire: unknown message type")
	// ErrVersion is returned when the format version nibble is not one this build
	// understands — the guard that keeps a future field from decoding as garbage.
	ErrVersion = errors.New("meld: wire: unsupported format version")
)

// Version is the current wire-format version, carried in the high nibble of every
// datagram's leading byte. Bump it ONLY for an incompatible base-layout change;
// additive fields use the tail-extension policy (see the package doc) and do not.
const Version = 1

// Message-type tags (the low nibble of the leading byte).
const (
	typeSystematic      = 0x01
	typeRepair          = 0x02
	typeFeedback        = 0x03
	typeClockProbe      = 0x04 // receiver → sender clock-offset probe (N4)
	typeClockEcho       = 0x05 // sender → receiver echo of the probe
	typeHandshakeInit   = 0x06 // initiator → responder handshake message 1 (docs/encryption.md)
	typeHandshakeResp   = 0x07 // responder → initiator handshake message 2
	typeHandshakeCookie = 0x08 // responder → initiator mac2 cookie reply (under load)
	typeMTUProbe        = 0x09 // sender → receiver padded DPLPMTUD probe (RFC 8899)
	typeMTUProbeAck     = 0x0A // receiver → sender probe acknowledgement
	typeMask            = 0x0F
)

// lead composes the leading byte from the current version and a type tag.
func lead(typeTag uint8) uint8 { return (Version << 4) | typeTag }

// splitLead returns the type nibble of b[0] after checking the version nibble,
// or an error (ErrVersion before ErrType, so a mis-versioned datagram is reported
// as such rather than as an unknown type).
func splitLead(b0 uint8) (typeTag uint8, err error) {
	if b0>>4 != Version {
		return 0, ErrVersion
	}
	t := b0 & typeMask
	switch t {
	case typeSystematic, typeRepair, typeFeedback, typeClockProbe, typeClockEcho,
		typeHandshakeInit, typeHandshakeResp, typeHandshakeCookie,
		typeMTUProbe, typeMTUProbeAck:
		return t, nil
	default:
		return 0, ErrType
	}
}

// symbolHeader is the BASE header length preceding a Symbol payload (v1, including the
// 1-byte host-stamped PathID). Two optional, Flags-gated extensions follow the base in a
// fixed order: an 8-byte send timestamp (flagSendTS) then a 9-byte frame descriptor
// (flagDesc). The Flags bits live in the header's Flags byte (h[29]).
const (
	symbolHeader = 30
	sendTSLen    = 8 // flagSendTS extension: SendTimestamp (int64)
	// flagDesc extension (variable): FrameStart(4) + FrameLen(2) + descFlags(1) + nRefs(1)
	// + nRefs×RefStart(4). descHeadLen is the fixed head; descMaxRefs caps the count so a
	// forged nRefs cannot over-read (a B-frame needs 2 references; AV1's buffer holds 7).
	descHeadLen = 8
	descMaxRefs = 15
	flagSendTS  = 0x01
	flagDesc    = 0x02
	// descFlags bits inside the frame-descriptor extension's flags byte.
	descRAP         = 0x01
	descDiscardable = 0x02
)

// MaxFeedbackGens is the number of consecutive generations (from the delivery
// cursor) whose rank deficit a Feedback report carries, so the sender can repair
// every deficient generation in parallel rather than only the blocking one.
const MaxFeedbackGens = 32

// feedbackLen is the v1 BASE feedback length (through Deficits). feedbackLenExt adds the
// CongestionLoss (N1) and Burstiness (N2) tail fields; after it comes the variable per-path
// multipath section (N5): one nPaths byte, then nPaths PathLoss values, then nPaths+1
// SlotDist values. Per the extension policy (package doc), the encoder always writes the
// fullest form and the decoder reads each tail group only when present — so a peer that
// knows only an earlier prefix ignores the rest, no version bump.
const (
	feedbackLen      = 21 + MaxFeedbackGens
	feedbackLenExt   = feedbackLen + 4
	feedbackMaxPaths = 8 // bound on the per-path section (a forged nPaths cannot allocate unboundedly)
)

// Kind distinguishes a verbatim source symbol from a coded repair symbol.
type Kind uint8

// Symbol kinds.
const (
	// Systematic carries one source symbol verbatim (coefficient = unit vector).
	Systematic Kind = iota
	// Repair carries a random linear combination of a window of source symbols.
	Repair
)

// Symbol is the normalized coded symbol crossing the waist. For a Systematic
// symbol, SrcIndex is the source id and N/RepairKey are unused. For a Repair
// symbol, WindowBase+N delimit the window the combination spans and RepairKey
// regenerates the GF(2^8) coefficients (internal/code.GenCoeffs).
//
// Priority and Deadline are the (currently minimal) slice of the generic media
// descriptor the core acts on for unequal protection and deadline eviction; the
// full descriptor (dependency chains, decode targets — see
// docs/media-awareness.md) is added here as the media shaper lands.
type Symbol struct {
	Flow       uint32 // the flow this symbol belongs to
	Epoch      uint16 // generation of the flow; bumps on flow reset / key update (reserved; 0 until used)
	PathID     uint8  // which path the host sent this symbol on (host-stamped; 0 == single path / path 0)
	Kind       Kind
	WindowBase uint32 // low edge of the coding window
	SrcIndex   uint32 // Systematic: source id. Repair: repair counter.
	N          uint16 // Repair: window width covered
	RepairKey  uint16 // Repair: coefficient PRNG seed
	Priority   uint8  // descriptor: protection tier (0 = most disposable)
	Deadline   int64  // descriptor: decode-by, in clock microseconds
	// SendTimestamp, when non-zero, is the sender's clock time (microseconds) at
	// emission, carried in an 8-byte header extension flagged by flagSendTS (N4
	// refinement): with the receiver's receive time it gives a per-symbol one-way
	// delay whose VARIATION is offset-invariant, refining the clock-offset estimate
	// and exposing per-symbol latency. Zero ⇒ not carried (the base 30-byte header).
	SendTimestamp int64

	// HasFrameDesc gates the frame-descriptor extension (flagDesc): the access-unit
	// dependency the receiver uses to compute loss propagation parse-free (WP6). The
	// sender stamps it on SYSTEMATIC symbols only — coding rebuilds payloads, not
	// headers, so a recovered symbol carries no descriptor, and the receiver infers the
	// frames it did not directly receive. FrameStart is the access unit's FIRST source id
	// (a stable per-frame identity that also bounds the frame's id range, so the receiver
	// attributes any id — recovered or lost — to a frame by position); FrameLen is the
	// access unit's chunk count, so the frame's exact id range is [FrameStart,
	// FrameStart+FrameLen) and an id outside it (a generation-fill phantom, or an unknown
	// frame's id) is NOT mis-attributed; FrameRefs are the first source ids of EVERY
	// dependency frame (a B-frame's two bracketing anchors, a P-frame's one) — the unit is
	// decodable only if all of them are; FrameRAP marks a random-access point;
	// FrameDiscardable marks a unit nothing references.
	HasFrameDesc     bool
	FrameStart       uint32
	FrameLen         uint16
	FrameRefs        []uint32
	FrameRAP         bool
	FrameDiscardable bool

	Payload []byte // coded bytes, sized to the validated path MTU
}

// EncodeSymbol appends the encoded Symbol to dst and returns the extended slice.
func EncodeSymbol(dst []byte, s Symbol) []byte {
	typeTag := uint8(typeSystematic)
	if s.Kind == Repair {
		typeTag = typeRepair
	}
	var h [symbolHeader]byte
	h[0] = lead(typeTag)
	binary.BigEndian.PutUint32(h[1:], s.Flow)
	binary.BigEndian.PutUint16(h[5:], s.Epoch)
	h[7] = s.PathID
	binary.BigEndian.PutUint32(h[8:], s.WindowBase)
	binary.BigEndian.PutUint32(h[12:], s.SrcIndex)
	binary.BigEndian.PutUint16(h[16:], s.N)
	binary.BigEndian.PutUint16(h[18:], s.RepairKey)
	h[20] = s.Priority
	binary.BigEndian.PutUint64(h[21:], uint64(s.Deadline))
	// h[29] is the Flags byte; extensions follow the base in a fixed order (send
	// timestamp, then frame descriptor), each Flags-gated, before the payload.
	if s.SendTimestamp != 0 {
		h[29] |= flagSendTS
	}
	if s.HasFrameDesc {
		h[29] |= flagDesc
	}
	dst = append(dst, h[:]...)
	if s.SendTimestamp != 0 {
		var ts [sendTSLen]byte
		binary.BigEndian.PutUint64(ts[:], uint64(s.SendTimestamp))
		dst = append(dst, ts[:]...)
	}
	if s.HasFrameDesc {
		var head [descHeadLen]byte
		binary.BigEndian.PutUint32(head[0:], s.FrameStart)
		binary.BigEndian.PutUint16(head[4:], s.FrameLen)
		if s.FrameRAP {
			head[6] |= descRAP
		}
		if s.FrameDiscardable {
			head[6] |= descDiscardable
		}
		n := len(s.FrameRefs)
		if n > 255 {
			n = 255
		}
		head[7] = byte(n)
		dst = append(dst, head[:]...)
		for i := 0; i < n; i++ {
			var r [4]byte
			binary.BigEndian.PutUint32(r[:], s.FrameRefs[i])
			dst = append(dst, r[:]...)
		}
	}
	return append(dst, s.Payload...)
}

// DecodeSymbol parses a Symbol datagram. The returned Payload aliases b; copy it
// to retain it. A short, mis-versioned, or non-symbol buffer returns an error and
// never panics.
func DecodeSymbol(b []byte) (Symbol, error) {
	if len(b) < symbolHeader {
		return Symbol{}, ErrShort
	}
	typeTag, err := splitLead(b[0])
	if err != nil {
		return Symbol{}, err
	}
	var s Symbol
	switch typeTag {
	case typeSystematic:
		s.Kind = Systematic
	case typeRepair:
		s.Kind = Repair
	default:
		return Symbol{}, ErrType
	}
	s.Flow = binary.BigEndian.Uint32(b[1:])
	s.Epoch = binary.BigEndian.Uint16(b[5:])
	s.PathID = b[7]
	s.WindowBase = binary.BigEndian.Uint32(b[8:])
	s.SrcIndex = binary.BigEndian.Uint32(b[12:])
	s.N = binary.BigEndian.Uint16(b[16:])
	s.RepairKey = binary.BigEndian.Uint16(b[18:])
	s.Priority = b[20]
	s.Deadline = int64(binary.BigEndian.Uint64(b[21:]))
	off := symbolHeader
	if b[29]&flagSendTS != 0 {
		if len(b) < off+sendTSLen {
			return Symbol{}, ErrShort
		}
		s.SendTimestamp = int64(binary.BigEndian.Uint64(b[off:]))
		off += sendTSLen
	}
	if b[29]&flagDesc != 0 {
		if len(b) < off+descHeadLen {
			return Symbol{}, ErrShort
		}
		s.HasFrameDesc = true
		s.FrameStart = binary.BigEndian.Uint32(b[off:])
		s.FrameLen = binary.BigEndian.Uint16(b[off+4:])
		df := b[off+6]
		s.FrameRAP = df&descRAP != 0
		s.FrameDiscardable = df&descDiscardable != 0
		n := int(b[off+7])
		off += descHeadLen
		if len(b) < off+n*4 {
			return Symbol{}, ErrShort
		}
		if n > 0 {
			s.FrameRefs = make([]uint32, n)
			for i := range s.FrameRefs {
				s.FrameRefs[i] = binary.BigEndian.Uint32(b[off:])
				off += 4
			}
		}
	}
	s.Payload = b[off:]
	return s, nil
}

// Feedback is the receiver's cumulative, idempotent report to the sender: the
// state of decoding, not an event. A lost report costs nothing — the next one
// carries the same truth, advanced. The sender's redundancy controller sizes the
// proactive code rate from LossRate and trims with Deficit.
type Feedback struct {
	Flow           uint32
	Epoch          uint16 // flow generation (reserved; mirrors Symbol.Epoch; 0 until used)
	DecodedLowEdge uint32 // everything below this source id is recovered + delivered
	HighestSeen    uint32 // highest source id observed (gap = work outstanding)
	Deficit        uint16 // extra independent symbols the live window needs
	EcnCE          uint16 // CE-marked fraction of received symbols this interval, parts per 65535 (N3 / L4S)
	// LossRate is the receiver's smoothed estimate of the channel erasure rate
	// (the fraction of symbols the network dropped), measured from gaps in the
	// dense systematic source-id sequence and reported as parts per 65535. The
	// sender feed-forwards it into the variance-aware code rate (see
	// internal/flow): a conservative (max-filtered) estimate so a burst is
	// covered, not just the mean.
	LossRate uint16
	// Deficits[i] is the rank deficit of the i-th generation from the delivery
	// cursor (Deficits[0] is the blocking generation, == Deficit). The sender
	// repairs every deficient generation in parallel, not just the blocking one,
	// so a backlog of deficient generations recovers concurrently — the coded
	// analog of an ARQ NACK covering all gaps at once. A deficit > 255 saturates.
	Deficits [MaxFeedbackGens]uint8

	// --- tail extension (N0 extension policy: appended, length-gated) ---

	// CongestionLoss is the PRE-recovery wire-loss count since the last report (N1):
	// source symbols the network dropped, measured before the decoder and NEVER
	// decremented on a successful decode. This is the honest congestion signal a
	// future delay/loss CC loop consumes (RFC 9265) — distinct from LossRate (which
	// the FEC sizer consumes) only in unit (count vs smoothed rate); both are
	// pre-recovery, so coding cannot hide the signal.
	CongestionLoss uint16
	// Burstiness is the receiver's smoothed mean loss-run length in Q8 fixed-point
	// (units of 1/256 symbol; 256 == an i.i.d. channel, > 256 == bursty) (N2). With
	// LossRate it gives the sender a 2-parameter Gilbert estimate (marginal loss +
	// mean burst) to size repair against the burst tail, not the binomial tail.
	Burstiness uint16

	// --- multipath tail extension (N5: appended, length-gated, variable per N paths) ---

	// PathLoss is the receiver's per-path marginal erasure rate (one entry per path, parts
	// per 65535 like LossRate) — the per-path delivery signal that weights the scheduler's
	// repair placement toward the better deliverers. SlotDist is the per-slot erasure-COUNT
	// histogram: SlotDist[j] is the fraction of aligned N-path slots in which exactly j of
	// the N paths erased their symbol (len == len(PathLoss)+1, parts per 65535). Union-decode
	// failure depends only on the total erasure count, so SlotDist is the exact statistic the
	// correlation-aware joint-tail sizer (internal/flow.repairForJointTailN) convolves — it
	// embeds the cross-path correlation a per-path-marginals-only sizer misses. Both nil on a
	// single path (the sizer keeps its single-path binomial/GE form). Up to feedbackMaxPaths.
	PathLoss []uint16
	SlotDist []uint16
}

// EncodeFeedback appends the encoded Feedback to dst and returns the slice. It
// always writes the fullest form (base + CongestionLoss/Burstiness + per-path tail).
func EncodeFeedback(dst []byte, f Feedback) []byte {
	var h [feedbackLenExt]byte
	h[0] = lead(typeFeedback)
	binary.BigEndian.PutUint32(h[1:], f.Flow)
	binary.BigEndian.PutUint16(h[5:], f.Epoch)
	binary.BigEndian.PutUint32(h[7:], f.DecodedLowEdge)
	binary.BigEndian.PutUint32(h[11:], f.HighestSeen)
	binary.BigEndian.PutUint16(h[15:], f.Deficit)
	binary.BigEndian.PutUint16(h[17:], f.EcnCE)
	binary.BigEndian.PutUint16(h[19:], f.LossRate)
	copy(h[21:], f.Deficits[:])
	binary.BigEndian.PutUint16(h[feedbackLen:], f.CongestionLoss)
	binary.BigEndian.PutUint16(h[feedbackLen+2:], f.Burstiness)
	dst = append(dst, h[:]...)
	// Variable per-path section: one nPaths byte, then PathLoss[n], then SlotDist[n+1].
	// Written only when the two arrays are consistent and within bounds; otherwise a lone
	// nPaths=0 marks a single-path report.
	n := len(f.PathLoss)
	if n == 0 || n > feedbackMaxPaths || len(f.SlotDist) != n+1 {
		return append(dst, 0)
	}
	dst = append(dst, byte(n))
	var tmp [2]byte
	for _, v := range f.PathLoss {
		binary.BigEndian.PutUint16(tmp[:], v)
		dst = append(dst, tmp[:]...)
	}
	for _, v := range f.SlotDist {
		binary.BigEndian.PutUint16(tmp[:], v)
		dst = append(dst, tmp[:]...)
	}
	return dst
}

// DecodeFeedback parses a Feedback datagram. The base fields require feedbackLen
// bytes; the CongestionLoss/Burstiness tail and the variable per-path section
// (nPaths + PathLoss + SlotDist) are each read only when present (length- and
// bound-gated), so a shorter encoder decodes cleanly with the absent fields zero. A
// short, mis-versioned, or non-feedback buffer returns an error and never panics.
func DecodeFeedback(b []byte) (Feedback, error) {
	if len(b) < feedbackLen {
		return Feedback{}, ErrShort
	}
	typeTag, err := splitLead(b[0])
	if err != nil {
		return Feedback{}, err
	}
	if typeTag != typeFeedback {
		return Feedback{}, ErrType
	}
	var f Feedback
	f.Flow = binary.BigEndian.Uint32(b[1:])
	f.Epoch = binary.BigEndian.Uint16(b[5:])
	f.DecodedLowEdge = binary.BigEndian.Uint32(b[7:])
	f.HighestSeen = binary.BigEndian.Uint32(b[11:])
	f.Deficit = binary.BigEndian.Uint16(b[15:])
	f.EcnCE = binary.BigEndian.Uint16(b[17:])
	f.LossRate = binary.BigEndian.Uint16(b[19:])
	copy(f.Deficits[:], b[21:feedbackLen])
	if len(b) >= feedbackLenExt {
		f.CongestionLoss = binary.BigEndian.Uint16(b[feedbackLen:])
		f.Burstiness = binary.BigEndian.Uint16(b[feedbackLen+2:])
	}
	// Variable per-path section (length- and bound-gated so a forged nPaths cannot allocate
	// unboundedly or over-read): nPaths byte, then PathLoss[n], then SlotDist[n+1].
	if off := feedbackLenExt; len(b) > off {
		n := int(b[off])
		off++
		if n > 0 && n <= feedbackMaxPaths && len(b) >= off+n*2+(n+1)*2 {
			f.PathLoss = make([]uint16, n)
			for i := 0; i < n; i++ {
				f.PathLoss[i] = binary.BigEndian.Uint16(b[off:])
				off += 2
			}
			f.SlotDist = make([]uint16, n+1)
			for i := 0; i <= n; i++ {
				f.SlotDist[i] = binary.BigEndian.Uint16(b[off:])
				off += 2
			}
		}
	}
	return f, nil
}

// PeekType returns the message-type tag of a datagram (so the host can demux
// symbols from feedback before fully decoding) after validating the version. It
// never panics; a mis-versioned datagram returns ErrVersion.
func PeekType(b []byte) (uint8, error) {
	if len(b) == 0 {
		return 0, ErrShort
	}
	return splitLead(b[0])
}

// IsSymbol reports whether t (from PeekType) tags a media symbol.
func IsSymbol(t uint8) bool { return t == typeSystematic || t == typeRepair }

// IsFeedback reports whether t tags a feedback report.
func IsFeedback(t uint8) bool { return t == typeFeedback }

// IsClockProbe reports whether t tags a clock-offset probe.
func IsClockProbe(t uint8) bool { return t == typeClockProbe }

// IsClockEcho reports whether t tags a clock-offset echo.
func IsClockEcho(t uint8) bool { return t == typeClockEcho }

// ClockProbe is the receiver→sender half of the 2-message clock-offset exchange
// (N4): the receiver sends its local time T0 and the sender echoes it. All times are
// microseconds in each host's own clock; the receiver recovers the offset from the
// round trip and translates its local time into the sender's frame, so the core's
// deadline comparison is correct cross-host (no clock read in the core).
type ClockProbe struct{ T0 int64 }

const clockProbeLen = 9

// EncodeClockProbe appends the encoded probe to dst.
func EncodeClockProbe(dst []byte, p ClockProbe) []byte {
	var h [clockProbeLen]byte
	h[0] = lead(typeClockProbe)
	binary.BigEndian.PutUint64(h[1:], uint64(p.T0))
	return append(dst, h[:]...)
}

// DecodeClockProbe parses a probe; short/mis-versioned/wrong-type buffers error.
func DecodeClockProbe(b []byte) (ClockProbe, error) {
	if len(b) < clockProbeLen {
		return ClockProbe{}, ErrShort
	}
	t, err := splitLead(b[0])
	if err != nil {
		return ClockProbe{}, err
	}
	if t != typeClockProbe {
		return ClockProbe{}, ErrType
	}
	return ClockProbe{T0: int64(binary.BigEndian.Uint64(b[1:]))}, nil
}

// ClockEcho is the sender→receiver reply: the probe's T0, the sender's clock T1 when
// it received the probe, and T2 when it sends the echo. With the receiver's receive
// time T3, the offset is ((T1−T0)+(T2−T3))/2 and the round trip (T3−T0)−(T2−T1).
type ClockEcho struct{ T0, T1, T2 int64 }

const clockEchoLen = 25

// EncodeClockEcho appends the encoded echo to dst.
func EncodeClockEcho(dst []byte, e ClockEcho) []byte {
	var h [clockEchoLen]byte
	h[0] = lead(typeClockEcho)
	binary.BigEndian.PutUint64(h[1:], uint64(e.T0))
	binary.BigEndian.PutUint64(h[9:], uint64(e.T1))
	binary.BigEndian.PutUint64(h[17:], uint64(e.T2))
	return append(dst, h[:]...)
}

// DecodeClockEcho parses an echo; short/mis-versioned/wrong-type buffers error.
func DecodeClockEcho(b []byte) (ClockEcho, error) {
	if len(b) < clockEchoLen {
		return ClockEcho{}, ErrShort
	}
	t, err := splitLead(b[0])
	if err != nil {
		return ClockEcho{}, err
	}
	if t != typeClockEcho {
		return ClockEcho{}, ErrType
	}
	return ClockEcho{
		T0: int64(binary.BigEndian.Uint64(b[1:])),
		T1: int64(binary.BigEndian.Uint64(b[9:])),
		T2: int64(binary.BigEndian.Uint64(b[17:])),
	}, nil
}

// IsMTUProbe reports whether t tags a DPLPMTUD probe.
func IsMTUProbe(t uint8) bool { return t == typeMTUProbe }

// IsMTUProbeAck reports whether t tags a DPLPMTUD probe acknowledgement.
func IsMTUProbeAck(t uint8) bool { return t == typeMTUProbeAck }

// mtuProbeHdr is the fixed prefix of a probe: the lead byte plus a 4-byte correlation
// nonce. The rest of the datagram is zero padding sized so the whole datagram tests a
// candidate path MTU (RFC 8899 §3 — a probe is padded to the size under test, sent with
// Don't-Fragment so a path that cannot carry it drops rather than fragments it).
const mtuProbeHdr = 5

// mtuProbeAckLen is the full length of a probe acknowledgement: lead + nonce + the size the
// receiver actually observed on the wire (so the sender confirms what physically arrived).
const mtuProbeAckLen = 7

// EncodeMTUProbe appends a probe datagram of exactly size bytes (clamped up to the header
// length): the lead byte, the correlation nonce, then zero padding to size. The padding is
// the point — it makes the datagram large enough to test the candidate path MTU.
func EncodeMTUProbe(dst []byte, nonce uint32, size int) []byte {
	if size < mtuProbeHdr {
		size = mtuProbeHdr
	}
	off := len(dst)
	dst = append(dst, make([]byte, size)...)
	dst[off] = lead(typeMTUProbe)
	binary.BigEndian.PutUint32(dst[off+1:], nonce)
	return dst
}

// DecodeMTUProbe parses a probe, returning its nonce and the size that ACTUALLY arrived
// (len(b) — what physically traversed the path, which is what the ack must confirm).
func DecodeMTUProbe(b []byte) (nonce uint32, size int, err error) {
	if len(b) < mtuProbeHdr {
		return 0, 0, ErrShort
	}
	t, err := splitLead(b[0])
	if err != nil {
		return 0, 0, err
	}
	if t != typeMTUProbe {
		return 0, 0, ErrType
	}
	return binary.BigEndian.Uint32(b[1:]), len(b), nil
}

// EncodeMTUProbeAck appends a probe acknowledgement: the lead byte, the probe's nonce, and
// the size the receiver observed (capped at uint16; probe sizes never approach that).
func EncodeMTUProbeAck(dst []byte, nonce uint32, size uint16) []byte {
	var h [mtuProbeAckLen]byte
	h[0] = lead(typeMTUProbeAck)
	binary.BigEndian.PutUint32(h[1:], nonce)
	binary.BigEndian.PutUint16(h[5:], size)
	return append(dst, h[:]...)
}

// DecodeMTUProbeAck parses a probe acknowledgement into its nonce and confirmed size.
func DecodeMTUProbeAck(b []byte) (nonce uint32, size uint16, err error) {
	if len(b) < mtuProbeAckLen {
		return 0, 0, ErrShort
	}
	t, err := splitLead(b[0])
	if err != nil {
		return 0, 0, err
	}
	if t != typeMTUProbeAck {
		return 0, 0, ErrType
	}
	return binary.BigEndian.Uint32(b[1:]), binary.BigEndian.Uint16(b[5:]), nil
}

// IsHandshakeInit reports whether t tags a handshake message 1 (initiator → responder).
func IsHandshakeInit(t uint8) bool { return t == typeHandshakeInit }

// IsHandshakeResp reports whether t tags a handshake message 2 (responder → initiator).
func IsHandshakeResp(t uint8) bool { return t == typeHandshakeResp }

// IsHandshakeCookie reports whether t tags a mac2 cookie reply (responder → initiator).
func IsHandshakeCookie(t uint8) bool { return t == typeHandshakeCookie }

// EncodeHandshakeCookie frames an opaque cookie-reply payload behind the version nibble.
func EncodeHandshakeCookie(dst, payload []byte) []byte {
	return append(append(dst, lead(typeHandshakeCookie)), payload...)
}

// EncodeHandshakeInit frames an opaque handshake-message-1 payload (the crypto layer's
// bytes) behind the version nibble. The payload is produced and consumed by
// internal/crypto; wire only carries it, so the key schedule stays out of the waist.
func EncodeHandshakeInit(dst, payload []byte) []byte {
	return append(append(dst, lead(typeHandshakeInit)), payload...)
}

// EncodeHandshakeResp frames an opaque handshake-message-2 payload.
func EncodeHandshakeResp(dst, payload []byte) []byte {
	return append(append(dst, lead(typeHandshakeResp)), payload...)
}

// DecodeHandshake returns the opaque handshake payload of a datagram (the bytes after
// the leading version/type byte) for the given handshake type, after validating the
// version. A short, mis-versioned, or wrong-type buffer returns an error and never
// panics. The returned payload aliases b.
func DecodeHandshake(b []byte) (payload []byte, err error) {
	if len(b) < 1 {
		return nil, ErrShort
	}
	t, err := splitLead(b[0])
	if err != nil {
		return nil, err
	}
	if t != typeHandshakeInit && t != typeHandshakeResp && t != typeHandshakeCookie {
		return nil, ErrType
	}
	return b[1:], nil
}
