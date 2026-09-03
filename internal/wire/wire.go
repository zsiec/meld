// Package wire defines Meld's narrow waist: the normalized Symbol and Feedback
// types every path encodes/decodes through, plus their byte codec. The
// deterministic core (internal/flow) speaks only these types; coding-coefficient
// regeneration, codec specifics, and the per-codec media descriptor are erased on
// either side of this seam. New behavior is a new field here, never a special
// case inside the core.
//
// # Versioning
//
// The leading byte packs a 4-bit format VERSION in its high nibble and the
// message-type tag in its low nibble: byte0 = (Version<<4) | type. A decoder that
// sees any other version returns ErrVersion. Version 1 is the authoritative
// research format; the codec accepts exactly the layout defined in this package.
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
	// ErrInvalid is returned when a datagram is well-formed enough to parse its base
	// header but declares an invalid bounded field, such as an oversized sparse list.
	ErrInvalid = errors.New("meld: wire: invalid field")
)

// Version is the current wire-format version, carried in the high nibble of every
// datagram's leading byte. This repository defines version 1 directly; research
// changes rewrite that format in place.
const Version = 1

// Message-type tags (the low nibble of the leading byte).
const (
	typeSystematic      = 0x01
	typeRepair          = 0x02
	typeFeedback        = 0x03
	typeClockProbe      = 0x04 // receiver → sender clock-offset probe
	typeClockEcho       = 0x05 // sender → receiver echo of the probe
	typeHandshakeInit   = 0x06 // initiator → responder handshake message 1 (docs/encryption.md)
	typeHandshakeResp   = 0x07 // responder → initiator handshake message 2
	typeHandshakeCookie = 0x08 // responder → initiator mac2 cookie reply (under load)
	typeMTUProbe        = 0x09 // sender → receiver padded DPLPMTUD probe (RFC 8899)
	typeMTUProbeAck     = 0x0A // receiver → sender probe acknowledgement
	typeSparseRepair    = 0x0B // repair over an explicit sparse source-id set
	typeUnitRepair      = 0x0C // exact source retransmission, paced/accounted as repair
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
		typeMTUProbe, typeMTUProbeAck, typeSparseRepair, typeUnitRepair:
		return t, nil
	default:
		return 0, ErrType
	}
}

// symbolHeader is the header length preceding a Symbol payload (v1, including the
// 1-byte host-stamped PathID). Three optional, Flags-gated extensions follow the base in a
// fixed order: an 8-byte send timestamp (flagSendTS), a variable frame descriptor
// (flagDesc), then a 4-byte source length (flagSourceLen). The Flags bits live in
// the header's Flags byte (h[29]).
const (
	symbolHeader = 30
	sendTSLen    = 8 // flagSendTS extension: SendTimestamp (int64)
	sourceLenLen = 4 // flagSourceLen extension: source length or compact-repair prefix width
	// flagDesc extension (variable): FrameStart(4) + FrameLen(2) + descFlags(1) + nRefs(1)
	// + nRefs×RefStart(4). descHeadLen is the fixed head; descMaxRefs caps the count so a
	// forged nRefs cannot over-read (a B-frame needs 2 references; AV1's buffer holds 7).
	descHeadLen   = 8
	descMaxRefs   = 15
	sparseMaxIDs  = 64
	flagSendTS    = 0x01
	flagDesc      = 0x02
	flagSourceLen = 0x04
	flagMask      = flagSendTS | flagDesc | flagSourceLen
	// descFlags bits inside the frame-descriptor extension's flags byte.
	descRAP             = 0x01
	descDiscardable     = 0x02
	descNonPicture      = 0x04
	descRecoveryRefresh = 0x08
	descLTR             = 0x10
	descFlagMask        = descRAP | descDiscardable | descNonPicture | descRecoveryRefresh | descLTR
)

// MaxFeedbackGens is the number of consecutive generations (from the delivery
// cursor) whose rank deficit a Feedback report carries, so the sender can repair
// every deficient generation in parallel rather than only the blocking one.
const MaxFeedbackGens = 32

// feedbackLen is the fixed prefix through Deficits. feedbackLenExt includes
// CongestionLoss and Burstiness; the variable path section and fixed policy tail
// follow it. DecodeFeedback requires the complete current layout.
const (
	feedbackLen           = 21 + MaxFeedbackGens
	feedbackLenExt        = feedbackLen + 4
	feedbackMediaStatsLen = 16
	feedbackLTRLen        = 6 // NewestDecodableLTR (4) + BrokenAnchors (2), after the media stats
	feedbackOutageRunLen  = 2 // largest receiver-classified outage run since the prior report
	feedbackFixedTailLen  = feedbackMediaStatsLen + feedbackLTRLen + 1 + 8 + 2 + feedbackOutageRunLen
	feedbackMaxPaths      = 8 // bound on the per-path section (a forged nPaths cannot allocate unboundedly)
)

// Kind distinguishes exact source values, algebraic repair equations, and
// repair-class exact retransmissions.
type Kind uint8

// Symbol kinds.
const (
	// Systematic carries one source symbol verbatim (coefficient = unit vector).
	Systematic Kind = iota
	// Repair carries a random linear combination of a window of source symbols.
	Repair
	// SparseRepair carries a random linear combination of explicitly listed source
	// ids. It is used for protected/reference-layer repair without coding unrelated
	// disposable columns in the same contiguous span.
	SparseRepair
	// UnitRepair carries one exact-length source retransmission. It enters the
	// decoder as a systematic value but remains repair-class traffic for source-
	// first pacing and accounting.
	UnitRepair
)

// Symbol is the normalized coded symbol crossing the waist. For a Systematic
// symbol, SrcIndex is the source id and N/RepairKey are unused. For a Repair
// symbol, WindowBase+N delimit the window the combination spans and RepairKey
// regenerates the GF(2^8) coefficients (internal/code.GenCoeffs). For a
// SparseRepair symbol, SparseIDs lists the source columns explicitly and N is
// len(SparseIDs). For UnitRepair, SrcIndex names the exact retransmitted source.
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
	WindowBase uint32   // low edge of the coding window
	SrcIndex   uint32   // Systematic: source id. Repair: repair counter.
	N          uint16   // Repair: window width covered
	RepairKey  uint16   // Repair: coefficient PRNG seed
	SparseIDs  []uint32 // SparseRepair: explicitly coded source ids, in coefficient order
	Priority   uint8    // descriptor: protection tier (0 = most disposable)
	Deadline   int64    // descriptor: decode-by, in clock microseconds
	// SendTimestamp, when non-zero, is the sender's clock time (microseconds) at
	// emission, carried in an 8-byte header extension flagged by flagSendTS. With the
	// receiver's receive time it gives a per-symbol one-way
	// delay whose VARIATION is offset-invariant, refining the clock-offset estimate
	// and exposing per-symbol latency. Zero ⇒ not carried (the base 30-byte header).
	SendTimestamp int64
	// HasSourceLength gates SourceLength. For Systematic and UnitRepair it is the
	// exact unpadded coded-source length after any host encryption transform. For
	// Repair and SparseRepair it is the transmitted application-prefix width; the
	// receiver restores omitted zero padding before GF arithmetic.
	HasSourceLength bool
	SourceLength    uint32

	// HasFrameDesc gates the frame-descriptor extension (flagDesc): the access-unit
	// dependency the receiver uses to compute loss propagation without codec parsing. The
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
	// FrameRecoveryRefresh marks a reference slice participating in a signaled intra-refresh
	// interval; FrameDiscardable marks a unit nothing references; FrameNonPicture marks
	// metadata/parameter material that is not a displayed coded picture; FrameLTR marks a
	// long-term-reference candidate the encoder retains, so the receiver can report the
	// newest decodable one (Feedback.NewestDecodableLTR) as the LTR-resync anchor.
	HasFrameDesc         bool
	FrameStart           uint32
	FrameLen             uint16
	FrameRefs            []uint32
	FrameRAP             bool
	FrameRecoveryRefresh bool
	FrameDiscardable     bool
	FrameNonPicture      bool
	FrameLTR             bool

	Payload []byte // coded bytes, sized to the validated path MTU
}

// EncodeSymbol appends the encoded Symbol to dst and returns the extended slice.
func EncodeSymbol(dst []byte, s Symbol) []byte {
	typeTag := uint8(typeSystematic)
	switch s.Kind {
	case Repair:
		typeTag = typeRepair
	case SparseRepair:
		typeTag = typeSparseRepair
		n := len(s.SparseIDs)
		if n > int(^uint16(0)) {
			n = int(^uint16(0))
		}
		s.N = uint16(n)
	case UnitRepair:
		typeTag = typeUnitRepair
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
	if s.HasSourceLength {
		h[29] |= flagSourceLen
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
		if s.FrameRecoveryRefresh {
			head[6] |= descRecoveryRefresh
		}
		if s.FrameDiscardable {
			head[6] |= descDiscardable
		}
		if s.FrameNonPicture {
			head[6] |= descNonPicture
		}
		if s.FrameLTR {
			head[6] |= descLTR
		}
		n := len(s.FrameRefs)
		if n > descMaxRefs {
			n = descMaxRefs
		}
		head[7] = byte(n)
		dst = append(dst, head[:]...)
		for i := 0; i < n; i++ {
			var r [4]byte
			binary.BigEndian.PutUint32(r[:], s.FrameRefs[i])
			dst = append(dst, r[:]...)
		}
	}
	if s.HasSourceLength {
		var n [sourceLenLen]byte
		binary.BigEndian.PutUint32(n[:], s.SourceLength)
		dst = append(dst, n[:]...)
	}
	if s.Kind == SparseRepair {
		n := int(s.N)
		if n > len(s.SparseIDs) {
			n = len(s.SparseIDs)
		}
		for i := 0; i < n; i++ {
			var id [4]byte
			binary.BigEndian.PutUint32(id[:], s.SparseIDs[i])
			dst = append(dst, id[:]...)
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
	case typeSparseRepair:
		s.Kind = SparseRepair
	case typeUnitRepair:
		s.Kind = UnitRepair
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
	if b[29]&^flagMask != 0 {
		return Symbol{}, ErrInvalid
	}
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
		if df&^descFlagMask != 0 {
			return Symbol{}, ErrInvalid
		}
		s.FrameRAP = df&descRAP != 0
		s.FrameRecoveryRefresh = df&descRecoveryRefresh != 0
		s.FrameDiscardable = df&descDiscardable != 0
		s.FrameNonPicture = df&descNonPicture != 0
		s.FrameLTR = df&descLTR != 0
		n := int(b[off+7])
		off += descHeadLen
		if n > descMaxRefs {
			return Symbol{}, ErrInvalid
		}
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
	if b[29]&flagSourceLen != 0 {
		if len(b) < off+sourceLenLen {
			return Symbol{}, ErrShort
		}
		s.HasSourceLength = true
		s.SourceLength = binary.BigEndian.Uint32(b[off:])
		off += sourceLenLen
	}
	if s.Kind == SparseRepair {
		n := int(s.N)
		if n <= 0 || n > sparseMaxIDs {
			return Symbol{}, ErrInvalid
		}
		if len(b) < off+n*4 {
			return Symbol{}, ErrShort
		}
		s.SparseIDs = make([]uint32, n)
		for i := range s.SparseIDs {
			s.SparseIDs[i] = binary.BigEndian.Uint32(b[off:])
			off += 4
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
	EcnCE          uint16 // CE-marked fraction of received symbols this interval, parts per 65535
	// LossRate is the receiver's smoothed estimate of the channel erasure rate
	// (the fraction of symbols the network dropped), measured from gaps in the
	// dense systematic source-id sequence and reported as parts per 65535. The
	// sender feed-forwards it into the variance-aware code rate (see
	// internal/flow): a conservative (max-filtered) estimate so a burst is
	// covered, not just the mean.
	LossRate uint16
	// In generation mode, Deficits[i] is the rank deficit of the i-th generation
	// from the delivery cursor (Deficits[0] is the blocking generation, ==
	// Deficit). In sliding mode the same fixed 32 bytes extend Missing with either
	// four bitmap words or a compact run representation. See docs/wireformat.md.
	// The profile-specific union keeps feedback at a fixed size.
	Deficits [MaxFeedbackGens]uint8

	// --- channel observations ---

	// CongestionLoss is the pre-recovery wire-loss count since the last report:
	// source symbols the network dropped, measured before the decoder and NEVER
	// decremented on a successful decode. This is the honest congestion signal a
	// future delay/loss CC loop consumes (RFC 9265) — distinct from LossRate (which
	// the FEC sizer consumes) only in unit (count vs smoothed rate); both are
	// pre-recovery, so coding cannot hide the signal.
	CongestionLoss uint16
	// Burstiness is the receiver's smoothed mean loss-run length in Q8 fixed-point
	// (units of 1/256 symbol; 256 == an i.i.d. channel, > 256 == bursty). With
	// LossRate it gives the sender a 2-parameter Gilbert estimate (marginal loss +
	// mean burst) to size repair against the burst tail, not the binomial tail.
	Burstiness uint16

	// --- variable multipath observations ---

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

	// --- media-damage observations ---

	// Frames/DecodableFrames and Keyframes/DecodableKeyframes are cumulative receiver-side
	// media decodability counters derived from WriteFrame descriptors. They let the sender's
	// zero-config control loop distinguish ordinary wire loss from dependency damage and ask
	// an attached encoder for a shorter recovery cadence when long bursts are damaging
	// reference islands. Zero means absent or no descriptors observed.
	Frames             uint32
	DecodableFrames    uint32
	Keyframes          uint32
	DecodableKeyframes uint32

	// --- LTR-resync state ---

	// NewestDecodableLTR is FrameStart+1 of the newest frame flagged FrameLTR that the
	// receiver has RESOLVED decodable (delivered whole with its dependency closure
	// intact) — the safe reference an encoder can resync against after a broken chain
	// (EncoderControl.Resync). 0 = none yet. Cumulative and idempotent like the rest of
	// feedback: frames resolve in cursor order, so the value is monotonic.
	NewestDecodableLTR uint32
	// BrokenAnchors counts REFERENCED pictures (non-discardable, so their loss cascades)
	// that resolved undecodable at or after the newest decodable LTR — the reference-chain
	// damage signal the resync controller acts on, distinct from DecodableFrames (whose
	// deltas also count broken disposable leaves, which no resync can help). Cumulative,
	// wrapping uint16: the consumer differences successive reports, so wrap is harmless.
	BrokenAnchors uint16

	// DeadPaths is a bitmap of paths the receiver has classified as in OUTAGE (bit i =
	// path i): a per-path consecutive lost-slot run beyond the recovery horizon while
	// other paths delivered. The sender fails systematic placement over to the live
	// paths and probes the dead ones with droppable repair; any admitted arrival on a
	// dead path clears its bit (coding-native fast failover). 0 = all paths alive
	// (always 0 on single-path flows).
	DeadPaths uint8

	// Missing is the first word of the sliding profile's NACK bitmap for the stuck
	// neighborhood's rank-closing basis: bit k names DecodedLowEdge+k. Deficits
	// carries the continuation. Named ids are unresolved FREE columns below the
	// receiver's decode frontier, so every delivered UNIT value removes one
	// independent degree of freedom. Zero means the first word has no candidate;
	// continuation candidates may still exist.
	Missing uint64

	// SettledLost counts source ids the receiver's SETTLED walk confirmed lost since
	// the last report: an id counts only after lower-neighborhood arrivals plus a
	// reorder holdoff prove it absent, so a merely reordered-late arrival never
	// counts. This is the reorder-tolerant clean/dirty evidence the sender's
	// confirmed-clean floor decay keys on — the raw-order signals (LossRate,
	// CongestionLoss, instantaneous Deficit/Missing) read dirty on almost every
	// report under real reorder even at zero true loss, permanently blocking the
	// decay. The settled walk feeds NOTHING else: the sizing estimators keep the
	// raw-order walks. Saturates at 65535.
	SettledLost uint16

	// OutageRun is the largest source-symbol loss run since the prior report that
	// exceeded the receiver's measured recovery horizon. Unlike Burstiness, which
	// deliberately excludes such unrecoverable interiors when outage composure is
	// enabled, this signal preserves fade geometry for automatic repair-time
	// diversity. It saturates at 65535; zero means no qualifying run was observed.
	OutageRun uint16
}

// EncodeFeedback appends the complete version-1 Feedback to dst and returns the
// slice.
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
		dst = append(dst, 0)
		return encodeFeedbackMediaStats(dst, f)
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
	return encodeFeedbackMediaStats(dst, f)
}

func encodeFeedbackMediaStats(dst []byte, f Feedback) []byte {
	var media [feedbackMediaStatsLen]byte
	binary.BigEndian.PutUint32(media[0:], f.Frames)
	binary.BigEndian.PutUint32(media[4:], f.DecodableFrames)
	binary.BigEndian.PutUint32(media[8:], f.Keyframes)
	binary.BigEndian.PutUint32(media[12:], f.DecodableKeyframes)
	dst = append(dst, media[:]...)
	var ltr [feedbackLTRLen]byte
	binary.BigEndian.PutUint32(ltr[0:], f.NewestDecodableLTR)
	binary.BigEndian.PutUint16(ltr[4:], f.BrokenAnchors)
	dst = append(dst, ltr[:]...)
	dst = append(dst, f.DeadPaths)
	var miss [8]byte
	binary.BigEndian.PutUint64(miss[:], f.Missing)
	dst = append(dst, miss[:]...)
	var settled [2]byte
	binary.BigEndian.PutUint16(settled[:], f.SettledLost)
	dst = append(dst, settled[:]...)
	var outage [feedbackOutageRunLen]byte
	binary.BigEndian.PutUint16(outage[:], f.OutageRun)
	return append(dst, outage[:]...)
}

// DecodeFeedback parses the complete version-1 feedback layout. A short,
// mis-versioned, malformed, or non-feedback buffer returns an error and never
// panics.
func DecodeFeedback(b []byte) (Feedback, error) {
	if len(b) < feedbackLenExt+1 {
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
	f.CongestionLoss = binary.BigEndian.Uint16(b[feedbackLen:])
	f.Burstiness = binary.BigEndian.Uint16(b[feedbackLen+2:])
	off := feedbackLenExt
	n := int(b[off])
	off++
	if n > feedbackMaxPaths {
		return Feedback{}, ErrInvalid
	}
	if n > 0 {
		pathBytes := n*2 + (n+1)*2
		if len(b) < off+pathBytes {
			return Feedback{}, ErrShort
		}
		f.PathLoss = make([]uint16, n)
		for i := range f.PathLoss {
			f.PathLoss[i] = binary.BigEndian.Uint16(b[off:])
			off += 2
		}
		f.SlotDist = make([]uint16, n+1)
		for i := range f.SlotDist {
			f.SlotDist[i] = binary.BigEndian.Uint16(b[off:])
			off += 2
		}
	}
	if len(b) < off+feedbackFixedTailLen {
		return Feedback{}, ErrShort
	}
	if len(b) != off+feedbackFixedTailLen {
		return Feedback{}, ErrInvalid
	}
	f.Frames = binary.BigEndian.Uint32(b[off:])
	f.DecodableFrames = binary.BigEndian.Uint32(b[off+4:])
	f.Keyframes = binary.BigEndian.Uint32(b[off+8:])
	f.DecodableKeyframes = binary.BigEndian.Uint32(b[off+12:])
	off += feedbackMediaStatsLen
	f.NewestDecodableLTR = binary.BigEndian.Uint32(b[off:])
	f.BrokenAnchors = binary.BigEndian.Uint16(b[off+4:])
	off += feedbackLTRLen
	f.DeadPaths = b[off]
	off++
	f.Missing = binary.BigEndian.Uint64(b[off:])
	off += 8
	f.SettledLost = binary.BigEndian.Uint16(b[off:])
	off += 2
	f.OutageRun = binary.BigEndian.Uint16(b[off:])
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
func IsSymbol(t uint8) bool {
	return t == typeSystematic || t == typeRepair || t == typeSparseRepair || t == typeUnitRepair
}

// IsSystematic reports whether t (from PeekType) tags a fresh source symbol.
// Hosts use this distinction to keep recovery traffic from queueing ahead of
// later source data at a shared transmit pacer.
func IsSystematic(t uint8) bool { return t == typeSystematic }

// IsUnitRepair reports whether t tags an exact source retransmission. Hosts use
// this distinction to release deadline-critical exact closure ahead of queued
// fungible equations while preserving source priority and the aggregate budget.
func IsUnitRepair(t uint8) bool { return t == typeUnitRepair }

// IsFeedback reports whether t tags a feedback report.
func IsFeedback(t uint8) bool { return t == typeFeedback }

// IsClockProbe reports whether t tags a clock-offset probe.
func IsClockProbe(t uint8) bool { return t == typeClockProbe }

// IsClockEcho reports whether t tags a clock-offset echo.
func IsClockEcho(t uint8) bool { return t == typeClockEcho }

// ClockProbe is the receiver→sender half of the 2-message clock-offset exchange
// the receiver sends its local time T0 and the sender echoes it. All times are
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
