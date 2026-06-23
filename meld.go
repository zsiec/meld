// Package meld is the public API of the Meld transport — Media Erasure-coded Live
// Delivery: a coded, survivable transport for live and contribution-grade video. A
// Sender streams media chunks;
// a Receiver delivers them in order, recovering loss from the sliding-window
// network code rather than naming-and-retransmitting missing packets.
//
// This package is a thin facade over the internal sans-I/O core (internal/flow) and
// the UDP host (internal/session); see the README and docs/ for the architecture.
package meld

import (
	"time"

	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/flow"
	"github.com/zsiec/meld/internal/session"
)

// Config parameterizes a Sender/Receiver pair. Both ends must agree on Flow,
// SymbolSize, GenSize, and BufferMicros; Redundancy is the sender's proactive
// code rate (repair symbols per source symbol).
type Config struct {
	// Flow identifies the flow on the wire.
	Flow uint32
	// SymbolSize is the fixed media-chunk / coded-symbol size in bytes.
	SymbolSize int
	// GenSize is the coding generation (window) size in source symbols.
	GenSize int
	// Redundancy is the FLOOR proactive code rate (repair per source symbol); the
	// controller raises the rate above it as the measured loss requires.
	Redundancy float64
	// TargetFailure is the per-generation decode-failure probability the redundancy
	// controller sizes the proactive code rate to (the QoS knob). 0 ⇒ default 1e-3.
	TargetFailure float64
	// BufferMicros is the playout/deadline budget in microseconds.
	BufferMicros int64
	// ElasticMicros (0 = off) is the burst-elastic deadline extension: a generation still in a
	// rank deficit at its nominal deadline is held this much longer for reactive recovery, so the
	// sender carries a smaller proactive burst margin. Ready symbols deliver immediately; only the
	// deficit symbols a burst hit incur the extra latency (a transient p99 excursion for overhead).
	ElasticMicros int64
	// Sliding selects the band-form sliding-window coder instead of the default
	// generation coder. It codes continuous, fungible repair over one elastic window
	// and delivers each symbol the instant it decodes.
	//
	// Reach for it when the latency budget (BufferMicros) is TIGHT relative to the
	// RTT — above all when the budget is SMALLER than the round trip: low-latency
	// contribution over a long-haul lossy link, where an ARQ retransmit and the
	// generation coder's feedback-driven reactive tier cannot recover in time but
	// this coder's continuous, RTT-independent proactive repair can. It also costs
	// less repair overhead (a wider coding window needs a smaller variance margin),
	// so it suits bandwidth-constrained links.
	//
	// It is NOT a universal low-latency win: at a GENEROUS budget with a low RTT the
	// generation coder delivers at LOWER latency — a wider recovery band trades
	// latency for overhead efficiency — so leave Sliding off for low-RTT or
	// relaxed-deadline paths. Decode costs O(CodingWindow²) per symbol.
	Sliding bool
	// CodingWindow is the MAX sliding band width in source symbols — the recovery
	// span and O(window²) decode-cost cap. The sender adapts the effective span below
	// it to fit the deadline budget, so this is a ceiling, not a fixed width. 0 ⇒
	// default. Ignored unless Sliding.
	CodingWindow int
	// CongestionControl enables the delay-based congestion controller: it derives the
	// send-rate budget from the standing-queue delay (loss-agnostic, since coding masks
	// loss) and throttles REPAIR to stay within it — protecting media goodput and
	// surfacing a target rate the source should pace within. Off ⇒ a static rate
	// ceiling only. Leave off until validated on your paths.
	CongestionControl bool
	// Pace enables the host transmit pacer: the sender smooths coded datagrams onto the
	// wire at a rate slaved to the congestion/ceiling budget (never a second controller)
	// and backpressures Write when the send queue would grow past the deadline — so an
	// encoder burst (a keyframe) is spread across the budget instead of dumped as a
	// microburst, and the source is bounded by the budget rather than bloating a buffer.
	// On by default (DefaultConfig). Turn off to transmit each emit immediately.
	Pace bool
	// MaxBitrate caps the sender's aggregate emitted rate in bits/sec: media
	// (systematic) is never dropped, but REPAIR is throttled to hold the total under
	// this ceiling. 0 ⇒ a generous default (100 Mbps). Use it to bound redundancy to a
	// fixed bandwidth budget (e.g. 2x the source, to compare like-for-like against
	// duplicate-everything bonding).
	MaxBitrate int64
	// EvictBrokenFrames turns on media-aware early eviction: when a frame fed via
	// WriteFrame can no longer decode — its own loss, or a dead reference sub-tree — the
	// receiver abandons it (and its dependents) immediately instead of waiting out each
	// deadline, so the next keyframe resyncs sooner and the sender stops repairing the
	// dead GOP. Requires WriteFrame descriptors; a no-op for plain Write byte streams. Off
	// by default: it drops doomed-but-recoverable bytes to deliver whole DECODABLE frames
	// faster, so only flows that consume whole pictures want it.
	EvictBrokenFrames bool
	// Passphrase enables encryption (docs/encryption.md): when non-empty, the Sender and
	// Receiver run an X25519 + ML-KEM-768 hybrid post-quantum handshake before any media
	// and AEAD-seal every chunk (encrypt-then-code, forward-secret, authenticated). Both
	// ends must use the same Passphrase. Empty ⇒ cleartext (unchanged). Encrypted chunks
	// are at most SymbolSize-16 bytes (the AEAD tag). Supported on both the single-path
	// and Multipath APIs (the handshake rides path 0). NewSender blocks until the secure
	// channel is established.
	Passphrase string
	// Salt domain-separates the Argon2id passphrase stretch (docs/encryption.md): both
	// ends must use the same Salt, and distinct deployments should use distinct salts so a
	// passphrase reused across them yields unrelated PSKs. Empty ⇒ a fixed default salt.
	// Ignored unless Passphrase is set.
	Salt string
	// Argon2Time, Argon2MemoryKiB, and Argon2Threads tune the Argon2id work factor (RFC
	// 9106): iterations, memory in KiB, and lanes. Both ends must agree. Any field left 0
	// takes the default (t=3, m=64 MiB, p=4). Raise them to harden a weak passphrase
	// against offline guessing at the cost of a slower handshake. Ignored unless Passphrase
	// is set.
	Argon2Time      uint32
	Argon2MemoryKiB uint32
	Argon2Threads   uint8
	// EpochSize is the number of source symbols sealed under one epoch key before the
	// sender ratchets to a fresh key (forward secrecy granularity). Both ends must agree.
	// 0 ⇒ default. Ignored unless Passphrase is set.
	EpochSize uint32
	// CookieThreshold is the per-tick handshake-attempt count above which the Receiver
	// switches on the mac2 return-routability cookie (anti-amplification under flood). 0 ⇒
	// a generous default that keeps the cookie gate dormant. Ignored unless Passphrase is
	// set.
	CookieThreshold uint32
}

// toSecurity builds the host's security config from the public Config, or nil for a
// cleartext flow.
func (c Config) toSecurity() *session.SecurityConfig {
	if c.Passphrase == "" {
		return nil
	}
	sec := &session.SecurityConfig{
		Passphrase:      []byte(c.Passphrase),
		EpochSize:       c.EpochSize,
		CookieThreshold: c.CookieThreshold,
		// Zero fields here are filled with the RFC 9106 defaults by SecurityConfig.params.
		Params: crypto.Argon2idParams{
			Time:    c.Argon2Time,
			Memory:  c.Argon2MemoryKiB,
			Threads: c.Argon2Threads,
		},
	}
	if c.Salt != "" {
		sec.Salt = []byte(c.Salt)
	}
	return sec
}

// MaxChunk returns the largest media chunk Write accepts: SymbolSize, or SymbolSize
// minus the AEAD tag when a Passphrase is set.
func (c Config) MaxChunk() int {
	if c.Passphrase != "" {
		return c.SymbolSize - crypto.Overhead
	}
	return c.SymbolSize
}

// DefaultConfig returns a starting configuration (1316-byte chunks, generation
// 16, 25% redundancy, 200 ms buffer).
func DefaultConfig() Config {
	c := flow.DefaultConfig()
	return Config{
		SymbolSize:    c.SymbolSize,
		GenSize:       c.GenSize,
		Redundancy:    c.Redundancy,
		TargetFailure: c.TargetFailure,
		BufferMicros:  c.BufferMicros,
		Pace:          c.Pace, // on by default (flow.DefaultConfig)
	}
}

func (c Config) toFlow() flow.Config {
	return flow.Config{
		Flow:              c.Flow,
		SymbolSize:        c.SymbolSize,
		GenSize:           c.GenSize,
		Redundancy:        c.Redundancy,
		TargetFailure:     c.TargetFailure,
		BufferMicros:      c.BufferMicros,
		ElasticMicros:     c.ElasticMicros,
		Sliding:           c.Sliding,
		CodingWindow:      c.CodingWindow,
		CongestionControl: c.CongestionControl,
		Pace:              c.Pace,
		MaxBitrate:        c.MaxBitrate,
		EvictBrokenFrames: c.EvictBrokenFrames,
	}
}

// toFlowPaths is toFlow with the path count set (coding-native multipath, N5): the
// generation is spread across paths and decoded from the union.
func (c Config) toFlowPaths(paths int) flow.Config {
	fc := c.toFlow()
	fc.Paths = paths
	return fc
}

// FrameDesc is the per-access-unit media descriptor a shaper hands WriteFrame: the
// protection tier plus the dependency the receiver needs to compute decodable-frame
// stats parse-free (WP6). FrameID identifies the access unit (the shaper's unit id);
// RefFrameIDs are its dependency access units (a B-frame's two anchors, a P-frame's one);
// Chunks is its total chunk count (so the receiver knows its id range); RAP marks a
// keyframe; Discardable marks a unit nothing references.
type FrameDesc struct {
	Priority    uint8
	FrameID     uint32
	RefFrameIDs []uint32
	Chunks      uint16
	RAP         bool
	Discardable bool
}

func (d FrameDesc) toFlow() flow.FrameDesc {
	return flow.FrameDesc{
		Priority: d.Priority, FrameID: d.FrameID, RefFrameIDs: d.RefFrameIDs,
		Chunks: d.Chunks, RAP: d.RAP, Discardable: d.Discardable,
	}
}

// FrameStats is the receiver's parse-free media-frame decodability snapshot (WP6): how
// many access units / keyframes were decodable (delivered with their dependency closure
// intact) versus total — a picture-level QoE signal the receiver computes from the wire
// descriptors without parsing the codec.
type FrameStats struct {
	Frames             uint64
	DecodableFrames    uint64
	Keyframes          uint64
	DecodableKeyframes uint64
}

func frameStatsFromFlow(s flow.FrameStats) FrameStats {
	return FrameStats{
		Frames: s.Frames, DecodableFrames: s.DecodableFrames,
		Keyframes: s.Keyframes, DecodableKeyframes: s.DecodableKeyframes,
	}
}

// SenderStats reports what a Sender has emitted.
type SenderStats struct {
	Source         uint64 // systematic symbols sent
	Repair         uint64 // repair symbols sent (total)
	ReactiveRepair uint64 // the subset sent in response to a feedback rank deficit
	Throttled      uint64 // repair symbols dropped by the aggregate rate ceiling
}

// ReceiverStats reports a Receiver's delivery outcomes.
type ReceiverStats struct {
	Delivered  uint64 // source symbols delivered in order
	Lost       uint64 // source symbols unrecoverable before their deadline
	Recovered  uint64 // source symbols reconstructed from repair (not received directly)
	Duplicates uint64 // redundant arrivals
	WireLost   uint64 // pre-recovery wire loss (the honest congestion signal; never decremented on decode)
	Rejected   uint64 // inbound symbols refused by the resource-safety admission cap
	Evicted    uint64 // source ids dropped early as undecodable (Config.EvictBrokenFrames)
}

// Sender transmits a coded media flow to a remote Receiver.
type Sender struct{ s *session.Sender }

// NewSender dials remote ("host:port") and starts a coded sender.
func NewSender(remote string, cfg Config) (*Sender, error) {
	s, err := session.NewSender(remote, cfg.toFlow(), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &Sender{s}, nil
}

// Write streams one media chunk (<= SymbolSize bytes) at the base protection tier.
func (s *Sender) Write(p []byte) (int, error) { return s.s.Write(p) }

// WriteUnit streams one media chunk carrying a protection tier (0 = most disposable …
// higher = protect harder; the priority a media shaper assigns from the bitstream).
// The coder sizes that generation's repair to the tier and, under a budget ceiling,
// sheds disposable repair before critical — unequal protection that keeps parameter
// sets, keyframes, and the base layer decodable when the budget is tight (WP6).
func (s *Sender) WriteUnit(p []byte, priority uint8) (int, error) { return s.s.WriteUnit(p, priority) }

// WriteFrame streams one media chunk carrying the full access-unit descriptor (tier +
// dependency). The descriptor is stamped on the wire so the receiver computes
// decodable-frame stats parse-free; use it for every chunk of an access unit, sharing
// one FrameDesc. Sizing still keys only on Priority.
func (s *Sender) WriteFrame(p []byte, fd FrameDesc) (int, error) {
	return s.s.WriteFrame(p, fd.toFlow())
}

// Flush protects and flushes a partially filled final generation.
func (s *Sender) Flush() { s.s.Flush() }

// Stats returns emission counters.
func (s *Sender) Stats() SenderStats {
	st := s.s.Stats()
	return SenderStats{Source: st.Source, Repair: st.Repair, ReactiveRepair: st.ReactiveRepair, Throttled: st.Throttled}
}

// Close flushes the tail and releases the socket.
func (s *Sender) Close() error { return s.s.Close() }

// Receiver receives a coded media flow and delivers it in order.
type Receiver struct{ r *session.Receiver }

// NewReceiver binds bind ("host:port"; use :0 for an ephemeral port) and starts a
// coded receiver.
func NewReceiver(bind string, cfg Config) (*Receiver, error) {
	r, err := session.NewReceiver(bind, cfg.toFlow(), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &Receiver{r}, nil
}

// Read returns the next in-order media chunk into p.
func (r *Receiver) Read(p []byte) (int, error) { return r.r.Read(p) }

// SetReadDeadline bounds subsequent Read calls.
func (r *Receiver) SetReadDeadline(t time.Time) error { return r.r.SetReadDeadline(t) }

// LocalAddr returns the bound address (after binding :0).
func (r *Receiver) LocalAddr() string { return r.r.LocalAddr() }

// Stats returns delivery counters.
func (r *Receiver) Stats() ReceiverStats {
	st := r.r.Stats()
	return ReceiverStats{Delivered: st.Delivered, Lost: st.Lost, Recovered: st.Recovered,
		Duplicates: st.Duplicates, WireLost: st.WireLost, Rejected: st.Rejected, Evicted: st.Evicted}
}

// FrameStats returns the parse-free media-frame decodability snapshot (populated only
// for senders using WriteFrame; zero otherwise).
func (r *Receiver) FrameStats() FrameStats { return frameStatsFromFlow(r.r.FrameStats()) }

// Close releases the socket.
func (r *Receiver) Close() error { return r.r.Close() }

// MultipathSender transmits a coded media flow spread across two network paths
// (coding-native multipath, N5): the generation is split across the paths and the
// receiver decodes from the union, so two lossy paths add diversity rather than the
// N× cost of duplicating every packet (ST 2022-7). Repair is sized against the JOINT
// erasure tail of the paths and metered toward the better deliverer.
type MultipathSender struct{ s *session.MultipathSender }

// NewMultipathSender dials one socket per remote (remotes[i] is path i) and starts a
// coded multipath sender. Two paths are supported today; the path count is taken from
// len(remotes).
func NewMultipathSender(remotes []string, cfg Config) (*MultipathSender, error) {
	s, err := session.NewMultipathSender(remotes, cfg.toFlowPaths(len(remotes)), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &MultipathSender{s}, nil
}

// Write streams one media chunk (<= SymbolSize bytes); the core places it on a path.
func (s *MultipathSender) Write(p []byte) (int, error) { return s.s.Write(p) }

// WriteUnit streams one media chunk carrying a protection tier (unequal protection,
// WP6); the core sizes its repair to the tier and places it across the paths.
func (s *MultipathSender) WriteUnit(p []byte, priority uint8) (int, error) {
	return s.s.WriteUnit(p, priority)
}

// WriteFrame streams one media chunk carrying the full access-unit descriptor (WP6); the
// core sizes its repair to the tier, places it across the paths, and stamps the
// dependency so the receiver computes decodable-frame stats parse-free.
func (s *MultipathSender) WriteFrame(p []byte, fd FrameDesc) (int, error) {
	return s.s.WriteFrame(p, fd.toFlow())
}

// Flush protects and flushes a partially filled final generation.
func (s *MultipathSender) Flush() { s.s.Flush() }

// Stats returns emission counters (aggregate across paths).
func (s *MultipathSender) Stats() SenderStats {
	st := s.s.Stats()
	return SenderStats{Source: st.Source, Repair: st.Repair, ReactiveRepair: st.ReactiveRepair, Throttled: st.Throttled}
}

// Close flushes the tail and releases every path socket.
func (s *MultipathSender) Close() error { return s.s.Close() }

// MultipathReceiver receives a coded multipath flow over one socket per path and
// delivers it in order, recovering loss from the union of symbols across the paths.
type MultipathReceiver struct{ r *session.MultipathReceiver }

// NewMultipathReceiver binds one socket per bind address (binds[i] is path i; use :0
// for an ephemeral port) and starts a coded multipath receiver.
func NewMultipathReceiver(binds []string, cfg Config) (*MultipathReceiver, error) {
	r, err := session.NewMultipathReceiver(binds, cfg.toFlowPaths(len(binds)), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &MultipathReceiver{r}, nil
}

// Read returns the next in-order media chunk into p.
func (r *MultipathReceiver) Read(p []byte) (int, error) { return r.r.Read(p) }

// SetReadDeadline bounds subsequent Read calls.
func (r *MultipathReceiver) SetReadDeadline(t time.Time) error { return r.r.SetReadDeadline(t) }

// LocalAddrs returns the bound address of each path socket (after binding :0).
func (r *MultipathReceiver) LocalAddrs() []string { return r.r.LocalAddrs() }

// Stats returns delivery counters.
func (r *MultipathReceiver) Stats() ReceiverStats {
	st := r.r.Stats()
	return ReceiverStats{Delivered: st.Delivered, Lost: st.Lost, Recovered: st.Recovered,
		Duplicates: st.Duplicates, WireLost: st.WireLost, Rejected: st.Rejected, Evicted: st.Evicted}
}

// FrameStats returns the parse-free media-frame decodability snapshot.
func (r *MultipathReceiver) FrameStats() FrameStats { return frameStatsFromFlow(r.r.FrameStats()) }

// Close releases every path socket.
func (r *MultipathReceiver) Close() error { return r.r.Close() }
