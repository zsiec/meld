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
	"log"
	"time"

	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/flow"
	"github.com/zsiec/meld/internal/session"
)

// Config parameterizes a Sender/Receiver pair. Both ends must agree on Flow,
// SymbolSize, and BufferMicros; generation-mode deployments must also agree on
// their generation constraints. DefaultConfig selects the automatic sliding
// recovery path, so applications do not choose FEC, ARQ, or burst-copy modes.
type Config struct {
	// Flow identifies the flow on the wire.
	Flow uint32
	// SymbolSize is the maximum application chunk and fixed algebraic width in
	// bytes. Systematic datagrams carry only the exact bytes written.
	SymbolSize int
	// GenSize is the coding generation (window) size in source symbols. With
	// AdaptiveGenSize off this is the exact, fixed generation width; with it on it is the
	// floor (the budget-below-RTT width).
	GenSize int
	// AdaptiveGenSize widens the generation toward 64 source symbols when the latency
	// budget (BufferMicros) comfortably exceeds a reactive round (NominalRTTMicros +
	// feedback), which amortizes the proactive repair margin over more symbols and CUTS
	// REPAIR OVERHEAD substantially at a generous budget (the bench shows roughly a halving
	// 16→64), with no loss of completeness. It stays at GenSize when the budget is below a
	// reactive round (budget < RTT), and the fill-time gate (NominalBitrateBps) holds it narrow
	// at low bitrate, where a wider generation would otherwise raise latency.
	//
	// RECOMMENDED for a known fixed-rate path. A real-timing sweep (bitrate × RTT × loss ×
	// burst, vs librist/libsrt) finds it lower-overhead everywhere AND the only config that
	// stays complete in the worst corner — 50 Mbps, bursty 10% loss, high RTT — where the fixed
	// GenSize-16 default drops a few points of delivery (96–98%); with this on, the wider window
	// holds ~100% there at ~3× lower p50 and lower overhead. Enable it WITH both hints below.
	//
	// Off by default — not because it loses (it does not) but because it cannot be a safe
	// default: it is inert without NominalRTTMicros, and the generation STRIDE must be identical
	// on both ends from the first packet, so BOTH ends must set the SAME GenSize, BufferMicros,
	// AdaptiveGenSize, NominalRTTMicros, and NominalBitrateBps — a mismatch corrupts delivery, a
	// guarantee a library default cannot make. NewSender/NewReceiver warn (Config.Check) if it is
	// enabled without the hints. Generation coder only (ignored when Sliding).
	AdaptiveGenSize bool
	// AutoGenSize is the zero-config form of AdaptiveGenSize and is ON BY DEFAULT (DefaultConfig):
	// the SENDER measures its own RTT and source rate and sizes the generation
	// itself — no NominalRTTMicros / NominalBitrateBps, and nothing to set on the receiver, which
	// follows the per-generation width stamped on every symbol. It re-sizes automatically if the
	// bitrate or RTT changes mid-stream, and starts narrow-and-safe until it has measured the path.
	// Set it on the sender (harmless on the receiver). Takes precedence over AdaptiveGenSize.
	// Generation coder only (ignored when Sliding).
	AutoGenSize bool
	// NominalRTTMicros is a static path round-trip-time hint, in microseconds, used only to
	// derive the AdaptiveGenSize width — it must be shared and static so both ends compute the
	// same generation width. Set it to your path's expected RTT. 0 ⇒ AdaptiveGenSize does not
	// widen. Ignored unless AdaptiveGenSize.
	NominalRTTMicros int64
	// NominalBitrateBps is your source bitrate in bits/sec. It gates AdaptiveGenSize by generation
	// FILL TIME so the generation widens only when it fills fast enough that the wait doesn't cost
	// latency: at a high bitrate (the win is free) it widens fully; at a low bitrate it stays near
	// GenSize. This makes one config safe across bitrates — the lever helps where it helps and no-ops
	// where a slow-filling wide generation would raise latency. Shared/static like NominalRTTMicros.
	// 0 ⇒ no fill gate. Ignored unless AdaptiveGenSize.
	NominalBitrateBps int64
	// ProactiveDecay lowers steady-state repair overhead on a reactive-capable link (latency
	// budget comfortably above the RTT) by letting the reactive tier carry the variance margin:
	// the proactive code rate sends the mean expected loss plus a reactive-scaled fraction of the
	// margin, and reactive repair tops up the generations that drew above-mean loss. It is a no-op
	// where reactive cannot land (RTT ≥ budget), so it never under-protects there. A burst guard
	// makes it self-revert to full protection on a bursty channel, so it sheds only the i.i.d.
	// variance tail. ON by default (DefaultConfig) — proven within ~1 point of the full-protection
	// baseline across a regime sweep, at parity below ~2% loss. Set false for the lowest tail latency
	// at higher overhead. Single-path only (multipath keeps the joint-tail set-point); sender-side
	// policy (the two ends may differ).
	ProactiveDecay bool
	// ReorderHoldoffMicros is a receiver-side reorder window for the channel-loss estimate: a source id
	// missing from the in-order sequence is counted as lost (feeding the loss/burst estimators that size
	// proactive repair) only once it has been missing this long, not the instant a higher id arrives.
	// Under real-timing reorder a higher id routinely arrives before the lower ones (merely late, not
	// lost) and the estimators over-count loss, over-sizing proactive repair severalfold. 0 ⇒ off.
	// Single-path only for now. Receiver-side; the two ends may differ. Overridden by AutoReorderHoldoff.
	ReorderHoldoffMicros int64
	// AutoReorderHoldoff sizes the generation receiver's loss-estimator reorder
	// window from measured spread instead of a fixed ReorderHoldoffMicros. It is
	// zero-config, self-disabling without reorder, single-path, receiver-side, and
	// on in DefaultConfig. The sliding receiver uses its separately validated
	// settled-loss holdoff.
	AutoReorderHoldoff bool
	// Redundancy is the FLOOR proactive code rate (repair per source symbol); the
	// controller raises the rate above it as the measured loss requires. On a link
	// CONFIRMED clean whose latency budget affords at least two reactive repair
	// rounds, proactive repair decays to zero and any loss onset is caught
	// reactively — protection there recovers nothing and is pure overhead; the
	// first loss evidence restores full sizing instantly. Cleanliness is judged by
	// the receiver's SETTLED walk (an id counts lost only after a reorder holdoff
	// proves it absent), so plain reorder neither blocks the decay nor fakes
	// cleanliness; against a peer predating that evidence, every raw loss signal
	// must be quiet instead. Warmup, absent feedback, idle intervals, and tight
	// budgets always retain the full floor.
	Redundancy float64
	// TargetFailure is the per-generation decode-failure probability the redundancy
	// controller sizes the proactive code rate to (the QoS knob). 0 ⇒ default 1e-3.
	TargetFailure float64
	// BufferMicros is the playout/deadline budget in microseconds.
	BufferMicros int64
	// Sliding selects the band-form sliding-window coder used by DefaultConfig. It
	// codes continuous, fungible repair over one elastic window and automatically
	// selects proactive equations, staged exact closure, or burst-spaced copies from
	// measured conditions. Set false only for a generation-mode or control
	// deployment; transient path changes do not require changing this field.
	// Sliding decode costs O(CodingWindow²) per symbol.
	Sliding bool
	// CodingWindow is the MAX sliding band width in source symbols — the recovery
	// span and O(window²) decode-cost cap. The sender adapts the effective span below
	// it to fit the deadline budget, so this is a ceiling, not a fixed width. 0 ⇒
	// default. Ignored unless Sliding.
	CodingWindow int
	// CongestionControl enables the delay-based congestion controller:
	// it derives the send-rate budget from the standing-queue delay (loss-agnostic,
	// since coding masks loss) and throttles REPAIR to stay within it — protecting media
	// goodput and surfacing a target rate the source should pace within. Off means a
	// static rate ceiling only. Leave off until validated on your paths.
	CongestionControl bool
	// Pace enables the host transmit pacer: the sender smooths coded datagrams onto the
	// wire at a rate slaved to the congestion/ceiling budget (never a second controller)
	// and backpressures Write when the send queue would grow past the deadline — so an
	// encoder burst (a keyframe) is spread across the budget instead of dumped as a
	// microburst, and the source is bounded by the budget rather than bloating a buffer.
	// On by default (DefaultConfig). Turn off to transmit each emit immediately.
	Pace bool
	// ProbeMTU enables host-side DPLPMTUD (RFC 8899): the sender probes the path MTU with
	// padded, Don't-Fragment datagrams and detects size black holes. It discovers and
	// reports the PLPMTU; it does not resize SymbolSize automatically. Off by default.
	ProbeMTU bool
	// MaxProbeMTU is the largest UDP payload size DPLPMTUD probes for. 0 selects the
	// host default. Ignored unless ProbeMTU.
	MaxProbeMTU int
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
	// dead GOP. Works in both generation and sliding profiles. Requires WriteFrame
	// descriptors; a no-op for plain Write byte streams. Off by default: it drops doomed-but-recoverable bytes to deliver whole DECODABLE frames
	// faster, so only flows that consume whole pictures want it.
	EvictBrokenFrames bool
	// FrameAtomic delivers each WriteFrame access unit ALL-OR-NOTHING: the app gets a whole
	// frame once it is fully recoverable in time, or a clean gap (no fragment) if any chunk is
	// lost, its reference sub-tree is dead, or its deadline passes — so the decoder conceals a
	// missing frame (freeze) instead of rendering a corrupt one. The perceptual form of
	// EvictBrokenFrames; supersedes it when set. Generation profile only today;
	// sliding accepts WriteFrame descriptors but does not yet deliver frame-atomically.
	// Requires WriteFrame; a no-op for byte streams.
	// OFF by default (DefaultConfig): WriteFrame metadata is useful for protection even when
	// the application still wants recoverable bytes delivered. Enable this for consumers that
	// prefer clean frame gaps over partial access units.
	FrameAtomic bool
	// ShedTopLayerOverBudget makes the sender proactively drop the top temporal layer (the
	// highest-TemporalID, Discardable, non-reference WriteFrame units) when the source bitrate
	// exceeds the rate budget — a clean transport-level temporal downscale ("drop every other
	// frame") that keeps the base layer low-latency instead of queueing. Discardable frames have
	// no dependents, so the receiver sees a smooth lower-fps stream, not loss. OFF by default (a
	// deliberate ABR-style policy). FrameAtomic provides a separate opt-in all-or-nothing
	// receive policy for consumers that want clean frame gaps.
	ShedTopLayerOverBudget bool
	// RepairWithinBudget caps proactive repair to the rate budget (media + repair ≤ budget),
	// so a tight latency budget sheds protection gracefully instead of the host pacer queueing
	// the overage as delay on media past the deadline. ON by default (DefaultConfig); sender-side
	// (no both-ends agreement needed). Set false for the unbounded-repair behavior.
	RepairWithinBudget bool
	// OutageAware enables two-regime channel control: redundancy is provisioned for the
	// RECOVERABLE loss regime only. A loss run far beyond the recovery horizon (an outage —
	// longer than any repair could arrive within the deadline budget) is excluded from the
	// loss/burst estimates that size repair, and reactive repair that provably cannot land
	// in time is skipped — so an unrecoverable outage no longer drives the sender to spend
	// maximum overhead on windows it cannot save, or to keep paying outage-scale overhead
	// after the channel recovers. Delivery on recoverable loss is unchanged; the honest
	// wire-loss counters are never censored. Payload-agnostic (no media metadata needed).
	// ON by default (DefaultConfig); set false for the outage-blind estimators.
	OutageAware bool
	// SlidingReactiveShift enables an aggressive reactive-offload benchmark override.
	// The default path already performs persistence-gated exact closure and
	// deadline-qualified burst copies automatically. This flag bypasses some of the
	// conservative persistence/headroom gates for controlled A/B work and is not a
	// deployment mode. Sender-side.
	SlidingReactiveShift bool
	// HeadroomAwareSizing enables a delay-inferred headroom benchmark override. The
	// default path always enforces the source-first byte governor derived from
	// MaxBitrate and measured source cadence; this flag adds a secondary estimator
	// and is retained only for controlled A/B work. Sender-side.
	HeadroomAwareSizing bool
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

// DefaultConfig returns a starting configuration (1316-byte chunks, sliding coded
// repair, generation-size floor 16 for generation fallback, 200 ms buffer).
func DefaultConfig() Config {
	c := flow.DefaultConfig()
	return Config{
		SymbolSize:         c.SymbolSize,
		GenSize:            c.GenSize,
		Redundancy:         c.Redundancy,
		TargetFailure:      c.TargetFailure,
		BufferMicros:       c.BufferMicros,
		Sliding:            c.Sliding,
		Pace:               c.Pace,               // on by default (flow.DefaultConfig)
		ProactiveDecay:     c.ProactiveDecay,     // on by default (flow.DefaultConfig)
		AutoGenSize:        c.AutoGenSize,        // on by default (flow.DefaultConfig): zero-config adaptive width
		RepairWithinBudget: c.RepairWithinBudget, // on by default (flow.DefaultConfig): repair within the rate budget
		FrameAtomic:        c.FrameAtomic,        // opt-in: all-or-nothing picture delivery
		AutoReorderHoldoff: c.AutoReorderHoldoff, // on by default (flow.DefaultConfig): self-tuning loss-estimate reorder window
		OutageAware:        c.OutageAware,        // on by default (flow.DefaultConfig): two-regime outage composure
	}
}

func (c Config) toFlow() flow.Config {
	return flow.Config{
		Flow:                   c.Flow,
		SymbolSize:             c.SymbolSize,
		GenSize:                c.GenSize,
		AdaptiveGenSize:        c.AdaptiveGenSize,
		AutoGenSize:            c.AutoGenSize,
		NominalRTTMicros:       c.NominalRTTMicros,
		NominalBitrateBps:      c.NominalBitrateBps,
		ProactiveDecay:         c.ProactiveDecay,
		ReorderHoldoffMicros:   c.ReorderHoldoffMicros,
		AutoReorderHoldoff:     c.AutoReorderHoldoff,
		Redundancy:             c.Redundancy,
		TargetFailure:          c.TargetFailure,
		BufferMicros:           c.BufferMicros,
		Sliding:                c.Sliding,
		CodingWindow:           c.CodingWindow,
		ProtectedRepairPhasing: true,
		CongestionControl:      c.CongestionControl,
		Pace:                   c.Pace,
		ProbeMTU:               c.ProbeMTU,
		MaxProbeMTU:            c.MaxProbeMTU,
		MaxBitrate:             c.MaxBitrate,
		EvictBrokenFrames:      c.EvictBrokenFrames,
		FrameAtomic:            c.FrameAtomic,
		ShedTopLayerOverBudget: c.ShedTopLayerOverBudget,
		RepairWithinBudget:     c.RepairWithinBudget,
		OutageAware:            c.OutageAware,
		SlidingReactiveShift:   c.SlidingReactiveShift,
		HeadroomAwareSizing:    c.HeadroomAwareSizing,
	}
}

// toFlowPaths is toFlow with the path count set for coding-native multipath: the
// generation is spread across paths and decoded from the union.
func (c Config) toFlowPaths(paths int) flow.Config {
	fc := c.toFlow()
	// Multipath is still generation-profile internally. Public multipath remains usable from
	// DefaultConfig while the sliding multipath path is implemented below the session layer.
	fc.Sliding = false
	fc.Paths = paths
	return fc
}

// FrameDesc is the per-access-unit media descriptor a shaper hands WriteFrame: the
// protection tier plus the dependency the receiver needs to compute decodable-frame
// stats without parsing the codec. FrameID identifies the access unit (the shaper's unit id);
// RefFrameIDs are its dependency access units (a B-frame's two anchors, a P-frame's one);
// Chunks is its total chunk count (so the receiver knows its id range); RAP marks a
// keyframe; RecoveryRefresh marks a reference slice inside a signaled intra-refresh
// interval; Discardable marks a unit nothing references; NonPicture marks metadata/
// parameter material that is not a displayed coded picture.
type FrameDesc struct {
	Priority        uint8
	FrameID         uint32
	RefFrameIDs     []uint32
	Chunks          uint16
	TemporalID      uint8 // temporal-scalability layer (0 = base; higher = leaf frames shed first)
	RAP             bool
	RecoveryRefresh bool
	Discardable     bool
	NonPicture      bool
	// LTR marks a long-term-reference candidate the encoder retains in its DPB. The
	// receiver reports the newest decodable one, and when the reference chain breaks
	// Meld raises EncoderControl.Resync naming it — so the encoder can resync by
	// coding its next frame against that LTR (P-frame cost) instead of waiting for
	// the next scheduled keyframe. Mark sparingly (every few reference frames).
	LTR bool
}

func (d FrameDesc) toFlow() flow.FrameDesc {
	return flow.FrameDesc{
		Priority: d.Priority, FrameID: d.FrameID, RefFrameIDs: d.RefFrameIDs,
		Chunks: d.Chunks, TemporalID: d.TemporalID, RAP: d.RAP,
		RecoveryRefresh: d.RecoveryRefresh, Discardable: d.Discardable, NonPicture: d.NonPicture,
		LTR: d.LTR,
	}
}

// FrameStats is the receiver's parse-free media-frame decodability snapshot: how
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
	Source                uint64 // systematic symbols sent
	Repair                uint64 // repair symbols sent (total)
	ReactiveRepair        uint64 // the subset sent in response to a feedback rank deficit
	Throttled             uint64 // repair symbols dropped by the aggregate rate ceiling
	DeadlineRepairSkips   uint64 // repair symbols suppressed because they could not arrive before the coded window deadline
	HeadroomTightens      uint64 // headroom-cap tighten events: sustained wire-saturation evidence capped the proactive set-point (sliding profile)
	RecoveryCadenceFrames uint16 // encoder max recovery interval request; 0 means relaxed
	// SourceWireBytesMean is the recent mean encoded systematic datagram size used
	// by source-first recovery admission.
	SourceWireBytesMean uint64

	// Per-mechanism attribution of Repair. The five contribution counters below sum to Repair on
	// the sliding profile (Config.Sliding); on the generation profile they read zero
	// and Repair/ReactiveRepair carry the only split.

	// RepairProactive counts band repair emitted from the warm proactive credit
	// (write/flush/idle cadence). Sliding profile only; zero on the generation profile.
	RepairProactive uint64
	// RepairProactiveCold counts proactive band repair emitted under the cold-start
	// floor, before feedback primes the channel estimate. Sliding profile only.
	RepairProactiveCold uint64
	// RepairSingleton counts dedicated per-chunk singleton repair for protected
	// references where reactive repair cannot land in time. Sliding profile only.
	RepairSingleton uint64
	// RepairSparse counts sparse protected repair (UEP/anchor groups, scheduled or
	// feedback-driven). Sliding profile only.
	RepairSparse uint64
	// RepairDeficit counts deficit-answering reactive window repair (the
	// retrospective reactive tier). Sliding profile only.
	RepairDeficit uint64
	// RepairExact counts missing-driven exact retransmissions selected after coded
	// repair leaves a reported residual. It is a subset of RepairDeficit, not an
	// additional contribution to Repair.
	RepairExact uint64
	// RepairBurstDuplicate counts deadline-qualified delayed compact repetitions
	// selected for a measured burst path. It is a subset of RepairProactive.
	RepairBurstDuplicate uint64
	// RepairOutageDiversity counts proactive band equations moved across a
	// receiver-classified outage span. It is a subset of RepairProactive and does
	// not add to the controller's redundancy rate.
	RepairOutageDiversity uint64
	// RepairEpoch counts proactive equations whose ordinary repair credit
	// was assigned by the automatic allocator to isolated fixed-geometry epochs.
	// It is a subset of RepairProactive.
	RepairEpoch uint64
	// EpochBlocks counts stable 16-source blocks opened by the automatic
	// fixed/sliding allocator, including blocks that close with no admitted row.
	EpochBlocks uint64
	// EpochDemandQ8 is the latest fixed-geometry demand, where 256 is the
	// strongest request and zero means no current allocation demand.
	EpochDemandQ8 uint16
	// EpochCorrelationQ8 is the latest repeated burst-memory confidence.
	EpochCorrelationQ8 uint16
	// EpochMemoryQ8 is confirmed burst/outage memory after promotion.
	EpochMemoryQ8 uint16
	// EpochShareQ8 is the epoch share selected for the latest block;
	// the remainder of ordinary proactive credit stayed in sliding RLNC.
	EpochShareQ8 uint16
	// RepairCompacted counts dense and sparse equations serialized without their
	// trailing zero application bytes.
	RepairCompacted uint64
	// RepairBytesSaved is the number of wire bytes omitted by compact equation
	// serialization. Control admission remains charged at full equation width.
	RepairBytesSaved uint64
}

// EncoderControl is Meld's advisory source-control output for an attached encoder.
// It does not create a separate deployable profile: a host may apply the request,
// and if the encoder cannot comply, Meld continues with the same transport loop.
type EncoderControl struct {
	// TargetBitrateBps asks the encoder to cap its source payload so transport
	// recovery has room inside the current total-rate budget. Zero means no
	// active reduction request.
	TargetBitrateBps int64
	// RecoveryCadenceFrames asks the encoder to bound the distance between recovery
	// points, in displayed frames. 0 means no active request. Encoders may satisfy
	// this with keyframes, recovery-point SEI, or intra-refresh.
	RecoveryCadenceFrames uint16
	// Resync asks the encoder to code its next frame referencing ResyncRefFrameID — a
	// frame it marked FrameDesc.LTR that the receiver has confirmed decodable — because
	// the live reference chain is broken. Honoring it resurrects the stream at P-frame
	// cost instead of waiting out the GOP for the next keyframe. An encoder that no
	// longer retains that LTR ignores the request.
	Resync           bool
	ResyncRefFrameID uint32
}

func encoderControlFromFlow(c flow.EncoderControl) EncoderControl {
	return EncoderControl{
		TargetBitrateBps:      c.TargetBitrateBps,
		RecoveryCadenceFrames: c.RecoveryCadenceFrames,
		Resync:                c.Resync,
		ResyncRefFrameID:      c.ResyncRefFrameID,
	}
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
	// Outages counts loss runs classified as beyond the recovery horizon — so long
	// that no repair for their interior could have arrived before its deadline, at
	// any redundancy setting. OutageSymbols is their summed span. Both are telemetry,
	// counted whether or not Config.OutageAware censors the sizing estimators;
	// WireLost always counts the full runs.
	Outages       uint64
	OutageSymbols uint64
}

func receiverStatsFromFlow(st flow.ReceiverStats) ReceiverStats {
	return ReceiverStats{
		Delivered: st.Delivered, Lost: st.Lost, Recovered: st.Recovered,
		Duplicates: st.Duplicates, WireLost: st.WireLost, Rejected: st.Rejected, Evicted: st.Evicted,
		Outages: st.Outages, OutageSymbols: st.OutageSymbols,
	}
}

// Check returns advisory warnings about a Config whose options are set in a way that
// makes them inert or likely to under-perform — an empty slice when the config is sound.
// It is pure (no I/O) and may be called before NewSender/NewReceiver, which surface these
// warnings via the standard logger. The findings are advisory, not errors: a flagged
// config still runs (just not as intended).
//
// Today it checks the AdaptiveGenSize hints: that lever is the recommended setting for a
// fixed-rate path (it lowers overhead everywhere and is the only config that stays complete
// in the worst burst+high-RTT corner — see the AdaptiveGenSize doc), but it is inert without
// NominalRTTMicros and unsafe at low bitrate without NominalBitrateBps, and both must match on
// the two ends.
func (c Config) Check() []string {
	var w []string
	if c.AdaptiveGenSize && !c.AutoGenSize && !c.Sliding {
		switch {
		case c.NominalRTTMicros <= 0:
			w = append(w, "meld: AdaptiveGenSize is on but NominalRTTMicros is unset — the generation "+
				"will NOT widen (the lever is inert). Either set NominalRTTMicros + NominalBitrateBps "+
				"(both ends, matched), or — simpler — use AutoGenSize, which measures them itself and "+
				"needs nothing on the receiver.")
		case c.NominalBitrateBps <= 0:
			w = append(w, "meld: AdaptiveGenSize is on without NominalBitrateBps — the fill-time gate is "+
				"OFF, so a low-bitrate path can regress latency (a wide generation fills slowly and "+
				"head-of-line-blocks). Set NominalBitrateBps to your source rate (both ends), or — "+
				"simpler — use AutoGenSize, which measures it itself.")
		}
	}
	return w
}

// Substrate is the datagram transport meld runs over: ReadFrom/WriteTo/LocalAddr/Close (the
// datagram subset of net.PacketConn). A UDP socket satisfies it, and so does any host-provided
// pipe — a WebTransport session, a WASM bridge to the browser's transport, an in-process pair.
// Implement it to run the coder over a transport meld does not own (and whose pacing/congestion
// control the host, not meld, governs).
type Substrate = session.Substrate

// Sender transmits a coded media flow to a remote Receiver.
type Sender struct{ s *session.Sender }

// NewSender dials remote ("host:port") and starts a coded sender.
func NewSender(remote string, cfg Config) (*Sender, error) {
	for _, warn := range cfg.Check() {
		log.Println(warn)
	}
	s, err := session.NewSender(remote, cfg.toFlow(), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &Sender{s}, nil
}

// NewSenderOver starts a coded sender over a caller-provided Substrate instead of dialing an
// address — the seam for running meld over WebTransport, a WASM bridge, or any datagram transport.
// The host owns the substrate's pacing and congestion response; meld owns the coding.
func NewSenderOver(sub Substrate, cfg Config) (*Sender, error) {
	for _, warn := range cfg.Check() {
		log.Println(warn)
	}
	s, err := session.NewSenderOver(sub, cfg.toFlow(), cfg.toSecurity())
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
// sets, keyframes, and the base layer decodable when the budget is tight.
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
	return senderStatsFromFlow(s.s.Stats())
}

func senderStatsFromFlow(st flow.SenderStats) SenderStats {
	return SenderStats{
		Source: st.Source, Repair: st.Repair, ReactiveRepair: st.ReactiveRepair,
		Throttled: st.Throttled, DeadlineRepairSkips: st.DeadlineRepairSkips, HeadroomTightens: st.HeadroomTightens,
		RecoveryCadenceFrames: st.RecoveryCadenceFrames, SourceWireBytesMean: st.SourceWireBytesMean,
		RepairProactive: st.RepairProactive, RepairProactiveCold: st.RepairProactiveCold,
		RepairSingleton: st.RepairSingleton, RepairSparse: st.RepairSparse, RepairDeficit: st.RepairDeficit,
		RepairExact: st.RepairExact, RepairBurstDuplicate: st.RepairBurstDuplicate,
		RepairOutageDiversity: st.RepairOutageDiversity,
		RepairEpoch:           st.RepairEpoch,
		EpochBlocks:           st.EpochBlocks,
		EpochDemandQ8:         st.EpochDemandQ8,
		EpochCorrelationQ8:    st.EpochCorrelationQ8,
		EpochMemoryQ8:         st.EpochMemoryQ8,
		EpochShareQ8:          st.EpochShareQ8,
		RepairCompacted:       st.RepairCompacted, RepairBytesSaved: st.RepairBytesSaved,
	}
}

// EncoderControl returns Meld's current advisory encoder-control request.
func (s *Sender) EncoderControl() EncoderControl { return encoderControlFromFlow(s.s.EncoderControl()) }

// PathMTU returns the discovered path PLPMTU in bytes, or 0 when ProbeMTU is off.
func (s *Sender) PathMTU() int { return s.s.PathMTU() }

// PathMTUBlackHoles returns the number of size-black-hole events DPLPMTUD detected.
func (s *Sender) PathMTUBlackHoles() int { return s.s.PathMTUBlackHoles() }

// Close flushes the tail and releases the socket.
func (s *Sender) Close() error { return s.s.Close() }

// Receiver receives a coded media flow and delivers it in order.
type Receiver struct{ r *session.Receiver }

// NewReceiver binds bind ("host:port"; use :0 for an ephemeral port) and starts a
// coded receiver.
func NewReceiver(bind string, cfg Config) (*Receiver, error) {
	for _, warn := range cfg.Check() {
		log.Println(warn)
	}
	r, err := session.NewReceiver(bind, cfg.toFlow(), cfg.toSecurity())
	if err != nil {
		return nil, err
	}
	return &Receiver{r}, nil
}

// NewReceiverOver starts a coded receiver over a caller-provided Substrate instead of binding an
// address — the receive-side seam for WebTransport, a WASM browser bridge, or any datagram transport.
func NewReceiverOver(sub Substrate, cfg Config) (*Receiver, error) {
	for _, warn := range cfg.Check() {
		log.Println(warn)
	}
	r, err := session.NewReceiverOver(sub, cfg.toFlow(), cfg.toSecurity())
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
	return receiverStatsFromFlow(r.r.Stats())
}

// FrameStats returns the parse-free media-frame decodability snapshot (populated only
// for generation-profile senders using WriteFrame; zero otherwise).
func (r *Receiver) FrameStats() FrameStats { return frameStatsFromFlow(r.r.FrameStats()) }

// Close releases the socket.
func (r *Receiver) Close() error { return r.r.Close() }

// MultipathSender transmits a coded media flow spread across two network paths
// using coding-native multipath: the generation is split across the paths and the
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

// WriteUnit streams one media chunk carrying a protection tier; the core sizes its
// repair to the tier and places it across the paths.
func (s *MultipathSender) WriteUnit(p []byte, priority uint8) (int, error) {
	return s.s.WriteUnit(p, priority)
}

// WriteFrame streams one media chunk carrying the full access-unit descriptor; the
// core sizes its repair to the tier, places it across the paths, and stamps the
// dependency so the receiver computes decodable-frame stats parse-free.
func (s *MultipathSender) WriteFrame(p []byte, fd FrameDesc) (int, error) {
	return s.s.WriteFrame(p, fd.toFlow())
}

// Flush protects and flushes a partially filled final generation.
func (s *MultipathSender) Flush() { s.s.Flush() }

// Stats returns emission counters (aggregate across paths).
func (s *MultipathSender) Stats() SenderStats {
	return senderStatsFromFlow(s.s.Stats())
}

// EncoderControl returns Meld's current advisory encoder-control request.
func (s *MultipathSender) EncoderControl() EncoderControl {
	return encoderControlFromFlow(s.s.EncoderControl())
}

// PathMTUs returns each path's discovered PLPMTU in bytes. Entries are 0 when ProbeMTU is off.
func (s *MultipathSender) PathMTUs() []int { return s.s.PathMTUs() }

// PathMTU returns the path-set minimum PLPMTU in bytes, or 0 when ProbeMTU is off.
func (s *MultipathSender) PathMTU() int { return s.s.PathMTU() }

// PathMTUBlackHoles returns the aggregate number of size-black-hole events DPLPMTUD detected.
func (s *MultipathSender) PathMTUBlackHoles() int { return s.s.PathMTUBlackHoles() }

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
	return receiverStatsFromFlow(r.r.Stats())
}

// FrameStats returns the parse-free media-frame decodability snapshot.
func (r *MultipathReceiver) FrameStats() FrameStats { return frameStatsFromFlow(r.r.FrameStats()) }

// Close releases every path socket.
func (r *MultipathReceiver) Close() error { return r.r.Close() }
