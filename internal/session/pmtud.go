package session

import "github.com/zsiec/meld/internal/clock"

// This file is the host-side Datagram PL PMTU Discovery state machine (RFC 8899),
// one instance per path. It exists because a size-based path black hole is invisible to
// the rest of Meld: repair symbols are the same size as the source they protect, so the
// FEC that masks loss is itself dropped, and the delay-based congestion controller is
// loss-agnostic (RFC 9265) so it never reacts (proven in
// internal/flow/pmtud_blackhole_test.go). Explicit probing is the only escape.
//
// Like the pacer, the DECISION LOGIC is a pure, clockless state machine taking explicit
// `now` — no socket, no timer, no goroutine — so it is unit-testable deterministically.
// The host (Sender) wraps it: it sends padded probe datagrams with Don't-Fragment set,
// feeds back ack/timeout events, and surfaces the discovered PLPMTU. Phase 1 discovers and
// reports the PLPMTU per path (and the path-set minimum); it does NOT yet resize symbols
// (that is the wire/coder contract change of a later phase).

// pmtudPhase is the RFC 8899 search state (§5.2).
type pmtudPhase int

const (
	// pmtudBase confirms the conservative base PLPMTU works before searching upward.
	pmtudBase pmtudPhase = iota
	// pmtudSearching probes upward for the largest size the path passes.
	pmtudSearching
	// pmtudComplete is the steady state: the PLPMTU is known; a raise timer re-probes
	// upward periodically and a confirmation probe detects a black hole (the path shrank).
	pmtudComplete
	// pmtudError means even the base PLPMTU does not pass — the path is unusable at the
	// safe floor; the host runs at the floor and should alarm.
	pmtudError
)

// pmtudConfig parameterizes the state machine. Zero fields take the defaults below.
type pmtudConfig struct {
	Base            int   // safe starting PLPMTU (UDP payload bytes) assumed to pass most paths
	Max             int   // largest PLPMTU to probe for (interface MTU / config ceiling)
	Granularity     int   // stop the binary search when the good/bad gap is within this
	MaxProbes       int   // consecutive losses at a size before it is declared failed (RFC PROBE_COUNT)
	ProbeTimeoutUs  int64 // wait for a probe ack before counting it lost (RFC PROBE_TIMER)
	RaiseIntervalUs int64 // in Complete, re-search upward this often (RFC PMTU_RAISE_TIMER)
	ConfirmEveryUs  int64 // in Complete, confirm the PLPMTU still passes this often (black-hole guard)
}

// DPLPMTUD defaults. Base 1200 is the widely-safe floor (≈ IPv6 min minus headroom); the
// timers are tuned tighter than RFC 8899's multi-second/minute defaults because live media
// must escape a black hole fast, not in 15 minutes.
const (
	defaultPMTUDBase      = 1200
	defaultPMTUDMax       = 1500
	defaultPMTUDGran      = 8
	defaultPMTUDMaxProbes = 3
	defaultProbeTimeoutUs = 500_000    // 500 ms
	defaultRaiseEveryUs   = 60_000_000 // 60 s — occasional optimistic re-probe upward
	defaultConfirmEveryUs = 15_000_000 // 15 s — black-hole confirmation cadence
)

func (c pmtudConfig) withDefaults() pmtudConfig {
	if c.Base <= 0 {
		c.Base = defaultPMTUDBase
	}
	if c.Max < c.Base {
		c.Max = defaultPMTUDMax
	}
	if c.Max < c.Base {
		c.Max = c.Base
	}
	if c.Granularity <= 0 {
		c.Granularity = defaultPMTUDGran
	}
	if c.MaxProbes <= 0 {
		c.MaxProbes = defaultPMTUDMaxProbes
	}
	if c.ProbeTimeoutUs <= 0 {
		c.ProbeTimeoutUs = defaultProbeTimeoutUs
	}
	if c.RaiseIntervalUs <= 0 {
		c.RaiseIntervalUs = defaultRaiseEveryUs
	}
	if c.ConfirmEveryUs <= 0 {
		c.ConfirmEveryUs = defaultConfirmEveryUs
	}
	return c
}

// pmtudState is the pure per-path DPLPMTUD state machine. Time enters only through the
// explicit `now` argument to tick/onAck/onProbeFail; it never reads a clock.
type pmtudState struct {
	cfg   pmtudConfig
	phase pmtudPhase

	plpmtu int // current effective PLPMTU (host sizes within this); starts at base
	good   int // largest confirmed-good probe size
	bad    int // smallest confirmed-bad probe size (0 = none found yet this search)

	// outstanding probe
	inflight   bool
	confirming bool // the outstanding probe is a Complete-state confirmation (failure ⇒ black hole)
	probeSize  int
	sentAt     clock.Timestamp
	tries      int

	lastRaise   clock.Timestamp // when the raise timer was last reset (Complete)
	lastConfirm clock.Timestamp // when the PLPMTU was last confirmed (Complete)

	blackHoles int // count of black-hole events detected (stat)
}

// newPMTUD builds a per-path state machine. It starts in Base at the conservative base
// PLPMTU (which the host may use for sizing immediately) and confirms it before searching.
func newPMTUD(cfg pmtudConfig) *pmtudState {
	cfg = cfg.withDefaults()
	return &pmtudState{cfg: cfg, phase: pmtudBase, plpmtu: cfg.Base, good: cfg.Base}
}

// PLPMTU is the current effective path PLPMTU (UDP payload bytes). Safe to read any time.
func (p *pmtudState) PLPMTU() int { return p.plpmtu }

// Phase reports the search state (for stats/observability).
func (p *pmtudState) Phase() pmtudPhase { return p.phase }

// BlackHoles is the number of black-hole events detected on this path.
func (p *pmtudState) BlackHoles() int { return p.blackHoles }

// onAck records that a probe of `size` was acknowledged by the peer.
func (p *pmtudState) onAck(now clock.Timestamp, size int) {
	if !p.inflight || size != p.probeSize {
		return // stale/duplicate ack
	}
	p.inflight, p.tries = false, 0
	if p.confirming {
		p.confirming, p.lastConfirm = false, now
		return // PLPMTU still passes — nothing to change
	}
	if size > p.good {
		p.good, p.plpmtu = size, size
	}
	if p.phase == pmtudBase {
		p.phase = pmtudSearching // base confirmed — search upward
	}
}

// onProbeFail records that the outstanding probe size failed definitively (lost MaxProbes
// times). In Base this means the path cannot carry the floor (Error); in Complete a failed
// confirmation is a black hole — drop to base and re-search.
func (p *pmtudState) onProbeFail(now clock.Timestamp, size int) {
	p.inflight, p.tries = false, 0
	if p.confirming {
		// The current PLPMTU stopped passing: the path shrank. Drop to the safe base and
		// re-discover from there — the live-media-fast analog of RFC 8899's black-hole
		// response.
		p.confirming = false
		p.blackHoles++
		p.plpmtu, p.good, p.bad = p.cfg.Base, p.cfg.Base, 0
		p.phase = pmtudBase
		return
	}
	if p.phase == pmtudBase {
		p.phase = pmtudError // even the base does not pass
		return
	}
	if p.bad == 0 || size < p.bad {
		p.bad = size
	}
}

// candidate returns the next size to probe while searching, or 0 if the search has
// converged. It is an optimistic binary search: probe the ceiling first (most paths carry
// it), then bisect between the largest good and smallest bad until they are within the
// granularity.
func (p *pmtudState) candidate() int {
	if p.bad == 0 {
		if p.good < p.cfg.Max {
			return p.cfg.Max
		}
		return 0 // already at the ceiling
	}
	if p.bad-p.good <= p.cfg.Granularity {
		return 0 // converged
	}
	return (p.good + p.bad) / 2
}

// startProbe arms a probe of `size` and returns it for the host to send.
func (p *pmtudState) startProbe(now clock.Timestamp, size int, confirm bool) (int, bool) {
	p.inflight, p.confirming, p.probeSize, p.sentAt, p.tries = true, confirm, size, now, 0
	return size, true
}

// tick advances the state machine to `now` and returns a probe size to send (and true), or
// (0,false) if nothing is due. The host calls it each cadence; on a returned probe it sends
// a padded datagram of that size with DF set, and calls onAck when the peer confirms it.
func (p *pmtudState) tick(now clock.Timestamp) (int, bool) {
	for {
		if p.phase == pmtudError {
			return 0, false
		}
		if p.inflight {
			if now.Sub(p.sentAt) < p.cfg.ProbeTimeoutUs {
				return 0, false // still waiting for the ack
			}
			p.tries++
			if p.tries < p.cfg.MaxProbes {
				p.sentAt = now
				return p.probeSize, true // retransmit the probe
			}
			p.onProbeFail(now, p.probeSize) // exhausted — definitive failure
			continue                        // re-evaluate with the probe cleared
		}
		switch p.phase {
		case pmtudBase:
			return p.startProbe(now, p.cfg.Base, false)
		case pmtudSearching:
			c := p.candidate()
			if c == 0 {
				p.phase, p.lastRaise, p.lastConfirm = pmtudComplete, now, now
				return 0, false
			}
			return p.startProbe(now, c, false)
		case pmtudComplete:
			if p.plpmtu < p.cfg.Max && now.Sub(p.lastRaise) >= p.cfg.RaiseIntervalUs {
				p.lastRaise, p.bad, p.phase = now, 0, pmtudSearching // re-search upward
				continue
			}
			if now.Sub(p.lastConfirm) >= p.cfg.ConfirmEveryUs {
				return p.startProbe(now, p.plpmtu, true) // black-hole confirmation
			}
			return 0, false
		default:
			return 0, false
		}
	}
}

// pathSetMin returns the minimum PLPMTU across a set of per-path state machines — the size
// a generation spread across all paths must fit, since any symbol may traverse any path and
// repair is fungible across the union (N5). Returns 0 for an empty set.
func pathSetMin(ps []*pmtudState) int {
	m := 0
	for _, p := range ps {
		if p == nil {
			continue
		}
		if m == 0 || p.PLPMTU() < m {
			m = p.PLPMTU()
		}
	}
	return m
}
