package session

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
)

// testPMTUDCfg is a fast-timer config for deterministic tests (µs).
func testPMTUDCfg() pmtudConfig {
	return pmtudConfig{
		Base: 1200, Max: 1500, Granularity: 8, MaxProbes: 3,
		ProbeTimeoutUs: 10_000, RaiseIntervalUs: 200_000, ConfirmEveryUs: 100_000,
	}
}

// drivePMTUD simulates the host loop against a path whose MTU is pathMTU(now): it ticks
// every ProbeTimeoutUs, acking any probe whose size fits the path and silently dropping
// (black-holing) any larger probe so its timeout fires on subsequent ticks. Deterministic.
func drivePMTUD(p *pmtudState, durUs int64, pathMTU func(now clock.Timestamp) int) {
	tick := p.cfg.ProbeTimeoutUs
	for now := clock.Timestamp(0); int64(now) < durUs; now = now.Add(tick) {
		if size, send := p.tick(now); send && size <= pathMTU(now) {
			p.onAck(now.Add(1_000), size) // ack ~1 ms later, well within the probe timeout
		}
	}
}

func fixedMTU(m int) func(clock.Timestamp) int { return func(clock.Timestamp) int { return m } }

// TestPMTUD_DiscoversFullCeiling: a path that carries the max probes straight to the
// ceiling and completes.
func TestPMTUD_DiscoversFullCeiling(t *testing.T) {
	p := newPMTUD(testPMTUDCfg())
	drivePMTUD(p, 1_000_000, fixedMTU(1500))
	if p.Phase() != pmtudComplete {
		t.Fatalf("phase = %v, want Complete", p.Phase())
	}
	if p.PLPMTU() != 1500 {
		t.Fatalf("PLPMTU = %d, want 1500", p.PLPMTU())
	}
}

// TestPMTUD_DiscoversMidPMTU: a path with a non-obvious limit is found by binary search to
// within the granularity (and never above the true limit — a too-large PLPMTU would
// black-hole real traffic).
func TestPMTUD_DiscoversMidPMTU(t *testing.T) {
	const truePMTU = 1400
	p := newPMTUD(testPMTUDCfg())
	drivePMTUD(p, 2_000_000, fixedMTU(truePMTU))
	if p.Phase() != pmtudComplete {
		t.Fatalf("phase = %v, want Complete", p.Phase())
	}
	if got := p.PLPMTU(); got > truePMTU || got < truePMTU-p.cfg.Granularity {
		t.Fatalf("PLPMTU = %d, want within (%d, %d]", got, truePMTU-p.cfg.Granularity, truePMTU)
	}
}

// TestPMTUD_BaseFailsIsError: a path that cannot even carry the base PLPMTU lands in Error
// (the host runs at the floor and alarms) — it must never silently claim a working PLPMTU.
func TestPMTUD_BaseFailsIsError(t *testing.T) {
	p := newPMTUD(testPMTUDCfg())
	drivePMTUD(p, 1_000_000, fixedMTU(1000)) // below base 1200
	if p.Phase() != pmtudError {
		t.Fatalf("phase = %v, want Error", p.Phase())
	}
}

// TestPMTUD_BlackHoleShrinkReDiscovers is the headline of Phase 1: a path that worked at
// 1500 silently shrinks to 1300 mid-stream. The confirmation probe at 1500 fails, the
// state machine declares a black hole, drops to the base, and re-discovers ~1300.
func TestPMTUD_BlackHoleShrinkReDiscovers(t *testing.T) {
	const shrinkAt = 500_000
	const after = 1300
	p := newPMTUD(testPMTUDCfg())
	drivePMTUD(p, 3_000_000, func(now clock.Timestamp) int {
		if int64(now) < shrinkAt {
			return 1500
		}
		return after
	})
	if p.BlackHoles() < 1 {
		t.Fatalf("expected a black hole to be detected, got %d", p.BlackHoles())
	}
	if got := p.PLPMTU(); got > after || got < after-p.cfg.Granularity {
		t.Fatalf("after black hole, PLPMTU = %d, want re-discovered ≈%d", got, after)
	}
	// Note: phase legitimately oscillates Complete↔Searching here because PLPMTU (1300) <
	// Max (1500), so the RFC 8899 raise timer keeps optimistically re-probing upward; the
	// invariant is that re-search never LOWERS the discovered PLPMTU (good only grows), so
	// the value above is stable regardless of where the sim stops.
}

// TestPMTUD_GrowthRaises: a path that grows from 1400 to 1500 is picked up by the raise
// timer's periodic optimistic re-probe.
func TestPMTUD_GrowthRaises(t *testing.T) {
	const growAt = 600_000
	p := newPMTUD(testPMTUDCfg())
	drivePMTUD(p, 3_000_000, func(now clock.Timestamp) int {
		if int64(now) < growAt {
			return 1400
		}
		return 1500
	})
	if p.PLPMTU() != 1500 {
		t.Fatalf("after growth, PLPMTU = %d, want 1500 (raise timer should re-probe up)", p.PLPMTU())
	}
}

// TestPMTUD_ProbeRetransmit: a probe lost fewer than MaxProbes times is retransmitted and
// still succeeds (transient loss must not be mistaken for a size limit).
func TestPMTUD_ProbeRetransmit(t *testing.T) {
	p := newPMTUD(testPMTUDCfg())
	now := clock.Timestamp(0)
	// Base probe.
	size, send := p.tick(now)
	if !send || size != 1200 {
		t.Fatalf("first tick: size=%d send=%v, want base probe", size, send)
	}
	// Lose it once: advance past the timeout; the state machine must RETRANSMIT, not fail.
	now = now.Add(p.cfg.ProbeTimeoutUs)
	size, send = p.tick(now)
	if !send || size != 1200 {
		t.Fatalf("after one loss: size=%d send=%v, want retransmit of base", size, send)
	}
	// Now it gets through.
	p.onAck(now.Add(1_000), 1200)
	if p.Phase() != pmtudSearching {
		t.Fatalf("phase = %v, want Searching after base confirmed", p.Phase())
	}
}

// TestPMTUD_Deterministic: identical simulated path ⇒ identical outcome (pure state machine).
func TestPMTUD_Deterministic(t *testing.T) {
	run := func() (int, int, pmtudPhase) {
		p := newPMTUD(testPMTUDCfg())
		drivePMTUD(p, 3_000_000, func(now clock.Timestamp) int {
			if int64(now) < 500_000 {
				return 1452
			}
			return 1300
		})
		return p.PLPMTU(), p.BlackHoles(), p.Phase()
	}
	m1, b1, ph1 := run()
	m2, b2, ph2 := run()
	if m1 != m2 || b1 != b2 || ph1 != ph2 {
		t.Fatalf("non-deterministic: (%d,%d,%v) vs (%d,%d,%v)", m1, b1, ph1, m2, b2, ph2)
	}
}

// TestPMTUD_PathSetMin: the coder must size to the smallest PLPMTU across the path set,
// since a generation spans paths and any symbol may traverse the worst one.
func TestPMTUD_PathSetMin(t *testing.T) {
	a := newPMTUD(testPMTUDCfg())
	drivePMTUD(a, 1_000_000, fixedMTU(1500))
	b := newPMTUD(testPMTUDCfg())
	drivePMTUD(b, 2_000_000, fixedMTU(1400))
	c := newPMTUD(pmtudConfig{Base: 1200, Max: 1280, Granularity: 8, MaxProbes: 3, ProbeTimeoutUs: 10_000})
	drivePMTUD(c, 1_000_000, fixedMTU(1280))
	got := pathSetMin([]*pmtudState{a, b, c})
	if got != c.PLPMTU() || got > 1280 {
		t.Fatalf("pathSetMin = %d, want the smallest path (%d)", got, c.PLPMTU())
	}
}
