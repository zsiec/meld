package flow

// Tests for the opt-in headroom-aware proactive sizing (Config.HeadroomAwareSizing,
// PREREG Amendment 9): flag-off inertness (the default path must be byte-identical
// to the pre-arc-9 sizer), the flag-on win on a genuinely capacity-limited paced
// link (the breaker/set-point limit cycle the arc-8 isolation named), and no false
// tighten on clean links.

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// headroomCell is the arc-8 isolation cell: 10% GE-12 loss through a 1 MiB/s paced
// wire at rtt 60ms / budget 90ms — the sizer's honest set-point exceeds the wire's
// ~2x headroom, so uncapped sizing enters the boom/slam limit cycle.
func headroomCell(headroom bool) (simResult, *SlidingSender) {
	cfg := sweepDefaultCfg(90_000)
	cfg.HeadroomAwareSizing = headroom
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	res := simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: 6_000,
		sliding: true, drop: sweepDrop(sweepCell{loss: 0.10, burst: 12}, 0),
		paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: 3}.runCores(s, r)
	return res, s
}

// TestHeadroomSizingBreaksLimitCycle pins the flag's win: on the capacity-limited
// cell the capped sizer must deliver strictly more, at lower overhead, with the
// tighten path actually engaging — and the four invariants intact on both arms.
func TestHeadroomSizingBreaksLimitCycle(t *testing.T) {
	t.Parallel()
	off, sOff := headroomCell(false)
	on, sOn := headroomCell(true)
	assertCoreInvariants(t, off, 6_000, "headroom off")
	assertCoreInvariants(t, on, 6_000, "headroom on")
	t.Logf("off: deliv=%d ovh=%.1f%% tightens=%d | on: deliv=%d ovh=%.1f%% tightens=%d",
		off.deliveredInTime, 100*off.overhead(), sOff.stats.HeadroomTightens,
		on.deliveredInTime, 100*on.overhead(), sOn.stats.HeadroomTightens)
	if sOff.stats.HeadroomTightens != 0 || sOff.headroomCap < maxRepairFactor {
		t.Fatalf("flag off must be inert: tightens=%d cap=%.2f", sOff.stats.HeadroomTightens, sOff.headroomCap)
	}
	if sOn.stats.HeadroomTightens == 0 {
		t.Fatal("flag on never tightened on a saturating wire")
	}
	if on.deliveredInTime <= off.deliveredInTime {
		t.Fatalf("headroom sizing must beat the limit cycle: %d <= %d", on.deliveredInTime, off.deliveredInTime)
	}
	if on.overhead() >= off.overhead() {
		t.Fatalf("headroom sizing must cost less: %.3f >= %.3f", on.overhead(), off.overhead())
	}
}

// TestHeadroomSizingQuietOnCleanLink pins the no-false-tighten property: a clean
// paced link never presents the f-low + standing-queue conjunction, so the cap
// stays inactive and delivery is complete.
func TestHeadroomSizingQuietOnCleanLink(t *testing.T) {
	t.Parallel()
	cfg := sweepDefaultCfg(90_000)
	cfg.HeadroomAwareSizing = true
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	res := simLink{cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: 6_000,
		sliding: true, drop: func(wire.Symbol) bool { return false },
		paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: 3}.runCores(s, r)
	assertCoreInvariants(t, res, 6_000, "headroom clean link")
	if res.delivered != 6_000 {
		t.Fatalf("clean link delivered %d/6000", res.delivered)
	}
	if s.stats.HeadroomTightens != 0 {
		t.Fatalf("clean link tightened %d times (false saturation)", s.stats.HeadroomTightens)
	}
}
