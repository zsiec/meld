package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
)

// TestEffectiveProtectionTier pins the temporal-depth unequal-protection gradient
// protection): a PURE top-layer generation loosens one protection tier per temporal level past the
// reference layer — the forward-looking proxy for descendant fan-out, since a frame deeper in the
// hierarchical-B GOP is decoded FROM by exponentially fewer downstream frames. A generation carrying
// a reference/base frame keeps its discrete tier (the reference still needs full protection), and a
// codec-blind byte stream (center tier, sentinel TID) is untouched.
func TestEffectiveProtectionTier(t *testing.T) {
	const none = noTemporalID // byte-stream sentinel: no frame descriptor seen
	cases := []struct {
		name     string
		pri, tid uint8
		want     int
	}{
		{"byte-stream: center tier, sentinel TID", uepCenterTier, none, uepCenterTier},
		// The sentinel must NEVER be read as a deep layer: a fresh sizer probe (genMaxPri=0,
		// genMinTID=noTemporalID) must keep the discrete disposable tier, not loosen to oblivion.
		{"no-frame sizer probe: disposable tier, sentinel TID", 0, none, 0},
		{"RAP keeps its tier regardless of depth", 3, 4, 3},
		{"base keeps its tier regardless of depth", 2, 4, 2},
		{"enhancement (TID 1) not loosened", 1, 1, 1},
		{"first leaf layer (TID 2) flat at disposable", 0, 2, 0},
		{"one level deeper (TID 3) loosens a tier", 0, 3, -1},
		{"two levels deeper (TID 4) loosens two", 0, 4, -2},
	}
	for _, c := range cases {
		if got := effectiveProtectionTier(c.pri, c.tid); got != c.want {
			t.Errorf("%s: effectiveProtectionTier(pri=%d, tid=%d)=%d, want %d", c.name, c.pri, c.tid, got, c.want)
		}
	}
}

// TestTemporalDepthProtection proves the gradient reaches the proactive repair sizer end-to-end: at
// equal disposable class and equal channel erasure, a deeper top-layer generation (fewer descendants
// in the GOP dependency DAG) is provisioned strictly LESS repair than a shallower one, while a
// reference/base generation still earns far MORE than any leaf — unequal protection refined by
// temporal depth, not just the discrete four-tier collapse.
func TestTemporalDepthProtection(t *testing.T) {
	repairFor := func(pri, tid uint8) uint64 {
		const sym, gen = 64, 8
		cfg := Config{Flow: 1, SymbolSize: sym, GenSize: gen, Redundancy: 0, BufferMicros: 200_000}
		s := NewSender(cfg)
		s.pEst = 0.2 // a fixed lossy channel so the per-tier target drives a non-trivial repair count
		now := clock.Timestamp(0)
		for i := 0; i < gen; i++ {
			fd := FrameDesc{Priority: pri, FrameID: uint32(i), Chunks: 1, TemporalID: tid, Discardable: pri == 0}
			s.WriteFrame(now, make([]byte, sym), fd)
			now = now.Add(1_000)
		}
		s.closeGen(now) // size + emit the proactive repair for this single generation
		return s.Stats().Repair
	}

	leafShallow := repairFor(0, 2) // disposable, first leaf layer (TID 2 == reference depth: flat)
	leafDeep := repairFor(0, 3)    // disposable, one temporal level deeper (fewer descendants)
	base := repairFor(2, 0)        // base / reference layer (most descendants)
	t.Logf("proactive repair: base(TID0)=%d  leaf(TID2)=%d  leaf(TID3)=%d", base, leafShallow, leafDeep)

	if leafShallow == 0 {
		t.Fatalf("test setup: shallow leaf provisioned no repair at p=0.2 — cannot show a gradient")
	}
	if leafDeep >= leafShallow {
		t.Errorf("a deeper top layer must earn LESS repair: TID3=%d >= TID2=%d", leafDeep, leafShallow)
	}
	if base <= leafShallow {
		t.Errorf("a reference/base generation must earn MORE repair than any leaf: base=%d <= leaf=%d", base, leafShallow)
	}
}
