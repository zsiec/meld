package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// splitmix64 / coinU give a deterministic, order-independent per-symbol coin so the
// UEP and flat runs see the same systematic-loss pattern regardless of how much repair
// each emits.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func coinU(seed uint64, parts ...uint32) float64 {
	h := seed
	for _, p := range parts {
		h = splitmix64(h ^ uint64(p))
	}
	return float64(splitmix64(h)>>11) / float64(uint64(1)<<53)
}

// runUEP streams a synthetic GOP through Sender/Receiver over an i.i.d.-loss channel,
// either with unequal protection (uep: each unit written at its shaper-assigned tier)
// or flat protection (every unit at the base tier). Both are held to the SAME rate
// ceiling (cfg.MaxBitrate), so it is an equal-budget comparison. Returns the decodable-
// keyframe rate, decodable-frame rate, and the realized
// repair overhead.
func runUEP(t *testing.T, cfg Config, units []shape.Unit, uep bool, lossP float64, seed uint64) (keyframeRate, frameRate, overhead float64, ordered bool) {
	t.Helper()
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	delivered := map[uint32]bool{}
	ordered = true
	var lastDeliv int64 = -1

	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				continue
			}
			var key uint32
			if sym.Kind == wire.Systematic {
				key = sym.SrcIndex // identical systematic-loss pattern across UEP/flat runs
			} else {
				key = (sym.WindowBase*100003 + uint32(sym.RepairKey)) | (1 << 31)
			}
			if coinU(seed, uint32(sym.Kind), key) < lossP {
				continue
			}
			r.FeedSymbol(now, d)
		}
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			id := chunkID(d)
			if int64(id) <= lastDeliv {
				ordered = false // delivery must be strictly in order
			}
			lastDeliv = int64(id)
			delivered[id] = true
		}
	}

	var nextID uint32
	chunksPer := make([]int, len(units))
	for ui, u := range units {
		nch := (u.Size + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		chunksPer[ui] = nch
		pri := uint8(uepCenterTier)
		if uep {
			pri = u.Class.Wire()
		}
		for c := 0; c < nch; c++ {
			// The synthetic unit sizes are expressed in cfg.SymbolSize chunks. Feed
			// full chunks here so the equal-rate budget remains tight now that the
			// protocol no longer pads short systematic payloads on the wire.
			s.WriteUnit(now, simChunk(cfg.SymbolSize, nextID), pri)
			nextID++
			pump()
			now = now.Add(testTick)
			s.Tick(now)
			r.Tick(now)
			pump()
		}
		s.Flush(now) // each unit closes its own generation(s) at one protection tier
		pump()
	}
	// Settle so reactive repair within the deadline window converges, then advance past
	// every deadline to release stuck generations.
	settle := int(cfg.BufferMicros/testTick) + 8*cfg.GenSize
	for k := 0; k < settle; k++ {
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	now = now.Add(cfg.BufferMicros + int64(nextID)*testTick)
	r.Tick(now)
	pump()

	// A unit is delivered iff all of its source chunks were delivered.
	unitDelivered := make(map[uint32]bool, len(units))
	var idCursor uint32
	for ui, u := range units {
		all := true
		for c := 0; c < chunksPer[ui]; c++ {
			if !delivered[idCursor] {
				all = false
			}
			idCursor++
		}
		unitDelivered[u.ID] = all
	}
	keyframeRate = shape.DecodableKeyframeRate(units, unitDelivered)
	frameRate = shape.DecodableFrameRate(units, unitDelivered)
	st := s.Stats()
	if st.Source > 0 {
		overhead = 100 * float64(st.Repair) / float64(st.Source)
	}
	return keyframeRate, frameRate, overhead, ordered
}

// TestUEPSweep logs decodable-frame-rate + overhead for unequal vs flat protection
// across a budget cap, to find the regime where UEP earns its keep (exploratory; not
// an assertion).
func TestUEPSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep")
	}
	units := shape.GenerateGOP(12, 16)
	for _, cap := range []int64{0, 8_000_000, 6_000_000, 4_000_000, 3_000_000, 2_000_000} {
		cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 200_000, MaxBitrate: cap}
		uK, uF, uO, _ := runUEP(t, cfg, units, true, 0.40, 0x5EED)
		fK, fF, fO, _ := runUEP(t, cfg, units, false, 0.40, 0x5EED)
		t.Logf("cap=%-9d  UEP key=%.3f frame=%.3f ovhd=%.0f%% | flat key=%.3f frame=%.3f ovhd=%.0f%%",
			cap, uK, uF, uO, fK, fF, fO)
	}
}

// TestUEPProtectsKeyframesAtEqualBudget verifies that at a tight, equal budget over a heavy
// (40%) i.i.d.-loss channel, unequal protection keeps the keyframes — and thus the GOPs
// they anchor — decodable, where flat protection spreads the same budget across the many
// disposable leaves, fails to hold the keyframes, and collapses. Run over several loss
// patterns; UEP's decodable-keyframe rate must beat flat's every time, and hold a high
// absolute level. Delivery stays in order under both (an invariant check).
func TestUEPProtectsKeyframesAtEqualBudget(t *testing.T) {
	units := shape.GenerateGOP(16, 16)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 200_000, MaxBitrate: 4_000_000}
	const lossP = 0.40
	for seed := uint64(1); seed <= 6; seed++ {
		uK, _, uO, uOrd := runUEP(t, cfg, units, true, lossP, seed*0x9E3779B1)
		fK, _, fO, fOrd := runUEP(t, cfg, units, false, lossP, seed*0x9E3779B1)
		if !uOrd || !fOrd {
			t.Fatalf("seed %d: out-of-order delivery (uep=%v flat=%v)", seed, uOrd, fOrd)
		}
		t.Logf("seed %d: UEP keyframe=%.3f ovhd=%.0f%% | flat keyframe=%.3f ovhd=%.0f%%", seed, uK, uO, fK, fO)
		// UEP holds the keyframes at a high level...
		if uK < 0.70 {
			t.Fatalf("seed %d: UEP keyframe rate %.3f below 0.70 — not holding the anchors under the budget", seed, uK)
		}
		// ...where flat, at the same budget, holds materially fewer. The margin is ≥15 points:
		// it was ≥20 against the original reactive controller, but the per-generation reactive
		// sizing (no warmup/EWMA lag) lifted the FLAT baseline's keyframe recovery a notch
		// (e.g. seed 1: 0.625 → 0.688) — flat leans harder on reactive repair — so UEP's absolute
		// advantage narrowed even though UEP keyframe recovery itself is unchanged and still wins.
		if uK < fK+0.15 {
			t.Fatalf("seed %d: UEP keyframe %.3f did not beat flat %.3f by a clear margin at equal budget", seed, uK, fK)
		}
	}
}
