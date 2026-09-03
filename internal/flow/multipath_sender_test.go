package flow

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// makeChunkN returns a fixed-size source chunk with id in its first 4 bytes (payload
// content is irrelevant to these header-placement/sizing tests).
func makeChunkN(id uint32) []byte {
	b := make([]byte, testSym)
	binary.BigEndian.PutUint32(b, id)
	return b
}

// drainSenderSymbols decodes every datagram currently queued on the sender.
func drainSenderSymbols(t *testing.T, s *Sender) []wire.Symbol {
	t.Helper()
	var out []wire.Symbol
	for {
		d, ok := s.PollSend()
		if !ok {
			return out
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		out = append(out, sym)
	}
}

func mpSenderConfig() Config {
	c := Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: 0, BufferMicros: testBuf, Paths: 2}
	return c
}

// slot2Ppm builds the 2-path per-slot erasure-count histogram [pNone, pOne, pTwo] (ppm)
// from the per-path marginals and the joint both-erased rate — the sizer input a 2-path
// receiver reports, used by the tests to seed the sender directly.
func slot2Ppm(pa, pb, pBoth float64) []int {
	a, b, both := ppm(pa), ppm(pb), ppm(pBoth)
	return []int{1_000_000 - a - b + both, a + b - 2*both, both}
}

// TestMultipathPlacement: in 2-path mode the sender stamps each systematic symbol
// with path = id mod 2 (the round-robin the receiver mirrors for co-loss), every
// repair symbol carries a valid path id, and once a path is marked the better
// deliverer the repair load skews toward it.
func TestMultipathPlacement(t *testing.T) {
	cfg := mpSenderConfig()
	s := NewSender(cfg)
	// Give the sender a loss estimate so it actually provisions repair to place.
	s.pathLossPpm, s.slotDistPpm = []int{ppm(0.3), ppm(0.3)}, slot2Ppm(0.3, 0.3, 0.09)

	now := clock.Timestamp(0)
	const gens = 4
	for i := 0; i < gens*cfg.GenSize; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(testTick)
	}
	s.Flush(now)
	syms := drainSenderSymbols(t, s)

	var sysCount, repCount int
	var repPerPath [2]int
	for _, sym := range syms {
		if sym.PathID > 1 {
			t.Fatalf("path id %d out of range for 2 paths", sym.PathID)
		}
		switch sym.Kind {
		case wire.Systematic:
			sysCount++
			if want := uint8(sym.SrcIndex % 2); sym.PathID != want {
				t.Fatalf("systematic id %d on path %d, want round-robin path %d", sym.SrcIndex, sym.PathID, want)
			}
		case wire.Repair:
			repCount++
			repPerPath[sym.PathID]++
		}
	}
	if sysCount != gens*cfg.GenSize {
		t.Fatalf("emitted %d systematic, want %d", sysCount, gens*cfg.GenSize)
	}
	if repCount == 0 {
		t.Fatal("no repair emitted despite a 30%% loss estimate")
	}
	// Even quality ⇒ repair splits roughly evenly across the two paths.
	if d := absI(repPerPath[0] - repPerPath[1]); d > repCount/2+1 {
		t.Fatalf("repair split %v too lopsided under equal quality", repPerPath)
	}

	// Now mark path 0 the far better deliverer; a fresh batch of repair should skew to it.
	s.sched.setQuality([]int{950_000, 200_000})
	var skew [2]int
	for k := 0; k < 600; k++ {
		skew[s.sched.repairPath()]++
	}
	if skew[0] <= skew[1] {
		t.Fatalf("repair did not skew to the better path: %v", skew)
	}
}

// TestMultipathSizingProvisionsForCorrelation: at the SENDER, the proactive repair
// count rises when the two paths' losses are correlated (the joint erasure tail is
// heavier), and reduces to the single-path binomial set-point when they are
// independent. This is expressed through repairCountFor, the function the
// generation close actually calls.
func TestMultipathSizingProvisionsForCorrelation(t *testing.T) {
	cfg := mpSenderConfig()
	s := NewSender(cfg)
	s.genMaxPri = uint8(uepCenterTier) // base tier ⇒ the configured TargetFailure (no UEP scaling)
	const (
		p     = 0.30
		delta = 1e-3
	)

	// Independent paths: pBoth = pa*pb. Should track the single-path binomial sizer.
	s.pathLossPpm, s.slotDistPpm = []int{ppm(p), ppm(p)}, slot2Ppm(p, p, p*p)
	rIndep := s.repairCountFor(cfg.GenSize)
	rBinom := repairForTarget(cfg.GenSize, p, delta, maxRepairFactor)
	if d := rIndep - rBinom; d < -2 || d > 2 {
		t.Fatalf("independent-path sizing r=%d differs from binomial r=%d by %d", rIndep, rBinom, d)
	}

	// Correlated paths (same marginals, heavier joint tail): must provision strictly more.
	rho := 0.9
	pBoth := p*p + rho*math.Sqrt(p*(1-p)*p*(1-p))
	s.slotDistPpm = slot2Ppm(p, p, pBoth)
	rCorr := s.repairCountFor(cfg.GenSize)
	if rCorr <= rIndep {
		t.Fatalf("correlated sizing r=%d did not exceed the independent r=%d", rCorr, rIndep)
	}

	// The emitted proactive repair count equals the sizer (placement never changes it).
	s2 := NewSender(cfg)
	s2.pathLossPpm, s2.slotDistPpm = []int{ppm(p), ppm(p)}, slot2Ppm(p, p, pBoth)
	now := clock.Timestamp(0)
	for i := 0; i < cfg.GenSize; i++ { // exactly one generation
		s2.Write(now, makeChunkN(uint32(i)))
		now = now.Add(testTick)
	}
	var emitted int
	for _, sym := range drainSenderSymbols(t, s2) {
		if sym.Kind == wire.Repair {
			emitted++
		}
	}
	if emitted != rCorr {
		t.Fatalf("emitted %d proactive repair, want repairCountFor=%d", emitted, rCorr)
	}
}
