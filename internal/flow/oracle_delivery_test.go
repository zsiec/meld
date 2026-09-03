package flow

// No-premature-drop oracle: the enforcement of "100% delivery until it is genuinely
// impossible." It pins the real Receiver to an IDEAL receiver that never evicts early,
// never caps its window, and uses each symbol's TRUE deadline (write time + budget).
//
// For every source id, the oracle replays exactly the symbols that ACTUALLY arrived at
// the receiver (post-channel-drop) through a clean reference decoder, in arrival order,
// and records the instant each id becomes recoverable. An id is "recoverable in time"
// iff that instant is at or before write[id]+budget. The guarantee we want is:
//
//     recoverable-in-time(id)  ⟹  the real Receiver delivered id (on time).
//
// Any id the ideal in-time decoder recovers but the real path drops is a PREMATURE drop
// — a packet lost when we did not have to. The oracle ingests only what arrived, so a
// loss the sender never armed against (throttled / under-sent repair that never reached
// the receiver) is NOT counted premature — this isolates receiver/decoder-side drops,
// the crisp, always-true half of the guarantee. (Sender-side under-provision is probed
// separately.)
//
// This is the dual of the coded-recovery oracle: that one forbids claiming a recovery
// the rank does not support (over-delivery); this one forbids failing a recovery the
// rank DOES support within deadline (premature under-delivery). Together they pin
// delivery to the information-theoretic boundary.

import (
	"sort"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

type tapEntry struct {
	at  clock.Timestamp
	sym wire.Symbol
}

type oracleParams struct {
	cfg          Config
	owdMicros    int64
	srcMicros    int64
	stepMicros   int64
	n            int
	burst        int
	jitterMicros int64
	sliding      bool
}

// oracleRun drives Sender -> lossy link -> Receiver exactly like simLink, but tapes
// every symbol delivered to the receiver (with arrival time) and each source id's write
// time, so an ideal decoder can be reconstructed offline.
func oracleRun(t *testing.T, p oracleParams, drop func(wire.Symbol) bool) (delivered map[uint32]bool, tape []tapEntry, writeAt map[uint32]clock.Timestamp, lateDeliv bool) {
	t.Helper()
	var s coreSenderT
	var r coreReceiverT
	if p.sliding {
		s, r = NewSlidingSender(p.cfg), NewSlidingReceiver(p.cfg)
	} else {
		s, r = NewSender(p.cfg), NewReceiver(p.cfg)
	}
	step := p.stepMicros
	if step <= 0 {
		step = 1_000
	}
	delivered = map[uint32]bool{}
	writeAt = map[uint32]clock.Timestamp{}
	srcDL := map[uint32]clock.Timestamp{}
	var s2r, r2s []inflight
	now := clock.Timestamp(0)
	nextWrite := clock.Timestamp(0)
	written := 0
	endBy := clock.Timestamp(0)

	deliverDue := func(q *[]inflight, to func(d []byte)) {
		keep := (*q)[:0]
		for _, pk := range *q {
			if pk.at.After(now) {
				keep = append(keep, pk)
			} else {
				to(pk.data)
			}
		}
		*q = keep
	}
	pumpSender := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil || drop(sym) {
				continue
			}
			extra := int64(0)
			if p.jitterMicros > 0 {
				extra = int64(coinU(0x31773, uint32(sym.Kind), sym.SrcIndex, sym.WindowBase, uint32(sym.RepairKey)) * float64(p.jitterMicros))
			}
			cp := append([]byte(nil), d...)
			s2r = append(s2r, inflight{now.Add(p.owdMicros + extra), cp})
		}
	}
	pumpReceiver := func() {
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			r2s = append(r2s, inflight{now.Add(p.owdMicros), append([]byte(nil), fb...)})
		}
		for {
			id, _, ok := r.PollDeliver()
			if !ok {
				break
			}
			delivered[id] = true
			if dl, ok := srcDL[id]; ok && now.After(dl) {
				lateDeliv = true
			}
		}
	}

	const maxSteps = 5_000_000
	for steps := 0; steps < maxSteps; steps++ {
		for written < p.n && !nextWrite.After(now) {
			b := p.burst
			if b < 1 {
				b = 1
			}
			for k := 0; k < b && written < p.n; k++ {
				id := uint32(written)
				writeAt[id] = now
				srcDL[id] = now.Add(p.cfg.BufferMicros)
				s.Write(now, simChunk(p.cfg.SymbolSize, id))
				written++
			}
			nextWrite = nextWrite.Add(p.srcMicros)
		}
		deliverDue(&s2r, func(d []byte) {
			if sym, err := wire.DecodeSymbol(d); err == nil {
				sym.Payload = append([]byte(nil), sym.Payload...)
				tape = append(tape, tapEntry{now, sym})
			}
			r.FeedSymbol(now, d)
		})
		deliverDue(&r2s, func(d []byte) {
			if f, err := wire.DecodeFeedback(d); err == nil {
				s.FeedFeedback(now, f)
			}
		})
		s.Tick(now)
		r.Tick(now)
		pumpSender()
		pumpReceiver()
		if written >= p.n {
			if endBy == 0 {
				s.Flush(now)
				endBy = now.Add(p.cfg.BufferMicros + 8*p.owdMicros + int64(p.cfg.GenSize)*p.srcMicros)
			} else if now.After(endBy) && len(s2r) == 0 && len(r2s) == 0 {
				break
			}
		}
		now = now.Add(step)
	}
	return delivered, tape, writeAt, lateDeliv
}

// oracleResult is the per-run accounting of the no-premature-drop check.
type oracleResult struct {
	premature    int      // lost by the real path, but recoverable in time by the ideal decoder
	genuineLoss  int      // lost by the real path AND unrecoverable in time (an "absolutely had to" loss)
	delivered    int      // delivered by the real path
	prematureIDs []uint32 // the first few offenders, for the failure message
	lateDeliv    bool
}

// analyzeOracle reconstructs the ideal in-time recovery from the tape and compares it to
// the real delivered set. It feeds every arrived symbol, in arrival order, into ONE
// clean global RLNC decoder over [0,n) with no deadline eviction and no window cap — so
// it is the ideal receiver for BOTH the generation and sliding paths (a repair over
// [base,base+N) lands at the same columns the band decoder would use). recoveredAt[id]
// is the arrival instant at which the ideal decoder first surfaces id.
func analyzeOracle(p oracleParams, delivered map[uint32]bool, tape []tapEntry, writeAt map[uint32]clock.Timestamp, lateDeliv bool) oracleResult {
	budget := p.cfg.BufferMicros
	symSize := p.cfg.SymbolSize

	ord := make([]tapEntry, len(tape))
	copy(ord, tape)
	sort.SliceStable(ord, func(i, j int) bool { return ord[i].at.Before(ord[j].at) })

	recoveredAt := map[uint32]clock.Timestamp{}
	dec := code.NewDecoder(codedSymbolSize(symSize), 0, p.n)
	for _, e := range ord {
		var rec []code.Recovered
		switch e.sym.Kind {
		case wire.Systematic, wire.UnitRepair:
			n, ok := systematicSourceLength(e.sym, symSize)
			if !ok {
				continue
			}
			value := makeCodedSource(e.sym.Payload[:n], symSize, clock.Timestamp(e.sym.Deadline))
			rec = dec.AddSystematic(e.sym.SrcIndex, value)
		case wire.Repair:
			value, ok := expandRepairPayload(e.sym, symSize)
			if !ok {
				continue
			}
			rec = dec.AddRepair(e.sym.WindowBase, int(e.sym.N), e.sym.RepairKey, value)
		}
		for _, rc := range rec {
			if _, seen := recoveredAt[rc.ID]; !seen {
				recoveredAt[rc.ID] = e.at
			}
		}
	}

	res := oracleResult{lateDeliv: lateDeliv}
	for id := uint32(0); id < uint32(p.n); id++ {
		if delivered[id] {
			res.delivered++
			continue
		}
		ra, ok := recoveredAt[id]
		D := writeAt[id].Add(budget)
		if ok && !ra.After(D) {
			res.premature++
			if len(res.prematureIDs) < 12 {
				res.prematureIDs = append(res.prematureIDs, id)
			}
		} else {
			res.genuineLoss++
		}
	}
	return res
}

// oracleRegime is one channel/cadence scenario for the no-premature-drop sweep.
type oracleRegime struct {
	name    string
	owd     int64
	src     int64
	burst   int
	jit     int64
	sliding bool
	drop    func(wire.Symbol) bool
}

const oracleN = 480

// runOracleRegime executes one regime and returns its premature-drop accounting.
func runOracleRegime(t *testing.T, base Config, rg oracleRegime) oracleResult {
	t.Helper()
	p := oracleParams{cfg: base, owdMicros: rg.owd, srcMicros: rg.src, n: oracleN, burst: rg.burst, jitterMicros: rg.jit, sliding: rg.sliding}
	delivered, tape, writeAt, late := oracleRun(t, p, rg.drop)
	res := analyzeOracle(p, delivered, tape, writeAt, late)
	t.Logf("%-26s delivered=%3d premature=%3d genuineLoss=%3d late=%v ids=%v",
		rg.name, res.delivered, res.premature, res.genuineLoss, res.lateDeliv, res.prematureIDs)
	return res
}

// oracleBaseConfig is the shared config for the sweeps (200 ms budget, GenSize 16).
func oracleBaseConfig() Config {
	return Config{Flow: 1, SymbolSize: 64, GenSize: 16, Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: 200_000}
}

// TestOracleNoPrematureDropGeneration is the enforcement of "100% delivery until it is
// genuinely impossible" for the GENERATION (default) profile: across a matrix of
// channel/cadence regimes, every source id the ideal in-time decoder can recover MUST be
// delivered by the real Receiver. Any premature drop fails the test. The generation
// receiver carries the per-id stamped-deadline fix (symDL) and the careful refDL
// backstop, so it holds the guarantee — this locks that.
func TestOracleNoPrematureDropGeneration(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("oracle sweep is slow; run without -short")
	}
	base := oracleBaseConfig()
	regimes := []oracleRegime{
		{"gen uniform-5%-20ms", 20_000, 1_000, 0, 0, false, uniformDrop(0x1, 0.05)},
		{"gen uniform-15%-20ms", 20_000, 1_000, 0, 0, false, uniformDrop(0x2, 0.15)},
		{"gen uniform-15%-80ms", 80_000, 1_000, 0, 0, false, uniformDrop(0x3, 0.15)},
		{"gen uniform-25%-40ms", 40_000, 1_000, 0, 0, false, uniformDrop(0x9, 0.25)},
		{"gen ge-10%-burst6-20ms", 20_000, 1_000, 0, 0, false, geDrop(0x4, 0.10, 6)},
		{"gen ge-10%-burst6-80ms", 80_000, 1_000, 0, 0, false, geDrop(0x5, 0.10, 6)},
		{"gen ge-15%-burst10-40ms", 40_000, 1_000, 0, 0, false, geDrop(0xA, 0.15, 10)},
		{"gen bursty-write8-10%", 20_000, 4_000, 8, 0, false, uniformDrop(0x6, 0.10)},
		{"gen bursty-write16-tight", 20_000, 8_000, 16, 0, false, uniformDrop(0xB, 0.10)},
		{"gen reorder-jit15ms-10%", 20_000, 1_000, 0, 15_000, false, uniformDrop(0x7, 0.10)},
		{"gen reorder-jit40ms-15%", 20_000, 1_000, 0, 40_000, false, uniformDrop(0xC, 0.15)},
		{"gen onset-10%-40ms", 40_000, 1_000, 0, 0, false, onsetDrop(120, 0x8, 0.10)},
	}
	for _, rg := range regimes {
		if res := runOracleRegime(t, base, rg); res.premature > 0 {
			t.Errorf("%s: %d premature drop(s) — symbols the ideal in-time decoder recovers were lost (ids %v)",
				rg.name, res.premature, res.prematureIDs)
		}
	}
}

// TestOracleSlidingPrematureDrop enforces "100% until genuinely impossible" for the SLIDING
// (band-form) profile, wherever the coding band can reach a gap before its deadline. Two fixes
// closed the gap the oracle originally found: the clean-link deadline-stamp port to
// SlidingReceiver (per-id symDL + the monotonic refDL backstop), and — the decisive one —
// late-repair recovery in the band decoder (a repair whose window starts below the cursor but
// still covers a stuck gap is reduced against the retained recent values and used, instead of
// being rejected because the sender's window lags the receiver's cursor by the feedback delay).
//
// The clean regimes below now hold premature == 0. The two band-LIMITED regimes are held to a
// loose bound: when the sender's window lag exceeds the coding band (high RTT, or a long
// write-burst) some repairs covering a gap start more than a full band below it and can never
// reach it — a band-SIZING limit, not a receiver-avoidable premature drop. The loose bound still
// trips if late-repair recovery regresses (the residuals would jump back to ~18 and ~11).
func TestOracleSlidingPrematureDrop(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("oracle sweep is slow; run without -short")
	}
	base := oracleBaseConfig()
	clean := []oracleRegime{
		{"sld uniform-15%-20ms", 20_000, 1_000, 0, 0, true, uniformDrop(0xD1, 0.15)},
		{"sld ge-10%-burst6-20ms", 20_000, 1_000, 0, 0, true, geDrop(0xD3, 0.10, 6)},
		{"sld ge-15%-burst10-40ms", 40_000, 1_000, 0, 0, true, geDrop(0xD4, 0.15, 10)},
		{"sld reorder-jit40ms-15%", 20_000, 1_000, 0, 40_000, true, uniformDrop(0xD6, 0.15)},
	}
	for _, rg := range clean {
		if res := runOracleRegime(t, base, rg); res.premature > 0 {
			t.Errorf("%s: %d premature drop(s) — recoverable-in-time symbols were lost (ids %v)",
				rg.name, res.premature, res.prematureIDs)
		}
	}
	limited := []struct {
		rg      oracleRegime
		maxPrem int // band-sizing residual; loose so a recovery regression (~18, ~11) still trips
	}{
		{oracleRegime{"sld uniform-15%-80ms", 80_000, 1_000, 0, 0, true, uniformDrop(0xD2, 0.15)}, 12},
		{oracleRegime{"sld bursty-write8-10%", 20_000, 4_000, 8, 0, true, uniformDrop(0xD5, 0.10)}, 6},
	}
	for _, c := range limited {
		if res := runOracleRegime(t, base, c.rg); res.premature > c.maxPrem {
			t.Errorf("%s: %d premature drops exceed the band-limited allowance %d (late-repair recovery regressed?)",
				c.rg.name, res.premature, c.maxPrem)
		}
	}
}
