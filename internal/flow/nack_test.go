package flow

// Tests for the NACK-bitmap unit-repair hybrid (wire.Feedback.Missing +
// SlidingSender.answerMissing): the receiver's stuck-neighborhood bitmap, the
// sender's unit-repair answer with its gates (cycle dedup, retention clip,
// provably-dead skip), and the end-to-end burst recovery through the real loop.

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/gf"
	"github.com/zsiec/meld/internal/wire"
)

// TestBandDecoderMissingIn pins the bitmap semantics: missing = covered-but-
// unproducible; delivered, solved, and beyond-frontier ids are never reported.
func TestBandDecoderMissingIn(t *testing.T) {
	d := code.NewBandDecoder(8, 16, 1024)
	mk := func(id uint32) []byte { b := make([]byte, 8); b[0] = byte(id); return b }
	// ids 0,1 delivered; 3,4 arrive (2 missing); frontier ends at 5.
	for _, id := range []uint32{0, 1, 3, 4} {
		d.AddSystematic(id, mk(id))
	}
	for {
		if _, ok := d.Deliver(); !ok {
			break
		}
	}
	got := d.MissingIn(d.Cursor())
	if d.Cursor() != 2 {
		t.Fatalf("cursor = %d, want 2", d.Cursor())
	}
	if got != 1 { // bit 0 = id 2 missing; ids 3,4 solved; id 5+ beyond frontier
		t.Fatalf("MissingIn = %b, want 1", got)
	}
}

// TestUnitRepairAnswersMissing pins the sender half: set bits are answered with
// unit repairs (WindowBase=id, N=1, payload = the retained source), an in-flight
// unit is not re-sent within a cycle, and ids outside retention are skipped.
func TestUnitRepairAnswersMissing(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 200_000, SlidingReactiveShift: true}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)
	for i := 0; i < 24; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 16 // evidence: the receiver reported wire loss

	fb := wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 3,
		Missing: 0b1011} // ids 4,5,7 missing
	s.FeedFeedback(now, fb)
	units := map[uint32]int{}
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.Repair && sym.N == 1 && sym.RepairKey == 0 {
			units[sym.WindowBase]++
			// The wire payload is the key-0 COEFFICIENT product of the source (what
			// the decoder's GenCoeffs(0,1) expects), not the raw bytes.
			want := make([]byte, testSym)
			gf.MulAdd(want, makeChunkN(sym.WindowBase), code.GenCoeffs(0, 1)[0])
			for i := range want {
				if sym.Payload[i] != want[i] {
					t.Fatalf("unit repair for id %d carries wrong bytes", sym.WindowBase)
				}
			}
		}
	}
	for _, id := range []uint32{4, 5, 7} {
		if units[id] != 1 {
			t.Fatalf("unit repairs for id %d = %d, want exactly 1 (units=%v)", id, units[id], units)
		}
	}
	// Same report inside the cycle: deduped, no re-send.
	s.FeedFeedback(now.Add(5_000), fb)
	if extra := len(drainSlidingSymbols(t, s)); extra != 0 {
		t.Fatalf("in-flight units re-sent within the cycle: %d symbols", extra)
	}
	// After a cycle the still-missing id is answered again.
	later := now.Add(reactiveCycleMicros(s.rttMicros) + 1_000)
	s.FeedFeedback(later, wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 1, Missing: 0b1})
	resent := 0
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.Repair && sym.N == 1 && sym.WindowBase == 4 {
			resent++
		}
	}
	if resent != 1 {
		t.Fatalf("post-cycle re-answer = %d, want 1", resent)
	}
	// Outside retention: no emission, no crash.
	s.enc.SlideTo(20)
	s.wireLossBudget = 8
	s.FeedFeedback(later.Add(200_000), wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 1, Missing: 0b1})
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.Repair && sym.N == 1 && sym.WindowBase == 4 {
			t.Fatal("unit repair emitted for an id outside retention")
		}
	}
}

// TestNACKUnitRecoversBurstEndToEnd runs the real loop over a hard mid-stream burst
// at a reactive-capable budget: the units must engage (deficit answered as unit
// repairs) and the burst must fully recover, with the four invariants intact.
func TestNACKUnitRecoversBurstEndToEnd(t *testing.T) {
	t.Parallel()
	const (
		n      = 1_200
		owd    = 30_000
		src    = 500
		budget = 150_000
	)
	cfg := Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.05, TargetFailure: 1e-2, BufferMicros: budget, SlidingReactiveShift: true}
	ch := &pathOutageChannel{path: 0, from: 400, to: 464}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	sl := simLink{cfg: cfg, owdMicros: owd, srcMicros: src, n: n, sliding: true, drop: ch.drop}
	res := sl.runCores(s, r)
	assertCoreInvariants(t, res, n, "nack-unit burst")
	if res.delivered != n {
		t.Fatalf("burst not fully recovered: %d/%d", res.delivered, n)
	}
	if len(s.unitSentAt) == 0 && res.sstats.ReactiveRepair == 0 {
		t.Fatal("neither units nor reactive repair engaged")
	}
}
