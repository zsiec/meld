package flow

// Tests for the NACK-bitmap unit-repair hybrid (wire.Feedback.Missing +
// SlidingSender.answerMissing): the receiver's stuck-neighborhood bitmap, the
// sender's unit-repair answer with its gates (cycle dedup, retention clip,
// provably-dead skip), and the end-to-end burst recovery through the real loop.

import (
	"bytes"
	"slices"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
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

func TestBandDecoderClosureInSelectsIndependentColumns(t *testing.T) {
	newDecoder := func() *code.BandDecoder {
		d := code.NewBandDecoder(8, 16, 1024)
		// One row constrains ids 0 and 1; id 0 is its pivot, while ids 1 and 2
		// are the two free columns. Cover id 2 without adding information.
		d.AddSparseRepair([]uint32{0, 1}, 7, make([]byte, 8))
		d.Cover(2)
		return d
	}
	d := newDecoder()
	if got := d.MissingIn(0); got != 0b111 {
		t.Fatalf("unresolved bitmap = %03b, want 111", got)
	}
	if got := d.ClosureIn(0); got != 0b110 {
		t.Fatalf("closure basis = %03b, want free columns 110", got)
	}
	if d.Deficit() != 2 {
		t.Fatalf("deficit = %d, want 2", d.Deficit())
	}

	// The two basis values close the system. Choosing the earliest two unresolved
	// values instead spends both packets but leaves the unrelated free id 2 open.
	d.AddSystematic(1, make([]byte, 8))
	d.AddSystematic(2, make([]byte, 8))
	if d.Deficit() != 0 {
		t.Fatalf("basis answers left deficit %d", d.Deficit())
	}
	redundant := newDecoder()
	redundant.AddSystematic(0, make([]byte, 8))
	redundant.AddSystematic(1, make([]byte, 8))
	if redundant.Deficit() != 1 {
		t.Fatalf("arbitrary unresolved answers left deficit %d, want 1", redundant.Deficit())
	}
}

func TestSlidingClosureExtensions(t *testing.T) {
	t.Run("bitmap", func(t *testing.T) {
		fb := wire.Feedback{DecodedLowEdge: 100, Deficit: 3, Missing: 1 << 2}
		setClosureExtensions(&fb, []uint64{1 << 3, 0, 1 << 9})
		if got, want := closureIDs(fb), []uint32{102, 167, 301}; !slices.Equal(got, want) {
			t.Fatalf("bitmap closure ids = %v, want %v", got, want)
		}
	})

	t.Run("deep range", func(t *testing.T) {
		fb := wire.Feedback{DecodedLowEdge: 100, Deficit: 12, Missing: 0b11}
		masks := make([]uint64, 7)
		// One run crosses the word boundary at offsets 126..130; another sits
		// beyond the raw four-word continuation horizon.
		masks[0] = uint64(0b11) << 62
		masks[1] = 0b111
		masks[6] = uint64(0b11111) << 8
		setClosureExtensions(&fb, masks)
		if _, ok := closureRanges(fb); !ok {
			t.Fatal("deep run-like closure did not select range encoding")
		}
		want := []uint32{100, 101, 226, 227, 228, 229, 230, 556, 557, 558, 559, 560}
		if got := closureIDs(fb); !slices.Equal(got, want) {
			t.Fatalf("range closure ids = %v, want %v", got, want)
		}
	})

	t.Run("full receive window", func(t *testing.T) {
		fb := wire.Feedback{DecodedLowEdge: 10_000, Deficit: slidingMaxWin, Missing: ^uint64(0)}
		masks := make([]uint64, slidingMaxWin/closureWordBits-1)
		for i := range masks {
			masks[i] = ^uint64(0)
		}
		setClosureExtensions(&fb, masks)
		if _, ok := closureRanges(fb); !ok {
			t.Fatal("full-window closure did not select range encoding")
		}
		ids := closureIDs(fb)
		if len(ids) != slidingMaxWin || ids[0] != fb.DecodedLowEdge ||
			ids[len(ids)-1] != fb.DecodedLowEdge+slidingMaxWin-1 {
			t.Fatalf("full-window closure = len %d, [%d,%d]", len(ids), ids[0], ids[len(ids)-1])
		}
	})
}

func TestUnitRepairAnswersExtendedClosure(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 400_000, SlidingReactiveShift: true,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)
	for i := 0; i < 180; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 8
	fb := wire.Feedback{Flow: 1, HighestSeen: 180, DecodedLowEdge: 0, Deficit: 2}
	setClosureExtensions(&fb, []uint64{1 << 6, 1 << 25}) // ids 70 and 153
	s.FeedFeedback(now, fb)
	var got []uint32
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.UnitRepair {
			got = append(got, sym.SrcIndex)
		}
	}
	if want := []uint32{70, 153}; !slices.Equal(got, want) {
		t.Fatalf("extended unit repairs = %v, want %v", got, want)
	}
}

// TestUnitRepairAnswersMissing pins the sender half: set bits are answered with
// unit repairs (WindowBase=id, N=1, payload = the retained source), an in-flight
// unit is not re-sent within a cycle, and ids outside retention are skipped.
func TestUnitRepairAnswersMissing(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 200_000, SlidingReactiveShift: true,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(0)
	for i := 0; i < 24; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 16 // evidence: the receiver reported wire loss

	fb := wire.Feedback{
		Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 3,
		Missing: 0b1011,
	} // ids 4,5,7 missing
	s.FeedFeedback(now, fb)
	units := map[uint32]int{}
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.UnitRepair {
			units[sym.SrcIndex]++
			if !bytes.Equal(sym.Payload, makeChunkN(sym.SrcIndex)) {
				t.Fatalf("unit repair for id %d carries wrong bytes", sym.SrcIndex)
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
		if sym.Kind == wire.UnitRepair && sym.SrcIndex == 4 {
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
		if sym.Kind == wire.UnitRepair && sym.SrcIndex == 4 {
			t.Fatal("unit repair emitted for an id outside retention")
		}
	}
}

func TestUnitRepairAnswersOnlyRankDeficit(t *testing.T) {
	newSender := func() (*SlidingSender, clock.Timestamp) {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: 200_000, SlidingReactiveShift: true,
		}
		s := NewSlidingSender(cfg)
		now := clock.Timestamp(1)
		for i := 0; i < 24; i++ {
			s.Write(now, makeChunkN(uint32(i)))
			now = now.Add(1_000)
		}
		drainSlidingSymbols(t, s)
		s.wireLossBudget = 16
		return s, now
	}

	// A stale or malformed peer may name more candidates than its Deficit. Keep
	// the sender bounded by the advertised rank gap and prefer the earliest ids;
	// a current receiver reports a closure basis whose bit count already matches.
	s, now := newSender()
	fb := wire.Feedback{
		Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 2,
		Missing: 0b11_1111,
	}
	if got := s.answerMissing(now, fb); got != 2 {
		t.Fatalf("answered rank = %d, want 2", got)
	}
	var ids []uint32
	for _, sym := range drainSlidingSymbols(t, s) {
		if sym.Kind == wire.UnitRepair {
			ids = append(ids, sym.SrcIndex)
		}
	}
	if want := []uint32{4, 5}; !slices.Equal(ids, want) {
		t.Fatalf("unit ids = %v, want earliest blocking ids %v", ids, want)
	}

	// Count an in-flight unit anywhere in the bitmap before choosing another.
	// A single-pass head-first scan would emit id 4 before noticing id 9 was
	// already on the wire, overshooting a one-rank deficit.
	s, now = newSender()
	s.unitSentAt = map[uint32]clock.Timestamp{9: now}
	fb.Deficit = 1
	if got := s.answerMissing(now.Add(1), fb); got != 1 {
		t.Fatalf("in-flight answered rank = %d, want 1", got)
	}
	if extra := len(drainSlidingSymbols(t, s)); extra != 0 {
		t.Fatalf("emitted %d redundant units beside an in-flight rank answer", extra)
	}
}

func TestExactRepairWaitsForPersistentHole(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 200_000,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(1)
	for i := 0; i < 24; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 8
	fb := wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 2, Missing: 0b11}
	if got := s.answerMissing(now, fb); got != 0 {
		t.Fatalf("first missing report emitted %d units", got)
	}
	if units := drainSlidingSymbols(t, s); len(units) != 0 {
		t.Fatalf("first report queued %d symbols", len(units))
	}
	if got := s.answerMissing(now.Add(eventFeedbackMinMicros), fb); got != 2 {
		t.Fatalf("persistent report emitted %d units, want 2", got)
	}
	if st := s.Stats(); st.RepairExact != 2 || st.RepairDeficit < st.RepairExact {
		t.Fatalf("exact-repair attribution = %+v", st)
	}
}

func TestClusteredExactRepairCrossesAtLastUsefulDispatch(t *testing.T) {
	newSender := func(disable bool) (*SlidingSender, clock.Timestamp) {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: 60_000,
		}
		s := NewSlidingSender(cfg)
		s.disableExactCrossover = disable
		s.burstQ8 = burstBandThresholdQ8
		now := clock.Timestamp(1)
		for i := 0; i < 24; i++ {
			s.Write(now, makeChunkN(uint32(i)))
			now = now.Add(1_000)
		}
		drainSlidingSymbols(t, s)
		s.wireLossBudget = 8
		return s, now
	}

	// At the default 50 ms RTT, a 60 ms buffer is shorter than the nominal
	// 72.5 ms reactive cycle. Proactive coding has already had its opportunity by
	// the first residual report, and another report cannot return in time.
	fb := wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 0, Deficit: 1, Missing: 1}
	candidate, now := newSender(false)
	if candidate.reactiveReachable() {
		t.Fatal("test requires a buffer shorter than the nominal reactive cycle")
	}
	if !candidate.exactLastUsefulDispatch(now, 0) {
		t.Fatal("test did not reach the last useful exact dispatch")
	}
	if got := candidate.answerMissing(now, fb); got != 1 {
		t.Fatalf("first clustered exact answers = %d, want 1", got)
	}
	units := drainSlidingSymbols(t, candidate)
	if len(units) != 1 || units[0].Kind != wire.UnitRepair || units[0].SrcIndex != 0 {
		t.Fatalf("clustered crossover emissions = %+v, want unit repair for id 0", units)
	}

	control, now := newSender(true)
	if got := control.answerMissing(now, fb); got != 0 {
		t.Fatalf("persistence-only control answered %d, want 0", got)
	}
	if got := len(drainSlidingSymbols(t, control)); got != 0 {
		t.Fatalf("persistence-only control emitted %d symbols", got)
	}
}

func TestExactRepairCrossoverWaitsWhenAnotherDecisionFits(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 200_000,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(1)
	for i := 0; i < 24; i++ {
		s.Write(now, makeChunkN(uint32(i)))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 8
	fb := wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 0, Deficit: 1, Missing: 1}
	if s.exactLastUsefulDispatch(now, 0) {
		t.Fatal("roomy deadline incorrectly reached exact crossover")
	}
	if got := s.answerMissing(now, fb); got != 0 {
		t.Fatalf("isolated first report answered %d, want coded-first wait", got)
	}
}

func TestExactRepairCrossoverPreservesScarceHeadroom(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: 256, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 60_000, MaxBitrate: 2_800_000,
		RepairWithinBudget: true,
	}
	s := NewSlidingSender(cfg)
	now := clock.Timestamp(1)
	for i := 0; i < 64; i++ {
		s.Write(now, make([]byte, cfg.SymbolSize))
		now = now.Add(1_000)
	}
	drainSlidingSymbols(t, s)
	s.burstQ8 = burstBandThresholdQ8
	s.wireLossBudget = 8
	if got := s.sourceHeadroomRate(); got >= exactCrossoverHeadroomMin {
		t.Fatalf("test requires scarce exact headroom, got %.3f", got)
	}
	fb := wire.Feedback{Flow: 1, HighestSeen: 64, DecodedLowEdge: 4, Deficit: 1, Missing: 1}
	if got := s.answerMissing(now, fb); got != 0 {
		t.Fatalf("scarce-headroom first report answered %d exact units", got)
	}
}

func TestCompactUnitRepairCarriesExactSourceBytes(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
		Redundancy: 0, BufferMicros: 200_000,
	}
	s := NewSlidingSender(cfg)
	r := NewSlidingReceiver(cfg)
	now := clock.Timestamp(1)
	want := []byte{1, 2, 3, 4, 5}
	s.Write(now, want)
	drainSlidingSymbols(t, s)
	s.wireLossBudget = 1
	if !s.emitUnitRepair(now.Add(10_000), 0) {
		t.Fatal("compact unit repair was not emitted")
	}
	d, ok := s.PollSend()
	if !ok {
		t.Fatal("missing compact unit datagram")
	}
	sym, err := wire.DecodeSymbol(d)
	if err != nil {
		t.Fatal(err)
	}
	if sym.Kind != wire.UnitRepair || sym.SourceLength != uint32(len(want)) || !bytes.Equal(sym.Payload, want) {
		t.Fatalf("compact unit = kind %v sourceLen %d payload %v", sym.Kind, sym.SourceLength, sym.Payload)
	}
	r.FeedSymbol(now.Add(20_000), d)
	id, got, ok := r.PollDeliver()
	if !ok || id != 0 || !bytes.Equal(got, want) {
		t.Fatalf("compact unit delivery = id %d payload %v ok=%v", id, got, ok)
	}
}

func TestAutomaticBurstDuplicateIsMeasuredDelayedAndDeadlineBound(t *testing.T) {
	newSender := func(buffer int64) *SlidingSender {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: buffer, MaxBitrate: 10_000_000, RepairWithinBudget: true,
		}
		s := NewSlidingSender(cfg)
		s.fbCount = coldStartFeedbacks
		s.burstQ8 = 24 * 256
		s.interMicros = 1_000
		s.rttMicros = 100_000
		return s
	}

	const start = clock.Timestamp(1_000_000)
	eligible := newSender(100_000)
	eligible.Write(start, []byte{1, 2, 3, 4, 5})
	drainSlidingSymbols(t, eligible)
	if len(eligible.burstDuplicates) != 1 {
		t.Fatalf("queued burst duplicates = %d, want 1", len(eligible.burstDuplicates))
	}

	eligible.Tick(start.Add(23_999))
	for _, sym := range drainSlidingSymbols(t, eligible) {
		if sym.Kind == wire.UnitRepair {
			t.Fatal("burst duplicate emitted before its measured separation")
		}
	}
	eligible.Tick(start.Add(24_000))
	units := 0
	for _, sym := range drainSlidingSymbols(t, eligible) {
		if sym.Kind != wire.UnitRepair {
			continue
		}
		units++
		if sym.SrcIndex != 0 || sym.SourceLength != 5 || !bytes.Equal(sym.Payload, []byte{1, 2, 3, 4, 5}) {
			t.Fatalf("burst duplicate = %+v", sym)
		}
	}
	if units != 1 {
		t.Fatalf("on-time burst duplicates = %d, want 1", units)
	}
	if st := eligible.Stats(); st.RepairBurstDuplicate != 1 || st.RepairProactive < st.RepairBurstDuplicate {
		t.Fatalf("burst-duplicate attribution = %+v", st)
	}

	for _, tc := range []struct {
		name    string
		buffer  int64
		burstQ8 int
	}{
		{name: "cannot-fit", buffer: 75_000, burstQ8: 24 * 256},
		{name: "feedback-fits", buffer: 200_000, burstQ8: 24 * 256},
		{name: "not-bursty", buffer: 100_000, burstQ8: burstQ8One},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSender(tc.buffer)
			s.burstQ8 = tc.burstQ8
			s.Write(start, []byte{1, 2, 3, 4, 5})
			if len(s.burstDuplicates) != 0 {
				t.Fatalf("queued %d ineligible burst duplicates", len(s.burstDuplicates))
			}
		})
	}
}

func TestOutageRunArmsDeadlineBoundDiversity(t *testing.T) {
	newSender := func(disabled bool, buffer int64) *SlidingSender {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: buffer, MaxBitrate: 10_000_000, RepairWithinBudget: true,
		}
		s := NewSlidingSender(cfg)
		s.fbCount = coldStartFeedbacks
		s.burstQ8 = burstQ8One
		s.pEst = 0.10
		s.interMicros = 1_000
		s.rttMicros = 100_000
		s.disableOutageDiversity = disabled
		// This is the white-box timing-lane contract. Keep the block
		// selector from consuming the same proactive credit under test.
		s.disableEpochRepair = true
		return s
	}

	const now = clock.Timestamp(1_000_000)
	for _, tc := range []struct {
		name     string
		feedback wire.Feedback
		disabled bool
		buffer   int64
		reactive bool
		want     bool
	}{
		{name: "classified-outage", feedback: wire.Feedback{OutageRun: 73}, buffer: 100_000, want: true},
		{name: "feedback-fits", feedback: wire.Feedback{OutageRun: 73}, buffer: 200_000, reactive: true},
		{name: "zero-run", feedback: wire.Feedback{}, buffer: 100_000},
		{name: "ab-disabled", feedback: wire.Feedback{OutageRun: 73}, disabled: true, buffer: 100_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSender(tc.disabled, tc.buffer)
			if got := s.reactiveReachable(); got != tc.reactive {
				t.Fatalf("reactive reachability = %v, want %v", got, tc.reactive)
			}
			s.noteOutageRun(now, tc.feedback)
			for id := 0; id < 12; id++ {
				s.Write(now.Add(int64(id)*1_000), []byte{1, 2, 3, 4, 5})
			}
			if got := len(s.outageRepairs); (got > 0) != tc.want {
				t.Fatalf("queued outage-diversity repairs = %d, want activation %v", got, tc.want)
			}
			if !tc.want {
				return
			}
			// The reported 73-symbol fade cannot be retained by a 16-symbol
			// coding window, so the equation is clipped to the latest safe slot.
			p := s.outageRepairs[0]
			if got, want := p.releaseAt, now.Add(int64(p.base)*1_000+15_000); got != want {
				t.Fatalf("release = %d, want deadline/retention-clipped %d", got, want)
			}
			drainSlidingSymbols(t, s)
			s.Tick(p.releaseAt)
			if st := s.Stats(); st.RepairOutageDiversity == 0 || st.RepairProactive < st.RepairOutageDiversity {
				t.Fatalf("outage-diversity attribution = %+v", st)
			}
		})
	}
}

func TestAutomaticARQBulkNeedsClosureSlack(t *testing.T) {
	const missing = uint64(0xff)
	makeSender := func(buffer int64) (*SlidingSender, clock.Timestamp) {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 16,
			Redundancy: 0, BufferMicros: buffer, MaxBitrate: 1_200_000, RepairWithinBudget: true,
		}
		s := NewSlidingSender(cfg)
		now := clock.Timestamp(1)
		for i := 0; i < 24; i++ {
			s.Write(now, makeChunkN(uint32(i)))
			now = now.Add(1_000)
		}
		drainSlidingSymbols(t, s)
		s.wireLossBudget = 16
		return s, now
	}
	fb := wire.Feedback{Flow: 1, HighestSeen: 24, DecodedLowEdge: 4, Deficit: 8, Missing: missing}

	// At the default 50 ms RTT, 100 ms holds one honest cycle but not the
	// persistence plus unit-closure threshold. Bulk exact repair must stay dormant.
	tight, now := makeSender(100_000)
	if got := tight.answerMissing(now, fb); got != 0 {
		t.Fatalf("tight first report emitted %d units", got)
	}
	if got := tight.answerMissing(now.Add(eventFeedbackMinMicros), fb); got != 0 {
		t.Fatalf("tight persistent report emitted %d bulk units", got)
	}

	// With closure slack, proactive FEC has already reached the receiver before
	// feedback exposes the residual, so exact closure starts on that report.
	roomy, now := makeSender(200_000)
	if got := roomy.answerMissing(now, fb); got != 8 {
		t.Fatalf("roomy first report emitted %d units, want 8 (headroom %.3f, source bytes %d, interval %d)",
			got, roomy.sourceHeadroomRate(), roomy.sourceWireBytes, roomy.interMicros)
	}

	// With full-sized sources, compact units cost about as much as an equation.
	// Abundant headroom therefore keeps a burst residual on fungible coded repair.
	roomy.fbCount = coldStartFeedbacks
	roomy.burstQ8 = 24 * burstQ8One
	roomy.sourceWireBytes = 32
	roomy.interMicros = 1_000
	if rate := roomy.sourceHeadroomRate(); rate <= bulkExactHeadroomMax {
		t.Fatalf("test requires abundant headroom, got %.3f", rate)
	}
	if roomy.exactClosureReachable(fb) {
		t.Fatal("bulk closure engaged without a material unit-packet byte advantage")
	}
}

func TestCompactBulkClosureRequiresMaterialByteAdvantage(t *testing.T) {
	newSender := func(withFullSource bool) *SlidingSender {
		t.Helper()
		cfg := Config{
			Flow: 1, SymbolSize: 512, Sliding: true, CodingWindow: 64,
			BufferMicros: 800_000, MaxBitrate: 4_000_000, RepairWithinBudget: true,
		}
		s := NewSlidingSender(cfg)
		for id := 0; id < 16; id++ {
			size := 32
			if withFullSource && id == 10 {
				size = cfg.SymbolSize
			}
			s.Write(clock.Timestamp(id*1_000+1), bytes.Repeat([]byte{0xff}, size))
		}
		drainSlidingSymbols(t, s)
		s.fbCount = coldStartFeedbacks
		s.burstQ8 = 24 * burstQ8One
		s.rttMicros = 200_000
		s.interMicros = 1_000
		return s
	}
	fb := wire.Feedback{Flow: 1, DecodedLowEdge: 0, HighestSeen: 16, Deficit: 5, Missing: 0x1f}
	if newSender(false).exactClosureReachable(fb) {
		t.Fatal("uniform short sources selected named closure without a material equation cost")
	}
	mixed := newSender(true)
	if !mixed.exactClosureReachable(fb) {
		t.Fatal("mixed-width burst residual did not select deficit-bounded compact closure")
	}
}

func TestAutomaticExactOffloadRequiresCyclesAndEconomy(t *testing.T) {
	cfg := Config{
		Flow: 1, SymbolSize: 512, Sliding: true, CodingWindow: 64,
		BufferMicros: 300_000,
	}
	s := NewSlidingSender(cfg)
	s.rttMicros = 100_000 // honest cycle = 135 ms; 300 ms holds two
	s.sourceWireBytes = 200
	now := clock.Timestamp(1_000_000)
	s.noteExactOffload(now, wire.Feedback{Deficit: 32, Missing: ^uint64(0)})
	s.lastFBAt = now
	if !s.exactOffloadOn() {
		t.Fatal("economical exact repair with two cycles and a deep residual did not take ownership")
	}

	s.exactOffloadUntil = 0
	s.noteExactOffload(now, wire.Feedback{Deficit: 24, Missing: (uint64(1) << 24) - 1})
	if s.exactOffloadOn() {
		t.Fatal("exact offload engaged for a moderate residual")
	}
	s.noteExactOffload(now, wire.Feedback{Deficit: 32, Missing: ^uint64(0)})

	s.cfg.BufferMicros = 200_000
	if s.exactOffloadOn() {
		t.Fatal("exact offload engaged with fewer than two complete cycles")
	}
	s.cfg.BufferMicros = 300_000
	s.sourceWireBytes = int64(repairWireBaseBytes+codedSymbolSize(s.cfg.SymbolSize)) / 2
	if s.exactOffloadOn() {
		t.Fatal("exact offload engaged when a unit was not materially cheaper than an equation")
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
	cfg := Config{
		Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32,
		Redundancy: 0.05, TargetFailure: 1e-2, BufferMicros: budget, SlidingReactiveShift: true,
	}
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
