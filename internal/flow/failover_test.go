package flow

// Coding-native path failover (the outage detector × the N5 multipath machinery):
// pre-registered in scratchpad/outage-composure/PREREG-multipath-failover.md. A total
// path outage is the ST 2022-7 survival case; round-robin placement otherwise keeps
// feeding the dead path half the source for the outage's whole duration. The receiver
// detects a per-path outage from its aligned-slot walk (consecutive lost slots beyond
// the recovery horizon while other paths deliver), reports it in Feedback.DeadPaths;
// the sender fails systematic placement over to the live set, probes the dead path
// with droppable repair, and restores round-robin when an arrival clears the bit.

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// pathOutageChannel drops every datagram stamped for `path` within an emission-count
// window [from, to) — a total single-path outage over a time span (emission order is
// time order in the sim). Everything else passes.
type pathOutageChannel struct {
	path     uint8
	from, to int
	seen     int
}

func (c *pathOutageChannel) drop(sym wire.Symbol) bool {
	n := c.seen
	c.seen++
	return sym.PathID == c.path && n >= c.from && n < c.to
}

// failoverCell runs the 2-path outage scenario through the full loop at real
// propagation delay with the budget ≈ RTT (reactive repair cannot substitute for
// placement: one cycle exceeds the budget), returning the sim result plus placement
// tallies inside and after the outage window.
func failoverCell(t *testing.T, off bool) (res simResult, rx *Receiver, deadSrcInOutage, liveSrcInOutage, tailOnDead int) {
	t.Helper()
	const (
		n              = 6_000
		owd            = 50_000
		src            = 500 // 2 symbols/ms aggregate; slots complete every 1 ms
		budget         = 100_000
		outFrom, outTo = 2_000, 4_600 // ~800+ ms of emissions with path 0 dead
	)
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: 0.15,
		TargetFailure: 1e-3, BufferMicros: budget, Paths: 2}
	ch := &pathOutageChannel{path: 0, from: outFrom, to: outTo}
	var srcSeen int
	var tailWindow []uint8
	sl := simLink{
		cfg: cfg, owdMicros: owd, srcMicros: src, n: n,
		drop: ch.drop,
		tap: func(sym wire.Symbol, dropped bool) {
			if sym.Kind != wire.Systematic {
				return
			}
			srcSeen++
			if ch.seen > outFrom && ch.seen <= outTo {
				if sym.PathID == 0 {
					deadSrcInOutage++
				} else {
					liveSrcInOutage++
				}
			}
			tailWindow = append(tailWindow, sym.PathID)
			if len(tailWindow) > 400 {
				tailWindow = tailWindow[1:]
			}
		},
	}
	// The white-box off-switch pins pre-failover behavior for the paired baseline arm.
	s := NewSender(cfg)
	if off {
		s.sched.failoverOff = true
	}
	r := NewReceiver(cfg)
	res = sl.runWith(s, r)
	for _, p := range tailWindow {
		if p == 0 {
			tailOnDead++
		}
	}
	return res, r, deadSrcInOutage, liveSrcInOutage, tailOnDead
}

// TestPathFailoverMoney is bar F1 + F3: with failover, a total 800 ms path outage
// costs only the detection window (delivery ≥ 94%), placement moves off the dead path
// after detection, and round-robin resumes on the revived path by stream end; the
// baseline scheduler keeps feeding the dead path and loses ≥ 2× more.
func TestPathFailoverMoney(t *testing.T) {
	t.Parallel()
	res, rx, dead, live, tail := failoverCell(t, false)
	assertCoreInvariants(t, res, res.n, "failover")
	deliv := float64(res.delivered) / float64(res.n) * 100
	t.Logf("failover: deliv %.2f%% lost=%d | src in outage window dead-path=%d live-path=%d | tail dead-path share=%d/400 | recovered=%d",
		deliv, res.stats.Lost, dead, live, tail, res.stats.Recovered)
	if deliv < 94 {
		t.Fatalf("failover delivery %.2f%% below the 94%% bar", deliv)
	}
	// The failover/revival cycle must leave the co-loss machinery ALIVE: the layout
	// kill-switch tripping here means placement never realigned with id%paths after
	// revival (the cursor-parity/catch-up regression class) — a second outage later
	// in the session would go undetected.
	if !rx.mpEnabled {
		t.Fatal("mismatch kill-switch tripped across failover/revival: co-loss and future failover detection are dead")
	}
	if rx.deadPaths != 0 {
		t.Fatalf("deadPaths = %#x at stream end, want 0 (revival did not clear)", rx.deadPaths)
	}
	// Placement failed over: within the outage window the dead path carried only the
	// pre-detection prefix — a small fraction of what round-robin would have sent.
	if rr := (dead + live) / 2; dead > rr/2 {
		t.Fatalf("dead-path source during outage = %d, want well under the round-robin share %d", dead, rr)
	}
	// Revival: by the stream tail the dead path is carrying source again (probes
	// cleared the bit and round-robin resumed).
	if tail < 100 {
		t.Fatalf("revived path carries only %d/400 of the tail source — round-robin did not resume", tail)
	}

	base, _, bDead, bLive, _ := failoverCell(t, true)
	assertCoreInvariants(t, base, base.n, "baseline")
	bDeliv := float64(base.delivered) / float64(base.n) * 100
	t.Logf("baseline:  deliv %.2f%% lost=%d | src in outage window dead-path=%d live-path=%d",
		bDeliv, base.stats.Lost, bDead, bLive)
	if bDeliv > deliv-3 {
		t.Fatalf("failover advantage too small: %.2f%% vs baseline %.2f%%", deliv, bDeliv)
	}
	if bDead*3 < bLive {
		t.Fatalf("baseline arm unexpectedly moved off the dead path (dead=%d live=%d) — the off-switch failed", bDead, bLive)
	}
}

// TestPathFailoverQuietOnLossyPaths is bar F2: an ordinary lossy-but-alive 2-path
// channel (the N5 operating regime) must never trip the dead-path verdict, and
// placement must stay byte-identical to plain round-robin.
func TestPathFailoverQuietOnLossyPaths(t *testing.T) {
	t.Parallel()
	const n = 3_200 // a multiple of GenSize: a partial tail generation would add phantom ids to the accounting check
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, Redundancy: 0.15,
		TargetFailure: 1e-3, BufferMicros: 150_000, Paths: 2}
	var misplaced int
	sl := simLink{
		cfg: cfg, owdMicros: 20_000, srcMicros: 500, n: n,
		drop: uniformDrop(0xFEED, 0.25),
		tap: func(sym wire.Symbol, dropped bool) {
			if sym.Kind == wire.Systematic && uint32(sym.PathID) != sym.SrcIndex%2 {
				misplaced++
			}
		},
	}
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	res := sl.runWith(s, r)
	assertCoreInvariants(t, res, n, "lossy-alive")
	if misplaced != 0 {
		t.Fatalf("%d systematics deviated from round-robin on an alive channel", misplaced)
	}
	if r.deadPaths != 0 {
		t.Fatalf("dead-path verdict %#x on a merely-lossy channel", r.deadPaths)
	}
	if !r.mpEnabled {
		t.Fatal("co-loss estimation disabled on an alive channel")
	}
}

// TestPathSchedulerFailoverPlacement pins the scheduler unit rules: id-derived
// live-only placement under a dead mask, the periodic dead-path repair probe,
// all-dead clamped to none-dead, and the exact id-mod-paths placement restored on
// clear — after a failover of ANY length, odd included (a free-running cursor
// drifted a parity out of phase after an odd-length failover, so every post-revival
// stamp mismatched the receiver's model and permanently tripped the layout
// kill-switch).
func TestPathSchedulerFailoverPlacement(t *testing.T) {
	s := newPathScheduler(3)
	s.setDead(0b010)
	for id := uint32(0); id < 12; id++ {
		if p := s.systematicPath(id); p == 1 {
			t.Fatalf("systematic %d placed on the dead path", id)
		}
	}
	probes, live := 0, 0
	for i := 0; i < 8*probeRepairEvery; i++ {
		if p := s.repairPath(); p == 1 {
			probes++
		} else {
			live++
		}
	}
	if probes != 8 {
		t.Fatalf("dead-path probes = %d over 8 probe windows, want 8", probes)
	}
	// All-dead is clamped: placement keeps running over every path.
	s.setDead(0b111)
	if s.anyDead() {
		t.Fatal("all-dead mask was not clamped to none-dead")
	}
	// Cleared after an ODD-length failover: placement is id%paths immediately, with
	// no residual phase from the failover span.
	s2 := newPathScheduler(2)
	s2.setDead(0b01)
	id := uint32(0)
	for ; id < 7; id++ { // odd failover length
		if p := s2.systematicPath(id); p != 1 {
			t.Fatalf("id %d placed on dead path %d during failover", id, p)
		}
	}
	s2.setDead(0)
	for ; id < 20; id++ {
		if p := s2.systematicPath(id); uint32(p) != id%2 {
			t.Fatalf("post-revival placement id %d → path %d, want id%%2 = %d", id, p, id%2)
		}
	}
}

// TestReceiverPathOutageDetection pins the receiver half in isolation: a path whose
// slots go all-lost beyond the threshold (while the other delivers) is reported dead;
// all-paths-lost slots mark nothing; an arrival on the dead path clears the bit.
func TestReceiverPathOutageDetection(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, BufferMicros: 100_000, Paths: 2}
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	feed := func(id uint32, path uint8) {
		sym := wire.Symbol{Flow: 1, Kind: wire.Systematic, WindowBase: genBaseOf(id, testGen),
			SrcIndex: id, N: testGen, PathID: path,
			Deadline: int64(now.Add(cfg.BufferMicros)), Payload: make([]byte, testSym)}
		r.FeedSymbol(now, wire.EncodeSymbol(nil, sym))
		now = now.Add(500)
	}
	// Prime the horizon inputs and the slot walk with clean alternating delivery.
	var id uint32
	for ; id < 400; id += 2 {
		feed(id, 0)
		feed(id+1, 1)
	}
	// Path 0 dies: only odd ids (path 1) arrive. Keep the per-id cadence identical
	// (advance time for the missing even feed too, or the interval EWMA drifts and
	// the threshold moves under the test). The gap walk reveals the path-0 losses.
	th := r.pathOutageSlotThreshold()
	fed := 0
	for ; fed < 4*th && r.deadPaths == 0; fed++ {
		now = now.Add(500) // the dead path's unsent slot position
		feed(id+1, 1)
		id += 2
	}
	if r.deadPaths != 0b01 {
		t.Fatalf("deadPaths = %#x after %d one-sided slots (initial threshold %d), want 0b01", r.deadPaths, fed, th)
	}
	if fed > 2*th {
		t.Fatalf("verdict took %d one-sided slots, more than 2x the initial threshold %d", fed, th)
	}
	// An arrival on path 0 (the sender's probe) clears the verdict instantly.
	feed(id, 0)
	if r.deadPaths != 0 {
		t.Fatalf("deadPaths = %#x after a path-0 arrival, want 0", r.deadPaths)
	}
	if !r.mpEnabled {
		t.Fatal("mismatch kill-switch tripped during failover/revival")
	}
}

// TestDeadPathProbeBackstopWithoutRepairVolume pins the revival-probe backstop:
// with a path dead and a sender emitting ZERO ordinary repair (Redundancy 0, no
// measured loss on the survivor — the regime the clean-link floor decay also
// reaches under DefaultConfig), dedicated probe repairs must still flow to the dead
// path at the probeGenEvery cadence, or a physically-recovered path could never be
// observed (the receiver clears the dead bit only on an arrival stamped with that
// path) and would stay latched dead for the rest of the session.
func TestDeadPathProbeBackstopWithoutRepairVolume(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: 8, Redundancy: 0, BufferMicros: 100_000, Paths: 2}
	s := NewSender(cfg)
	s.FeedFeedback(clock.Timestamp(1), wire.Feedback{Flow: 1, DeadPaths: 0b01})
	now := clock.Timestamp(1_000)
	const gens = 8
	probes, otherRepair, deadSrc := 0, 0, 0
	for i := 0; i < gens*int(cfg.GenSize); i++ {
		s.WriteUnit(now, makeChunkN(uint32(i)), 0)
		now = now.Add(1_000)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				t.Fatalf("DecodeSymbol: %v", err)
			}
			switch {
			case sym.Kind == wire.Repair && sym.PathID == 0:
				probes++
			case sym.Kind == wire.Repair:
				otherRepair++
			case sym.Kind == wire.Systematic && sym.PathID == 0:
				deadSrc++
			}
		}
	}
	if deadSrc != 0 {
		t.Fatalf("%d systematics placed on the dead path", deadSrc)
	}
	if want := gens / probeGenEvery; probes < want {
		t.Fatalf("probe repairs = %d over %d closes, want >= %d (the backstop cadence): a zero-repair sender never revives a dead path", probes, gens, want)
	}
	if otherRepair != 0 {
		t.Fatalf("%d non-probe repairs emitted at Redundancy 0 with no loss — the backstop should be the only repair", otherRepair)
	}
}

// TestReceiverAllPathsLostIsNotFailover: a total (all-path) outage must not mark any
// path dead — that is the aggregate outage regime, and failing over needs somewhere
// to fail over TO.
func TestReceiverAllPathsLostIsNotFailover(t *testing.T) {
	cfg := Config{Flow: 1, SymbolSize: testSym, GenSize: testGen, BufferMicros: 100_000, Paths: 2}
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	feed := func(id uint32, path uint8) {
		sym := wire.Symbol{Flow: 1, Kind: wire.Systematic, WindowBase: genBaseOf(id, testGen),
			SrcIndex: id, N: testGen, PathID: path,
			Deadline: int64(now.Add(cfg.BufferMicros)), Payload: make([]byte, testSym)}
		r.FeedSymbol(now, wire.EncodeSymbol(nil, sym))
		now = now.Add(500)
	}
	var id uint32
	for ; id < 400; id += 2 {
		feed(id, 0)
		feed(id+1, 1)
	}
	// Both paths dark for far beyond the threshold, then both resume.
	id += 4_000
	for k := 0; k < 40; k++ {
		feed(id, 0)
		feed(id+1, 1)
		id += 2
	}
	if r.deadPaths != 0 {
		t.Fatalf("deadPaths = %#x after an ALL-paths outage, want 0", r.deadPaths)
	}
}

// runWith is simLink.run with caller-constructed halves, so a test can reach white-box
// state (the scheduler off-switch, receiver verdicts) while reusing the sim loop.
func (sl simLink) runWith(s *Sender, r *Receiver) simResult {
	return sl.runCores(s, r)
}
