package flow

// Glass-trace replay harness (burst autopsy): reconstructs the EXACT forward
// symbol stream a glassbench probe trace recorded at the impairment relay —
// synthetic source payloads, repair payloads rebuilt from the recorded
// (key, base, n / sparse ids) via the same GenCoeffs schedule — and replays it
// into a bare SlidingReceiver on the trace's own timing (relay timestamp + owd).
// This isolates the RECEIVER CORE from the session/wire/frames-writer stack: per
// lost chunk it reports decode-vs-deadline directly from PollDeliver, so a
// glass-side delivery pathology can be attributed to the core or excluded from it.
//
// Env-gated: MELD_REPLAY_TRACE=<probe_trace.json> go test -run TestReplayGlassTrace -v ./internal/flow

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/gf"
	"github.com/zsiec/meld/internal/wire"
)

type replayEvent struct {
	Dropped        bool     `json:"dropped"`
	RelayTimestamp int64    `json:"relay_timestamp"`
	Kind           string   `json:"kind"`
	WindowBase     uint32   `json:"window_base"`
	SrcIndex       uint32   `json:"src_index"`
	N              uint16   `json:"n"`
	RepairKey      uint16   `json:"repair_key"`
	SparseIDs      []uint32 `json:"sparse_ids"`
	Priority       uint8    `json:"priority"`
	Deadline       int64    `json:"deadline"`
	SendTimestamp  int64    `json:"send_timestamp"`
	HasFrameDesc   bool     `json:"has_frame_desc"`
	FrameStart     uint32   `json:"frame_start"`
	FrameLen       uint16   `json:"frame_len"`
	FrameRAP       bool     `json:"frame_rap"`
	FrameDisc      bool     `json:"frame_discardable"`
	FrameNonPic    bool     `json:"frame_non_picture"`
	FrameRefs      []uint32 `json:"frame_refs"`
}

type replayTrace struct {
	Relay           []replayEvent `json:"relay_events"`
	FeedStartMicros int64         `json:"feed_start_micros"`
	PaceMicros      int64         `json:"pace_micros"`
	BudgetMicros    int64         `json:"budget_micros"`
}

// replayChunk is the deterministic synthetic source payload for seq.
func replayChunk(size int, seq uint32) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(uint32(i)*2654435761 + seq*40503 + 17)
	}
	return b
}

func TestReplayGlassTrace(t *testing.T) {
	path := os.Getenv("MELD_REPLAY_TRACE")
	if path == "" {
		t.Skip("set MELD_REPLAY_TRACE=<probe_trace.json> to replay a glass trace")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var tr replayTrace
	if err := json.Unmarshal(raw, &tr); err != nil {
		t.Fatal(err)
	}
	if len(tr.Relay) == 0 {
		t.Fatal("trace has no relay events")
	}
	const owdMicros = 30_000 // the cell's one-way delay (rtt60)
	symSize := 4 + 1316      // seqHdr + chunk (the bench wire symbol payload size)

	cfg := Config{Flow: 1, SymbolSize: symSize, Sliding: true, CodingWindow: 256,
		Redundancy: 0.15, TargetFailure: 1e-3, BufferMicros: tr.BudgetMicros,
		OutageAware: true, AutoReorderHoldoff: true, RepairWithinBudget: true,
		ProtectedRepairPhasing: true}
	if v := os.Getenv("MELD_REPLAY_NOFRAMES"); v != "" {
		t.Log("frames stripped from replay")
	}
	r := NewSlidingReceiver(cfg)

	// Source values for payload reconstruction.
	src := map[uint32][]byte{}
	getSrc := func(seq uint32) []byte {
		if b, ok := src[seq]; ok {
			return b
		}
		b := replayChunk(symSize, seq)
		src[seq] = b
		return b
	}
	buildRepair := func(e replayEvent) []byte {
		pay := make([]byte, symSize)
		if e.Kind == "sparse_repair" {
			coeffs := code.GenCoeffs(e.RepairKey, len(e.SparseIDs))
			for i, id := range e.SparseIDs {
				gf.MulAdd(pay, getSrc(id), coeffs[i])
			}
			return pay
		}
		n := int(e.N)
		coeffs := code.GenCoeffs(e.RepairKey, n)
		for j := 0; j < n; j++ {
			gf.MulAdd(pay, getSrc(e.WindowBase+uint32(j)), coeffs[j])
		}
		return pay
	}

	stripFrames := os.Getenv("MELD_REPLAY_NOFRAMES") != ""
	t0 := tr.Relay[0].RelayTimestamp
	lost := map[uint32]bool{}
	deliveredAt := map[uint32]int64{} // seq -> core-relative delivery micros
	var maxSeq uint32

	// Replay on a virtual clock: event arrival = (relayTS − t0) + owd; tick every 5ms.
	type inEv struct {
		at  int64
		dat []byte
	}
	var stream []inEv
	for _, e := range tr.Relay {
		at := e.RelayTimestamp - t0 + owdMicros
		if e.Kind == "systematic" {
			seq := e.SrcIndex
			if seq > maxSeq {
				maxSeq = seq
			}
			if e.Dropped {
				lost[seq] = true
				continue
			}
			sym := wire.Symbol{Flow: 1, Kind: wire.Systematic, WindowBase: seq, SrcIndex: seq, N: 1,
				Priority: e.Priority, Deadline: e.Deadline, SendTimestamp: e.SendTimestamp,
				Payload: getSrc(seq)}
			if e.HasFrameDesc && !stripFrames {
				sym.HasFrameDesc = true
				sym.FrameStart, sym.FrameLen = e.FrameStart, e.FrameLen
				sym.FrameRAP, sym.FrameDiscardable, sym.FrameNonPicture = e.FrameRAP, e.FrameDisc, e.FrameNonPic
				sym.FrameRefs = e.FrameRefs
			}
			stream = append(stream, inEv{at, wire.EncodeSymbol(nil, sym)})
			continue
		}
		if e.Dropped {
			continue
		}
		kind := wire.Repair
		if e.Kind == "sparse_repair" {
			kind = wire.SparseRepair
		}
		sym := wire.Symbol{Flow: 1, Kind: kind, WindowBase: e.WindowBase, SrcIndex: e.SrcIndex,
			N: e.N, RepairKey: e.RepairKey, SparseIDs: e.SparseIDs, Priority: e.Priority,
			Deadline: e.Deadline, SendTimestamp: e.SendTimestamp, Payload: buildRepair(e)}
		stream = append(stream, inEv{at, wire.EncodeSymbol(nil, sym)})
	}
	sort.SliceStable(stream, func(i, j int) bool { return stream[i].at < stream[j].at })

	// The wire deadlines are wall-clock (glass): rebase them onto the replay clock so
	// the core's eviction sees the same relative timing. Wall deadline D maps to
	// D − sendWall0 + (arrival-relative epoch)… simplest faithful mapping: the trace's
	// clocks are already consistent (deadline and relay timestamps share the wall
	// clock), so replay time = wall − t0 + owd keeps every relation intact as long as
	// symbols carry deadline − t0 + owd too. Decode, rebase, re-encode was avoided by
	// rebasing at build time above? No — Deadline fields were copied raw. Rebase here.
	for i := range stream {
		sym, err := wire.DecodeSymbol(stream[i].dat)
		if err != nil {
			t.Fatalf("replay self-check: %v", err)
		}
		sym.Deadline = sym.Deadline - t0 + owdMicros
		sym.SendTimestamp = sym.SendTimestamp - t0 + owdMicros
		stream[i].dat = wire.EncodeSymbol(nil, sym)
	}

	// Trigger attribution: for each delivery, the datagram whose ingestion released
	// it (kind + timing) — the instrument that names what finally completes a hole.
	type trig struct {
		kind   string
		at     int64
		base   uint32
		n      uint16
		sentAt int64
	}
	trigOf := map[uint32]trig{}
	lastFed := trig{}
	repairsIngested, repairsInnovative := 0, 0

	// Two replay clocks: virtual (default; 1ms grid, decode CPU is free) and REAL
	// (MELD_REPLAY_REALTIME=1: events fed at wall-clock offsets with 5ms ticks —
	// the session host's cadence — so decode CPU, scheduling, and tick granularity
	// cost what they cost in glass).
	realTime := os.Getenv("MELD_REPLAY_REALTIME") != ""
	idx := 0
	end := stream[len(stream)-1].at + tr.BudgetMicros + 8*owdMicros
	if realTime {
		wall0 := time.Now()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			now := time.Since(wall0).Microseconds()
			if now > end {
				break
			}
			for idx < len(stream) && stream[idx].at <= now {
				r.FeedSymbol(clock.Timestamp(time.Since(wall0).Microseconds()), stream[idx].dat)
				idx++
			}
			r.Tick(clock.Timestamp(time.Since(wall0).Microseconds()))
			for {
				id, _, ok := r.PollDeliver()
				if !ok {
					break
				}
				deliveredAt[id] = time.Since(wall0).Microseconds()
			}
			for {
				if _, ok := r.PollSend(); !ok {
					break
				}
			}
			<-tick.C
		}
	} else {
		now := int64(0)
		for now <= end {
			for idx < len(stream) && stream[idx].at <= now {
				sym, _ := wire.DecodeSymbol(stream[idx].dat)
				k := "systematic"
				if sym.Kind == wire.Repair {
					k = "repair"
				} else if sym.Kind == wire.SparseRepair {
					k = "sparse"
				}
				lastFed = trig{k, now, sym.WindowBase, sym.N, sym.SendTimestamp}
				preRank := r.dec.Rank()
				preCursor := r.dec.Cursor()
				r.FeedSymbol(clock.Timestamp(now), stream[idx].dat)
				if k != "systematic" {
					repairsIngested++
					// rank growth must account for cursor advance (delivered known
					// symbols leave the rank): innovation = Δ(rank) + Δ(cursor).
					if r.dec.Rank()+int(r.dec.Cursor())-preRank-int(preCursor) > 0 {
						repairsInnovative++
					}
				}
				idx++
				for {
					id, _, ok := r.PollDeliver()
					if !ok {
						break
					}
					deliveredAt[id] = now
					trigOf[id] = lastFed
				}
			}
			r.Tick(clock.Timestamp(now))
			for {
				id, _, ok := r.PollDeliver()
				if !ok {
					break
				}
				deliveredAt[id] = now
				trigOf[id] = trig{kind: "tick", at: now}
			}
			for {
				if _, ok := r.PollSend(); !ok {
					break
				}
			}
			now += 1_000
		}
	}

	// Verdicts for relay-dropped (recovered-or-never) chunks, against wire deadlines.
	var ontime, late, never int
	var lateBy []float64
	for seq := range lost {
		dl := tr.FeedStartMicros + int64(seq)*tr.PaceMicros + tr.BudgetMicros - t0 + owdMicros
		at, ok := deliveredAt[seq]
		switch {
		case !ok:
			never++
		case at <= dl:
			ontime++
		default:
			late++
			lateBy = append(lateBy, float64(at-dl)/1000)
		}
	}
	sort.Float64s(lateBy)
	med := 0.0
	if len(lateBy) > 0 {
		med = lateBy[len(lateBy)/2]
	}
	// Per-hole trigger attribution: what kind of ingestion finally released each
	// RELAY-DROPPED chunk, the releasing repair's window geometry, and the release
	// delay vs the chunk's own arrival-schedule time.
	trigCount := map[string]int{}
	var relDelayMs, relN, relEmitLagMs []float64
	for seq := range lost {
		tg, ok := trigOf[seq]
		if !ok {
			continue
		}
		trigCount[tg.kind]++
		if tg.kind == "repair" {
			schedAt := tr.FeedStartMicros + int64(seq)*tr.PaceMicros - t0 + owdMicros
			relDelayMs = append(relDelayMs, float64(tg.at-schedAt)/1000)
			relN = append(relN, float64(tg.n))
			relEmitLagMs = append(relEmitLagMs, float64(tg.at-tg.sentAt-owdMicros)/1000)
		}
	}
	q := func(xs []float64, p float64) float64 {
		if len(xs) == 0 {
			return 0
		}
		sort.Float64s(xs)
		return xs[int(p*float64(len(xs)-1))]
	}
	t.Logf("hole-release triggers: %v | repairs ingested=%d innovative=%d (%.0f%%)",
		trigCount, repairsIngested, repairsInnovative, 100*float64(repairsInnovative)/float64(max(repairsIngested, 1)))
	t.Logf("release delay after chunk's scheduled arrival, ms q25/50/75: %.0f / %.0f / %.0f",
		q(relDelayMs, .25), q(relDelayMs, .5), q(relDelayMs, .75))
	t.Logf("releasing-repair window n q25/50/75: %.0f / %.0f / %.0f", q(relN, .25), q(relN, .5), q(relN, .75))
	t.Logf("releasing repair: (ingest − its own send − owd) ms q25/50/75: %.0f / %.0f / %.0f  (how long the RELEASING equation existed before it was sent... 0 = sent fresh)",
		q(relEmitLagMs, .25), q(relEmitLagMs, .5), q(relEmitLagMs, .75))
	st := r.Stats()
	// Arbitered delivery: EVERY chunk (direct or recovered) judged against its wire
	// deadline — the exact accounting glassbench's -deadlinearbiter applies at the
	// sink, and the honest live-media metric.
	arbOK := 0
	for seq, at := range deliveredAt {
		dl := tr.FeedStartMicros + int64(seq)*tr.PaceMicros + tr.BudgetMicros - t0 + owdMicros
		if at <= dl {
			arbOK++
		}
	}
	t.Logf("replay: lost=%d -> ontime=%d late=%d never=%d (median late %.0fms) | delivered=%d ARBITERED=%d (%.1f%%) recovered=%d rxLost=%d evicted=%d droppedRows=%d",
		len(lost), ontime, late, never, med, st.Delivered, arbOK, 100*float64(arbOK)/float64(maxSeq+1), st.Recovered, st.Lost, st.Evicted, r.dec.DroppedRows())
	fmt.Println() // keep fmt import
}
