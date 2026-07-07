package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

type reportCase struct {
	Name                string  `json:"name"`
	Clip                string  `json:"clip"`
	Loss                float64 `json:"loss"`
	GEBurst             float64 `json:"ge_burst_packets"`
	RTTMs               int     `json:"rtt_ms"`
	RTTMult             int     `json:"rtt_mult"`
	BufferMs            int     `json:"buffer_ms"`
	BudgetMs            int     `json:"budget_ms"`
	BitrateMbps         float64 `json:"bitrate_mbps"`
	MaxMbps             float64 `json:"max_mbps"`
	ChunkSize           int     `json:"chunk_size"`
	Reps                int     `json:"reps"`
	Arms                string  `json:"arms"`
	SldWindow           int     `json:"sliding_window"`
	AutoEncoderCadence  bool    `json:"auto_encoder_cadence,omitempty"`
	AutoEncoderInterval int     `json:"auto_encoder_interval,omitempty"`
	AutoEncoderByteCap  float64 `json:"auto_encoder_byte_cap,omitempty"`
	AutoEncoderPSNRMin  float64 `json:"auto_encoder_psnr_min,omitempty"`
	JitterMs            int     `json:"jitter_ms"`
	SourceConstrained   bool    `json:"source_constrained"`
	DropDisposable      bool    `json:"drop_disposable_pictures"`
}

type benchReport struct {
	dir      string
	cas      reportCase
	results  []reportResult
	seeds    []reportSeed
	failures []failureReportRow
}

const (
	traceIslandRAPAnchor       = "rap_anchor"
	traceIslandFirstBaseChain  = "first_base_chain"
	traceIslandLaterBaseRefs   = "later_base_refs"
	traceIslandRecoveryRefresh = "recovery_refresh"
	traceIslandGenericRepair   = "generic_repair"
	traceIslandDisposable      = "disposable_source"
	traceIslandOtherSource     = "other_source"
)

type reportResult struct {
	Case         string
	Arm          string
	FFMean       float64
	FramePctMean float64
	KeyPctMean   float64
	RepairMean   float64
	ReactiveMean float64
	Failed       int
	Seeds        int
}

type reportSeed struct {
	Case                string
	Arm                 string
	Rep                 int
	Seed                int64
	FF                  int
	FramePct            float64
	KeyPct              float64
	Chunks              int
	TotalChunks         int
	TxSource            uint64
	TxRepair            uint64
	TxReactive          uint64
	TxThrottled         uint64
	RxDelivered         uint64
	RxRecovered         uint64
	RxLost              uint64
	RxEvicted           uint64
	RelayEnq            int64
	RelaySent           int64
	FirstMissingUnit    int64
	FirstMissingPicture int64
	FirstMissingKey     int64
	FirstBrokenUnit     int64
	FirstBrokenRef      int64
	TraceFile           string
}

type failureAttribution struct {
	Kind                string `json:"kind"`
	Cause               string `json:"cause"`
	UnitID              int64  `json:"unit_id"`
	SeqStart            int64  `json:"seq_start"`
	Island              string `json:"island"`
	DependencyRef       int64  `json:"dependency_ref"`
	TargetChunks        int    `json:"target_chunks"`
	MissingChunks       int    `json:"missing_chunks"`
	RepairCovering      int    `json:"repair_covering"`
	RepairInTime        int    `json:"repair_in_time"`
	RepairDropped       int    `json:"repair_dropped"`
	RepairSurvived      int    `json:"repair_survived"`
	RepairErased        bool   `json:"repair_erased"`
	SourceDependency    bool   `json:"source_dependency"`
	DependencyDelivered bool   `json:"dependency_delivered"`
	DependencyDecodable bool   `json:"dependency_decodable"`
}

type failureReportRow struct {
	Case     string
	Arm      string
	Rep      int
	Seed     int64
	FF       int
	FramePct float64
	KeyPct   float64
	Trace    string
	Failure  failureAttribution
}

type seedTrace struct {
	mu sync.Mutex `json:"-"`

	Case        reportCase         `json:"case"`
	Arm         string             `json:"arm"`
	Rep         int                `json:"rep"`
	Seed        int64              `json:"seed"`
	Score       seedTraceScore     `json:"score"`
	Stats       seedTraceStats     `json:"stats"`
	Missing     missingSummary     `json:"missing_summary"`
	Failure     failureAttribution `json:"failure_attribution"`
	Source      []sourceTrace      `json:"source_timeline"`
	MissingRuns []seqRun           `json:"missing_runs"`
	Relay       []relayTraceEvent  `json:"relay_events,omitempty"`
	// Feedback and Arrivals are the burst-autopsy streams (2026-07): every reverse-
	// path feedback report the relay carried, decoded (the bench runs cleartext), and
	// the sink's first-arrival time per chunk seq — together with the forward relay
	// events they reconstruct each GE burst's full recovery timeline: what was lost,
	// what the receiver reported, what repair answered, and when each chunk landed
	// against its deadline.
	Feedback []feedbackTraceEvent `json:"feedback_events,omitempty"`
	Arrivals []arrivalTraceEvent  `json:"arrivals,omitempty"`
	// FeedStartMicros anchors the per-chunk send schedule (chunk seq s left the source
	// at FeedStartMicros + s×PaceMicros; deadline = +BudgetMicros more).
	FeedStartMicros int64    `json:"feed_start_micros,omitempty"`
	PaceMicros      int64    `json:"pace_micros,omitempty"`
	BudgetMicros    int64    `json:"budget_micros,omitempty"`
	Notes           []string `json:"notes,omitempty"`
}

// feedbackTraceEvent is one decoded reverse-path feedback report seen at the relay.
type feedbackTraceEvent struct {
	RelayTimestamp     int64   `json:"relay_timestamp"`
	HighestSeen        uint32  `json:"highest_seen"`
	DecodedLowEdge     uint32  `json:"decoded_low_edge"`
	Deficit            uint16  `json:"deficit"`
	Deficits           []uint8 `json:"deficits,omitempty"`
	LossRate           uint16  `json:"loss_rate"`
	Burstiness         uint16  `json:"burstiness"`
	CongestionLoss     uint16  `json:"congestion_loss"`
	NewestDecodableLTR uint32  `json:"newest_decodable_ltr,omitempty"`
	BrokenAnchors      uint16  `json:"broken_anchors,omitempty"`
	Missing            uint64  `json:"missing,omitempty"`
	SettledLost        uint16  `json:"settled_lost,omitempty"`
	HasSettled         bool    `json:"has_settled,omitempty"`
}

// arrivalTraceEvent is one chunk's FIRST arrival at the receiver-side sink.
type arrivalTraceEvent struct {
	Seq      uint32 `json:"seq"`
	AtMicros int64  `json:"at_micros"`
}

type seedTraceScore struct {
	FFFrames int     `json:"ff_frames"`
	FramePct float64 `json:"frame_pct"`
	KeyPct   float64 `json:"key_pct"`
}

type seedTraceStats struct {
	Chunks      int    `json:"chunks"`
	TotalChunks int    `json:"total_chunks"`
	TxSource    uint64 `json:"tx_source"`
	TxRepair    uint64 `json:"tx_repair"`
	TxReactive  uint64 `json:"tx_reactive"`
	TxThrottled uint64 `json:"tx_throttled"`
	RxDelivered uint64 `json:"rx_delivered"`
	RxRecovered uint64 `json:"rx_recovered"`
	RxLost      uint64 `json:"rx_lost"`
	RxEvicted   uint64 `json:"rx_evicted"`
	RelayEnq    int64  `json:"relay_enq"`
	RelaySent   int64  `json:"relay_sent"`
}

type sourceTrace struct {
	Seq             uint32 `json:"seq"`
	UnitID          uint32 `json:"unit_id"`
	Picture         bool   `json:"picture"`
	RAP             bool   `json:"rap"`
	RecoveryRefresh bool   `json:"recovery_refresh,omitempty"`
	Discardable     bool   `json:"discardable"`
	TemporalID      uint8  `json:"temporal_id"`
	Priority        uint8  `json:"priority"`
	Class           string `json:"class"`
	Delivered       bool   `json:"delivered"`
}

type seqRun struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	Len   int    `json:"len"`
}

type relayTraceEvent struct {
	Index                int      `json:"index"`
	Dropped              bool     `json:"dropped"`
	DelayMicros          int64    `json:"delay_micros"`
	RelayTimestamp       int64    `json:"relay_timestamp,omitempty"`
	Kind                 string   `json:"kind"`
	WindowBase           uint32   `json:"window_base"`
	SrcIndex             uint32   `json:"src_index"`
	N                    uint16   `json:"n"`
	RepairKey            uint16   `json:"repair_key"`
	SparseIDs            []uint32 `json:"sparse_ids,omitempty"`
	Priority             uint8    `json:"priority"`
	Deadline             int64    `json:"deadline"`
	SendTimestamp        int64    `json:"send_timestamp"`
	HasFrameDesc         bool     `json:"has_frame_desc,omitempty"`
	FrameStart           uint32   `json:"frame_start,omitempty"`
	FrameLen             uint16   `json:"frame_len,omitempty"`
	FrameRAP             bool     `json:"frame_rap,omitempty"`
	FrameRecoveryRefresh bool     `json:"frame_recovery_refresh,omitempty"`
	FrameDiscardable     bool     `json:"frame_discardable,omitempty"`
	FrameNonPicture      bool     `json:"frame_non_picture,omitempty"`
	FrameRefs            []uint32 `json:"frame_refs,omitempty"`
}

func newBenchReport(dir string, cas reportCase) (*benchReport, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &benchReport{dir: dir, cas: cas}, nil
}

func makeCaseName(loss, geburst float64, rtt, mult, jitter int) string {
	lossPct := int(loss*100 + 0.5)
	prefix := "iid"
	if geburst >= 1 {
		prefix = fmt.Sprintf("burst%d", int(geburst+0.5))
	}
	name := fmt.Sprintf("%s_%d_rtt%d_%dx", prefix, lossPct, rtt, mult)
	if jitter > 0 {
		name += fmt.Sprintf("_j%d", jitter)
	}
	return name
}

func (r *benchReport) newTrace(arm string, rep int, seed int64) *seedTrace {
	return &seedTrace{
		Case: r.cas,
		Arm:  arm,
		Rep:  rep,
		Seed: seed,
	}
}

func (t *seedTrace) recordRelay(datagram []byte, dropped bool, delay time.Duration) {
	if t == nil {
		return
	}
	sym, err := wire.DecodeSymbol(datagram)
	if err != nil {
		return
	}
	kind := "systematic"
	switch sym.Kind {
	case wire.Repair:
		kind = "repair"
	case wire.SparseRepair:
		kind = "sparse_repair"
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Relay = append(t.Relay, relayTraceEvent{
		Index:                len(t.Relay),
		Dropped:              dropped,
		DelayMicros:          delay.Microseconds(),
		RelayTimestamp:       time.Now().UnixMicro(),
		Kind:                 kind,
		WindowBase:           sym.WindowBase,
		SrcIndex:             sym.SrcIndex,
		N:                    sym.N,
		RepairKey:            sym.RepairKey,
		SparseIDs:            append([]uint32(nil), sym.SparseIDs...),
		Priority:             sym.Priority,
		Deadline:             sym.Deadline,
		SendTimestamp:        sym.SendTimestamp,
		HasFrameDesc:         sym.HasFrameDesc,
		FrameStart:           sym.FrameStart,
		FrameLen:             sym.FrameLen,
		FrameRAP:             sym.FrameRAP,
		FrameRecoveryRefresh: sym.FrameRecoveryRefresh,
		FrameDiscardable:     sym.FrameDiscardable,
		FrameNonPicture:      sym.FrameNonPicture,
		FrameRefs:            append([]uint32(nil), sym.FrameRefs...),
	})
}

// recordFeedback decodes one reverse-path datagram as a feedback report (non-feedback
// control datagrams are ignored) and appends it to the autopsy stream.
func (t *seedTrace) recordFeedback(datagram []byte) {
	if t == nil {
		return
	}
	typ, err := wire.PeekType(datagram)
	if err != nil || !wire.IsFeedback(typ) {
		return
	}
	fb, err := wire.DecodeFeedback(datagram)
	if err != nil {
		return
	}
	var defs []uint8
	for i := len(fb.Deficits); i > 0; i-- {
		if fb.Deficits[i-1] != 0 {
			defs = append([]uint8(nil), fb.Deficits[:i]...)
			break
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Feedback = append(t.Feedback, feedbackTraceEvent{
		RelayTimestamp:     time.Now().UnixMicro(),
		HighestSeen:        fb.HighestSeen,
		DecodedLowEdge:     fb.DecodedLowEdge,
		Deficit:            fb.Deficit,
		Deficits:           defs,
		LossRate:           fb.LossRate,
		Burstiness:         fb.Burstiness,
		CongestionLoss:     fb.CongestionLoss,
		NewestDecodableLTR: fb.NewestDecodableLTR,
		BrokenAnchors:      fb.BrokenAnchors,
		Missing:            fb.Missing,
		SettledLost:        fb.SettledLost,
		HasSettled:         fb.HasSettled,
	})
}

// recordArrivals folds the sink's first-arrival map and the send-schedule anchor in.
func (t *seedTrace) recordArrivals(at map[uint32]time.Time, feedStart time.Time, paceUs int64, budgetMs int) {
	if t == nil || at == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Arrivals = t.Arrivals[:0]
	for seq, tm := range at {
		t.Arrivals = append(t.Arrivals, arrivalTraceEvent{Seq: seq, AtMicros: tm.UnixMicro()})
	}
	sort.Slice(t.Arrivals, func(i, j int) bool { return t.Arrivals[i].Seq < t.Arrivals[j].Seq })
	t.FeedStartMicros = feedStart.UnixMicro()
	t.PaceMicros = paceUs
	t.BudgetMicros = int64(budgetMs) * 1000
}

func (r *benchReport) addResult(row reportResult) {
	r.results = append(r.results, row)
}

func (r *benchReport) addSeed(c *chunked, arm string, rep int, seed int64, ff int, sc score, seqs map[uint32]bool, m *meldRunResult, tr *seedTrace) error {
	if tr == nil {
		tr = r.newTrace(arm, rep, seed)
	}
	ms := missingSummaryFor(c, seqs)
	row := reportSeed{
		Case:                r.cas.Name,
		Arm:                 arm,
		Rep:                 rep,
		Seed:                seed,
		FF:                  ff,
		FramePct:            sc.frameRate,
		KeyPct:              sc.keyRate,
		Chunks:              len(seqs),
		TotalChunks:         len(c.chunks),
		FirstMissingUnit:    ms.firstMissingUnit,
		FirstMissingPicture: ms.firstMissingPicture,
		FirstMissingKey:     ms.firstMissingKey,
		FirstBrokenUnit:     ms.firstBrokenUnit,
		FirstBrokenRef:      ms.firstBrokenRef,
	}
	if m != nil {
		row.TxSource = m.txStats.Source
		row.TxRepair = m.txStats.Repair
		row.TxReactive = m.txStats.ReactiveRepair
		row.TxThrottled = m.txStats.Throttled
		row.RxDelivered = m.rxStats.Delivered
		row.RxRecovered = m.rxStats.Recovered
		row.RxLost = m.rxStats.Lost
		row.RxEvicted = m.rxStats.Evicted
		row.RelayEnq = m.relayEnq
		row.RelaySent = m.relaySent
	} else {
		row.RelayEnq, row.RelaySent = -1, -1
	}

	tr.Score = seedTraceScore{FFFrames: ff, FramePct: sc.frameRate, KeyPct: sc.keyRate}
	tr.Stats = seedTraceStats{
		Chunks:      row.Chunks,
		TotalChunks: row.TotalChunks,
		TxSource:    row.TxSource,
		TxRepair:    row.TxRepair,
		TxReactive:  row.TxReactive,
		TxThrottled: row.TxThrottled,
		RxDelivered: row.RxDelivered,
		RxRecovered: row.RxRecovered,
		RxLost:      row.RxLost,
		RxEvicted:   row.RxEvicted,
		RelayEnq:    row.RelayEnq,
		RelaySent:   row.RelaySent,
	}
	tr.Missing = ms
	tr.Source = sourceTimeline(c, seqs)
	tr.MissingRuns = missingRuns(c, seqs)
	failure := failureAttributionFor(c, seqs, tr, ms)
	tr.Failure = failure
	if len(tr.Relay) == 0 {
		tr.Notes = append(tr.Notes, "relay_events are available for Meld arms; C-stack arms expose source/delivery traces only")
	}

	traceName := fmt.Sprintf("seed_trace_%s_%s_rep%d_seed%d.json", safeName(r.cas.Name), safeName(arm), rep, seed)
	tracePath := filepath.Join(r.dir, traceName)
	if err := writeJSON(tracePath, tr); err != nil {
		return err
	}
	row.TraceFile = traceName
	r.seeds = append(r.seeds, row)
	r.failures = append(r.failures, failureReportRow{
		Case:     r.cas.Name,
		Arm:      arm,
		Rep:      rep,
		Seed:     seed,
		FF:       ff,
		FramePct: sc.frameRate,
		KeyPct:   sc.keyRate,
		Trace:    traceName,
		Failure:  failure,
	})
	return nil
}

func sourceTimeline(c *chunked, seqs map[uint32]bool) []sourceTrace {
	out := make([]sourceTrace, 0, len(c.chunks))
	for i := range c.chunks {
		seq := uint32(i)
		u, ok := unitForSeq(c, seq)
		if !ok {
			out = append(out, sourceTrace{Seq: seq, Delivered: seqs[seq]})
			continue
		}
		out = append(out, sourceTrace{
			Seq:             seq,
			UnitID:          u.ID,
			Picture:         u.Picture,
			RAP:             u.RAP,
			RecoveryRefresh: u.RecoveryRefresh,
			Discardable:     u.Discardable,
			TemporalID:      u.TemporalID,
			Priority:        u.Class.Wire(),
			Class:           className(u.Class),
			Delivered:       seqs[seq],
		})
	}
	return out
}

func missingRuns(c *chunked, seqs map[uint32]bool) []seqRun {
	out := make([]seqRun, 0)
	open := false
	var start uint32
	for i := range c.chunks {
		seq := uint32(i)
		if !seqs[seq] {
			if !open {
				open, start = true, seq
			}
			continue
		}
		if open {
			out = append(out, seqRun{Start: start, End: seq - 1, Len: int(seq - start)})
			open = false
		}
	}
	if open {
		end := uint32(len(c.chunks) - 1)
		out = append(out, seqRun{Start: start, End: end, Len: int(end-start) + 1})
	}
	return out
}

func failureAttributionFor(c *chunked, seqs map[uint32]bool, tr *seedTrace, ms missingSummary) failureAttribution {
	out := failureAttribution{
		Kind:                "none",
		Cause:               "none",
		UnitID:              -1,
		SeqStart:            -1,
		DependencyRef:       -1,
		DependencyDelivered: true,
		DependencyDecodable: true,
	}
	deliveredUnits := c.deliveredUnits(seqs)
	decodable := shape.Decodable(c.units, deliveredUnits)
	targetUnit := int64(-1)
	switch {
	case ms.firstBrokenRef >= 0:
		out.Kind = "broken_dependency"
		out.DependencyRef = ms.firstBrokenRef
		out.SourceDependency = true
		targetUnit = ms.firstBrokenRef
	case ms.firstMissingPicture >= 0:
		out.Kind = "missing_source"
		targetUnit = ms.firstMissingPicture
	case ms.firstMissingUnit >= 0:
		out.Kind = "missing_source"
		targetUnit = ms.firstMissingUnit
	default:
		return out
	}
	out.UnitID = targetUnit
	if targetUnit < 0 {
		return out
	}
	unitID := uint32(targetUnit)
	chunks := append([]uint32(nil), c.unitChunks[unitID]...)
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	out.TargetChunks = len(chunks)
	if len(chunks) > 0 {
		out.SeqStart = int64(chunks[0])
		out.Island = dependencyIslandForUnit(c, unitID)
	}
	out.DependencyDelivered = deliveredUnits[unitID]
	out.DependencyDecodable = decodable[unitID]
	for _, seq := range chunks {
		if !seqs[seq] {
			out.MissingChunks++
		}
	}
	deadline := targetDeadline(tr, chunks)
	for _, ev := range relayEvents(tr) {
		if !repairCoversAny(ev, chunks) {
			continue
		}
		out.RepairCovering++
		if !repairInTime(ev, deadline) {
			continue
		}
		out.RepairInTime++
		if ev.Dropped {
			out.RepairDropped++
		} else {
			out.RepairSurvived++
		}
	}
	out.RepairErased = out.RepairInTime > 0 && out.RepairSurvived == 0
	out.Cause = failureCause(out)
	return out
}

func failureCause(f failureAttribution) string {
	switch {
	case f.Kind == "none":
		return "none"
	case f.SourceDependency:
		return "source_dependency"
	case f.RepairErased:
		return "repair_erased"
	case f.RepairInTime > 0 && f.RepairSurvived > 0:
		return "repair_present_insufficient"
	case f.RepairCovering > 0:
		return "repair_late_or_not_visible"
	case f.MissingChunks > 0:
		return "source_loss_no_repair"
	default:
		return "unknown"
	}
}

func relayEvents(tr *seedTrace) []relayTraceEvent {
	if tr == nil {
		return nil
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]relayTraceEvent(nil), tr.Relay...)
}

func targetDeadline(tr *seedTrace, chunks []uint32) int64 {
	if tr == nil || len(chunks) == 0 {
		return 0
	}
	want := make(map[uint32]bool, len(chunks))
	for _, seq := range chunks {
		want[seq] = true
	}
	var deadline int64
	for _, ev := range relayEvents(tr) {
		if ev.Kind != "systematic" || !want[ev.SrcIndex] || ev.Deadline == 0 {
			continue
		}
		if deadline == 0 || ev.Deadline < deadline {
			deadline = ev.Deadline
		}
	}
	return deadline
}

func repairCoversAny(ev relayTraceEvent, chunks []uint32) bool {
	if len(chunks) == 0 {
		return false
	}
	switch ev.Kind {
	case "repair":
		if ev.N == 0 {
			return false
		}
		start := ev.WindowBase
		end := uint64(start) + uint64(ev.N)
		for _, seq := range chunks {
			if seq >= start && uint64(seq) < end {
				return true
			}
		}
	case "sparse_repair":
		want := make(map[uint32]bool, len(chunks))
		for _, seq := range chunks {
			want[seq] = true
		}
		for _, id := range ev.SparseIDs {
			if want[id] {
				return true
			}
		}
	}
	return false
}

func repairInTime(ev relayTraceEvent, deadline int64) bool {
	if deadline == 0 {
		return true
	}
	t := ev.RelayTimestamp
	if t == 0 {
		t = ev.SendTimestamp
	}
	if t == 0 {
		return true
	}
	return t+ev.DelayMicros <= deadline
}

func dependencyIslandForUnit(c *chunked, unitID uint32) string {
	firstBase := firstBaseUnits(c, 6)
	for _, u := range c.units {
		if u.ID != unitID {
			continue
		}
		switch {
		case u.RecoveryRefresh:
			return traceIslandRecoveryRefresh
		case u.RAP || u.Class >= shape.ClassRAP:
			return traceIslandRAPAnchor
		case firstBase[u.ID]:
			return traceIslandFirstBaseChain
		case u.Class == shape.ClassBase && !u.Discardable:
			return traceIslandLaterBaseRefs
		case u.Discardable || u.Class <= shape.ClassEnhancement:
			return traceIslandDisposable
		default:
			return traceIslandOtherSource
		}
	}
	return traceIslandOtherSource
}

func firstBaseUnits(c *chunked, max int) map[uint32]bool {
	out := map[uint32]bool{}
	afterRAP := false
	baseCount := 0
	for _, u := range c.units {
		if u.RAP || u.Class >= shape.ClassRAP {
			afterRAP = true
			baseCount = 0
		}
		if afterRAP && u.Class == shape.ClassBase && !u.Discardable && baseCount < max {
			out[u.ID] = true
			baseCount++
		}
	}
	return out
}

func unitForSeq(c *chunked, seq uint32) (shape.Unit, bool) {
	for id, seqs := range c.unitChunks {
		for _, s := range seqs {
			if s == seq {
				for _, u := range c.units {
					if u.ID == id {
						return u, true
					}
				}
			}
		}
	}
	return shape.Unit{}, false
}

func className(c shape.PriorityClass) string {
	switch c {
	case shape.ClassDisposable:
		return "disposable"
	case shape.ClassEnhancement:
		return "enhancement"
	case shape.ClassBase:
		return "base"
	case shape.ClassRAP:
		return "rap"
	case shape.ClassParamSet:
		return "param_set"
	default:
		return strconv.Itoa(int(c))
	}
}

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func (r *benchReport) write() error {
	sort.Slice(r.results, func(i, j int) bool {
		if r.results[i].Case != r.results[j].Case {
			return r.results[i].Case < r.results[j].Case
		}
		return r.results[i].Arm < r.results[j].Arm
	})
	sort.Slice(r.seeds, func(i, j int) bool {
		if r.seeds[i].Case != r.seeds[j].Case {
			return r.seeds[i].Case < r.seeds[j].Case
		}
		if r.seeds[i].Arm != r.seeds[j].Arm {
			return r.seeds[i].Arm < r.seeds[j].Arm
		}
		return r.seeds[i].Rep < r.seeds[j].Rep
	})
	if err := r.writeResultsCSV(); err != nil {
		return err
	}
	if err := r.writeSeedsCSV(); err != nil {
		return err
	}
	if err := writeFailureReportCSV(filepath.Join(r.dir, "failure_report.csv"), r.failures); err != nil {
		return err
	}
	if err := writeFailureReportMD(filepath.Join(r.dir, "failure_report.md"), r.failures, 24); err != nil {
		return err
	}
	if err := r.writeMatrixMD(); err != nil {
		return err
	}
	return r.writeSummaryMD()
}

func (r *benchReport) writeResultsCSV() error {
	f, err := os.Create(filepath.Join(r.dir, "results.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"case", "arm", "ff_mean", "frame_pct_mean", "key_pct_mean", "repair_mean", "reactive_mean", "failed", "seeds"})
	for _, row := range r.results {
		w.Write([]string{
			row.Case, row.Arm,
			fmt.Sprintf("%.3f", row.FFMean),
			fmt.Sprintf("%.6f", row.FramePctMean),
			fmt.Sprintf("%.6f", row.KeyPctMean),
			fmt.Sprintf("%.3f", row.RepairMean),
			fmt.Sprintf("%.3f", row.ReactiveMean),
			strconv.Itoa(row.Failed),
			strconv.Itoa(row.Seeds),
		})
	}
	return w.Error()
}

func (r *benchReport) writeSeedsCSV() error {
	f, err := os.Create(filepath.Join(r.dir, "per_seed.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"case", "arm", "rep", "seed", "ff", "frame_pct", "key_pct", "chunks", "total_chunks", "tx_source", "tx_repair", "tx_reactive", "tx_throttled", "rx_delivered", "rx_recovered", "rx_lost", "rx_evicted", "relay_enq", "relay_sent", "first_missing_unit", "first_missing_picture", "first_missing_key", "first_broken_unit", "first_broken_ref", "trace_file"})
	for _, s := range r.seeds {
		w.Write([]string{
			s.Case, s.Arm, strconv.Itoa(s.Rep), strconv.FormatInt(s.Seed, 10),
			strconv.Itoa(s.FF),
			fmt.Sprintf("%.6f", s.FramePct),
			fmt.Sprintf("%.6f", s.KeyPct),
			strconv.Itoa(s.Chunks), strconv.Itoa(s.TotalChunks),
			strconv.FormatUint(s.TxSource, 10), strconv.FormatUint(s.TxRepair, 10),
			strconv.FormatUint(s.TxReactive, 10), strconv.FormatUint(s.TxThrottled, 10),
			strconv.FormatUint(s.RxDelivered, 10), strconv.FormatUint(s.RxRecovered, 10),
			strconv.FormatUint(s.RxLost, 10), strconv.FormatUint(s.RxEvicted, 10),
			strconv.FormatInt(s.RelayEnq, 10), strconv.FormatInt(s.RelaySent, 10),
			strconv.FormatInt(s.FirstMissingUnit, 10), strconv.FormatInt(s.FirstMissingPicture, 10),
			strconv.FormatInt(s.FirstMissingKey, 10), strconv.FormatInt(s.FirstBrokenUnit, 10),
			strconv.FormatInt(s.FirstBrokenRef, 10), s.TraceFile,
		})
	}
	return w.Error()
}

func writeFailureReportCSV(path string, rows []failureReportRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{
		"case", "arm", "rep", "seed", "ff", "frame_pct", "key_pct",
		"failure_kind", "failure_cause", "failure_unit", "failure_seq_start", "dependency_island", "dependency_ref",
		"target_chunks", "missing_chunks", "repair_covering", "repair_in_time", "repair_dropped", "repair_survived",
		"repair_erased", "source_dependency", "dependency_delivered", "dependency_decodable", "trace_file",
	})
	for _, row := range rows {
		fa := row.Failure
		w.Write([]string{
			row.Case, row.Arm, strconv.Itoa(row.Rep), strconv.FormatInt(row.Seed, 10),
			strconv.Itoa(row.FF), fmt.Sprintf("%.6f", row.FramePct), fmt.Sprintf("%.6f", row.KeyPct),
			fa.Kind, fa.Cause, strconv.FormatInt(fa.UnitID, 10), strconv.FormatInt(fa.SeqStart, 10), fa.Island, strconv.FormatInt(fa.DependencyRef, 10),
			strconv.Itoa(fa.TargetChunks), strconv.Itoa(fa.MissingChunks), strconv.Itoa(fa.RepairCovering), strconv.Itoa(fa.RepairInTime),
			strconv.Itoa(fa.RepairDropped), strconv.Itoa(fa.RepairSurvived), strconv.FormatBool(fa.RepairErased),
			strconv.FormatBool(fa.SourceDependency), strconv.FormatBool(fa.DependencyDelivered), strconv.FormatBool(fa.DependencyDecodable),
			row.Trace,
		})
	}
	return w.Error()
}

func writeFailureReportMD(path string, rows []failureReportRow, limit int) error {
	if limit <= 0 {
		limit = len(rows)
	}
	cp := append([]failureReportRow(nil), rows...)
	sort.SliceStable(cp, func(i, j int) bool {
		a, b := cp[i], cp[j]
		if a.Failure.Kind == "none" && b.Failure.Kind != "none" {
			return false
		}
		if a.Failure.Kind != "none" && b.Failure.Kind == "none" {
			return true
		}
		if a.FF != b.FF {
			return a.FF < b.FF
		}
		if a.Case != b.Case {
			return a.Case < b.Case
		}
		if a.Arm != b.Arm {
			return a.Arm < b.Arm
		}
		return a.Rep < b.Rep
	})
	var b strings.Builder
	fmt.Fprintf(&b, "# Per-Seed Failure Report\n\n")
	fmt.Fprintf(&b, "Each row attributes the first decode failure to a dependency island and records whether repair covering that island existed before the source deadline and whether those repair packets were erased by the burst mask.\n\n")
	fmt.Fprintf(&b, "| case | arm | rep | seed | ff | kind | cause | island | unit | missing | repair in time | dropped | survived | source dep | trace |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
	n := 0
	for _, row := range cp {
		if n >= limit {
			break
		}
		fa := row.Failure
		trace := row.Trace
		if trace != "" {
			trace = fmt.Sprintf("[%s](%s)", trace, trace)
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | %d | %d | %d | `%s` | `%s` | `%s` | %d | %d/%d | %d | %d | %d | %t | %s |\n",
			row.Case, row.Arm, row.Rep, row.Seed, row.FF, fa.Kind, fa.Cause, fa.Island, fa.UnitID,
			fa.MissingChunks, fa.TargetChunks, fa.RepairInTime, fa.RepairDropped, fa.RepairSurvived, fa.SourceDependency, trace)
		n++
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func (r *benchReport) writeMatrixMD() error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Glassbench Matrix\n\n")
	fmt.Fprintf(&b, "Case: `%s`\n\n", r.cas.Name)
	fmt.Fprintf(&b, "| arm | ff mean | frame %% | key %% | repair | reactive | failed |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, row := range r.results {
		fmt.Fprintf(&b, "| `%s` | %.1f | %.1f | %.1f | %.1f | %.1f | %d |\n",
			row.Arm, row.FFMean, row.FramePctMean*100, row.KeyPctMean*100, row.RepairMean, row.ReactiveMean, row.Failed)
	}
	fmt.Fprintf(&b, "\n## Worst Seeds\n\n")
	fmt.Fprintf(&b, "| arm | rep | seed | ff | first missing picture | first broken ref | trace |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, seed := range worstSeedByArm(r.seeds) {
		fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %d | %d | [%s](%s) |\n",
			seed.Arm, seed.Rep, seed.Seed, seed.FF, seed.FirstMissingPicture, seed.FirstBrokenRef, seed.TraceFile, seed.TraceFile)
	}
	return os.WriteFile(filepath.Join(r.dir, "matrix.md"), []byte(b.String()), 0o644)
}

func (r *benchReport) writeSummaryMD() error {
	bestMeld, haveMeld := bestResult(r.results, func(a string) bool { return strings.HasPrefix(a, "meld") })
	bestARQ, haveARQ := bestResult(r.results, func(a string) bool { return a == "libsrt" || a == "librist" })
	var b strings.Builder
	fmt.Fprintf(&b, "# Glassbench Report\n\n")
	fmt.Fprintf(&b, "This report is meant to support a Bret-Victor-style ladder workflow: macro result, per-seed spread, then concrete seed traces.\n\n")
	fmt.Fprintf(&b, "- `results.csv`: one row per arm aggregate\n")
	fmt.Fprintf(&b, "- `per_seed.csv`: every seed with first-missing/first-broken fields\n")
	fmt.Fprintf(&b, "- `failure_report.csv` / `failure_report.md`: per-seed dependency-island failure attribution\n")
	fmt.Fprintf(&b, "- `matrix.md`: arm table and worst-seed links\n")
	fmt.Fprintf(&b, "- `seed_trace_*.json`: source timeline, missing runs, and Meld relay source/repair events\n\n")
	if haveMeld && haveARQ {
		delta := bestMeld.FFMean - bestARQ.FFMean
		fmt.Fprintf(&b, "Best Meld: `%s` %.1f ff frames. Best ARQ: `%s` %.1f ff frames. Delta: %.1f.\n\n",
			bestMeld.Arm, bestMeld.FFMean, bestARQ.Arm, bestARQ.FFMean, delta)
		if delta < 0 {
			if seed, ok := worstSeedForArm(r.seeds, bestMeld.Arm); ok {
				fmt.Fprintf(&b, "Bad macro cell: Meld trails ARQ. Start with worst `%s` seed: [%s](%s).\n\n",
					bestMeld.Arm, seed.TraceFile, seed.TraceFile)
			}
		}
	}
	fmt.Fprintf(&b, "## Case\n\n")
	blob, _ := json.MarshalIndent(r.cas, "", "  ")
	fmt.Fprintf(&b, "```json\n%s\n```\n", blob)
	return os.WriteFile(filepath.Join(r.dir, "SUMMARY.md"), []byte(b.String()), 0o644)
}

func bestResult(rows []reportResult, accept func(string) bool) (reportResult, bool) {
	var best reportResult
	ok := false
	for _, r := range rows {
		if !accept(r.Arm) || r.Failed > 0 {
			continue
		}
		if !ok || r.FFMean > best.FFMean || (r.FFMean == best.FFMean && r.FramePctMean > best.FramePctMean) {
			best, ok = r, true
		}
	}
	return best, ok
}

func worstSeedByArm(seeds []reportSeed) []reportSeed {
	by := map[string]reportSeed{}
	for _, s := range seeds {
		cur, ok := by[s.Arm]
		if !ok || s.FF < cur.FF || (s.FF == cur.FF && s.FirstMissingPicture >= 0 && (cur.FirstMissingPicture < 0 || s.FirstMissingPicture < cur.FirstMissingPicture)) {
			by[s.Arm] = s
		}
	}
	out := make([]reportSeed, 0, len(by))
	for _, s := range by {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arm < out[j].Arm })
	return out
}

func worstSeedForArm(seeds []reportSeed, arm string) (reportSeed, bool) {
	var best reportSeed
	ok := false
	for _, s := range seeds {
		if s.Arm != arm {
			continue
		}
		if !ok || s.FF < best.FF {
			best, ok = s, true
		}
	}
	return best, ok
}

func safeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}
