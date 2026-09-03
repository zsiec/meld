package main

import (
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zsiec/meld/internal/shape"
)

// extend replicates a chunked stream k times with fresh sequence and unit IDs.
// Retaining the complete descriptor graph keeps media-aware and media-blind arms
// on the same longer source timeline while giving feedback-driven transports time
// to leave cold start.
func extend(c *chunked, k int) *chunked {
	if k <= 1 {
		return c
	}
	out := &chunked{chunkSize: c.chunkSize, unitChunks: map[uint32][]uint32{}, format: c.format}
	var unitStride uint32
	for _, unit := range c.units {
		if unit.ID >= unitStride {
			unitStride = unit.ID + 1
		}
	}
	var seq uint32
	for rep := 0; rep < k; rep++ {
		unitOffset := uint32(rep) * unitStride
		for _, shaped := range c.shaped {
			copyShaped := shaped
			copyShaped.Unit.ID += unitOffset
			copyShaped.Unit.RefersTo = append([]uint32(nil), shaped.Unit.RefersTo...)
			for i := range copyShaped.Unit.RefersTo {
				copyShaped.Unit.RefersTo[i] += unitOffset
			}
			out.shaped = append(out.shaped, copyShaped)
			out.units = append(out.units, copyShaped.Unit)
			for _, originalSeq := range c.unitChunks[shaped.Unit.ID] {
				out.unitChunks[copyShaped.Unit.ID] = append(out.unitChunks[copyShaped.Unit.ID],
					uint32(rep*len(c.chunks))+originalSeq)
			}
		}
		for _, pkt := range c.chunks {
			np := make([]byte, len(pkt))
			copy(np, pkt)
			binary.BigEndian.PutUint32(np[:seqHdr], seq)
			out.chunks = append(out.chunks, np)
			seq++
		}
	}
	return out
}

// parseRTTs parses a comma list of RTTs in ms.
func parseRTTs(s string) []int {
	var out []int
	for _, f := range strings.Split(s, ",") {
		if v, err := strconv.Atoi(strings.TrimSpace(f)); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

func parseFloatList(s string) []float64 {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		if v, err := strconv.ParseFloat(strings.TrimSpace(f), 64); err == nil && v >= 0 {
			out = append(out, v)
		}
	}
	return out
}

// benchmarkSeed gives every benchmark mode the same deterministic seed series.
// Keeping probe, bisection, direct, and publication runs aligned makes a seed
// trace reproducible across tools instead of silently sampling a different
// impairment ensemble.
func benchmarkSeed(rep int) int64 {
	return int64(rep)*7919 + 13
}

func benchmarkSeeds(reps int) []int64 {
	seeds := make([]int64, 0, max(reps, 0))
	for rep := 1; rep <= reps; rep++ {
		seeds = append(seeds, benchmarkSeed(rep))
	}
	return seeds
}

// sweepSupported reports whether the sweep can drive this arm.
func sweepSupported(arm string) bool {
	if isMeldArm(arm) {
		return true
	}
	switch arm {
	case "libsrt", "libsrt-fec", "librist", "oracle-source", "oracle-ideal":
		return true
	}
	return false
}

// sweepArm runs one transport at (loss, rtt, budget) and returns the delivery
// fraction (delivered chunks / total). Meld runs in its default AutoGenSize,
// media-blind config — the deployable "auto" setup, not a hand-tuned one.
func sweepArm(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax, seed int64) float64 {
	var got map[uint32]bool
	if isMeldArm(arm) {
		// Burst autopsy (GLASSTRACE=dir): capture the full relay/feedback/arrival
		// trace for meld arms in probe mode — the hole regime needs LONG streams
		// (streamk-extended), which only the probe path runs; the report machinery's
		// traces exist only in the ffprobe mode.
		if dir := os.Getenv("GLASSTRACE"); dir != "" {
			tr := &seedTrace{Arm: arm, Seed: seed}
			got = runMeldNamedTrace(c, arm, loss, rtt, budget, paceUs, meldMax, seed, tr).got
			writeStandaloneTrace(dir, arm, rtt, budget, seed, tr)
		} else {
			got = runMeldNamed(c, arm, loss, rtt, budget, paceUs, meldMax, seed).got
		}
	} else {
		switch arm {
		case "libsrt":
			got = runLibsrt(c, loss, rtt, budget, paceUs, seed, "")
		case "libsrt-fec":
			got = runLibsrt(c, loss, rtt, budget, paceUs, seed, "fec,cols:10,rows:5,arq:onreq")
		case "librist":
			got = runLibrist(c, loss, rtt, budget, paceUs, seed)
		default:
			return math.NaN()
		}
	}
	if len(c.chunks) == 0 || got == nil {
		return math.NaN()
	}
	return float64(len(got)) / float64(len(c.chunks))
}

// writeStandaloneTrace dumps one probe-mode seed trace as JSON into dir (burst
// autopsy; best-effort — a write error is reported on stderr, never fatal).
func writeStandaloneTrace(dir, arm string, rtt, budget int, seed int64, tr *seedTrace) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "glasstrace:", err)
		return
	}
	name := fmt.Sprintf("probe_trace_%s_rtt%d_bud%d_seed%d.json", strings.ReplaceAll(arm, "-", "_"), rtt, budget, seed)
	b, err := json.MarshalIndent(tr, "", " ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "glasstrace:", err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "glasstrace:", err)
	}
}

// runProbe measures delivery at a FIXED budget (atXRTT × RTT) per arm/RTT and
// prints the full per-seed spread (min/median/max). Unlike the bisection sweep it
// makes no pass/fail decision, so it gives a clean, noise-honest head-to-head —
// the right tool for "is coder A better than B at this operating point."
func runProbe(c *chunked, loss float64, paceUs, meldMax int64, mbps float64, rtts []int, reps int, atXRTT float64, streamK int, arms []string) {
	cl := extend(c, streamK)
	total := len(cl.chunks)
	mode := "i.i.d."
	if geBurstPkts >= 1 {
		mode = fmt.Sprintf("GE burst (mean %.0f pkt ≈ %.0f ms)", geBurstPkts,
			geBurstPkts*float64((cl.chunkSize+seqHdr)*8)/(mbps*1e6)*1e3)
	}
	fmt.Printf("# DELIVERY PROBE @ %.2f×RTT — loss %.0f%% %s, %d chunks/run (%.0f Mbps), %d seeds\n",
		atXRTT, loss*100, mode, total, mbps, reps)
	for _, arm := range arms {
		if !sweepSupported(arm) {
			continue
		}
		fmt.Printf("\n## %s\n", arm)
		fmt.Printf("# %-8s %-10s %-30s\n", "RTT(ms)", "budget", "delivery min/median/max")
		for _, rtt := range rtts {
			budget := int(atXRTT*float64(rtt) + 0.5)
			ds := make([]float64, 0, reps)
			failed := false
			for s := 1; s <= reps; s++ {
				d := sweepArm(arm, cl, loss, rtt, budget, paceUs, meldMax, benchmarkSeed(s))
				if math.IsNaN(d) {
					failed = true
					break
				}
				ds = append(ds, d)
			}
			if failed || len(ds) == 0 {
				fmt.Printf("  %-8d %-10d FAILED\n", rtt, budget)
				continue
			}
			sort.Float64s(ds)
			fmt.Printf("  %-8d %-10d %.3f%% / %.3f%% / %.3f%%\n",
				rtt, budget, ds[0]*100, ds[reps/2]*100, ds[reps-1]*100)
		}
	}
}

// measureDeliv runs `reps` seeds at (loss, rtt, budget) and returns a conservative
// low quantile of the per-seed delivery fractions (drops the single worst run,
// usually a subprocess flake rather than transport behavior).
func measureDeliv(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax int64, reps int) float64 {
	ds := make([]float64, 0, reps)
	for s := 1; s <= reps; s++ {
		d := sweepArm(arm, c, loss, rtt, budget, paceUs, meldMax, benchmarkSeed(s))
		if math.IsNaN(d) {
			return math.NaN()
		}
		ds = append(ds, d)
	}
	sort.Float64s(ds)
	return ds[reps/4]
}

// runSweep is the iso-quality minimum-latency experiment: for each transport and
// RTT, find B_min — the smallest deadline budget at which delivery still clears
// the quality bar q (e.g. 0.999). Budgets are swept as multiples of RTT, ascending,
// stopping at the first that clears the bar. The pass decision uses a conservative
// low quantile of the per-seed deliveries (drops the single worst run, which is
// usually a subprocess flake rather than transport behavior), and the absolute
// min is printed alongside for transparency. Meld's B_min curve sitting below
// SRT/RIST — in absolute ms, widening with RTT — is the equal-quality-lower-latency
// claim. Overhead at the operating point is NOT yet measured here (next slice).
func runSweep(c *chunked, loss float64, paceUs, meldMax int64, mbps float64, rtts []int, reps int, q float64, streamK int, arms []string) {
	cl := extend(c, streamK)
	total := len(cl.chunks)
	const hiMult = 6.0 // upper budget bound, ×RTT
	mode := "i.i.d."
	if geBurstPkts >= 1 {
		mode = fmt.Sprintf("GE burst (mean %.0f pkt ≈ %.0f ms)", geBurstPkts,
			geBurstPkts*float64((cl.chunkSize+seqHdr)*8)/(mbps*1e6)*1e3)
	}
	fmt.Printf("# ISO-QUALITY MIN-LATENCY — loss %.0f%% %s, Q=%.2f%% delivery\n", loss*100, mode, q*100)
	fmt.Printf("# %d chunks/run (%.0f Mbps), %d seeds; B_min by bisection in ms; pass = conservative low quantile.\n", total, mbps, reps)
	fmt.Printf("# '>%.0f×' = did not reach Q by %.0f×RTT. Feasibility floor: budget must exceed OWD (= 0.5×RTT).\n", hiMult, hiMult)
	for _, arm := range arms {
		if !sweepSupported(arm) {
			continue
		}
		fmt.Printf("\n## %s\n", arm)
		fmt.Printf("# %-8s %-10s %-11s %-12s\n", "RTT(ms)", "Bmin(ms)", "Bmin(×RTT)", "deliv@Bmin")
		for _, rtt := range rtts {
			owd := rtt / 2
			res := rtt / 5 // bisection resolution (≈0.2×RTT)
			if res < 10 {
				res = 10
			}
			hi := int(hiMult*float64(rtt) + 0.5)
			hiDeliv := measureDeliv(arm, cl, loss, rtt, hi, paceUs, meldMax, reps)
			if math.IsNaN(hiDeliv) {
				fmt.Printf("  %-8d %-10s %-11s %s\n", rtt, "FAILED", "—", "(runner failed)")
				continue
			}
			if hiDeliv < q {
				fmt.Printf("  %-8d %-10s %-11s %s\n", rtt, fmt.Sprintf(">%.0f×RTT", hiMult), "—", "(never cleared Q)")
				continue
			}
			lo := owd // budget at/below OWD cannot deliver
			failed := false
			for hi-lo > res {
				mid := (lo + hi) / 2
				if mid <= owd {
					lo = mid
					continue
				}
				d := measureDeliv(arm, cl, loss, rtt, mid, paceUs, meldMax, reps)
				if math.IsNaN(d) {
					failed = true
					break
				}
				if d >= q {
					hi = mid
				} else {
					lo = mid
				}
			}
			if failed {
				fmt.Printf("  %-8d %-10s %-11s %s\n", rtt, "FAILED", "—", "(runner failed)")
				continue
			}
			d := measureDeliv(arm, cl, loss, rtt, hi, paceUs, meldMax, reps)
			if math.IsNaN(d) {
				fmt.Printf("  %-8d %-10s %-11s %s\n", rtt, "FAILED", "—", "(runner failed)")
				continue
			}
			fmt.Printf("  %-8d %-10d %-11s %.3f%%\n",
				rtt, hi, fmt.Sprintf("%.2f×", float64(hi)/float64(rtt)), d*100)
		}
	}
}

type macroFrontierOptions struct {
	SuiteName        string
	SuiteDescription string
	Losses           []float64
	Bursts           []float64
	RTTs             []int
	Mults            []float64
	Arms             []string
	Reps             int
	FloorMs          int
	PaceUs           int64
	MeldMax          int64
	Mbps             float64
	WireMbps         float64
	ChunkSize        int
	TotalPics        int
	OutDir           string
	TopN             int
	JitterMs         int
	JitterPlanes     []int
	ShardCount       int
	ShardIndex       int

	SourceID            string
	SourceClip          string
	SourceCodec         string
	SourceRepeats       int
	SourceFFFrames      int
	AVCOpts             shape.AVCOptions
	AutoEncoderCadence  bool
	AutoEncoderInterval int
	AutoEncoderByteCap  float64
	AutoEncoderPSNRMin  float64
}

func macroJitterPlanes(opts macroFrontierOptions) []int {
	if len(opts.JitterPlanes) > 0 {
		return opts.JitterPlanes
	}
	return []int{opts.JitterMs}
}

func macroTotalCells(opts macroFrontierOptions) int {
	return len(opts.Losses) * len(opts.Bursts) * len(opts.RTTs) * len(opts.Mults) * len(macroJitterPlanes(opts))
}

func macroShardCells(opts macroFrontierOptions) int {
	count := opts.ShardCount
	if count <= 0 {
		count = 1
	}
	index := opts.ShardIndex
	total := macroTotalCells(opts)
	if index < 0 || index >= count || total == 0 {
		return 0
	}
	if index >= total {
		return 0
	}
	return 1 + (total-1-index)/count
}

type macroFrontierRow struct {
	SourceID                     string
	SourceClip                   string
	SourceCodec                  string
	Case                         string
	Loss                         float64
	Burst                        float64
	RTT                          int
	Mult                         float64
	Budget                       int
	Jitter                       int
	Arm                          string
	SourceMode                   string
	SourceInterval               int
	SourcePackets                int
	SourceBytes                  int64
	SourcePSNR                   float64
	SourceFallback               string
	SourceFFFrames               int
	FFMean                       float64
	FFStddev                     float64
	FramePctMean                 float64
	KeyPctMean                   float64
	DeliveredMean                float64
	DeliveryPctMean              float64
	ContinuityFramesMean         float64
	RuntimeMsMean                float64
	RuntimeMsStddev              float64
	ProcessUserMsMean            float64
	ProcessSystemMsMean          float64
	ProcessMaxRSSKBMean          float64
	TxSourceMean                 float64
	RepairMean                   float64
	ReactiveMean                 float64
	TxThrottledMean              float64
	RepairExactMean              float64
	RepairBurstDuplicateMean     float64
	RepairOutageDiversityMean    float64
	RepairEpochMean              float64
	EpochBlocksMean              float64
	EpochDemandQ8Mean            float64
	EpochCorrelationQ8Mean       float64
	EpochMemoryQ8Mean            float64
	EpochShareQ8Mean             float64
	RepairCompactedMean          float64
	RepairBytesSavedMean         float64
	RepairOverheadPct            float64
	RelayForwardEnqueuedMean     float64
	RelayForwardSentMean         float64
	RelayForwardDroppedMean      float64
	RelayForwardSentBytesMean    float64
	RelayForwardDroppedBytesMean float64
	RelayReverseSentMean         float64
	RelayReverseSentBytesMean    float64
	Failed                       int
	Seeds                        int
}

type macroGapRow struct {
	SourceID           string
	SourceClip         string
	SourceCodec        string
	Case               string
	Loss               float64
	Burst              float64
	RTT                int
	Mult               float64
	Budget             int
	Jitter             int
	BurstMs            float64
	TheoryMeld         bool
	BestMeld           string
	MeldSourceMode     string
	MeldSourceInterval int
	MeldSourcePackets  int
	MeldSourceBytes    int64
	MeldSourcePSNR     float64
	MeldSourceFallback string
	MeldFF             float64
	MeldFFSD           float64
	MeldRuntimeMs      float64
	MeldRepairOverhead float64
	MeldRelaySentBytes float64
	BestARQ            string
	ARQSourcePackets   int
	ARQSourceBytes     int64
	ARQFF              float64
	ARQFFSD            float64
	ARQRuntimeMs       float64
	ARQRelaySentBytes  float64
	Seeds              int
	DeltaFF            float64
	DeltaNoise         float64
	DeltaPct           float64
	RuntimeDeltaMs     float64
	RelayByteDeltaPct  float64
	MeldFrame          float64
	ARQFrame           float64
	MeldKey            float64
	ARQKey             float64
}

type armRunMetrics struct {
	ElapsedMs        float64
	DeliveredPackets int
	Relay            relayMetrics
	Process          processMetrics
}

type macroSourceCache struct {
	base      *chunked
	baseBytes int64
	clip      string
	chunkSize int
	avcOpts   shape.AVCOptions
	byteCap   float64
	minPSNR   float64
	repeats   int
	bySource  map[string]*macroSourceVariant
	cleanups  []func()
}

type macroSourceVariant struct {
	chunked  *chunked
	mode     string
	interval int
	psnr     float64
	fallback string
}

type macroSourceRequest struct {
	interval int
}

func newMacroSourceCache(base *chunked, opts macroFrontierOptions) *macroSourceCache {
	return &macroSourceCache{
		base:      base,
		baseBytes: chunkedPayloadBytes(base),
		clip:      opts.SourceClip,
		chunkSize: opts.ChunkSize,
		avcOpts:   opts.AVCOpts,
		byteCap:   opts.AutoEncoderByteCap,
		minPSNR:   opts.AutoEncoderPSNRMin,
		repeats:   opts.SourceRepeats,
		bySource:  map[string]*macroSourceVariant{"": {chunked: base}},
	}
}

func (m *macroSourceCache) close() {
	for _, cleanup := range m.cleanups {
		cleanup()
	}
}

func (m *macroSourceCache) sourceFor(req macroSourceRequest) (*macroSourceVariant, error) {
	if req.interval <= 0 {
		return &macroSourceVariant{chunked: m.base}, nil
	}
	key := macroSourceRequestKey(req)
	if c := m.bySource[key]; c != nil {
		return c, nil
	}
	if m.clip == "" {
		return &macroSourceVariant{chunked: m.base, fallback: "no_source_clip"}, nil
	}
	var (
		path    string
		cleanup func()
		psnr    float64
	)
	if m.byteCap > 0 || m.minPSNR > 0 {
		maxBytes := int64(0)
		if m.byteCap > 0 {
			maxBytes = int64(float64(m.baseBytes) * m.byteCap)
		}
		res, err := transcodeX264CadenceBounded(m.clip, req.interval, maxBytes, m.minPSNR)
		if err != nil {
			return nil, err
		}
		if !res.OK {
			v := &macroSourceVariant{chunked: m.base, fallback: res.Reason, psnr: res.PSNR}
			m.bySource[key] = v
			return v, nil
		}
		path, cleanup, psnr = res.Path, res.Cleanup, res.PSNR
	} else {
		var err error
		path, cleanup, err = transcodeX264CadenceCRF(m.clip, req.interval, 0)
		if err != nil {
			return nil, err
		}
	}
	c, err := chunkClip(path, m.chunkSize, m.avcOpts)
	if err != nil {
		cleanup()
		return nil, err
	}
	c = extend(c, m.repeats)
	v := &macroSourceVariant{chunked: c, mode: x264CadenceIntraRefresh, interval: req.interval, psnr: psnr}
	m.bySource[key] = v
	m.cleanups = append(m.cleanups, cleanup)
	return v, nil
}

func macroSourceRequestKey(req macroSourceRequest) string {
	if req.interval <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", x264CadenceIntraRefresh, req.interval)
}

func chunkedPayloadBytes(c *chunked) int64 {
	if c == nil {
		return 0
	}
	var n int64
	for _, pkt := range c.chunks {
		if len(pkt) > seqHdr {
			n += int64(len(pkt) - seqHdr)
		}
	}
	return n
}

func macroAutoEncoderSource(loss, burst float64, rtt int, mult float64, budget int, opts macroFrontierOptions) macroSourceRequest {
	if !opts.AutoEncoderCadence || opts.SourceCodec != formatAVC.name() {
		return macroSourceRequest{}
	}
	burstMs := burstDurationMs(burst, opts.ChunkSize, opts.Mbps)
	if !theoreticalMeldOpportunity(burst, burstMs, rtt, budget) {
		return macroSourceRequest{}
	}
	if burst >= 48 {
		interval := opts.AutoEncoderInterval
		if interval <= 0 {
			interval = 24
		}
		return macroSourceRequest{interval: interval}
	}
	return macroSourceRequest{}
}

func runMacroFrontier(c *chunked, opts macroFrontierOptions) error {
	if opts.TotalPics <= 0 {
		opts.TotalPics = 1
	}
	if len(opts.Losses) == 0 || len(opts.Bursts) == 0 || len(opts.RTTs) == 0 || len(opts.Mults) == 0 || len(opts.Arms) == 0 {
		return fmt.Errorf("empty macro frontier grid: losses=%d bursts=%d rtts=%d mults=%d arms=%d",
			len(opts.Losses), len(opts.Bursts), len(opts.RTTs), len(opts.Mults), len(opts.Arms))
	}
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}
	if opts.ShardIndex < 0 || opts.ShardIndex >= opts.ShardCount {
		return fmt.Errorf("invalid frontier shard %d of %d", opts.ShardIndex, opts.ShardCount)
	}
	jitters := macroJitterPlanes(opts)
	for _, jitter := range jitters {
		if jitter < 0 {
			return fmt.Errorf("negative jitter plane %d", jitter)
		}
	}
	if opts.SourceRepeats < 1 {
		opts.SourceRepeats = 1
	}
	if opts.SourceRepeats > 1 {
		c = extend(c, opts.SourceRepeats)
		opts.TotalPics *= opts.SourceRepeats
		opts.SourceFFFrames *= opts.SourceRepeats
	}
	rows := make([]macroFrontierRow, 0)
	failures := make([]failureReportRow, 0)
	var failureSink *[]failureReportRow
	if opts.OutDir != "" {
		failureSink = &failures
	}
	sources := newMacroSourceCache(c, opts)
	defer sources.close()
	oldBurst := geBurstPkts
	defer func() { geBurstPkts = oldBurst }()
	oldJitter := jitterDur
	defer func() { jitterDur = oldJitter }()
	cellOrdinal := 0
	for _, jitter := range jitters {
		jitterDur = time.Duration(jitter) * time.Millisecond
		cellOpts := opts
		cellOpts.JitterMs = jitter
		for _, loss := range opts.Losses {
			for _, burst := range opts.Bursts {
				geBurstPkts = burst
				for _, rtt := range opts.RTTs {
					for _, mult := range opts.Mults {
						selected := cellOrdinal%opts.ShardCount == opts.ShardIndex
						cellOrdinal++
						if !selected {
							continue
						}
						budget := int(mult*float64(rtt) + 0.5)
						if budget < opts.FloorMs {
							budget = opts.FloorMs
						}
						caseName := macroCaseName(loss, burst, rtt, mult, budget, jitter)
						for _, arm := range opts.Arms {
							arm = strings.TrimSpace(arm)
							if arm == "" || !sweepSupported(arm) {
								continue
							}
							req := macroSourceRequest{}
							if arm == "meld-auto" {
								req = macroAutoEncoderSource(loss, burst, rtt, mult, budget, opts)
							}
							source, err := sources.sourceFor(req)
							if err != nil {
								return fmt.Errorf("%s %s source %s: %w", caseName, arm, macroSourceRequestKey(req), err)
							}
							row, err := runMacroFrontierCell(source.chunked, caseName, loss, burst, rtt, mult, budget, arm, source.mode, source.interval, cellOpts, failureSink)
							if err != nil {
								return fmt.Errorf("%s %s: %w", caseName, arm, err)
							}
							row.SourcePSNR = source.psnr
							row.SourceFallback = source.fallback
							rows = append(rows, row)
							if row.Failed > 0 || row.Seeds == 0 {
								fmt.Printf("%-36s %-18s FAILED (%d/%d)\n", caseName, arm, row.Failed, opts.Reps)
							} else {
								label := macroArmSourceLabel(arm, row.SourceMode, row.SourceInterval)
								fmt.Printf("%-36s %-18s ff=%6.1f sd=%4.1f frame=%5.1f key=%5.1f\n",
									caseName, label, row.FFMean, row.FFStddev, row.FramePctMean*100, row.KeyPctMean*100)
							}
						}
					}
				}
			}
		}
	}
	gaps := macroGapRows(rows, opts)
	printMacroFrontierSummary(gaps, opts)
	if opts.OutDir != "" {
		if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
			return err
		}
		if err := writeRunEnvironment(filepath.Join(opts.OutDir, "environment.json"), opts); err != nil {
			return err
		}
		if err := writeJSONFile(filepath.Join(opts.OutDir, "frontier_rows.json"), rows); err != nil {
			return err
		}
		if err := writeMacroFrontierRows(filepath.Join(opts.OutDir, "frontier_rows.csv"), rows); err != nil {
			return err
		}
		if err := writeMacroGapRows(filepath.Join(opts.OutDir, "frontier_gaps.csv"), gaps); err != nil {
			return err
		}
		if err := writeMacroFrontierMarkdown(filepath.Join(opts.OutDir, "FRONTIER.md"), gaps, opts); err != nil {
			return err
		}
		if err := writeFailureReportCSV(filepath.Join(opts.OutDir, "failure_report.csv"), failures); err != nil {
			return err
		}
		if err := writeFailureReportMD(filepath.Join(opts.OutDir, "failure_report.md"), failures, 48); err != nil {
			return err
		}
		if err := writeFairnessReports(opts.OutDir, rows, gaps, opts); err != nil {
			return err
		}
		if err := writeMacroCharts(opts.OutDir, rows, gaps, opts); err != nil {
			return err
		}
		if err := writeJSONFile(filepath.Join(opts.OutDir, "COMPLETE.json"), struct {
			Rows       int `json:"rows"`
			Cells      int `json:"cells"`
			ShardIndex int `json:"shard_index"`
			ShardCount int `json:"shard_count"`
		}{len(rows), macroShardCells(opts), opts.ShardIndex, opts.ShardCount}); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONFile(path string, value any) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(value)
}

func runMacroFrontierCell(c *chunked, caseName string, loss, burst float64, rtt int, mult float64, budget int, arm, sourceMode string, sourceInterval int, opts macroFrontierOptions, failures *[]failureReportRow) (macroFrontierRow, error) {
	row := macroFrontierRow{
		SourceID:       opts.SourceID,
		SourceClip:     opts.SourceClip,
		SourceCodec:    c.format.name(),
		Case:           caseName,
		Loss:           loss,
		Burst:          burst,
		RTT:            rtt,
		Mult:           mult,
		Budget:         budget,
		Jitter:         opts.JitterMs,
		Arm:            arm,
		SourceMode:     sourceMode,
		SourceInterval: sourceInterval,
		SourcePackets:  len(c.chunks),
		SourceBytes:    chunkedPayloadBytes(c),
		SourceFFFrames: opts.SourceFFFrames,
	}
	var ffSum int
	var ffs []float64
	var frameSum, keySum float64
	var deliveredSum, continuitySum int
	var runtimeSum float64
	var runtimeVals []float64
	var processUserSum, processSystemSum float64
	var processRSSSum int64
	var txSourceSum, repairSum, reactiveSum, txThrottledSum uint64
	var repairExactSum, repairBurstDuplicateSum, repairOutageDiversitySum, repairEpochSum uint64
	var epochBlocksSum, epochDemandQ8Sum, epochCorrelationQ8Sum uint64
	var epochMemoryQ8Sum, epochFixedMixQ8Sum uint64
	var repairCompactedSum, repairBytesSavedSum uint64
	var relayFwdEnqSum, relayFwdSentSum, relayFwdDropSum int64
	var relayFwdSentBytesSum, relayFwdDropBytesSum int64
	var relayRevSentSum, relayRevSentBytesSum int64
	var worstTrace *macroTraceCandidate
	for rep := 1; rep <= opts.Reps; rep++ {
		seed := benchmarkSeed(rep)
		var trace *seedTrace
		if failures != nil {
			trace = &seedTrace{Case: reportCase{Name: caseName}, Arm: arm, Rep: rep, Seed: seed}
		}
		res := macroRunArmTrace(arm, c, loss, rtt, budget, opts.PaceUs, opts.MeldMax, seed, trace)
		for attempt := 2; res.seqs == nil && macroExternalArm(arm) && attempt <= 3; attempt++ {
			fmt.Fprintf(os.Stderr, "%s %s rep %d: external process failed, retrying (%d/3)\n", caseName, arm, rep, attempt)
			time.Sleep(time.Duration(attempt-1) * 100 * time.Millisecond)
			res = macroRunArmTrace(arm, c, loss, rtt, budget, opts.PaceUs, opts.MeldMax, seed, trace)
		}
		if res.seqs == nil {
			row.Failed++
			continue
		}
		sc, stream, _ := grade(c, res.seqs)
		ff, err := c.ffprobeFrames(stream)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s rep %d ffprobe: %v\n", caseName, arm, rep, err)
			row.Failed++
			continue
		}
		ffs = append(ffs, float64(ff))
		ffSum += ff
		frameSum += sc.frameRate
		keySum += sc.keyRate
		deliveredSum += len(res.seqs)
		continuitySum += decodablePrefixFrames(c, res.seqs)
		runtimeSum += res.metrics.ElapsedMs
		runtimeVals = append(runtimeVals, res.metrics.ElapsedMs)
		processUserSum += res.metrics.Process.UserMs
		processSystemSum += res.metrics.Process.SystemMs
		processRSSSum += res.metrics.Process.MaxRSSKB
		relayFwdEnqSum += res.metrics.Relay.ForwardEnqueued
		relayFwdSentSum += res.metrics.Relay.ForwardSent
		relayFwdDropSum += res.metrics.Relay.ForwardDropped
		relayFwdSentBytesSum += res.metrics.Relay.ForwardSentB
		relayFwdDropBytesSum += res.metrics.Relay.ForwardDroppedB
		relayRevSentSum += res.metrics.Relay.ReverseSent
		relayRevSentBytesSum += res.metrics.Relay.ReverseSentB
		if res.meld != nil {
			txSourceSum += res.meld.txStats.Source
			repairSum += res.meld.txStats.Repair
			reactiveSum += res.meld.txStats.ReactiveRepair
			txThrottledSum += res.meld.txStats.Throttled
			repairExactSum += res.meld.txStats.RepairExact
			repairBurstDuplicateSum += res.meld.txStats.RepairBurstDuplicate
			repairOutageDiversitySum += res.meld.txStats.RepairOutageDiversity
			repairEpochSum += res.meld.txStats.RepairEpoch
			epochBlocksSum += res.meld.txStats.EpochBlocks
			epochDemandQ8Sum += uint64(res.meld.txStats.EpochDemandQ8)
			epochCorrelationQ8Sum += uint64(res.meld.txStats.EpochCorrelationQ8)
			epochMemoryQ8Sum += uint64(res.meld.txStats.EpochMemoryQ8)
			epochFixedMixQ8Sum += uint64(res.meld.txStats.EpochShareQ8)
			repairCompactedSum += res.meld.txStats.RepairCompacted
			repairBytesSavedSum += res.meld.txStats.RepairBytesSaved
		}
		if failures != nil {
			ms := missingSummaryFor(c, res.seqs)
			if trace == nil {
				trace = &seedTrace{Case: reportCase{Name: caseName}, Arm: arm, Rep: rep, Seed: seed}
			}
			trace.Source = sourceTimeline(c, res.seqs)
			trace.Missing = ms
			trace.MissingRuns = missingRuns(c, res.seqs)
			trace.Failure = failureAttributionFor(c, res.seqs, trace, ms)
			trace.Score = seedTraceScore{FFFrames: ff, FramePct: sc.frameRate, KeyPct: sc.keyRate}
			trace.Stats = macroSeedTraceStats(c, res)
			*failures = append(*failures, failureReportRow{
				Case:     caseName,
				Arm:      macroArmSourceLabel(arm, sourceMode, sourceInterval),
				Rep:      rep,
				Seed:     seed,
				FF:       ff,
				FramePct: sc.frameRate,
				KeyPct:   sc.keyRate,
				Failure:  trace.Failure,
			})
			if isMeldArm(arm) && trace.Failure.Kind != "none" {
				candidate := &macroTraceCandidate{
					trace:        trace,
					failureIndex: len(*failures) - 1,
				}
				if candidate.worseThan(worstTrace) {
					worstTrace = candidate
				}
			}
		}
		row.Seeds++
	}
	if row.Seeds > 0 {
		n := float64(row.Seeds)
		row.FFMean = float64(ffSum) / n
		row.FramePctMean = frameSum / n
		row.KeyPctMean = keySum / n
		row.DeliveredMean = float64(deliveredSum) / n
		if len(c.chunks) > 0 {
			row.DeliveryPctMean = row.DeliveredMean / float64(len(c.chunks))
		}
		row.ContinuityFramesMean = float64(continuitySum) / n
		row.RuntimeMsMean = runtimeSum / n
		row.ProcessUserMsMean = processUserSum / n
		row.ProcessSystemMsMean = processSystemSum / n
		row.ProcessMaxRSSKBMean = float64(processRSSSum) / n
		row.TxSourceMean = float64(txSourceSum) / n
		row.RepairMean = float64(repairSum) / n
		row.ReactiveMean = float64(reactiveSum) / n
		row.TxThrottledMean = float64(txThrottledSum) / n
		row.RepairExactMean = float64(repairExactSum) / n
		row.RepairBurstDuplicateMean = float64(repairBurstDuplicateSum) / n
		row.RepairOutageDiversityMean = float64(repairOutageDiversitySum) / n
		row.RepairEpochMean = float64(repairEpochSum) / n
		row.EpochBlocksMean = float64(epochBlocksSum) / n
		row.EpochDemandQ8Mean = float64(epochDemandQ8Sum) / n
		row.EpochCorrelationQ8Mean = float64(epochCorrelationQ8Sum) / n
		row.EpochMemoryQ8Mean = float64(epochMemoryQ8Sum) / n
		row.EpochShareQ8Mean = float64(epochFixedMixQ8Sum) / n
		row.RepairCompactedMean = float64(repairCompactedSum) / n
		row.RepairBytesSavedMean = float64(repairBytesSavedSum) / n
		if row.TxSourceMean > 0 {
			row.RepairOverheadPct = row.RepairMean / row.TxSourceMean
		}
		row.RelayForwardEnqueuedMean = float64(relayFwdEnqSum) / n
		row.RelayForwardSentMean = float64(relayFwdSentSum) / n
		row.RelayForwardDroppedMean = float64(relayFwdDropSum) / n
		row.RelayForwardSentBytesMean = float64(relayFwdSentBytesSum) / n
		row.RelayForwardDroppedBytesMean = float64(relayFwdDropBytesSum) / n
		row.RelayReverseSentMean = float64(relayRevSentSum) / n
		row.RelayReverseSentBytesMean = float64(relayRevSentBytesSum) / n
		if row.Seeds > 1 {
			var m2 float64
			for _, ff := range ffs {
				d := ff - row.FFMean
				m2 += d * d
			}
			row.FFStddev = math.Sqrt(m2 / float64(row.Seeds-1))
			m2 = 0
			for _, elapsed := range runtimeVals {
				d := elapsed - row.RuntimeMsMean
				m2 += d * d
			}
			row.RuntimeMsStddev = math.Sqrt(m2 / float64(row.Seeds-1))
		}
	}
	if worstTrace != nil {
		if err := worstTrace.write(opts.OutDir, caseName, arm, *failures); err != nil {
			return row, err
		}
	}
	return row, nil
}

func macroExternalArm(arm string) bool {
	return isCompetitorArm(arm)
}

type macroTraceCandidate struct {
	trace        *seedTrace
	failureIndex int
}

// worseThan orders diagnostics by user-visible damage before packet loss. A
// lower decoded-frame score is the first thing a reader needs to explain;
// frame/key completeness and missing-chunk count break ties deterministically.
func (c *macroTraceCandidate) worseThan(other *macroTraceCandidate) bool {
	if c == nil || c.trace == nil {
		return false
	}
	if other == nil || other.trace == nil {
		return true
	}
	a, b := c.trace, other.trace
	if a.Score.FFFrames != b.Score.FFFrames {
		return a.Score.FFFrames < b.Score.FFFrames
	}
	if a.Score.FramePct != b.Score.FramePct {
		return a.Score.FramePct < b.Score.FramePct
	}
	if a.Score.KeyPct != b.Score.KeyPct {
		return a.Score.KeyPct < b.Score.KeyPct
	}
	if a.Failure.MissingChunks != b.Failure.MissingChunks {
		return a.Failure.MissingChunks > b.Failure.MissingChunks
	}
	return a.Rep < b.Rep
}

func (c *macroTraceCandidate) write(dir, caseName, arm string, failures []failureReportRow) error {
	if c == nil || c.trace == nil {
		return nil
	}
	if dir == "" {
		return fmt.Errorf("cannot preserve diagnostic trace without a report directory")
	}
	if c.failureIndex < 0 || c.failureIndex >= len(failures) {
		return fmt.Errorf("diagnostic trace failure index %d outside %d rows", c.failureIndex, len(failures))
	}
	traceName := fmt.Sprintf("seed_trace_%s_%s_rep%d_seed%d.json",
		safeName(caseName), safeName(arm), c.trace.Rep, c.trace.Seed)
	if err := writeJSON(filepath.Join(dir, traceName), c.trace); err != nil {
		return fmt.Errorf("write worst-seed trace: %w", err)
	}
	failures[c.failureIndex].Trace = traceName
	return nil
}

func macroSeedTraceStats(c *chunked, res benchRun) seedTraceStats {
	stats := seedTraceStats{
		Chunks:      len(res.seqs),
		TotalChunks: len(c.chunks),
		RelayEnq:    res.metrics.Relay.ForwardEnqueued,
		RelaySent:   res.metrics.Relay.ForwardSent,
	}
	if res.meld == nil {
		return stats
	}
	stats.TxSource = res.meld.txStats.Source
	stats.TxRepair = res.meld.txStats.Repair
	stats.TxReactive = res.meld.txStats.ReactiveRepair
	stats.TxThrottled = res.meld.txStats.Throttled
	stats.RepairExact = res.meld.txStats.RepairExact
	stats.RepairBurstDuplicate = res.meld.txStats.RepairBurstDuplicate
	stats.RepairOutageDiversity = res.meld.txStats.RepairOutageDiversity
	stats.RepairEpoch = res.meld.txStats.RepairEpoch
	stats.EpochBlocks = res.meld.txStats.EpochBlocks
	stats.EpochDemandQ8 = res.meld.txStats.EpochDemandQ8
	stats.EpochCorrelationQ8 = res.meld.txStats.EpochCorrelationQ8
	stats.EpochMemoryQ8 = res.meld.txStats.EpochMemoryQ8
	stats.EpochShareQ8 = res.meld.txStats.EpochShareQ8
	stats.RepairCompacted = res.meld.txStats.RepairCompacted
	stats.RepairBytesSaved = res.meld.txStats.RepairBytesSaved
	stats.RxDelivered = res.meld.rxStats.Delivered
	stats.RxRecovered = res.meld.rxStats.Recovered
	stats.RxLost = res.meld.rxStats.Lost
	stats.RxEvicted = res.meld.rxStats.Evicted
	return stats
}

func decodablePrefixFrames(c *chunked, seqs map[uint32]bool) int {
	delivered := c.deliveredUnits(seqs)
	dec := shape.Decodable(c.units, delivered)
	frames := 0
	for _, sh := range c.shaped {
		if !sh.Unit.Picture {
			continue
		}
		if !dec[sh.Unit.ID] {
			return frames
		}
		frames++
	}
	return frames
}

func macroRunArmTrace(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax, seed int64, trace *seedTrace) benchRun {
	resetRelayMetrics()
	resetBenchProcMetrics()
	start := time.Now()
	withMetrics := func(run benchRun) benchRun {
		run.metrics = armRunMetrics{
			ElapsedMs:        float64(time.Since(start).Microseconds()) / 1000,
			DeliveredPackets: len(run.seqs),
			Relay:            snapshotRelayMetrics(),
			Process:          snapshotBenchProcMetrics(),
		}
		return run
	}
	if isMeldArm(arm) {
		res := runMeldNamedTrace(c, arm, loss, rtt, budget, paceUs, meldMax, seed, trace)
		return withMetrics(benchRun{seqs: res.got, meld: &res, trace: trace})
	}
	switch arm {
	case "oracle-source":
		return withMetrics(benchRun{seqs: allChunkSeqs(c), trace: trace})
	case "oracle-ideal":
		return withMetrics(benchRun{seqs: idealDeadlineSeqs(c, rtt, budget), trace: trace})
	case "libsrt":
		return withMetrics(benchRun{seqs: runLibsrt(c, loss, rtt, budget, paceUs, seed, ""), trace: trace})
	case "libsrt-fec":
		return withMetrics(benchRun{seqs: runLibsrt(c, loss, rtt, budget, paceUs, seed, "fec,cols:10,rows:5,arq:onreq"), trace: trace})
	case "librist":
		return withMetrics(benchRun{seqs: runLibrist(c, loss, rtt, budget, paceUs, seed), trace: trace})
	default:
		return withMetrics(benchRun{})
	}
}

func allChunkSeqs(c *chunked) map[uint32]bool {
	got := make(map[uint32]bool, len(c.chunks))
	for i := range c.chunks {
		got[uint32(i)] = true
	}
	return got
}

func idealDeadlineSeqs(c *chunked, rtt, budget int) map[uint32]bool {
	// Ideal coded transport ceiling: ignore erasures and repair scarcity, but keep
	// the physical one-way propagation deadline. If one-way delay exceeds playout
	// budget, even an ideal code cannot arrive in time.
	if budget*2 < rtt {
		return map[uint32]bool{}
	}
	return allChunkSeqs(c)
}

func macroGapRows(rows []macroFrontierRow, opts macroFrontierOptions) []macroGapRow {
	byCase := map[string][]macroFrontierRow{}
	for _, row := range rows {
		if row.Failed > 0 || row.Seeds == 0 {
			continue
		}
		byCase[row.Case] = append(byCase[row.Case], row)
	}
	out := make([]macroGapRow, 0, len(byCase))
	for name, rs := range byCase {
		meld, haveMeld := selectedMacroMeldRow(rs)
		competitor, haveCompetitor := bestMacroRow(rs, isCompetitorArm)
		if !haveMeld || !haveCompetitor {
			continue
		}
		burstMs := burstDurationMs(meld.Burst, opts.ChunkSize, opts.Mbps)
		out = append(out, macroGapRow{
			SourceID:           meld.SourceID,
			SourceClip:         meld.SourceClip,
			SourceCodec:        meld.SourceCodec,
			Case:               name,
			Loss:               meld.Loss,
			Burst:              meld.Burst,
			RTT:                meld.RTT,
			Mult:               meld.Mult,
			Budget:             meld.Budget,
			Jitter:             meld.Jitter,
			BurstMs:            burstMs,
			TheoryMeld:         theoreticalMeldOpportunity(meld.Burst, burstMs, meld.RTT, meld.Budget),
			BestMeld:           meld.Arm,
			MeldSourceMode:     meld.SourceMode,
			MeldSourceInterval: meld.SourceInterval,
			MeldSourcePackets:  meld.SourcePackets,
			MeldSourceBytes:    meld.SourceBytes,
			MeldSourcePSNR:     meld.SourcePSNR,
			MeldSourceFallback: meld.SourceFallback,
			MeldFF:             meld.FFMean,
			MeldFFSD:           meld.FFStddev,
			MeldRuntimeMs:      meld.RuntimeMsMean,
			MeldRepairOverhead: meld.RepairOverheadPct,
			MeldRelaySentBytes: meld.RelayForwardSentBytesMean,
			BestARQ:            competitor.Arm,
			ARQSourcePackets:   competitor.SourcePackets,
			ARQSourceBytes:     competitor.SourceBytes,
			ARQFF:              competitor.FFMean,
			ARQFFSD:            competitor.FFStddev,
			ARQRuntimeMs:       competitor.RuntimeMsMean,
			ARQRelaySentBytes:  competitor.RelayForwardSentBytesMean,
			Seeds:              min(meld.Seeds, competitor.Seeds),
			DeltaFF:            meld.FFMean - competitor.FFMean,
			DeltaNoise:         math.Hypot(meld.FFStddev, competitor.FFStddev),
			DeltaPct:           (meld.FFMean - competitor.FFMean) / float64(opts.TotalPics),
			RuntimeDeltaMs:     meld.RuntimeMsMean - competitor.RuntimeMsMean,
			RelayByteDeltaPct:  pctDeltaFloat(meld.RelayForwardSentBytesMean, competitor.RelayForwardSentBytesMean),
			MeldFrame:          meld.FramePctMean,
			ARQFrame:           competitor.FramePctMean,
			MeldKey:            meld.KeyPctMean,
			ARQKey:             competitor.KeyPctMean,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TheoryMeld != out[j].TheoryMeld {
			return out[i].TheoryMeld
		}
		return out[i].DeltaFF > out[j].DeltaFF
	})
	return out
}

func isCompetitorArm(arm string) bool {
	switch arm {
	case "libsrt", "libsrt-fec", "librist":
		return true
	default:
		return false
	}
}

func selectedMacroMeldRow(rows []macroFrontierRow) (macroFrontierRow, bool) {
	if row, ok := bestMacroRow(rows, func(a string) bool { return a == "meld-auto" }); ok {
		return row, true
	}
	return bestMacroRow(rows, func(a string) bool { return strings.HasPrefix(a, "meld") })
}

func bestMacroRow(rows []macroFrontierRow, accept func(string) bool) (macroFrontierRow, bool) {
	var best macroFrontierRow
	ok := false
	for _, row := range rows {
		if !accept(row.Arm) {
			continue
		}
		if !ok || row.FFMean > best.FFMean || (row.FFMean == best.FFMean && row.FramePctMean > best.FramePctMean) {
			best, ok = row, true
		}
	}
	return best, ok
}

func printMacroFrontierSummary(gaps []macroGapRow, opts macroFrontierOptions) {
	topN := opts.TopN
	if topN <= 0 {
		topN = 8
	}
	fmt.Printf("\n# Macro Frontier Summary\n")
	fmt.Printf("# delta = deployable Meld ffprobe frames - best SRT/RIST competitor (meld-auto when present)\n\n")
	printGapBlock("Stable theory-opportunity Meld wins", filterGapRows(gaps, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF > 0 && macroGapStable(g)
	}), topN)
	printGapBlock("Stable theory-opportunity Meld deficits", filterGapRows(gaps, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF < 0 && macroGapStable(g)
	}), topN)
	printGapBlock("Seed-noisy theory-opportunity gaps", filterGapRows(gaps, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF != 0 && !macroGapStable(g)
	}), topN)
	printGapBlock("Largest overall Meld wins", filterGapRows(gaps, func(g macroGapRow) bool {
		return g.DeltaFF > 0
	}), topN)
	printGapBlock("Largest overall Meld deficits", filterGapRows(gaps, func(g macroGapRow) bool {
		return g.DeltaFF < 0
	}), topN)
}

func printGapBlock(title string, gaps []macroGapRow, topN int) {
	sort.Slice(gaps, func(i, j int) bool {
		lower := strings.ToLower(title)
		if strings.Contains(lower, "seed-noisy") {
			return math.Abs(gaps[i].DeltaFF) > math.Abs(gaps[j].DeltaFF)
		}
		if strings.Contains(lower, "deficit") || strings.Contains(lower, "deficits") {
			return gaps[i].DeltaFF < gaps[j].DeltaFF
		}
		return gaps[i].DeltaFF > gaps[j].DeltaFF
	})
	if len(gaps) == 0 {
		fmt.Printf("## %s\nnone\n\n", title)
		return
	}
	if len(gaps) > topN {
		gaps = gaps[:topN]
	}
	fmt.Printf("## %s\n", title)
	fmt.Printf("%-36s %-16s %-16s %8s %8s %8s %8s %7s %8s %8s %8s %8s %7s\n", "case", "meld", "competitor", "meld", "other", "delta", "noise", "stable", "repair", "wire", "run_ms", "src_byte", "psnr")
	for _, g := range gaps {
		fmt.Printf("%-36s %-16s %-16s %8.1f %8.1f %+8.1f %8.1f %7t %8s %8s %+8.0f %8s %7s\n",
			g.Case, macroMeldLabel(g), g.BestARQ, g.MeldFF, g.ARQFF, g.DeltaFF, g.DeltaNoise, macroGapStable(g),
			formatSignedPct(g.MeldRepairOverhead), formatSignedPct(g.RelayByteDeltaPct), g.RuntimeDeltaMs,
			formatSignedPct(macroSourceByteDelta(g)), formatSourcePSNR(g.MeldSourcePSNR))
	}
	fmt.Println()
}

func macroMeldLabel(g macroGapRow) string {
	return macroArmSourceLabel(g.BestMeld, g.MeldSourceMode, g.MeldSourceInterval)
}

func macroArmSourceLabel(arm, mode string, interval int) string {
	switch {
	case mode == x264CadenceIntraRefresh && interval > 0:
		return fmt.Sprintf("%s[ir%d]", arm, interval)
	case mode != "" && interval > 0:
		return fmt.Sprintf("%s[%s%d]", arm, mode, interval)
	default:
		return arm
	}
}

func filterGapRows(rows []macroGapRow, keep func(macroGapRow) bool) []macroGapRow {
	out := make([]macroGapRow, 0)
	for _, row := range rows {
		if keep(row) {
			out = append(out, row)
		}
	}
	return out
}

func macroGapStable(g macroGapRow) bool {
	return g.Seeds >= 3 && (g.DeltaNoise == 0 || math.Abs(g.DeltaFF) > g.DeltaNoise)
}

func macroSourcePacketDelta(g macroGapRow) float64 {
	return pctDeltaInt(g.MeldSourcePackets, g.ARQSourcePackets)
}

func macroSourceByteDelta(g macroGapRow) float64 {
	return pctDeltaInt64(g.MeldSourceBytes, g.ARQSourceBytes)
}

func pctDeltaInt(cur, base int) float64 {
	if base == 0 {
		return 0
	}
	return (float64(cur) - float64(base)) / float64(base)
}

func pctDeltaInt64(cur, base int64) float64 {
	if base == 0 {
		return 0
	}
	return (float64(cur) - float64(base)) / float64(base)
}

func pctDeltaFloat(cur, base float64) float64 {
	if base == 0 {
		return 0
	}
	return (cur - base) / base
}

func formatSignedPct(v float64) string {
	return fmt.Sprintf("%+.1f%%", v*100)
}

func formatSourcePSNR(v float64) string {
	if v <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f", v)
}

func theoreticalMeldOpportunity(burst, burstMs float64, rtt, budget int) bool {
	retransmitFloor := 1.5 * float64(rtt)
	slackAfterRTT := float64(budget - rtt)
	if slackAfterRTT < 0 {
		slackAfterRTT = 0
	}
	return float64(budget) < retransmitFloor || (burst >= 1 && burstMs > slackAfterRTT)
}

func burstDurationMs(burst float64, chunkSize int, mbps float64) float64 {
	if burst < 1 || mbps <= 0 {
		return 0
	}
	return burst * float64((chunkSize+seqHdr)*8) / (mbps * 1e6) * 1e3
}

func macroCaseName(loss, burst float64, rtt int, mult float64, budget, jitter int) string {
	prefix := "iid"
	if burst >= 1 {
		prefix = fmt.Sprintf("burst%d", int(burst+0.5))
	}
	name := fmt.Sprintf("%s_loss%s_rtt%d_%sx_b%d", prefix, pctName(loss), rtt, multName(mult), budget)
	if jitter > 0 {
		name += fmt.Sprintf("_j%d", jitter)
	}
	return name
}

func pctName(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v*100), "0"), ".")
}

func multName(v float64) string {
	return strings.ReplaceAll(strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), "."), ".", "p")
}

func writeMacroFrontierRows(path string, rows []macroFrontierRow) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	defer finishCSVFile(f, w, &err)
	if err := w.Write([]string{
		"source_id", "source_clip", "source_codec", "case", "loss", "burst", "rtt_ms", "mult", "budget_ms", "jitter_ms", "arm",
		"source_mode", "source_interval", "source_packets", "source_bytes", "source_ff_frames", "source_psnr", "source_fallback",
		"ff_mean", "ff_stddev", "frame_pct_mean", "key_pct_mean", "delivered_mean", "delivery_pct_mean", "continuity_frames_mean",
		"runtime_ms_mean", "runtime_ms_stddev", "process_user_ms_mean", "process_system_ms_mean", "process_max_rss_kb_mean",
		"tx_source_mean", "tx_repair_mean", "tx_reactive_mean", "tx_throttled_mean",
		"repair_exact_mean", "repair_burst_duplicate_mean", "repair_outage_diversity_mean", "repair_epoch_mean",
		"epoch_blocks_mean", "epoch_demand_q8_mean", "epoch_correlation_q8_mean", "epoch_memory_q8_mean", "epoch_share_q8_mean",
		"repair_compacted_mean", "repair_bytes_saved_mean", "repair_overhead_pct",
		"relay_forward_enqueued_mean", "relay_forward_sent_mean", "relay_forward_dropped_mean", "relay_forward_sent_bytes_mean",
		"relay_forward_dropped_bytes_mean", "relay_reverse_sent_mean", "relay_reverse_sent_bytes_mean", "failed", "seeds",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.SourceID,
			r.SourceClip,
			r.SourceCodec,
			r.Case,
			fmt.Sprintf("%.6f", r.Loss),
			fmt.Sprintf("%.3f", r.Burst),
			strconv.Itoa(r.RTT),
			fmt.Sprintf("%.3f", r.Mult),
			strconv.Itoa(r.Budget),
			strconv.Itoa(r.Jitter),
			r.Arm,
			r.SourceMode,
			strconv.Itoa(r.SourceInterval),
			strconv.Itoa(r.SourcePackets),
			strconv.FormatInt(r.SourceBytes, 10),
			strconv.Itoa(r.SourceFFFrames),
			fmt.Sprintf("%.3f", r.SourcePSNR),
			r.SourceFallback,
			fmt.Sprintf("%.3f", r.FFMean),
			fmt.Sprintf("%.3f", r.FFStddev),
			fmt.Sprintf("%.6f", r.FramePctMean),
			fmt.Sprintf("%.6f", r.KeyPctMean),
			fmt.Sprintf("%.3f", r.DeliveredMean),
			fmt.Sprintf("%.6f", r.DeliveryPctMean),
			fmt.Sprintf("%.3f", r.ContinuityFramesMean),
			fmt.Sprintf("%.3f", r.RuntimeMsMean),
			fmt.Sprintf("%.3f", r.RuntimeMsStddev),
			fmt.Sprintf("%.3f", r.ProcessUserMsMean),
			fmt.Sprintf("%.3f", r.ProcessSystemMsMean),
			fmt.Sprintf("%.3f", r.ProcessMaxRSSKBMean),
			fmt.Sprintf("%.3f", r.TxSourceMean),
			fmt.Sprintf("%.3f", r.RepairMean),
			fmt.Sprintf("%.3f", r.ReactiveMean),
			fmt.Sprintf("%.3f", r.TxThrottledMean),
			fmt.Sprintf("%.3f", r.RepairExactMean),
			fmt.Sprintf("%.3f", r.RepairBurstDuplicateMean),
			fmt.Sprintf("%.3f", r.RepairOutageDiversityMean),
			fmt.Sprintf("%.3f", r.RepairEpochMean),
			fmt.Sprintf("%.3f", r.EpochBlocksMean),
			fmt.Sprintf("%.3f", r.EpochDemandQ8Mean),
			fmt.Sprintf("%.3f", r.EpochCorrelationQ8Mean),
			fmt.Sprintf("%.3f", r.EpochMemoryQ8Mean),
			fmt.Sprintf("%.3f", r.EpochShareQ8Mean),
			fmt.Sprintf("%.3f", r.RepairCompactedMean),
			fmt.Sprintf("%.3f", r.RepairBytesSavedMean),
			fmt.Sprintf("%.6f", r.RepairOverheadPct),
			fmt.Sprintf("%.3f", r.RelayForwardEnqueuedMean),
			fmt.Sprintf("%.3f", r.RelayForwardSentMean),
			fmt.Sprintf("%.3f", r.RelayForwardDroppedMean),
			fmt.Sprintf("%.3f", r.RelayForwardSentBytesMean),
			fmt.Sprintf("%.3f", r.RelayForwardDroppedBytesMean),
			fmt.Sprintf("%.3f", r.RelayReverseSentMean),
			fmt.Sprintf("%.3f", r.RelayReverseSentBytesMean),
			strconv.Itoa(r.Failed),
			strconv.Itoa(r.Seeds),
		}); err != nil {
			return err
		}
	}
	return nil
}

func writeMacroGapRows(path string, rows []macroGapRow) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	defer finishCSVFile(f, w, &err)
	if err := w.Write([]string{
		"source_id", "source_clip", "source_codec", "case", "loss", "burst", "rtt_ms", "mult", "budget_ms", "jitter_ms", "burst_ms",
		"theory_meld_opportunity", "best_meld", "meld_source_mode", "meld_source_interval", "meld_source_packets",
		"competitor_source_packets", "source_packet_delta_pct", "meld_source_bytes", "competitor_source_bytes", "source_byte_delta_pct",
		"meld_source_psnr", "meld_source_fallback", "meld_ff", "meld_ff_stddev", "best_competitor", "competitor_ff", "competitor_ff_stddev",
		"seeds", "delta_ff", "delta_noise", "delta_stable", "delta_pct", "runtime_delta_ms", "meld_runtime_ms", "competitor_runtime_ms",
		"meld_repair_overhead_pct", "relay_byte_delta_pct", "meld_relay_sent_bytes", "competitor_relay_sent_bytes",
		"meld_frame_pct", "competitor_frame_pct", "meld_key_pct", "competitor_key_pct",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.SourceID,
			r.SourceClip,
			r.SourceCodec,
			r.Case,
			fmt.Sprintf("%.6f", r.Loss),
			fmt.Sprintf("%.3f", r.Burst),
			strconv.Itoa(r.RTT),
			fmt.Sprintf("%.3f", r.Mult),
			strconv.Itoa(r.Budget),
			strconv.Itoa(r.Jitter),
			fmt.Sprintf("%.3f", r.BurstMs),
			strconv.FormatBool(r.TheoryMeld),
			r.BestMeld,
			r.MeldSourceMode,
			strconv.Itoa(r.MeldSourceInterval),
			strconv.Itoa(r.MeldSourcePackets),
			strconv.Itoa(r.ARQSourcePackets),
			fmt.Sprintf("%.6f", macroSourcePacketDelta(r)),
			strconv.FormatInt(r.MeldSourceBytes, 10),
			strconv.FormatInt(r.ARQSourceBytes, 10),
			fmt.Sprintf("%.6f", macroSourceByteDelta(r)),
			fmt.Sprintf("%.3f", r.MeldSourcePSNR),
			r.MeldSourceFallback,
			fmt.Sprintf("%.3f", r.MeldFF),
			fmt.Sprintf("%.3f", r.MeldFFSD),
			r.BestARQ,
			fmt.Sprintf("%.3f", r.ARQFF),
			fmt.Sprintf("%.3f", r.ARQFFSD),
			strconv.Itoa(r.Seeds),
			fmt.Sprintf("%.3f", r.DeltaFF),
			fmt.Sprintf("%.3f", r.DeltaNoise),
			strconv.FormatBool(macroGapStable(r)),
			fmt.Sprintf("%.6f", r.DeltaPct),
			fmt.Sprintf("%.3f", r.RuntimeDeltaMs),
			fmt.Sprintf("%.3f", r.MeldRuntimeMs),
			fmt.Sprintf("%.3f", r.ARQRuntimeMs),
			fmt.Sprintf("%.6f", r.MeldRepairOverhead),
			fmt.Sprintf("%.6f", r.RelayByteDeltaPct),
			fmt.Sprintf("%.3f", r.MeldRelaySentBytes),
			fmt.Sprintf("%.3f", r.ARQRelaySentBytes),
			fmt.Sprintf("%.6f", r.MeldFrame),
			fmt.Sprintf("%.6f", r.ARQFrame),
			fmt.Sprintf("%.6f", r.MeldKey),
			fmt.Sprintf("%.6f", r.ARQKey),
		}); err != nil {
			return err
		}
	}
	return nil
}

func writeMacroFrontierMarkdown(path string, rows []macroGapRow, opts macroFrontierOptions) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Macro Frontier\n\n")
	if opts.SuiteName != "" {
		fmt.Fprintf(&b, "Suite: `%s` — %s\n\n", opts.SuiteName, opts.SuiteDescription)
	}
	fmt.Fprintf(&b, "Grid: losses `%v`, bursts `%v`, RTTs `%v`, multipliers `%v`, reps `%d`.\n\n",
		opts.Losses, opts.Bursts, opts.RTTs, opts.Mults, opts.Reps)
	fmt.Fprintf(&b, "Forward jitter/reorder planes: `%v ms`; shard `%d/%d` covers `%d` of `%d` cells.\n\n",
		macroJitterPlanes(opts), opts.ShardIndex, opts.ShardCount, macroShardCells(opts), macroTotalCells(opts))
	fmt.Fprintf(&b, "Charts: [delta bars](charts/delta-bars.svg), [frontier heatmap](charts/frontier-heatmap.svg), [arm frames](charts/arm-frames.svg), [cost/gain](charts/cost-gain.svg).\n\n")
	fmt.Fprintf(&b, "Theory-opportunity cells are cells where a full ARQ retransmission is latency-tight (`budget < 1.5x RTT`) or burst duration exceeds post-RTT slack.\n\n")
	fmt.Fprintf(&b, "Gap rows compare the deployable Meld profile against the best successful SRT or RIST arm in the same cell. If `meld-auto` is present in the arm list, it is used instead of choosing the best experimental Meld variant.\n\n")
	fmt.Fprintf(&b, "Runtime is wall-clock time for the arm run. External-process CPU/RSS fields are captured for SRT/RIST subprocesses when the OS exposes rusage; Meld runs in-process, so its per-arm CPU/RSS needs a dedicated isolated runner before publication claims about CPU cost.\n\n")
	if opts.AutoEncoderCadence {
		fmt.Fprintf(&b, "Encoder cadence actuator model is enabled: `meld-auto[irN]` means the Meld arm used an encoder-controlled x264 intra-refresh source with bounded recovery interval N. ARQ arms used the baseline source. Source packet and byte deltas compare the Meld source variant with the ARQ baseline source.\n\n")
	}
	if opts.AutoEncoderByteCap > 0 || opts.AutoEncoderPSNRMin > 0 {
		fmt.Fprintf(&b, "Bounded encoder realism is enabled: source variants must satisfy")
		if opts.AutoEncoderByteCap > 0 {
			fmt.Fprintf(&b, " byte cap `%.2fx baseline`", opts.AutoEncoderByteCap)
		}
		if opts.AutoEncoderPSNRMin > 0 {
			if opts.AutoEncoderByteCap > 0 {
				fmt.Fprintf(&b, " and")
			}
			fmt.Fprintf(&b, " PSNR floor `%.1f dB`", opts.AutoEncoderPSNRMin)
		}
		fmt.Fprintf(&b, "; otherwise Meld falls back to the baseline source for that cell.\n\n")
	}
	fmt.Fprintf(&b, "Noise is the combined per-arm sample standard deviation of ffprobe frames over seeds. A gap is stable when `abs(delta ff)` is larger than that noise band.\n\n")
	writeMacroDecision(&b, rows)
	writeGapMarkdownBlock(&b, "Stable Theory-Opportunity Meld Wins", filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF > 0 && macroGapStable(g)
	}), opts.TopN)
	writeGapMarkdownBlock(&b, "Stable Theory-Opportunity Meld Deficits", filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF < 0 && macroGapStable(g)
	}), opts.TopN)
	writeGapMarkdownBlock(&b, "Seed-Noisy Theory-Opportunity Gaps", filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF != 0 && !macroGapStable(g)
	}), opts.TopN)
	writeGapMarkdownBlock(&b, "Largest Overall Meld Wins", filterGapRows(rows, func(g macroGapRow) bool {
		return g.DeltaFF > 0
	}), opts.TopN)
	writeGapMarkdownBlock(&b, "Largest Overall Meld Deficits", filterGapRows(rows, func(g macroGapRow) bool {
		return g.DeltaFF < 0
	}), opts.TopN)
	writeGapMarkdownBlock(&b, "Conservative Fallback Regressions", filterGapRows(rows, func(g macroGapRow) bool {
		return !g.TheoryMeld && g.DeltaFF < 0 && macroGapStable(g)
	}), opts.TopN)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeMacroDecision(b *strings.Builder, rows []macroGapRow) {
	theoryWins := filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF > 0
	})
	sort.Slice(theoryWins, func(i, j int) bool {
		return theoryWins[i].DeltaFF > theoryWins[j].DeltaFF
	})
	theoryDeficits := filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld && g.DeltaFF < 0
	})
	sort.Slice(theoryDeficits, func(i, j int) bool {
		return theoryDeficits[i].DeltaFF < theoryDeficits[j].DeltaFF
	})
	theoryRows := filterGapRows(rows, func(g macroGapRow) bool {
		return g.TheoryMeld
	})
	stableTheoryWins := filterGapRows(theoryWins, macroGapStable)
	stableTheoryDeficits := filterGapRows(theoryDeficits, macroGapStable)

	fmt.Fprintf(b, "## Frontier Call\n\n")
	if len(stableTheoryWins) > 0 {
		g := stableTheoryWins[0]
		fmt.Fprintf(b, "Selected positive target: `%s` (`%s` vs `%s`), %+0.1f ffprobe frames, %+0.1f%% frame completeness, %+0.1f%% key completeness. This is the strongest stable measured cell where the latency model says Meld should win.\n\n",
			g.Case, macroMeldLabel(g), g.BestARQ, g.DeltaFF, (g.MeldFrame-g.ARQFrame)*100, (g.MeldKey-g.ARQKey)*100)
	} else {
		switch {
		case len(theoryWins) > 0:
			g := theoryWins[0]
			fmt.Fprintf(b, "Strongest raw positive target is seed-noisy: `%s` (`%s` vs `%s`), %+0.1f ffprobe frames inside a %.1f-frame seed-spread band. Rerun with more reps before treating it as a macro frontier win.\n\n",
				g.Case, macroMeldLabel(g), g.BestARQ, g.DeltaFF, g.DeltaNoise)
		case len(theoryRows) > 0:
			fmt.Fprintf(b, "No stable positive theory-opportunity cell was found. Use the stable deficits below for thesis adjustment; rerun seed-noisy rows with more reps before drawing macro conclusions.\n\n")
		default:
			fmt.Fprintf(b, "No theory-opportunity cell was found on this grid. Expand the low-latency or burst-stress region before drawing thesis conclusions.\n\n")
		}
	}
	if len(stableTheoryDeficits) > 0 {
		g := stableTheoryDeficits[0]
		fmt.Fprintf(b, "Largest theory-opportunity deficit: `%s` (`%s` vs `%s`), %+0.1f ffprobe frames, %+0.1f%% frame completeness, %+0.1f%% key completeness. This is the first stable thesis-adjustment target if burst superiority is the claim.\n\n",
			g.Case, macroMeldLabel(g), g.BestARQ, g.DeltaFF, (g.MeldFrame-g.ARQFrame)*100, (g.MeldKey-g.ARQKey)*100)
	} else if len(theoryDeficits) > 0 {
		g := theoryDeficits[0]
		fmt.Fprintf(b, "Largest raw theory-opportunity deficit is seed-noisy: `%s` (`%s` vs `%s`), %+0.1f ffprobe frames inside a %.1f-frame seed-spread band. Rerun with more reps before treating it as a thesis-adjustment target.\n\n",
			g.Case, macroMeldLabel(g), g.BestARQ, g.DeltaFF, g.DeltaNoise)
	}
}

func writeGapMarkdownBlock(b *strings.Builder, title string, rows []macroGapRow, topN int) {
	sort.Slice(rows, func(i, j int) bool {
		lower := strings.ToLower(title)
		if strings.Contains(lower, "seed-noisy") {
			return math.Abs(rows[i].DeltaFF) > math.Abs(rows[j].DeltaFF)
		}
		if strings.Contains(lower, "deficit") {
			return rows[i].DeltaFF < rows[j].DeltaFF
		}
		return rows[i].DeltaFF > rows[j].DeltaFF
	})
	if topN <= 0 {
		topN = 8
	}
	if len(rows) > topN {
		rows = rows[:topN]
	}
	fmt.Fprintf(b, "## %s\n\n", title)
	if len(rows) == 0 {
		fmt.Fprintf(b, "None.\n\n")
		return
	}
	fmt.Fprintf(b, "| case | Meld | best competitor | Meld ff | competitor ff | delta ff | noise ff | stable | repair overhead | wire byte delta | runtime delta | frame delta | key delta | source byte delta | source PSNR | source fallback |\n")
	fmt.Fprintf(b, "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(b, "| `%s` | `%s` | `%s` | %.1f | %.1f | %+.1f | %.1f | %t | %s | %s | %+.0f ms | %+.1f%% | %+.1f%% | %s | %s | `%s` |\n",
			r.Case, macroMeldLabel(r), r.BestARQ, r.MeldFF, r.ARQFF, r.DeltaFF, r.DeltaNoise, macroGapStable(r),
			formatSignedPct(r.MeldRepairOverhead), formatSignedPct(r.RelayByteDeltaPct), r.RuntimeDeltaMs,
			(r.MeldFrame-r.ARQFrame)*100, (r.MeldKey-r.ARQKey)*100,
			formatSignedPct(macroSourceByteDelta(r)),
			formatSourcePSNR(r.MeldSourcePSNR), r.MeldSourceFallback)
	}
	fmt.Fprintf(b, "\n")
}
