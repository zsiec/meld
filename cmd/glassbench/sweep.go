package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/zsiec/meld/internal/shape"
)

// extend replicates a chunked stream k times with fresh, unique sequence numbers
// so a delivery-percentage measurement has enough packets to resolve a tight
// quality bar (99.9% over ~3000 chunks ≈ 3 packets, not the ~1 the native clip
// allows). Only .chunks is populated — delivery% needs nothing else, and Meld's
// media-blind WriteUnit path (uep=false) does not touch the unit metadata.
func extend(c *chunked, k int) *chunked {
	if k <= 1 {
		return c
	}
	out := &chunked{chunkSize: c.chunkSize}
	var seq uint32
	for rep := 0; rep < k; rep++ {
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

// sweepSupported reports whether the sweep can drive this arm (Meld-auto, the
// real libsrt, and the real libRIST — the iso-quality baselines).
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
		got = runMeldNamed(c, arm, loss, rtt, budget, paceUs, meldMax, seed).got
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
				d := sweepArm(arm, cl, loss, rtt, budget, paceUs, meldMax, int64(s))
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
		d := sweepArm(arm, c, loss, rtt, budget, paceUs, meldMax, int64(s))
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
	Losses    []float64
	Bursts    []float64
	RTTs      []int
	Mults     []float64
	Arms      []string
	Reps      int
	FloorMs   int
	PaceUs    int64
	MeldMax   int64
	Mbps      float64
	ChunkSize int
	TotalPics int
	OutDir    string
	TopN      int

	SourceClip          string
	AVCOpts             shape.AVCOptions
	AutoEncoderCadence  bool
	AutoEncoderInterval int
	AutoEncoderByteCap  float64
	AutoEncoderPSNRMin  float64
}

type macroFrontierRow struct {
	Case           string
	Loss           float64
	Burst          float64
	RTT            int
	Mult           float64
	Budget         int
	Arm            string
	SourceMode     string
	SourceInterval int
	SourcePackets  int
	SourceBytes    int64
	SourcePSNR     float64
	SourceFallback string
	FFMean         float64
	FFStddev       float64
	FramePctMean   float64
	KeyPctMean     float64
	RepairMean     float64
	ReactiveMean   float64
	Failed         int
	Seeds          int
}

type macroGapRow struct {
	Case               string
	Loss               float64
	Burst              float64
	RTT                int
	Mult               float64
	Budget             int
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
	BestARQ            string
	ARQSourcePackets   int
	ARQSourceBytes     int64
	ARQFF              float64
	ARQFFSD            float64
	DeltaFF            float64
	DeltaNoise         float64
	DeltaPct           float64
	MeldFrame          float64
	ARQFrame           float64
	MeldKey            float64
	ARQKey             float64
}

type macroSourceCache struct {
	base      *chunked
	baseBytes int64
	clip      string
	chunkSize int
	avcOpts   shape.AVCOptions
	byteCap   float64
	minPSNR   float64
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
		bySource:  map[string]*macroSourceVariant{"": &macroSourceVariant{chunked: base}},
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
	if !opts.AutoEncoderCadence {
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
	for _, loss := range opts.Losses {
		for _, burst := range opts.Bursts {
			geBurstPkts = burst
			for _, rtt := range opts.RTTs {
				for _, mult := range opts.Mults {
					budget := int(mult*float64(rtt) + 0.5)
					if budget < opts.FloorMs {
						budget = opts.FloorMs
					}
					caseName := macroCaseName(loss, burst, rtt, mult, budget)
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
						row := runMacroFrontierCell(source.chunked, caseName, loss, burst, rtt, mult, budget, arm, source.mode, source.interval, opts, failureSink)
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
	gaps := macroGapRows(rows, opts)
	printMacroFrontierSummary(gaps, opts)
	if opts.OutDir != "" {
		if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
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
	}
	return nil
}

func runMacroFrontierCell(c *chunked, caseName string, loss, burst float64, rtt int, mult float64, budget int, arm, sourceMode string, sourceInterval int, opts macroFrontierOptions, failures *[]failureReportRow) macroFrontierRow {
	row := macroFrontierRow{
		Case:           caseName,
		Loss:           loss,
		Burst:          burst,
		RTT:            rtt,
		Mult:           mult,
		Budget:         budget,
		Arm:            arm,
		SourceMode:     sourceMode,
		SourceInterval: sourceInterval,
		SourcePackets:  len(c.chunks),
		SourceBytes:    chunkedPayloadBytes(c),
	}
	var ffSum int
	var ffs []float64
	var frameSum, keySum float64
	var repairSum, reactiveSum uint64
	for rep := 1; rep <= opts.Reps; rep++ {
		seed := int64(rep)*7919 + 13
		var trace *seedTrace
		if failures != nil {
			trace = &seedTrace{Case: reportCase{Name: caseName}, Arm: arm, Rep: rep, Seed: seed}
		}
		res := macroRunArmTrace(arm, c, loss, rtt, budget, opts.PaceUs, opts.MeldMax, seed, trace)
		if res.seqs == nil {
			row.Failed++
			continue
		}
		sc, h264, _ := grade(c, res.seqs)
		ff, err := ffprobeFrames(h264)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s rep %d ffprobe: %v\n", caseName, arm, rep, err)
			row.Failed++
			continue
		}
		ffs = append(ffs, float64(ff))
		ffSum += ff
		frameSum += sc.frameRate
		keySum += sc.keyRate
		if res.meld != nil {
			repairSum += res.meld.txStats.Repair
			reactiveSum += res.meld.txStats.ReactiveRepair
		}
		if failures != nil {
			ms := missingSummaryFor(c, res.seqs)
			if trace == nil {
				trace = &seedTrace{Case: reportCase{Name: caseName}, Arm: arm, Rep: rep, Seed: seed}
			}
			trace.Source = sourceTimeline(c, res.seqs)
			trace.Missing = ms
			trace.Failure = failureAttributionFor(c, res.seqs, trace, ms)
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
		}
		row.Seeds++
	}
	if row.Seeds > 0 {
		n := float64(row.Seeds)
		row.FFMean = float64(ffSum) / n
		row.FramePctMean = frameSum / n
		row.KeyPctMean = keySum / n
		row.RepairMean = float64(repairSum) / n
		row.ReactiveMean = float64(reactiveSum) / n
		if row.Seeds > 1 {
			var m2 float64
			for _, ff := range ffs {
				d := ff - row.FFMean
				m2 += d * d
			}
			row.FFStddev = math.Sqrt(m2 / float64(row.Seeds-1))
		}
	}
	return row
}

func macroRunArm(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax, seed int64) benchRun {
	return macroRunArmTrace(arm, c, loss, rtt, budget, paceUs, meldMax, seed, nil)
}

func macroRunArmTrace(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax, seed int64, trace *seedTrace) benchRun {
	if isMeldArm(arm) {
		res := runMeldNamedTrace(c, arm, loss, rtt, budget, paceUs, meldMax, seed, trace)
		return benchRun{seqs: res.got, meld: &res, trace: trace}
	}
	switch arm {
	case "oracle-source":
		return benchRun{seqs: allChunkSeqs(c), trace: trace}
	case "oracle-ideal":
		return benchRun{seqs: idealDeadlineSeqs(c, rtt, budget), trace: trace}
	case "libsrt":
		return benchRun{seqs: runLibsrt(c, loss, rtt, budget, paceUs, seed, ""), trace: trace}
	case "libsrt-fec":
		return benchRun{seqs: runLibsrt(c, loss, rtt, budget, paceUs, seed, "fec,cols:10,rows:5,arq:onreq"), trace: trace}
	case "librist":
		return benchRun{seqs: runLibrist(c, loss, rtt, budget, paceUs, seed), trace: trace}
	default:
		return benchRun{}
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
		arq, haveARQ := bestMacroRow(rs, func(a string) bool { return a == "libsrt" || a == "libsrt-fec" || a == "librist" })
		if !haveMeld || !haveARQ {
			continue
		}
		burstMs := burstDurationMs(meld.Burst, opts.ChunkSize, opts.Mbps)
		out = append(out, macroGapRow{
			Case:               name,
			Loss:               meld.Loss,
			Burst:              meld.Burst,
			RTT:                meld.RTT,
			Mult:               meld.Mult,
			Budget:             meld.Budget,
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
			BestARQ:            arq.Arm,
			ARQSourcePackets:   arq.SourcePackets,
			ARQSourceBytes:     arq.SourceBytes,
			ARQFF:              arq.FFMean,
			ARQFFSD:            arq.FFStddev,
			DeltaFF:            meld.FFMean - arq.FFMean,
			DeltaNoise:         math.Hypot(meld.FFStddev, arq.FFStddev),
			DeltaPct:           (meld.FFMean - arq.FFMean) / float64(opts.TotalPics),
			MeldFrame:          meld.FramePctMean,
			ARQFrame:           arq.FramePctMean,
			MeldKey:            meld.KeyPctMean,
			ARQKey:             arq.KeyPctMean,
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
	fmt.Printf("# delta = deployable Meld ffprobe frames - best ARQ ffprobe frames (meld-auto when present)\n\n")
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
	fmt.Printf("%-36s %-16s %-16s %8s %8s %8s %8s %7s %8s %8s %7s\n", "case", "meld", "arq", "meld", "arq", "delta", "noise", "stable", "src_pkt", "src_byte", "psnr")
	for _, g := range gaps {
		fmt.Printf("%-36s %-16s %-16s %8.1f %8.1f %+8.1f %8.1f %7t %8s %8s %7s\n",
			g.Case, macroMeldLabel(g), g.BestARQ, g.MeldFF, g.ARQFF, g.DeltaFF, g.DeltaNoise, macroGapStable(g),
			formatSignedPct(macroSourcePacketDelta(g)), formatSignedPct(macroSourceByteDelta(g)), formatSourcePSNR(g.MeldSourcePSNR))
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
	return g.DeltaNoise == 0 || math.Abs(g.DeltaFF) > g.DeltaNoise
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

func macroCaseName(loss, burst float64, rtt int, mult float64, budget int) string {
	prefix := "iid"
	if burst >= 1 {
		prefix = fmt.Sprintf("burst%d", int(burst+0.5))
	}
	return fmt.Sprintf("%s_loss%s_rtt%d_%sx_b%d", prefix, pctName(loss), rtt, multName(mult), budget)
}

func pctName(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v*100), "0"), ".")
}

func multName(v float64) string {
	return strings.ReplaceAll(strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.2f", v), "0"), "."), ".", "p")
}

func writeMacroFrontierRows(path string, rows []macroFrontierRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"case", "loss", "burst", "rtt_ms", "mult", "budget_ms", "arm", "source_mode", "source_interval", "source_packets", "source_bytes", "source_psnr", "source_fallback", "ff_mean", "ff_stddev", "frame_pct_mean", "key_pct_mean", "repair_mean", "reactive_mean", "failed", "seeds"})
	for _, r := range rows {
		w.Write([]string{
			r.Case,
			fmt.Sprintf("%.6f", r.Loss),
			fmt.Sprintf("%.3f", r.Burst),
			strconv.Itoa(r.RTT),
			fmt.Sprintf("%.3f", r.Mult),
			strconv.Itoa(r.Budget),
			r.Arm,
			r.SourceMode,
			strconv.Itoa(r.SourceInterval),
			strconv.Itoa(r.SourcePackets),
			strconv.FormatInt(r.SourceBytes, 10),
			fmt.Sprintf("%.3f", r.SourcePSNR),
			r.SourceFallback,
			fmt.Sprintf("%.3f", r.FFMean),
			fmt.Sprintf("%.3f", r.FFStddev),
			fmt.Sprintf("%.6f", r.FramePctMean),
			fmt.Sprintf("%.6f", r.KeyPctMean),
			fmt.Sprintf("%.3f", r.RepairMean),
			fmt.Sprintf("%.3f", r.ReactiveMean),
			strconv.Itoa(r.Failed),
			strconv.Itoa(r.Seeds),
		})
	}
	return w.Error()
}

func writeMacroGapRows(path string, rows []macroGapRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{"case", "loss", "burst", "rtt_ms", "mult", "budget_ms", "burst_ms", "theory_meld_opportunity", "best_meld", "meld_source_mode", "meld_source_interval", "meld_source_packets", "arq_source_packets", "source_packet_delta_pct", "meld_source_bytes", "arq_source_bytes", "source_byte_delta_pct", "meld_source_psnr", "meld_source_fallback", "meld_ff", "meld_ff_stddev", "best_arq", "arq_ff", "arq_ff_stddev", "delta_ff", "delta_noise", "delta_stable", "delta_pct", "meld_frame_pct", "arq_frame_pct", "meld_key_pct", "arq_key_pct"})
	for _, r := range rows {
		w.Write([]string{
			r.Case,
			fmt.Sprintf("%.6f", r.Loss),
			fmt.Sprintf("%.3f", r.Burst),
			strconv.Itoa(r.RTT),
			fmt.Sprintf("%.3f", r.Mult),
			strconv.Itoa(r.Budget),
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
			fmt.Sprintf("%.3f", r.DeltaFF),
			fmt.Sprintf("%.3f", r.DeltaNoise),
			strconv.FormatBool(macroGapStable(r)),
			fmt.Sprintf("%.6f", r.DeltaPct),
			fmt.Sprintf("%.6f", r.MeldFrame),
			fmt.Sprintf("%.6f", r.ARQFrame),
			fmt.Sprintf("%.6f", r.MeldKey),
			fmt.Sprintf("%.6f", r.ARQKey),
		})
	}
	return w.Error()
}

func writeMacroFrontierMarkdown(path string, rows []macroGapRow, opts macroFrontierOptions) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Macro Frontier\n\n")
	fmt.Fprintf(&b, "Grid: losses `%v`, bursts `%v`, RTTs `%v`, multipliers `%v`, reps `%d`.\n\n",
		opts.Losses, opts.Bursts, opts.RTTs, opts.Mults, opts.Reps)
	fmt.Fprintf(&b, "Theory-opportunity cells are cells where a full ARQ retransmission is latency-tight (`budget < 1.5x RTT`) or burst duration exceeds post-RTT slack.\n\n")
	fmt.Fprintf(&b, "Gap rows compare the deployable Meld profile against the best ARQ-style arm. If `meld-auto` is present in the arm list, it is used instead of choosing the best experimental Meld variant.\n\n")
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
	fmt.Fprintf(b, "| case | best Meld | best ARQ | Meld ff | ARQ ff | delta ff | noise ff | stable | frame delta | key delta | source pkt delta | source byte delta | source PSNR | source fallback |\n")
	fmt.Fprintf(b, "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(b, "| `%s` | `%s` | `%s` | %.1f | %.1f | %+.1f | %.1f | %t | %+.1f%% | %+.1f%% | %s | %s | %s | `%s` |\n",
			r.Case, macroMeldLabel(r), r.BestARQ, r.MeldFF, r.ARQFF, r.DeltaFF, r.DeltaNoise, macroGapStable(r),
			(r.MeldFrame-r.ARQFrame)*100, (r.MeldKey-r.ARQKey)*100,
			formatSignedPct(macroSourcePacketDelta(r)), formatSignedPct(macroSourceByteDelta(r)),
			formatSourcePSNR(r.MeldSourcePSNR), r.MeldSourceFallback)
	}
	fmt.Fprintf(b, "\n")
}
