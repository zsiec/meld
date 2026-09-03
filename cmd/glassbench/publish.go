package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type publishSuite struct {
	Name            string
	Description     string
	Losses          []float64
	Bursts          []float64
	RTTs            []int
	Mults           []float64
	Jitters         []int
	Arms            []string
	TopN            int
	SourceRTTCycles int
	MinReps         int
	RequireCapacity bool
	RequireDeadline bool
}

// publishMaxChunkSize keeps source packets within the conservative payload
// ceiling shared by every external transport in the publication matrix.
const publishMaxChunkSize = 1284

func publicationChunkSize(requested int) int {
	if requested <= 0 || requested > publishMaxChunkSize {
		return publishMaxChunkSize
	}
	return requested
}

func primaryPublishArms() []string {
	return []string{
		"oracle-source", "oracle-ideal", "meld-auto",
		"libsrt", "libsrt-fec", "librist",
	}
}

func publishSuiteByName(name string) (publishSuite, bool) {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "":
		return publishSuite{}, false
	case "smoke":
		return publishSuite{
			Name:            "smoke",
			Description:     "Fast end-to-end publish-pipeline check with all primary arms.",
			Losses:          []float64{0.05},
			Bursts:          []float64{0},
			RTTs:            []int{100},
			Mults:           []float64{0.75, 1},
			Arms:            primaryPublishArms(),
			TopN:            8,
			SourceRTTCycles: 4,
		}, true
	case "full-envelope":
		return publishSuite{
			Name:            "full-envelope",
			Description:     "Complete capacity-matched cross-codec envelope spanning every publication loss, burst-memory, reorder, RTT, and deadline-ratio plane.",
			Losses:          []float64{0, 0.01, 0.03, 0.05, 0.10},
			Bursts:          []float64{0, 8, 24, 48, 96},
			RTTs:            []int{20, 50, 100, 200, 400},
			Mults:           []float64{0.5, 0.75, 1, 1.25, 1.5, 2, 3},
			Jitters:         []int{0, 10},
			Arms:            primaryPublishArms(),
			TopN:            24,
			SourceRTTCycles: 4,
			MinReps:         3,
			RequireCapacity: true,
			RequireDeadline: true,
		}, true
	case "codec-gate":
		return publishSuite{
			Name:            "codec-gate",
			Description:     "Cross-codec automatic-allocation gate spanning iid loss, medium/deep channel memory, tight deadlines, and a reactive-slack control.",
			Losses:          []float64{0.10},
			Bursts:          []float64{0, 24, 48},
			RTTs:            []int{100},
			Mults:           []float64{0.75, 1, 2},
			Arms:            primaryPublishArms(),
			TopN:            12,
			SourceRTTCycles: 4,
		}, true
	case "iid-frontier":
		return publishSuite{
			Name:            "iid-frontier",
			Description:     "Primary low-latency iid/tail-erasure frontier where coded transport should beat ARQ below or near RTT.",
			Losses:          []float64{0, 0.01, 0.03, 0.05, 0.10},
			Bursts:          []float64{0},
			RTTs:            []int{50, 100, 200, 400},
			Mults:           []float64{0.5, 0.75, 1, 1.25, 1.5},
			Arms:            primaryPublishArms(),
			TopN:            12,
			SourceRTTCycles: 4,
		}, true
	case "fallback-check":
		return publishSuite{
			Name:            "fallback-check",
			Description:     "Conservative-region guard where ARQ should have enough RTT slack and Meld-auto should not create stable regressions.",
			Losses:          []float64{0, 0.01, 0.03, 0.05},
			Bursts:          []float64{0, 8},
			RTTs:            []int{50, 100, 200, 400},
			Mults:           []float64{2, 3},
			Arms:            primaryPublishArms(),
			TopN:            12,
			SourceRTTCycles: 4,
		}, true
	case "bursty-frontier":
		return publishSuite{
			Name:            "bursty-frontier",
			Description:     "Burst-loss discovery map. This is a thesis test, not a placement-tuning loop.",
			Losses:          []float64{0.03, 0.05, 0.10},
			Bursts:          []float64{8, 24, 48, 96},
			RTTs:            []int{50, 100, 200, 400},
			Mults:           []float64{0.75, 1, 1.5, 2, 3},
			Arms:            primaryPublishArms(),
			TopN:            12,
			SourceRTTCycles: 4,
		}, true
	case "publish-core":
		return publishSuite{
			Name:            "publish-core",
			Description:     "Broad publication battery: no-loss sanity, iid loss, burst loss, RTT scaling, and latency-budget scaling.",
			Losses:          []float64{0, 0.01, 0.03, 0.05, 0.10},
			Bursts:          []float64{0, 8, 24, 48},
			RTTs:            []int{20, 50, 100, 200, 400},
			Mults:           []float64{0.5, 0.75, 1, 1.5, 2, 3},
			Arms:            primaryPublishArms(),
			TopN:            16,
			SourceRTTCycles: 4,
		}, true
	default:
		return publishSuite{}, false
	}
}

func publishSuiteNames() []string {
	names := []string{"smoke", "codec-gate", "full-envelope", "iid-frontier", "bursty-frontier", "fallback-check", "publish-core"}
	sort.Strings(names)
	return names
}

func runPublishSuite(c *chunked, suite publishSuite, opts macroFrontierOptions) error {
	if opts.OutDir == "" {
		return fmt.Errorf("-publishsuite requires -reportdir so artifacts are preserved")
	}
	opts.SuiteName = suite.Name
	opts.SuiteDescription = suite.Description
	opts.Losses = suite.Losses
	opts.Bursts = suite.Bursts
	opts.RTTs = suite.RTTs
	opts.Mults = suite.Mults
	if len(suite.Jitters) > 0 {
		opts.JitterPlanes = append([]int(nil), suite.Jitters...)
	}
	opts.Arms = suite.Arms
	opts.SourceRepeats = sourceRepeatsForHorizon(c, opts.PaceUs, suite.RTTs, suite.SourceRTTCycles)
	if opts.TopN <= 0 {
		opts.TopN = suite.TopN
	}
	if suite.MinReps > 0 && opts.Reps < suite.MinReps {
		return fmt.Errorf("suite %s requires at least %d matched repetitions (got %d)", suite.Name, suite.MinReps, opts.Reps)
	}
	if suite.RequireCapacity {
		if opts.WireMbps <= 0 || opts.MeldMax <= 0 {
			return fmt.Errorf("suite %s requires positive -wirembps and -maxmbps", suite.Name)
		}
		if math.Abs(float64(opts.MeldMax)/1e6-opts.WireMbps) > 1e-9 {
			return fmt.Errorf("suite %s requires the same Meld and shared-link capacity: max %.3f Mbps, wire %.3f Mbps",
				suite.Name, float64(opts.MeldMax)/1e6, opts.WireMbps)
		}
	}
	if suite.RequireDeadline && !*deadlineArbiter {
		return fmt.Errorf("suite %s requires -deadlinearbiter", suite.Name)
	}
	if suite.Name == "full-envelope" && opts.FloorMs != 0 {
		return fmt.Errorf("suite %s requires -buf 0 so every deadline ratio is measured exactly", suite.Name)
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return err
	}
	if err := writePublishPlan(filepath.Join(opts.OutDir, "PUBLISH.md"), suite, opts); err != nil {
		return err
	}
	return runMacroFrontier(c, opts)
}

func sourceRepeatsForHorizon(c *chunked, paceUs int64, rtts []int, cycles int) int {
	if c == nil || len(c.chunks) == 0 || paceUs <= 0 || cycles <= 0 {
		return 1
	}
	maxRTT := 0
	for _, rtt := range rtts {
		if rtt > maxRTT {
			maxRTT = rtt
		}
	}
	if maxRTT <= 0 {
		return 1
	}
	targetUs := int64(cycles) * int64(maxRTT) * 1000
	clipUs := int64(len(c.chunks)) * paceUs
	repeats := int((targetUs + clipUs - 1) / clipUs)
	if repeats < 1 {
		return 1
	}
	return repeats
}

type runEnvironment struct {
	GeneratedAt   string            `json:"generated_at"`
	Command       []string          `json:"command"`
	Suite         string            `json:"suite,omitempty"`
	Description   string            `json:"description,omitempty"`
	GOOS          string            `json:"goos"`
	GOARCH        string            `json:"goarch"`
	GoVersion     string            `json:"go_version"`
	GitRevision   string            `json:"git_revision,omitempty"`
	GitDirty      bool              `json:"git_dirty"`
	SourceID      string            `json:"source_id,omitempty"`
	SourceClip    string            `json:"source_clip,omitempty"`
	SourceCodec   string            `json:"source_codec,omitempty"`
	SourceFF      int               `json:"source_ff_frames,omitempty"`
	SourceRepeats int               `json:"source_repeats"`
	Losses        []float64         `json:"losses"`
	Bursts        []float64         `json:"bursts"`
	RTTs          []int             `json:"rtts_ms"`
	Multipliers   []float64         `json:"deadline_multipliers"`
	JitterPlanes  []int             `json:"jitter_planes_ms"`
	Arms          []string          `json:"arms"`
	Reps          int               `json:"repetitions"`
	SeedSchedule  []int64           `json:"seed_schedule"`
	SourceMbps    float64           `json:"source_mbps"`
	WireMbps      float64           `json:"wire_mbps"`
	MeldMaxMbps   float64           `json:"meld_max_mbps"`
	ChunkSize     int               `json:"chunk_size"`
	DeadlineGate  bool              `json:"deadline_arbiter"`
	ShardIndex    int               `json:"shard_index"`
	ShardCount    int               `json:"shard_count"`
	TotalCells    int               `json:"total_cells"`
	ShardCells    int               `json:"shard_cells"`
	BinarySHA256  string            `json:"binary_sha256"`
	Tools         map[string]string `json:"tools"`
}

func writeRunEnvironment(path string, opts macroFrontierOptions) (err error) {
	env := runEnvironment{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Command:       append([]string(nil), os.Args...),
		Suite:         opts.SuiteName,
		Description:   opts.SuiteDescription,
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		GoVersion:     runtime.Version(),
		GitRevision:   commandLine("git", "rev-parse", "--short", "HEAD"),
		GitDirty:      strings.TrimSpace(commandLine("git", "status", "--short")) != "",
		SourceID:      opts.SourceID,
		SourceClip:    opts.SourceClip,
		SourceCodec:   opts.SourceCodec,
		SourceFF:      opts.SourceFFFrames,
		SourceRepeats: opts.SourceRepeats,
		Losses:        append([]float64(nil), opts.Losses...),
		Bursts:        append([]float64(nil), opts.Bursts...),
		RTTs:          append([]int(nil), opts.RTTs...),
		Multipliers:   append([]float64(nil), opts.Mults...),
		JitterPlanes:  append([]int(nil), macroJitterPlanes(opts)...),
		Arms:          append([]string(nil), opts.Arms...),
		Reps:          opts.Reps,
		SeedSchedule:  benchmarkSeeds(opts.Reps),
		SourceMbps:    opts.Mbps,
		WireMbps:      opts.WireMbps,
		MeldMaxMbps:   float64(opts.MeldMax) / 1e6,
		ChunkSize:     opts.ChunkSize,
		DeadlineGate:  deadlineArbiter != nil && *deadlineArbiter,
		ShardIndex:    opts.ShardIndex,
		ShardCount:    opts.ShardCount,
		TotalCells:    macroTotalCells(opts),
		ShardCells:    macroShardCells(opts),
		BinarySHA256:  runningExecutableSHA256(),
		Tools: map[string]string{
			"ffmpeg":            toolVersion("ffmpeg", "-version"),
			"ffprobe":           toolVersion("ffprobe", "-version"),
			"srt-live-transmit": toolVersion("srt-live-transmit", "-version"),
			"ristreceiver":      localToolPresence("dev/librist/build/tools/ristreceiver"),
			"ristsender":        localToolPresence("dev/librist/build/tools/ristsender"),
		},
	}
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
	return enc.Encode(env)
}

func runningExecutableSHA256() string {
	path, err := os.Executable()
	if err != nil {
		return ""
	}
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func commandLine(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func toolVersion(name string, args ...string) string {
	if _, err := exec.LookPath(name); err != nil {
		return "missing"
	}
	out := commandLine(name, args...)
	if out == "" {
		return "present"
	}
	if i := strings.IndexByte(out, '\n'); i >= 0 {
		out = out[:i]
	}
	return strings.TrimSpace(out)
}

func localToolPresence(rel string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "unknown"
	}
	path := filepath.Join(home, rel)
	if st, err := os.Stat(path); err == nil && !st.IsDir() && st.Mode()&0o111 != 0 {
		return path
	}
	return "missing"
}

func writePublishPlan(path string, suite publishSuite, opts macroFrontierOptions) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Publish Benchmark Run\n\n")
	fmt.Fprintf(&b, "Suite: `%s`\n\n%s\n\n", suite.Name, suite.Description)
	fmt.Fprintf(&b, "This run is designed to produce defensible publication artifacts, not a tuning loop. Generated raw output stays in `scratchpad/`; curated summaries can be copied into docs after review.\n\n")
	fmt.Fprintf(&b, "## Industry Measurement Points\n\n")
	for _, row := range benchmarkMeasurementPoints() {
		fmt.Fprintf(&b, "- **%s:** %s\n", row.Name, row.Purpose)
	}
	fmt.Fprintf(&b, "\n## Grid\n\n")
	fmt.Fprintf(&b, "- Arms: `%s`\n", strings.Join(suite.Arms, ","))
	fmt.Fprintf(&b, "- Losses: `%v`\n", suite.Losses)
	fmt.Fprintf(&b, "- GE burst lengths: `%v` source-packet time quanta (`0` means iid; channel state does not advance on extra transport packets)\n", suite.Bursts)
	fmt.Fprintf(&b, "- RTTs: `%v` ms\n", suite.RTTs)
	fmt.Fprintf(&b, "- Latency budgets: `%v` x RTT, with floor `%d ms`\n", suite.Mults, opts.FloorMs)
	fmt.Fprintf(&b, "- Forward jitter/reorder planes: `%v` ms\n", macroJitterPlanes(opts))
	fmt.Fprintf(&b, "- Reps: `%d` seeds per cell\n", opts.Reps)
	fmt.Fprintf(&b, "- Seed schedule: `%v` (repetition `r` uses `7919*r + 13` in every benchmark mode)\n", benchmarkSeeds(opts.Reps))
	fmt.Fprintf(&b, "- Shard: `%d/%d` (`%d` of `%d` cells)\n", opts.ShardIndex, opts.ShardCount, macroShardCells(opts), macroTotalCells(opts))
	if opts.SourceID != "" {
		fmt.Fprintf(&b, "- Source: `%s` (`%s`, codec `%s`)\n", opts.SourceID, opts.SourceClip, opts.SourceCodec)
	} else {
		fmt.Fprintf(&b, "- Clip: `%s`\n", opts.SourceClip)
	}
	fmt.Fprintf(&b, "- Source repetitions: `%d` (at least `%d` cycles of the suite's largest RTT)\n", opts.SourceRepeats, suite.SourceRTTCycles)
	fmt.Fprintf(&b, "- Chunk size: `%d` bytes; paced bitrate: `%.1f Mbps`\n", opts.ChunkSize, opts.Mbps)
	if opts.WireMbps > 0 {
		fmt.Fprintf(&b, "- Shared forward-link capacity: `%.1f Mbps`; Meld source/repair ceiling: `%.1f Mbps`\n\n", opts.WireMbps, float64(opts.MeldMax)/1e6)
	} else {
		fmt.Fprintf(&b, "- Shared forward-link capacity: `unbounded` (pipeline/discovery only; not valid for equal-capacity cost claims)\n\n")
	}
	fmt.Fprintf(&b, "## Artifacts\n\n")
	fmt.Fprintf(&b, "- `environment.json`: command, git state, OS/arch, and tool versions\n")
	fmt.Fprintf(&b, "- `frontier_rows.json`: lossless machine-readable aggregate rows for shard merging\n")
	fmt.Fprintf(&b, "- `frontier_rows.csv`: one aggregate row per case/arm\n")
	fmt.Fprintf(&b, "- `frontier_gaps.csv`: deployable Meld vs best SRT/RIST competitor rows\n")
	fmt.Fprintf(&b, "- `FRONTIER.md`: sorted frontier call and gap tables\n")
	fmt.Fprintf(&b, "- `FAIRNESS.md` / `fairness.csv`: source, oracle, arm-presence, and conservative-regression guards\n")
	fmt.Fprintf(&b, "- `failure_report.csv` / `failure_report.md`: first-failure dependency attribution and links to the worst failing Meld trace in each cell\n")
	fmt.Fprintf(&b, "- `seed_trace_*.json`: exact relay, feedback, arrival, score, and protocol-counter evidence for those selected Meld failures\n")
	fmt.Fprintf(&b, "- `charts/*.svg`: publication-ready visual summaries\n")
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

type measurementPoint struct {
	Name    string
	Purpose string
}

func benchmarkMeasurementPoints() []measurementPoint {
	return []measurementPoint{
		{"Zero-loss sanity", "proves the source, chunker, transport shims, and ffprobe arbiter can reach the source ceiling."},
		{"Latency budget versus RTT", "separates below-RTT, one-RTT, and generous-buffer regimes where ARQ has different physical opportunity."},
		{"RTT scaling", "shows whether the transport depends on round-trip recovery or can recover before feedback returns."},
		{"Iid erasure tolerance", "measures random packet loss, the stable low-latency frontier where coded transport should have an advantage."},
		{"Burst/tail erasure tolerance", "tests Gilbert-Elliott loss runs and exposes dependency-island damage under longer fades."},
		{"Reorder/jitter sensitivity", "uses forward jitter where enabled to test whether loss estimation confuses reorder with erasure."},
		{"Media decodability", "uses ffprobe decoded frames plus frame/keyframe completeness, not packet delivery alone."},
		{"Repair overhead", "records Meld proactive and reactive repair counts so gains can be weighed against redundancy cost."},
		{"Oracle ceilings", "distinguishes protocol gaps from source-decoding or physical deadline ceilings."},
		{"Per-seed failure attribution", "records the first damaged dependency island and whether repair existed, arrived in time, or was erased."},
	}
}
