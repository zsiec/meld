package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type publishSuite struct {
	Name        string
	Description string
	Losses      []float64
	Bursts      []float64
	RTTs        []int
	Mults       []float64
	Arms        []string
	TopN        int
}

func publishSuiteByName(name string) (publishSuite, bool) {
	switch strings.TrimSpace(strings.ToLower(name)) {
	case "":
		return publishSuite{}, false
	case "smoke":
		return publishSuite{
			Name:        "smoke",
			Description: "Fast end-to-end publish-pipeline check with all primary arms.",
			Losses:      []float64{0.05},
			Bursts:      []float64{0},
			RTTs:        []int{100},
			Mults:       []float64{0.75, 1},
			Arms:        []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"},
			TopN:        8,
		}, true
	case "iid-frontier":
		return publishSuite{
			Name:        "iid-frontier",
			Description: "Primary low-latency iid/tail-erasure frontier where coded transport should beat ARQ below or near RTT.",
			Losses:      []float64{0, 0.01, 0.03, 0.05, 0.10},
			Bursts:      []float64{0},
			RTTs:        []int{50, 100, 200, 400},
			Mults:       []float64{0.5, 0.75, 1, 1.25, 1.5},
			Arms:        []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"},
			TopN:        12,
		}, true
	case "fallback-check":
		return publishSuite{
			Name:        "fallback-check",
			Description: "Conservative-region guard where ARQ should have enough RTT slack and Meld-auto should not create stable regressions.",
			Losses:      []float64{0, 0.01, 0.03, 0.05},
			Bursts:      []float64{0, 8},
			RTTs:        []int{50, 100, 200, 400},
			Mults:       []float64{2, 3},
			Arms:        []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"},
			TopN:        12,
		}, true
	case "bursty-frontier":
		return publishSuite{
			Name:        "bursty-frontier",
			Description: "Burst-loss discovery map. This is a thesis test, not a placement-tuning loop.",
			Losses:      []float64{0.03, 0.05, 0.10},
			Bursts:      []float64{8, 24, 48, 96},
			RTTs:        []int{50, 100, 200, 400},
			Mults:       []float64{0.75, 1, 1.5, 2, 3},
			Arms:        []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"},
			TopN:        12,
		}, true
	case "publish-core":
		return publishSuite{
			Name:        "publish-core",
			Description: "Broad publication battery: no-loss sanity, iid loss, burst loss, RTT scaling, and latency-budget scaling.",
			Losses:      []float64{0, 0.01, 0.03, 0.05, 0.10},
			Bursts:      []float64{0, 8, 24, 48},
			RTTs:        []int{20, 50, 100, 200, 400},
			Mults:       []float64{0.5, 0.75, 1, 1.5, 2, 3},
			Arms:        []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"},
			TopN:        16,
		}, true
	default:
		return publishSuite{}, false
	}
}

func publishSuiteNames() []string {
	names := []string{"smoke", "iid-frontier", "bursty-frontier", "fallback-check", "publish-core"}
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
	opts.Arms = suite.Arms
	if opts.TopN <= 0 {
		opts.TopN = suite.TopN
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return err
	}
	if err := writePublishPlan(filepath.Join(opts.OutDir, "PUBLISH.md"), suite, opts); err != nil {
		return err
	}
	return runMacroFrontier(c, opts)
}

type runEnvironment struct {
	GeneratedAt string            `json:"generated_at"`
	Command     []string          `json:"command"`
	Suite       string            `json:"suite,omitempty"`
	Description string            `json:"description,omitempty"`
	GOOS        string            `json:"goos"`
	GOARCH      string            `json:"goarch"`
	GoVersion   string            `json:"go_version"`
	GitRevision string            `json:"git_revision,omitempty"`
	GitDirty    bool              `json:"git_dirty"`
	SourceID    string            `json:"source_id,omitempty"`
	SourceClip  string            `json:"source_clip,omitempty"`
	SourceFF    int               `json:"source_ff_frames,omitempty"`
	Tools       map[string]string `json:"tools"`
}

func writeRunEnvironment(path string, opts macroFrontierOptions) error {
	env := runEnvironment{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Command:     append([]string(nil), os.Args...),
		Suite:       opts.SuiteName,
		Description: opts.SuiteDescription,
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		GoVersion:   runtime.Version(),
		GitRevision: commandLine("git", "rev-parse", "--short", "HEAD"),
		GitDirty:    strings.TrimSpace(commandLine("git", "status", "--short")) != "",
		SourceID:    opts.SourceID,
		SourceClip:  opts.SourceClip,
		SourceFF:    opts.SourceFFFrames,
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
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(env)
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
	fmt.Fprintf(&b, "- GE burst lengths: `%v` packets (`0` means iid)\n", suite.Bursts)
	fmt.Fprintf(&b, "- RTTs: `%v` ms\n", suite.RTTs)
	fmt.Fprintf(&b, "- Latency budgets: `%v` x RTT, with floor `%d ms`\n", suite.Mults, opts.FloorMs)
	fmt.Fprintf(&b, "- Reps: `%d` seeds per cell\n", opts.Reps)
	if opts.SourceID != "" {
		fmt.Fprintf(&b, "- Source: `%s` (`%s`)\n", opts.SourceID, opts.SourceClip)
	} else {
		fmt.Fprintf(&b, "- Clip: `%s`\n", opts.SourceClip)
	}
	fmt.Fprintf(&b, "- Chunk size: `%d` bytes; paced bitrate: `%.1f Mbps`\n\n", opts.ChunkSize, opts.Mbps)
	fmt.Fprintf(&b, "## Artifacts\n\n")
	fmt.Fprintf(&b, "- `environment.json`: command, git state, OS/arch, and tool versions\n")
	fmt.Fprintf(&b, "- `frontier_rows.csv`: one aggregate row per case/arm\n")
	fmt.Fprintf(&b, "- `frontier_gaps.csv`: deployable Meld vs best ARQ comparison rows\n")
	fmt.Fprintf(&b, "- `FRONTIER.md`: sorted frontier call and gap tables\n")
	fmt.Fprintf(&b, "- `FAIRNESS.md` / `fairness.csv`: source, oracle, arm-presence, and conservative-regression guards\n")
	fmt.Fprintf(&b, "- `failure_report.csv` / `failure_report.md`: first-failure dependency attribution\n")
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
