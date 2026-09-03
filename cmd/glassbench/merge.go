package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
)

type frontierComplete struct {
	Rows       int `json:"rows"`
	Cells      int `json:"cells"`
	ShardIndex int `json:"shard_index"`
	ShardCount int `json:"shard_count"`
}

type mergeSourceAudit struct {
	SourceID       string `json:"source_id"`
	SourceClip     string `json:"source_clip"`
	SourceCodec    string `json:"source_codec"`
	Shards         int    `json:"shards"`
	Cells          int    `json:"cells"`
	Rows           int    `json:"rows"`
	ArmsPerCell    int    `json:"arms_per_cell"`
	Repetitions    int    `json:"repetitions"`
	FailedRows     int    `json:"failed_rows"`
	MissingRows    int    `json:"missing_rows"`
	DuplicateRows  int    `json:"duplicate_rows"`
	FairnessIssues int    `json:"fairness_issues"`
}

type mergeAudit struct {
	GeneratedAt  string             `json:"generated_at"`
	Suite        string             `json:"suite"`
	ShardRoot    string             `json:"shard_root"`
	BinarySHA256 string             `json:"binary_sha256"`
	ShardCount   int                `json:"shard_count"`
	Sources      []mergeSourceAudit `json:"sources"`
	Complete     bool               `json:"complete"`
}

type mergeSourceSpec struct {
	ID    string
	Clip  string
	Codec string
}

type macroCaseSpec struct {
	Loss   float64
	Burst  float64
	RTT    int
	Mult   float64
	Budget int
	Jitter int
}

type shardArtifact struct {
	Dir      string
	Env      runEnvironment
	Complete frontierComplete
	Rows     []macroFrontierRow
}

func mergeMacroFrontierShards(shardRoot, outDir string, suite publishSuite, opts macroFrontierOptions, clips []string) error {
	if err := validateMergeRequest(shardRoot, outDir, suite, opts); err != nil {
		return err
	}
	sources, err := expectedMergeSources(suite, clips)
	if err != nil {
		return err
	}
	caseSpecs, err := expectedMacroCases(opts)
	if err != nil {
		return err
	}
	artifacts, binaryHash, err := loadShardArtifacts(shardRoot, suite, opts, sources)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	audit := mergeAudit{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		Suite:        suite.Name,
		ShardRoot:    shardRoot,
		BinarySHA256: binaryHash,
		ShardCount:   opts.ShardCount,
		Complete:     true,
	}
	for _, source := range sources {
		rows := make([]macroFrontierRow, 0, len(caseSpecs)*len(suite.Arms))
		var firstEnv runEnvironment
		for shard := 0; shard < opts.ShardCount; shard++ {
			artifact := artifacts[mergeArtifactKey(source.ID, shard)]
			if shard == 0 {
				firstEnv = artifact.Env
			}
			rows = append(rows, artifact.Rows...)
		}
		sourceAudit, err := auditMergedSource(rows, source, suite, opts, caseSpecs)
		if err != nil {
			return err
		}
		audit.Sources = append(audit.Sources, sourceAudit)

		sourceOpts := opts
		sourceOpts.OutDir = filepath.Join(outDir, source.ID)
		sourceOpts.ShardCount = 1
		sourceOpts.ShardIndex = 0
		sourceOpts.SourceID = source.ID
		sourceOpts.SourceClip = firstEnv.SourceClip
		sourceOpts.SourceCodec = source.Codec
		sourceOpts.SourceRepeats = firstEnv.SourceRepeats
		sourceOpts.SourceFFFrames = firstEnv.SourceFF
		sourceOpts.TotalPics = firstEnv.SourceFF
		if err := writeMergedSource(sourceOpts.OutDir, rows, sourceAudit, sourceOpts); err != nil {
			return fmt.Errorf("write merged source %s: %w", source.ID, err)
		}
	}
	if err := writeMergeIndex(outDir, audit); err != nil {
		return err
	}
	if err := writeJSONFile(filepath.Join(outDir, "MERGE_AUDIT.json"), audit); err != nil {
		return err
	}
	// This marker is deliberately last. Its presence means every validation and
	// every merged report completed successfully.
	return writeJSONFile(filepath.Join(outDir, "COMPLETE.json"), audit)
}

func validateMergeRequest(shardRoot, outDir string, suite publishSuite, opts macroFrontierOptions) error {
	if strings.TrimSpace(shardRoot) == "" || strings.TrimSpace(outDir) == "" {
		return fmt.Errorf("shard root and output directory are required")
	}
	if opts.ShardCount < 1 {
		return fmt.Errorf("frontier shard count must be positive")
	}
	if suite.MinReps > 0 && opts.Reps < suite.MinReps {
		return fmt.Errorf("suite %s requires at least %d repetitions (got %d)", suite.Name, suite.MinReps, opts.Reps)
	}
	if suite.RequireCapacity {
		if opts.WireMbps <= 0 || opts.MeldMax <= 0 {
			return fmt.Errorf("suite %s requires positive matched capacities", suite.Name)
		}
		if !sameFloat(float64(opts.MeldMax)/1e6, opts.WireMbps) {
			return fmt.Errorf("suite %s requires equal Meld and shared-link capacities", suite.Name)
		}
	}
	if suite.RequireDeadline && (deadlineArbiter == nil || !*deadlineArbiter) {
		return fmt.Errorf("suite %s requires -deadlinearbiter", suite.Name)
	}
	if suite.Name == "full-envelope" && opts.FloorMs != 0 {
		return fmt.Errorf("suite %s requires -buf 0", suite.Name)
	}
	return nil
}

func expectedMergeSources(suite publishSuite, clips []string) ([]mergeSourceSpec, error) {
	if len(clips) == 0 {
		return nil, fmt.Errorf("no source clips supplied")
	}
	seenID := map[string]bool{}
	seenCodec := map[string]bool{}
	out := make([]mergeSourceSpec, 0, len(clips))
	for _, clip := range clips {
		format, err := formatForClip(clip)
		if err != nil {
			return nil, err
		}
		id := sourceIDForClip(clip)
		if seenID[id] {
			return nil, fmt.Errorf("duplicate source id %q", id)
		}
		seenID[id] = true
		seenCodec[format.name()] = true
		out = append(out, mergeSourceSpec{ID: id, Clip: clip, Codec: format.name()})
	}
	if suite.Name == "full-envelope" {
		for _, codec := range []string{"avc", "hevc", "av1"} {
			if !seenCodec[codec] {
				return nil, fmt.Errorf("suite %s requires an %s source", suite.Name, codec)
			}
		}
		if len(out) != 3 {
			return nil, fmt.Errorf("suite %s requires exactly one AVC, HEVC, and AV1 source (got %d sources)", suite.Name, len(out))
		}
	}
	return out, nil
}

func expectedMacroCases(opts macroFrontierOptions) (map[string]macroCaseSpec, error) {
	out := make(map[string]macroCaseSpec, macroTotalCells(opts))
	for _, jitter := range macroJitterPlanes(opts) {
		for _, loss := range opts.Losses {
			for _, burst := range opts.Bursts {
				for _, rtt := range opts.RTTs {
					for _, mult := range opts.Mults {
						budget := int(mult*float64(rtt) + 0.5)
						if budget < opts.FloorMs {
							budget = opts.FloorMs
						}
						name := macroCaseName(loss, burst, rtt, mult, budget, jitter)
						if _, exists := out[name]; exists {
							return nil, fmt.Errorf("grid produces duplicate case name %q", name)
						}
						out[name] = macroCaseSpec{loss, burst, rtt, mult, budget, jitter}
					}
				}
			}
		}
	}
	return out, nil
}

func loadShardArtifacts(shardRoot string, suite publishSuite, opts macroFrontierOptions, sources []mergeSourceSpec) (map[string]shardArtifact, string, error) {
	paths, err := filepath.Glob(filepath.Join(shardRoot, "shard-*", "*", "frontier_rows.json"))
	if err != nil {
		return nil, "", err
	}
	if len(paths) == 0 {
		return nil, "", fmt.Errorf("no shard frontier_rows.json files found under %s", shardRoot)
	}
	expectedSource := map[string]mergeSourceSpec{}
	for _, source := range sources {
		expectedSource[source.ID] = source
	}
	artifacts := make(map[string]shardArtifact, len(paths))
	binaryHash := ""
	for _, rowsPath := range paths {
		dir := filepath.Dir(rowsPath)
		var env runEnvironment
		if err := readJSONFile(filepath.Join(dir, "environment.json"), &env); err != nil {
			return nil, "", fmt.Errorf("read %s environment: %w", dir, err)
		}
		var complete frontierComplete
		if err := readJSONFile(filepath.Join(dir, "COMPLETE.json"), &complete); err != nil {
			return nil, "", fmt.Errorf("read %s completion marker: %w", dir, err)
		}
		var rows []macroFrontierRow
		if err := readJSONFile(rowsPath, &rows); err != nil {
			return nil, "", fmt.Errorf("read %s: %w", rowsPath, err)
		}
		source, ok := expectedSource[env.SourceID]
		if !ok {
			return nil, "", fmt.Errorf("unexpected source %q in %s", env.SourceID, dir)
		}
		if err := validateShardEnvironment(env, complete, len(rows), source, suite, opts); err != nil {
			return nil, "", fmt.Errorf("invalid shard %s: %w", dir, err)
		}
		if binaryHash == "" {
			binaryHash = env.BinarySHA256
		} else if env.BinarySHA256 != binaryHash {
			return nil, "", fmt.Errorf("binary SHA-256 mismatch in %s: %s != %s", dir, env.BinarySHA256, binaryHash)
		}
		key := mergeArtifactKey(env.SourceID, env.ShardIndex)
		if _, exists := artifacts[key]; exists {
			return nil, "", fmt.Errorf("duplicate artifact for source %s shard %d", env.SourceID, env.ShardIndex)
		}
		artifacts[key] = shardArtifact{Dir: dir, Env: env, Complete: complete, Rows: rows}
	}
	for _, source := range sources {
		for shard := 0; shard < opts.ShardCount; shard++ {
			if _, ok := artifacts[mergeArtifactKey(source.ID, shard)]; !ok {
				return nil, "", fmt.Errorf("missing source %s shard %d/%d", source.ID, shard, opts.ShardCount)
			}
		}
	}
	if len(artifacts) != len(sources)*opts.ShardCount {
		return nil, "", fmt.Errorf("found %d artifacts, want exactly %d", len(artifacts), len(sources)*opts.ShardCount)
	}
	return artifacts, binaryHash, nil
}

func validateShardEnvironment(env runEnvironment, complete frontierComplete, rowCount int, source mergeSourceSpec, suite publishSuite, opts macroFrontierOptions) error {
	if env.Suite != suite.Name {
		return fmt.Errorf("suite = %q, want %q", env.Suite, suite.Name)
	}
	if env.SourceID != source.ID || env.SourceCodec != source.Codec {
		return fmt.Errorf("source = %s/%s, want %s/%s", env.SourceID, env.SourceCodec, source.ID, source.Codec)
	}
	if env.ShardCount != opts.ShardCount || env.ShardIndex < 0 || env.ShardIndex >= opts.ShardCount {
		return fmt.Errorf("shard = %d/%d, want count %d", env.ShardIndex, env.ShardCount, opts.ShardCount)
	}
	if env.BinarySHA256 == "" {
		return fmt.Errorf("missing executable SHA-256")
	}
	if !reflect.DeepEqual(env.Losses, opts.Losses) || !reflect.DeepEqual(env.Bursts, opts.Bursts) ||
		!reflect.DeepEqual(env.RTTs, opts.RTTs) || !reflect.DeepEqual(env.Multipliers, opts.Mults) ||
		!reflect.DeepEqual(env.JitterPlanes, macroJitterPlanes(opts)) || !reflect.DeepEqual(env.Arms, opts.Arms) {
		return fmt.Errorf("grid or arm list does not match requested suite")
	}
	if env.Reps != opts.Reps || !reflect.DeepEqual(env.SeedSchedule, benchmarkSeeds(opts.Reps)) {
		return fmt.Errorf("repetitions/seeds = %d/%v, want %d/%v", env.Reps, env.SeedSchedule, opts.Reps, benchmarkSeeds(opts.Reps))
	}
	if !sameFloat(env.SourceMbps, opts.Mbps) || !sameFloat(env.WireMbps, opts.WireMbps) ||
		!sameFloat(env.MeldMaxMbps, float64(opts.MeldMax)/1e6) || env.ChunkSize != opts.ChunkSize {
		return fmt.Errorf("capacity/source configuration differs from merge request")
	}
	wantDeadline := deadlineArbiter != nil && *deadlineArbiter
	if env.DeadlineGate != wantDeadline || (suite.RequireDeadline && !env.DeadlineGate) {
		return fmt.Errorf("deadline arbiter = %t, want %t", env.DeadlineGate, wantDeadline)
	}
	wantCells := macroShardCells(macroFrontierOptions{
		Losses:       opts.Losses,
		Bursts:       opts.Bursts,
		RTTs:         opts.RTTs,
		Mults:        opts.Mults,
		JitterPlanes: macroJitterPlanes(opts),
		ShardCount:   opts.ShardCount,
		ShardIndex:   env.ShardIndex,
	})
	wantRows := wantCells * len(supportedArms(opts.Arms))
	if env.TotalCells != macroTotalCells(opts) || env.ShardCells != wantCells {
		return fmt.Errorf("environment cells = %d/%d, want %d/%d", env.ShardCells, env.TotalCells, wantCells, macroTotalCells(opts))
	}
	if complete.ShardIndex != env.ShardIndex || complete.ShardCount != opts.ShardCount || complete.Cells != wantCells || complete.Rows != wantRows {
		return fmt.Errorf("completion marker = shard %d/%d, %d cells, %d rows; want shard %d/%d, %d cells, %d rows",
			complete.ShardIndex, complete.ShardCount, complete.Cells, complete.Rows,
			env.ShardIndex, opts.ShardCount, wantCells, wantRows)
	}
	if rowCount != wantRows {
		return fmt.Errorf("frontier row count = %d, want %d", rowCount, wantRows)
	}
	if suite.Name == "full-envelope" {
		for _, tool := range []string{"ffprobe", "srt-live-transmit", "ristreceiver", "ristsender"} {
			if value := env.Tools[tool]; value == "" || value == "missing" {
				return fmt.Errorf("required tool %s was not recorded as available", tool)
			}
		}
	}
	return nil
}

func auditMergedSource(rows []macroFrontierRow, source mergeSourceSpec, suite publishSuite, opts macroFrontierOptions, cases map[string]macroCaseSpec) (mergeSourceAudit, error) {
	wantArms := supportedArms(suite.Arms)
	wantArm := make(map[string]bool, len(wantArms))
	armOrder := make(map[string]int, len(wantArms))
	for i, arm := range wantArms {
		wantArm[arm] = true
		armOrder[arm] = i
	}
	audit := mergeSourceAudit{
		SourceID:    source.ID,
		SourceClip:  source.Clip,
		SourceCodec: source.Codec,
		Shards:      opts.ShardCount,
		Cells:       len(cases),
		Rows:        len(rows),
		ArmsPerCell: len(wantArms),
		Repetitions: opts.Reps,
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		spec, ok := cases[row.Case]
		if !ok {
			return audit, fmt.Errorf("source %s has unexpected case %q", source.ID, row.Case)
		}
		if row.SourceID != source.ID || row.SourceCodec != source.Codec {
			return audit, fmt.Errorf("source metadata mismatch in %s/%s", row.Case, row.Arm)
		}
		if !wantArm[row.Arm] {
			return audit, fmt.Errorf("source %s case %s has unexpected arm %q", source.ID, row.Case, row.Arm)
		}
		if !sameFloat(row.Loss, spec.Loss) || !sameFloat(row.Burst, spec.Burst) || row.RTT != spec.RTT ||
			!sameFloat(row.Mult, spec.Mult) || row.Budget != spec.Budget || row.Jitter != spec.Jitter {
			return audit, fmt.Errorf("source %s case %s has inconsistent grid coordinates", source.ID, row.Case)
		}
		key := row.Case + "\x00" + row.Arm
		if seen[key] {
			audit.DuplicateRows++
			return audit, fmt.Errorf("source %s duplicates %s/%s", source.ID, row.Case, row.Arm)
		}
		seen[key] = true
		if row.Failed != 0 || row.Seeds != opts.Reps {
			audit.FailedRows++
			return audit, fmt.Errorf("source %s case %s arm %s has failed=%d seeds=%d, want 0/%d", source.ID, row.Case, row.Arm, row.Failed, row.Seeds, opts.Reps)
		}
	}
	for caseName := range cases {
		for _, arm := range wantArms {
			if !seen[caseName+"\x00"+arm] {
				audit.MissingRows++
				return audit, fmt.Errorf("source %s is missing %s/%s", source.ID, caseName, arm)
			}
		}
	}
	wantRows := len(cases) * len(wantArms)
	if len(rows) != wantRows {
		return audit, fmt.Errorf("source %s has %d rows, want %d", source.ID, len(rows), wantRows)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Case != rows[j].Case {
			return rows[i].Case < rows[j].Case
		}
		return armOrder[rows[i].Arm] < armOrder[rows[j].Arm]
	})
	fairnessOpts := opts
	if len(rows) > 0 {
		fairnessOpts.SourceFFFrames = rows[0].SourceFFFrames
		fairnessOpts.TotalPics = rows[0].SourceFFFrames
	}
	gaps := macroGapRows(rows, fairnessOpts)
	for _, row := range fairnessRows(rows, gaps, fairnessOpts) {
		if len(row.MissingArms) > 0 || !row.MeldAutoPresent || !row.ARQPresent || !row.SameSourcePackets || !row.SameSourceBytes || !row.SourceCeilingOK {
			audit.FairnessIssues++
		}
	}
	if audit.FairnessIssues > 0 {
		return audit, fmt.Errorf("source %s has %d fairness/oracle issues", source.ID, audit.FairnessIssues)
	}
	return audit, nil
}

func writeMergedSource(outDir string, rows []macroFrontierRow, audit mergeSourceAudit, opts macroFrontierOptions) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	gaps := macroGapRows(rows, opts)
	if err := writeJSONFile(filepath.Join(outDir, "frontier_rows.json"), rows); err != nil {
		return err
	}
	if err := writeMacroFrontierRows(filepath.Join(outDir, "frontier_rows.csv"), rows); err != nil {
		return err
	}
	if err := writeMacroGapRows(filepath.Join(outDir, "frontier_gaps.csv"), gaps); err != nil {
		return err
	}
	if err := writeMacroFrontierMarkdown(filepath.Join(outDir, "FRONTIER.md"), gaps, opts); err != nil {
		return err
	}
	if err := writeFairnessReports(outDir, rows, gaps, opts); err != nil {
		return err
	}
	if err := writeMacroCharts(outDir, rows, gaps, opts); err != nil {
		return err
	}
	return writeJSONFile(filepath.Join(outDir, "MERGE_AUDIT.json"), audit)
}

func writeMergeIndex(outDir string, audit mergeAudit) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Complete Publish Envelope\n\n")
	fmt.Fprintf(&b, "Suite: `%s`\n\n", audit.Suite)
	fmt.Fprintf(&b, "All listed results passed exact shard, cell, arm, seed, capacity, deadline, source-equivalence, oracle-ceiling, and executable-identity checks.\n\n")
	fmt.Fprintf(&b, "- Shards: `%d`\n", audit.ShardCount)
	fmt.Fprintf(&b, "- Executable SHA-256: `%s`\n\n", audit.BinarySHA256)
	fmt.Fprintf(&b, "| source | codec | cells | rows | reports |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | ---: | --- |\n")
	for _, source := range audit.Sources {
		fmt.Fprintf(&b, "| `%s` | `%s` | %d | %d | [frontier](%s/FRONTIER.md), [fairness](%s/FAIRNESS.md), [audit](%s/MERGE_AUDIT.json) |\n",
			source.SourceID, source.SourceCodec, source.Cells, source.Rows, source.SourceID, source.SourceID, source.SourceID)
	}
	return os.WriteFile(filepath.Join(outDir, "README.md"), []byte(b.String()), 0o644)
}

func mergeArtifactKey(sourceID string, shard int) string {
	return fmt.Sprintf("%s\x00%d", sourceID, shard)
}

func sameFloat(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9
}

func readJSONFile(path string, dst any) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	return json.NewDecoder(f).Decode(dst)
}
