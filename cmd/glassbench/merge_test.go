package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMergeMacroFrontierShardsRequiresAndCombinesEveryRow(t *testing.T) {
	suite, ok := publishSuiteByName("smoke")
	if !ok {
		t.Fatal("smoke suite not found")
	}
	root := t.TempDir()
	out := filepath.Join(t.TempDir(), "merged")
	clip := "fixtures/test.h264"
	opts := macroFrontierOptions{
		SuiteName: suite.Name, SuiteDescription: suite.Description,
		Losses: suite.Losses, Bursts: suite.Bursts, RTTs: suite.RTTs, Mults: suite.Mults,
		Arms: suite.Arms, Reps: 3, Mbps: 8, ChunkSize: 1284, ShardCount: 2, TopN: 4,
	}
	writeSyntheticShards(t, root, suite, opts, clip)
	if err := mergeMacroFrontierShards(root, out, suite, opts, []string{clip}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	var audit mergeAudit
	if err := readJSONFile(filepath.Join(out, "COMPLETE.json"), &audit); err != nil {
		t.Fatalf("read completion marker: %v", err)
	}
	if !audit.Complete || len(audit.Sources) != 1 {
		t.Fatalf("unexpected audit: %+v", audit)
	}
	wantRows := macroTotalCells(opts) * len(suite.Arms)
	if audit.Sources[0].Rows != wantRows || audit.Sources[0].Cells != macroTotalCells(opts) {
		t.Fatalf("source audit = %+v, want %d rows", audit.Sources[0], wantRows)
	}

	// A completion marker cannot hide an incomplete repetition. The merger
	// re-audits every aggregate row rather than trusting marker counts alone.
	id := sourceIDForClip(clip)
	rowsPath := filepath.Join(root, "shard-000", id, "frontier_rows.json")
	var rows []macroFrontierRow
	if err := readJSONFile(rowsPath, &rows); err != nil {
		t.Fatal(err)
	}
	rows[0].Seeds = 2
	if err := writeJSONFile(rowsPath, rows); err != nil {
		t.Fatal(err)
	}
	err := mergeMacroFrontierShards(root, filepath.Join(t.TempDir(), "rejected"), suite, opts, []string{clip})
	if err == nil || !strings.Contains(err.Error(), "failed=0 seeds=2") {
		t.Fatalf("corrupt shard error = %v", err)
	}
}

func writeSyntheticShards(t *testing.T, root string, suite publishSuite, opts macroFrontierOptions, clip string) {
	t.Helper()
	id := sourceIDForClip(clip)
	format, err := formatForClip(clip)
	if err != nil {
		t.Fatal(err)
	}
	ordinal := 0
	byShard := make([][]macroFrontierRow, opts.ShardCount)
	for _, jitter := range macroJitterPlanes(opts) {
		for _, loss := range opts.Losses {
			for _, burst := range opts.Bursts {
				for _, rtt := range opts.RTTs {
					for _, mult := range opts.Mults {
						budget := int(mult*float64(rtt) + 0.5)
						if budget < opts.FloorMs {
							budget = opts.FloorMs
						}
						caseName := macroCaseName(loss, burst, rtt, mult, budget, jitter)
						shard := ordinal % opts.ShardCount
						ordinal++
						for armIndex, arm := range suite.Arms {
							ff := 80.0 + float64(armIndex)
							if arm == "oracle-source" || arm == "oracle-ideal" {
								ff = 100
							}
							byShard[shard] = append(byShard[shard], macroFrontierRow{
								SourceID: id, SourceClip: clip, SourceCodec: format.name(), SourceFFFrames: 100,
								Case: caseName, Loss: loss, Burst: burst, RTT: rtt, Mult: mult, Budget: budget, Jitter: jitter,
								Arm: arm, SourcePackets: 100, SourceBytes: 1000, FFMean: ff,
								FramePctMean: ff / 100, KeyPctMean: 1, Seeds: opts.Reps,
							})
						}
					}
				}
			}
		}
	}
	for shard, rows := range byShard {
		dir := filepath.Join(root, shardDirName(shard), id)
		if err := ensureDir(dir); err != nil {
			t.Fatal(err)
		}
		env := runEnvironment{
			Suite: suite.Name, SourceID: id, SourceClip: clip, SourceCodec: format.name(), SourceFF: 100, SourceRepeats: 1,
			Losses: opts.Losses, Bursts: opts.Bursts, RTTs: opts.RTTs, Multipliers: opts.Mults,
			JitterPlanes: macroJitterPlanes(opts), Arms: opts.Arms, Reps: opts.Reps, SeedSchedule: benchmarkSeeds(opts.Reps),
			SourceMbps: opts.Mbps, ChunkSize: opts.ChunkSize, ShardIndex: shard, ShardCount: opts.ShardCount,
			TotalCells: macroTotalCells(opts), ShardCells: macroShardCells(withShard(opts, shard)), BinarySHA256: "same-binary",
		}
		if err := writeJSONFile(filepath.Join(dir, "environment.json"), env); err != nil {
			t.Fatal(err)
		}
		if err := writeJSONFile(filepath.Join(dir, "frontier_rows.json"), rows); err != nil {
			t.Fatal(err)
		}
		complete := frontierComplete{Rows: len(rows), Cells: macroShardCells(withShard(opts, shard)), ShardIndex: shard, ShardCount: opts.ShardCount}
		if err := writeJSONFile(filepath.Join(dir, "COMPLETE.json"), complete); err != nil {
			t.Fatal(err)
		}
	}
}

func withShard(opts macroFrontierOptions, shard int) macroFrontierOptions {
	opts.ShardIndex = shard
	return opts
}

func shardDirName(shard int) string {
	return fmt.Sprintf("shard-%03d", shard)
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0o755)
}
