package main

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

func TestSweepSupportsDefaultArms(t *testing.T) {
	for _, arm := range []string{
		"meld",
		"meld-auto",
		"meld-flat",
		"meld-flat-unit",
		"meld-uep-unit",
		"meld-flat-frame",
		"meld-uep",
		"meld-uep-frame",
		"meld-uep-frame-atomic",
		"meld-uep-frame-noatomic",
		"meld-sld",
		"meld-sld-uep",
		"meld-repair-ceiling",
		"libsrt",
		"libsrt-fec",
		"librist",
		"oracle-source",
		"oracle-ideal",
	} {
		if !sweepSupported(arm) {
			t.Fatalf("sweepSupported(%q) = false", arm)
		}
	}
}

func TestMeldArmConfigs(t *testing.T) {
	tests := map[string]meldArmConfig{
		"meld":                    {name: "meld"},
		"meld-auto":               {name: "meld-auto", uep: true, frame: true, sliding: true},
		"meld-flat-unit":          {name: "meld-flat-unit"},
		"meld-uep-unit":           {name: "meld-uep-unit", uep: true},
		"meld-flat-frame":         {name: "meld-flat-frame", frame: true},
		"meld-uep":                {name: "meld-uep", uep: true, frame: true},
		"meld-uep-frame":          {name: "meld-uep-frame", uep: true, frame: true},
		"meld-uep-frame-atomic":   {name: "meld-uep-frame-atomic", uep: true, frame: true, frameAtomic: true},
		"meld-uep-frame-noatomic": {name: "meld-uep-frame-noatomic", uep: true, frame: true, disableFramePolicy: true},
		"meld-sld":                {name: "meld-sld", sliding: true},
		"meld-sld-uep":            {name: "meld-sld-uep", uep: true, frame: true, sliding: true},
		"meld-repair-ceiling":     {name: "meld-repair-ceiling", uep: true, frame: true, sliding: true, repairCeiling: true},
	}
	for name, want := range tests {
		got, ok := meldArm(name)
		if !ok {
			t.Fatalf("meldArm(%q) unsupported", name)
		}
		if got != want {
			t.Fatalf("meldArm(%q) = %+v, want %+v", name, got, want)
		}
	}
	if _, ok := meldArm("libsrt"); ok {
		t.Fatal("libsrt should not be a Meld arm")
	}
}

func TestOracleArms(t *testing.T) {
	c := &chunked{chunks: [][]byte{{0}, {1}, {2}}}
	all := allChunkSeqs(c)
	if len(all) != 3 || !all[0] || !all[1] || !all[2] {
		t.Fatalf("allChunkSeqs = %v, want all chunks", all)
	}
	if got := idealDeadlineSeqs(c, 100, 49); len(got) != 0 {
		t.Fatalf("idealDeadlineSeqs below one-way = %v, want none", got)
	}
	if got := idealDeadlineSeqs(c, 100, 50); len(got) != 3 {
		t.Fatalf("idealDeadlineSeqs at one-way len=%d, want 3", len(got))
	}
}

func TestMeldFrameDescCarriesTemporalLayer(t *testing.T) {
	sh := shape.Shaped{Unit: shape.Unit{
		ID:              42,
		Class:           shape.ClassDisposable,
		RefersTo:        []uint32{7, 11},
		TemporalID:      3,
		RAP:             false,
		RecoveryRefresh: true,
		Discardable:     true,
		Picture:         true,
	}}

	fd := meldFrameDesc(sh, 4, true)
	if fd.FrameID != 42 || fd.Chunks != 4 || fd.Priority != shape.ClassDisposable.Wire() ||
		fd.TemporalID != 3 || !fd.RecoveryRefresh || !fd.Discardable || fd.RAP || fd.NonPicture {
		t.Fatalf("UEP frame desc = %+v, want class/discardable temporal metadata preserved", fd)
	}
	if len(fd.RefFrameIDs) != 2 || fd.RefFrameIDs[0] != 7 || fd.RefFrameIDs[1] != 11 {
		t.Fatalf("frame desc refs = %v, want [7 11]", fd.RefFrameIDs)
	}

	flat := meldFrameDesc(sh, 4, false)
	if flat.Priority != 2 || flat.TemporalID != 3 {
		t.Fatalf("flat frame desc priority/temporal = %d/%d, want 2/3", flat.Priority, flat.TemporalID)
	}

	sh.Unit.Picture = false
	if meta := meldFrameDesc(sh, 1, true); !meta.NonPicture {
		t.Fatalf("metadata frame desc NonPicture = false, want true")
	}
}

func TestReassembleDeliveredUsesRawChunksNotDecodableOracle(t *testing.T) {
	c := &chunked{
		chunks: [][]byte{
			{0, 0, 0, 0, 0x65, 0x88},
			{0, 0, 0, 1, 0x99},
			{0, 0, 0, 2, 0x41, 0xaa},
		},
		shaped: []shape.Shaped{
			{Unit: shape.Unit{ID: 0, Picture: true}, Payload: []byte{0x65, 0x88, 0x99}},
			{Unit: shape.Unit{ID: 1, Picture: true, RefersTo: []uint32{0}}, Payload: []byte{0x41, 0xaa}},
		},
		units: []shape.Unit{
			{ID: 0, Picture: true},
			{ID: 1, Picture: true, RefersTo: []uint32{0}},
		},
		unitChunks: map[uint32][]uint32{
			0: {0, 1},
			1: {2},
		},
	}

	raw := c.reassembleDelivered(map[uint32]bool{1: true, 2: true})
	want := []byte{0, 0, 0, 1, 0x99, 0, 0, 0, 1, 0x41, 0xaa}
	if string(raw) != string(want) {
		t.Fatalf("raw reassembly = %v, want %v", raw, want)
	}
	_, modelPics := c.reassembleDecodable(c.deliveredUnits(map[uint32]bool{1: true, 2: true}))
	if modelPics != 0 {
		t.Fatalf("model reassembly predicted %d pictures, want 0 due partial dependency", modelPics)
	}
}

func TestFFProbeInvalidStreamIsZeroFrames(t *testing.T) {
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not installed")
	}
	n, err := ffprobeFrames([]byte{0, 0, 0, 1, 0xff, 0x00})
	if err != nil {
		t.Fatalf("ffprobe invalid stream error = %v, want zero-frame result", err)
	}
	if n != 0 {
		t.Fatalf("invalid stream frames = %d, want 0", n)
	}
}

func TestBenchProcExitedIsRepeatable(t *testing.T) {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "exit 7")
	} else {
		cmd = exec.Command("sh", "-c", "exit 7")
	}
	p, err := startBenchProc(cmd)
	if err != nil {
		t.Fatalf("startBenchProc: %v", err)
	}
	<-p.done
	err1, ok1 := p.exited()
	err2, ok2 := p.exited()
	if !ok1 || !ok2 {
		t.Fatalf("exited not repeatable: ok1=%v ok2=%v", ok1, ok2)
	}
	var exit1, exit2 *exec.ExitError
	if !errors.As(err1, &exit1) || !errors.As(err2, &exit2) {
		t.Fatalf("exit errors = %v / %v, want exec.ExitError", err1, err2)
	}
	p.stop()
}

func TestBenchReportWritesArtifacts(t *testing.T) {
	c := &chunked{
		chunks: [][]byte{
			{0, 0, 0, 0, 0x65},
			{0, 0, 0, 1, 0x41},
		},
		shaped: []shape.Shaped{
			{Unit: shape.Unit{ID: 0, Class: shape.ClassRAP, Picture: true, RAP: true}, Payload: []byte{0x65}},
			{Unit: shape.Unit{ID: 1, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{0}}, Payload: []byte{0x41}},
		},
		units: []shape.Unit{
			{ID: 0, Class: shape.ClassRAP, Picture: true, RAP: true},
			{ID: 1, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{0}},
		},
		unitChunks: map[uint32][]uint32{
			0: {0},
			1: {1},
		},
	}
	dir := t.TempDir()
	rep, err := newBenchReport(dir, reportCase{Name: "unit_case", Reps: 1, Arms: "meld-sld-uep"})
	if err != nil {
		t.Fatalf("newBenchReport: %v", err)
	}
	tr := rep.newTrace("meld-sld-uep", 1, 99)
	dg := wire.EncodeSymbol(nil, wire.Symbol{
		Flow:       1,
		Kind:       wire.Repair,
		WindowBase: 0,
		SrcIndex:   0,
		N:          1,
		RepairKey:  7,
		Priority:   3,
		Deadline:   1000,
		Payload:    []byte{0xaa},
	})
	tr.recordRelay(dg, true, 5)
	sparse := wire.EncodeSymbol(nil, wire.Symbol{
		Flow:      1,
		Kind:      wire.SparseRepair,
		SrcIndex:  8,
		RepairKey: 8,
		SparseIDs: []uint32{2, 5},
		Priority:  4,
		Deadline:  1000,
		Payload:   []byte{0xbb},
	})
	tr.recordRelay(sparse, false, 9)
	m := &meldRunResult{got: map[uint32]bool{0: true}, relayEnq: 1, relaySent: 1}
	if err := rep.addSeed(c, "meld-sld-uep", 1, 99, 1, score{frameRate: 0.5, keyRate: 1}, map[uint32]bool{0: true}, m, tr); err != nil {
		t.Fatalf("addSeed: %v", err)
	}
	rep.addResult(reportResult{Case: "unit_case", Arm: "meld-sld-uep", FFMean: 1, FramePctMean: 0.5, KeyPctMean: 1, Seeds: 1})
	if err := rep.write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	for _, name := range []string{"results.csv", "per_seed.csv", "matrix.md", "SUMMARY.md", "seed_trace_unit_case_meld_sld_uep_rep1_seed99.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	traceBytes, err := os.ReadFile(filepath.Join(dir, "seed_trace_unit_case_meld_sld_uep_rep1_seed99.json"))
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	var got seedTrace
	if err := json.Unmarshal(traceBytes, &got); err != nil {
		t.Fatalf("decode trace: %v", err)
	}
	if len(got.Relay) != 2 || got.Relay[1].Kind != "sparse_repair" || !slices.Equal(got.Relay[1].SparseIDs, []uint32{2, 5}) {
		t.Fatalf("sparse relay trace = %+v", got.Relay)
	}
}

func TestSweepArmFailureIsNaN(t *testing.T) {
	if !math.IsNaN(sweepArm("unknown", &chunked{}, 0, 0, 0, 0, 0, 0)) {
		t.Fatal("unknown sweep arm should report NaN failure")
	}
}

func TestParseFloatList(t *testing.T) {
	got := parseFloatList("0, 1.5, bad, -2, 3")
	want := []float64{0, 1.5, 3}
	if !slices.Equal(got, want) {
		t.Fatalf("parseFloatList = %v, want %v", got, want)
	}
}

func TestTheoreticalMeldOpportunity(t *testing.T) {
	if !theoreticalMeldOpportunity(0, 0, 100, 100) {
		t.Fatal("i.i.d. 1x RTT cell should be marked as ARQ-latency-tight Meld opportunity")
	}
	if theoreticalMeldOpportunity(0, 0, 100, 200) {
		t.Fatal("i.i.d. 2x RTT cell should not be marked as Meld opportunity")
	}
	if !theoreticalMeldOpportunity(24, 32, 100, 100) {
		t.Fatal("burst cell below 1.5x RTT should be a Meld opportunity")
	}
	if theoreticalMeldOpportunity(24, 32, 100, 200) {
		t.Fatal("burst shorter than post-RTT slack at 2x RTT should not be marked as theory opportunity")
	}
	if !theoreticalMeldOpportunity(80, 105, 100, 200) {
		t.Fatal("burst longer than post-RTT slack should be a Meld opportunity")
	}
}

func TestMacroGapRowsSelectsBestArms(t *testing.T) {
	rows := []macroFrontierRow{
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-uep", FFMean: 80, FramePctMean: 0.7, KeyPctMean: 0.8, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-sld-uep", FFMean: 90, FramePctMean: 0.8, KeyPctMean: 0.9, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "libsrt", FFMean: 70, FramePctMean: 0.6, KeyPctMean: 0.7, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "librist", FFMean: 85, FramePctMean: 0.75, KeyPctMean: 0.8, Seeds: 1},
	}
	gaps := macroGapRows(rows, macroFrontierOptions{ChunkSize: 1316, Mbps: 8, TotalPics: 144})
	if len(gaps) != 1 {
		t.Fatalf("macroGapRows len = %d, want 1", len(gaps))
	}
	g := gaps[0]
	if g.BestMeld != "meld-sld-uep" || g.BestARQ != "librist" || g.DeltaFF != 5 {
		t.Fatalf("gap = %+v, want best Meld vs best ARQ delta 5", g)
	}
	if !g.TheoryMeld {
		t.Fatalf("gap TheoryMeld=false, want true")
	}
}

func TestMacroGapRowsPrefersMeldAutoWhenPresent(t *testing.T) {
	rows := []macroFrontierRow{
		{Case: "c", Loss: 0.1, Burst: 0, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-auto", FFMean: 92, FramePctMean: 0.8, KeyPctMean: 0.9, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 0, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-sld-uep", FFMean: 120, FramePctMean: 1, KeyPctMean: 1, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 0, RTT: 100, Mult: 1, Budget: 100, Arm: "libsrt", FFMean: 100, FramePctMean: 0.9, KeyPctMean: 0.9, Seeds: 1},
	}
	gaps := macroGapRows(rows, macroFrontierOptions{ChunkSize: 1316, Mbps: 8, TotalPics: 144})
	if len(gaps) != 1 {
		t.Fatalf("macroGapRows len = %d, want 1", len(gaps))
	}
	g := gaps[0]
	if g.BestMeld != "meld-auto" || g.DeltaFF != -8 {
		t.Fatalf("gap = %+v, want deployable meld-auto delta -8", g)
	}
}

func TestMacroGapRowsCarriesSourceCost(t *testing.T) {
	rows := []macroFrontierRow{
		{Case: "c", Loss: 0.1, Burst: 48, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-auto", SourcePackets: 120, SourceBytes: 12000, FFMean: 100, Seeds: 1},
		{Case: "c", Loss: 0.1, Burst: 48, RTT: 100, Mult: 1, Budget: 100, Arm: "libsrt", SourcePackets: 100, SourceBytes: 8000, FFMean: 90, Seeds: 1},
	}
	gaps := macroGapRows(rows, macroFrontierOptions{ChunkSize: 1316, Mbps: 8, TotalPics: 144})
	if len(gaps) != 1 {
		t.Fatalf("macroGapRows len = %d, want 1", len(gaps))
	}
	g := gaps[0]
	if g.MeldSourcePackets != 120 || g.ARQSourcePackets != 100 {
		t.Fatalf("source packet cost not carried: %+v", g)
	}
	if got := macroSourcePacketDelta(g); math.Abs(got-0.20) > 1e-9 {
		t.Fatalf("source packet delta = %.6f, want 0.20", got)
	}
	if got := macroSourceByteDelta(g); math.Abs(got-0.50) > 1e-9 {
		t.Fatalf("source byte delta = %.6f, want 0.50", got)
	}
}

func TestMacroGapRowsPropagatesSeedNoise(t *testing.T) {
	rows := []macroFrontierRow{
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "meld-auto", FFMean: 127, FFStddev: 24, Seeds: 2},
		{Case: "c", Loss: 0.1, Burst: 24, RTT: 100, Mult: 1, Budget: 100, Arm: "libsrt", FFMean: 141, FFStddev: 5, Seeds: 2},
	}
	gaps := macroGapRows(rows, macroFrontierOptions{ChunkSize: 1316, Mbps: 8, TotalPics: 144})
	if len(gaps) != 1 {
		t.Fatalf("macroGapRows len = %d, want 1", len(gaps))
	}
	g := gaps[0]
	wantNoise := math.Hypot(24, 5)
	if math.Abs(g.DeltaNoise-wantNoise) > 1e-9 {
		t.Fatalf("DeltaNoise = %.6f, want %.6f", g.DeltaNoise, wantNoise)
	}
	if macroGapStable(g) {
		t.Fatalf("gap should be seed-noisy, got %+v", g)
	}
}

func TestMacroDecisionNamesSelectedTargetAndDeficit(t *testing.T) {
	rows := []macroGapRow{
		{Case: "weak_win", TheoryMeld: true, BestMeld: "meld", BestARQ: "libsrt", MeldFF: 120, ARQFF: 118, DeltaFF: 2, MeldFrame: 0.9, ARQFrame: 0.8, MeldKey: 1, ARQKey: 0.9},
		{Case: "big_win", TheoryMeld: true, BestMeld: "meld-sld-uep", BestARQ: "libsrt", MeldFF: 144, ARQFF: 137, DeltaFF: 7, MeldFrame: 1, ARQFrame: 0.85, MeldKey: 1, ARQKey: 0.95},
		{Case: "big_deficit", TheoryMeld: true, BestMeld: "meld-sld-uep", BestARQ: "libsrt", MeldFF: 122, ARQFF: 141, DeltaFF: -19, MeldFrame: 0.8, ARQFrame: 0.96, MeldKey: 0.88, ARQKey: 1},
	}
	var b strings.Builder
	writeMacroDecision(&b, rows)
	got := b.String()
	if !strings.Contains(got, "Selected positive target: `big_win`") {
		t.Fatalf("decision did not select strongest win:\n%s", got)
	}
	if !strings.Contains(got, "Largest theory-opportunity deficit: `big_deficit`") {
		t.Fatalf("decision did not name largest deficit:\n%s", got)
	}
}

func TestMacroDecisionFlagsSeedNoisyDeficit(t *testing.T) {
	rows := []macroGapRow{
		{Case: "burst24_loss10_rtt100_1x_b100", TheoryMeld: true, BestMeld: "meld-auto", BestARQ: "libsrt", MeldFF: 127, ARQFF: 141, DeltaFF: -14, DeltaNoise: 24},
	}
	var b strings.Builder
	writeMacroDecision(&b, rows)
	got := b.String()
	if !strings.Contains(got, "Largest raw theory-opportunity deficit is seed-noisy: `burst24_loss10_rtt100_1x_b100`") {
		t.Fatalf("decision did not flag noisy deficit:\n%s", got)
	}
	if strings.Contains(got, "Largest theory-opportunity deficit:") {
		t.Fatalf("noisy deficit should not be promoted as stable:\n%s", got)
	}
}

func TestMacroDecisionTreatsParityAsNonDeficit(t *testing.T) {
	rows := []macroGapRow{
		{Case: "parity", TheoryMeld: true, BestMeld: "meld-auto", BestARQ: "libsrt", MeldFF: 144, ARQFF: 144, DeltaFF: 0},
	}
	var b strings.Builder
	writeMacroDecision(&b, rows)
	got := b.String()
	if strings.Contains(got, "Largest theory-opportunity deficit") {
		t.Fatalf("parity should not be reported as a deficit:\n%s", got)
	}
}

func TestPublishSuiteSmokeIncludesPrimaryArms(t *testing.T) {
	suite, ok := publishSuiteByName("smoke")
	if !ok {
		t.Fatal("smoke suite not found")
	}
	for _, arm := range []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "librist"} {
		if !slices.Contains(suite.Arms, arm) {
			t.Fatalf("smoke suite missing arm %q: %v", arm, suite.Arms)
		}
	}
	if len(suite.Losses) == 0 || len(suite.RTTs) == 0 || len(suite.Mults) == 0 {
		t.Fatalf("smoke suite has empty grid: %+v", suite)
	}
}

func TestPublishSuiteFallbackCheckExists(t *testing.T) {
	suite, ok := publishSuiteByName("fallback-check")
	if !ok {
		t.Fatal("fallback-check suite not found")
	}
	if !slices.Contains(suite.Mults, 2) || !slices.Contains(suite.Mults, 3) {
		t.Fatalf("fallback-check should cover generous buffers: %+v", suite.Mults)
	}
	for _, arm := range []string{"meld-auto", "libsrt", "librist"} {
		if !slices.Contains(suite.Arms, arm) {
			t.Fatalf("fallback-check missing arm %q: %v", arm, suite.Arms)
		}
	}
}

func TestMacroChartsWriteSVGs(t *testing.T) {
	dir := t.TempDir()
	rows := []macroFrontierRow{
		{Case: "iid_loss5_rtt100_0p75x_b75", Loss: 0.05, Burst: 0, RTT: 100, Mult: 0.75, Budget: 75, Arm: "oracle-source", FFMean: 144, Seeds: 1},
		{Case: "iid_loss5_rtt100_0p75x_b75", Loss: 0.05, Burst: 0, RTT: 100, Mult: 0.75, Budget: 75, Arm: "oracle-ideal", FFMean: 144, Seeds: 1},
		{Case: "iid_loss5_rtt100_0p75x_b75", Loss: 0.05, Burst: 0, RTT: 100, Mult: 0.75, Budget: 75, Arm: "meld-auto", FFMean: 144, Seeds: 1},
		{Case: "iid_loss5_rtt100_0p75x_b75", Loss: 0.05, Burst: 0, RTT: 100, Mult: 0.75, Budget: 75, Arm: "libsrt", FFMean: 130, Seeds: 1},
		{Case: "iid_loss5_rtt100_0p75x_b75", Loss: 0.05, Burst: 0, RTT: 100, Mult: 0.75, Budget: 75, Arm: "librist", FFMean: 135, Seeds: 1},
	}
	gaps := macroGapRows(rows, macroFrontierOptions{ChunkSize: 1316, Mbps: 8, TotalPics: 144})
	if err := writeMacroCharts(dir, rows, gaps, macroFrontierOptions{TopN: 8}); err != nil {
		t.Fatalf("writeMacroCharts: %v", err)
	}
	for _, name := range []string{"delta-bars.svg", "frontier-heatmap.svg", "arm-frames.svg", "cost-gain.svg", "README.md"} {
		path := filepath.Join(dir, "charts", name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("missing chart %s: %v", name, err)
		}
		if strings.HasSuffix(name, ".svg") && !strings.Contains(string(b), "<svg") {
			t.Fatalf("chart %s is not svg:\n%s", name, b)
		}
	}
}

func TestFairnessRowsCatchSourceMismatch(t *testing.T) {
	rows := []macroFrontierRow{
		{Case: "c", Arm: "oracle-source", SourcePackets: 10, SourceBytes: 100, FFMean: 3, Seeds: 1},
		{Case: "c", Arm: "oracle-ideal", SourcePackets: 10, SourceBytes: 100, FFMean: 3, Seeds: 1},
		{Case: "c", Arm: "meld-auto", SourcePackets: 11, SourceBytes: 110, FFMean: 3, Seeds: 1},
		{Case: "c", Arm: "libsrt", SourcePackets: 10, SourceBytes: 100, FFMean: 2, Seeds: 1},
	}
	got := fairnessRows(rows, nil, macroFrontierOptions{
		Arms:           []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt"},
		SourceFFFrames: 3,
	})
	if len(got) != 1 {
		t.Fatalf("fairness rows = %d, want 1", len(got))
	}
	if got[0].SameSourcePackets || got[0].SameSourceBytes {
		t.Fatalf("source mismatch not detected: %+v", got[0])
	}
	if !got[0].SourceCeilingOK {
		t.Fatalf("source ceiling should pass: %+v", got[0])
	}
}
