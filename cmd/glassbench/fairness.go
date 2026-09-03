package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type fairnessRow struct {
	SourceID                 string
	Case                     string
	SuccessfulArms           []string
	MissingArms              []string
	SameSourcePackets        bool
	SameSourceBytes          bool
	SourcePacketMin          int
	SourcePacketMax          int
	SourceByteMin            int64
	SourceByteMax            int64
	OracleSourceFF           float64
	OracleIdealFF            float64
	SourceCeilingOK          bool
	MeldAutoPresent          bool
	ARQPresent               bool
	ConservativeRegression   bool
	ConservativeRegressionFF float64
}

func writeFairnessReports(outDir string, rows []macroFrontierRow, gaps []macroGapRow, opts macroFrontierOptions) error {
	fairness := fairnessRows(rows, gaps, opts)
	if err := writeFairnessCSV(filepath.Join(outDir, "fairness.csv"), fairness); err != nil {
		return err
	}
	return writeFairnessMD(filepath.Join(outDir, "FAIRNESS.md"), fairness, opts)
}

func fairnessRows(rows []macroFrontierRow, gaps []macroGapRow, opts macroFrontierOptions) []fairnessRow {
	wantArms := supportedArms(opts.Arms)
	gapByCase := map[string]macroGapRow{}
	for _, g := range gaps {
		gapByCase[g.Case] = g
	}
	byCase := map[string][]macroFrontierRow{}
	for _, row := range rows {
		byCase[row.Case] = append(byCase[row.Case], row)
	}
	out := make([]fairnessRow, 0, len(byCase))
	for name, rs := range byCase {
		success := map[string]macroFrontierRow{}
		for _, row := range rs {
			if row.Failed == 0 && row.Seeds > 0 {
				success[row.Arm] = row
			}
		}
		fr := fairnessRow{
			Case:            name,
			SourceCeilingOK: true,
		}
		for _, row := range rs {
			if row.SourceID != "" {
				fr.SourceID = row.SourceID
				break
			}
		}
		for _, arm := range wantArms {
			row, ok := success[arm]
			if !ok {
				fr.MissingArms = append(fr.MissingArms, arm)
				continue
			}
			fr.SuccessfulArms = append(fr.SuccessfulArms, arm)
			if row.Arm == "oracle-source" {
				fr.OracleSourceFF = row.FFMean
			}
			if row.Arm == "oracle-ideal" {
				fr.OracleIdealFF = row.FFMean
			}
			if row.Arm == "meld-auto" {
				fr.MeldAutoPresent = true
			}
			if row.Arm == "libsrt" || row.Arm == "libsrt-fec" || row.Arm == "librist" {
				fr.ARQPresent = true
			}
			updateSourceRange(&fr, row)
		}
		if opts.SourceFFFrames > 0 && fr.OracleSourceFF > 0 {
			fr.SourceCeilingOK = fr.OracleSourceFF+0.5 >= float64(opts.SourceFFFrames)
		}
		fr.SameSourcePackets = fr.SourcePacketMin == fr.SourcePacketMax
		fr.SameSourceBytes = fr.SourceByteMin == fr.SourceByteMax
		if g, ok := gapByCase[name]; ok && !g.TheoryMeld && g.DeltaFF < 0 && macroGapStable(g) {
			fr.ConservativeRegression = true
			fr.ConservativeRegressionFF = g.DeltaFF
		}
		out = append(out, fr)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].Case < out[j].Case
	})
	return out
}

func updateSourceRange(fr *fairnessRow, row macroFrontierRow) {
	if row.SourcePackets > 0 && (fr.SourcePacketMin == 0 || row.SourcePackets < fr.SourcePacketMin) {
		fr.SourcePacketMin = row.SourcePackets
	}
	if row.SourcePackets > fr.SourcePacketMax {
		fr.SourcePacketMax = row.SourcePackets
	}
	if row.SourceBytes > 0 && (fr.SourceByteMin == 0 || row.SourceBytes < fr.SourceByteMin) {
		fr.SourceByteMin = row.SourceBytes
	}
	if row.SourceBytes > fr.SourceByteMax {
		fr.SourceByteMax = row.SourceBytes
	}
}

func supportedArms(arms []string) []string {
	out := make([]string, 0, len(arms))
	for _, arm := range arms {
		arm = strings.TrimSpace(arm)
		if arm != "" && sweepSupported(arm) {
			out = append(out, arm)
		}
	}
	return out
}

func writeFairnessCSV(path string, rows []fairnessRow) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	w := csv.NewWriter(f)
	if err := w.Write([]string{
		"source_id", "case", "successful_arms", "missing_arms", "same_source_packets", "same_source_bytes",
		"source_packet_min", "source_packet_max", "source_byte_min", "source_byte_max",
		"oracle_source_ff", "oracle_ideal_ff", "source_ceiling_ok", "meld_auto_present", "arq_present",
		"conservative_regression", "conservative_regression_ff",
	}); err != nil {
		return err
	}
	for _, r := range rows {
		if err := w.Write([]string{
			r.SourceID,
			r.Case,
			strings.Join(r.SuccessfulArms, ";"),
			strings.Join(r.MissingArms, ";"),
			strconv.FormatBool(r.SameSourcePackets),
			strconv.FormatBool(r.SameSourceBytes),
			strconv.Itoa(r.SourcePacketMin),
			strconv.Itoa(r.SourcePacketMax),
			strconv.FormatInt(r.SourceByteMin, 10),
			strconv.FormatInt(r.SourceByteMax, 10),
			fmt.Sprintf("%.3f", r.OracleSourceFF),
			fmt.Sprintf("%.3f", r.OracleIdealFF),
			strconv.FormatBool(r.SourceCeilingOK),
			strconv.FormatBool(r.MeldAutoPresent),
			strconv.FormatBool(r.ARQPresent),
			strconv.FormatBool(r.ConservativeRegression),
			fmt.Sprintf("%.3f", r.ConservativeRegressionFF),
		}); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func writeFairnessMD(path string, rows []fairnessRow, opts macroFrontierOptions) error {
	var b strings.Builder
	total := len(rows)
	missing, sourceMismatch, ceilingFail, conservativeRegression := 0, 0, 0, 0
	for _, row := range rows {
		if len(row.MissingArms) > 0 || !row.MeldAutoPresent || !row.ARQPresent {
			missing++
		}
		if !row.SameSourcePackets || !row.SameSourceBytes {
			sourceMismatch++
		}
		if !row.SourceCeilingOK {
			ceilingFail++
		}
		if row.ConservativeRegression {
			conservativeRegression++
		}
	}
	fmt.Fprintf(&b, "# Fairness Guard\n\n")
	if opts.SourceID != "" {
		fmt.Fprintf(&b, "Source: `%s` (`%s`, codec `%s`).\n\n", opts.SourceID, opts.SourceClip, opts.SourceCodec)
	}
	if opts.WireMbps > 0 {
		fmt.Fprintf(&b, "Shared forward-link capacity: `%.1f Mbps`.\n\n", opts.WireMbps)
	} else {
		fmt.Fprintf(&b, "Shared forward-link capacity: `unbounded`; this run may validate plumbing and quality but cannot support an equal-capacity cost claim.\n\n")
	}
	fmt.Fprintf(&b, "- Cases checked: `%d`\n", total)
	fmt.Fprintf(&b, "- Cases with missing required arms: `%d`\n", missing)
	fmt.Fprintf(&b, "- Cases with source packet/byte mismatch: `%d`\n", sourceMismatch)
	fmt.Fprintf(&b, "- Cases where oracle-source missed the source ceiling: `%d`\n", ceilingFail)
	fmt.Fprintf(&b, "- Stable conservative-region Meld regressions: `%d`\n\n", conservativeRegression)
	if sourceMismatch > 0 && opts.AutoEncoderCadence {
		fmt.Fprintf(&b, "Source mismatches are expected only for cells where the encoder actuator changed the Meld source. Treat those rows as encoder-control experiments, not same-source transport comparisons.\n\n")
	}
	fmt.Fprintf(&b, "A publication claim should use cells with `meld-auto`, at least one ARQ arm, oracle rows, and matching source packets/bytes unless the section explicitly studies encoder control.\n\n")
	writeFairnessIssueTable(&b, rows)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeFairnessIssueTable(b *strings.Builder, rows []fairnessRow) {
	issues := make([]fairnessRow, 0)
	for _, row := range rows {
		if len(row.MissingArms) > 0 || !row.SameSourcePackets || !row.SameSourceBytes || !row.SourceCeilingOK || row.ConservativeRegression {
			issues = append(issues, row)
		}
	}
	fmt.Fprintf(b, "## Issues\n\n")
	if len(issues) == 0 {
		fmt.Fprintf(b, "None.\n\n")
		return
	}
	fmt.Fprintf(b, "| case | missing arms | same source | oracle ceiling | conservative regression |\n")
	fmt.Fprintf(b, "| --- | --- | --- | --- | ---: |\n")
	for _, row := range issues {
		fmt.Fprintf(b, "| `%s` | `%s` | %t/%t | %t | %.1f |\n",
			row.Case, strings.Join(row.MissingArms, ","), row.SameSourcePackets, row.SameSourceBytes,
			row.SourceCeilingOK, row.ConservativeRegressionFF)
	}
	fmt.Fprintf(b, "\n")
}
