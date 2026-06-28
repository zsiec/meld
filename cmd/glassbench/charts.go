package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func writeMacroCharts(outDir string, rows []macroFrontierRow, gaps []macroGapRow, opts macroFrontierOptions) error {
	chartDir := filepath.Join(outDir, "charts")
	if err := os.MkdirAll(chartDir, 0o755); err != nil {
		return err
	}
	if err := writeDeltaBarsSVG(filepath.Join(chartDir, "delta-bars.svg"), gaps, opts); err != nil {
		return err
	}
	if err := writeHeatmapSVG(filepath.Join(chartDir, "frontier-heatmap.svg"), gaps, opts); err != nil {
		return err
	}
	if err := writeArmFramesSVG(filepath.Join(chartDir, "arm-frames.svg"), rows, gaps, opts); err != nil {
		return err
	}
	if err := writeCostGainSVG(filepath.Join(chartDir, "cost-gain.svg"), gaps, opts); err != nil {
		return err
	}
	return writeChartsIndex(filepath.Join(chartDir, "README.md"))
}

func writeChartsIndex(path string) error {
	return os.WriteFile(path, []byte(`# Charts

- [Delta bars](delta-bars.svg): deployable Meld minus best ARQ, sorted by absolute frame delta.
- [Frontier heatmap](frontier-heatmap.svg): Meld-vs-ARQ frame delta across RTT and latency-budget cells.
- [Arm frames](arm-frames.svg): ffprobe decoded-frame means for selected high-signal cases.
- [Cost/gain](cost-gain.svg): frame delta versus observed relay byte delta and Meld repair overhead.
`), 0o644)
}

func writeDeltaBarsSVG(path string, gaps []macroGapRow, opts macroFrontierOptions) error {
	cp := append([]macroGapRow(nil), gaps...)
	sort.Slice(cp, func(i, j int) bool {
		return math.Abs(cp[i].DeltaFF) > math.Abs(cp[j].DeltaFF)
	})
	if len(cp) > 40 {
		cp = cp[:40]
	}
	rowH := 24
	top := 58
	width := 1280
	height := top + rowH*len(cp) + 40
	center := 730
	maxBar := 420.0
	maxDelta := 1.0
	for _, g := range cp {
		if v := math.Abs(g.DeltaFF); v > maxDelta {
			maxDelta = v
		}
	}
	var b strings.Builder
	svgHeader(&b, width, height, "Meld vs best ARQ frame delta")
	fmt.Fprintf(&b, `<text x="24" y="32" class="title">Meld-auto minus best ARQ, ffprobe frames</text>`)
	fmt.Fprintf(&b, `<line x1="%d" y1="46" x2="%d" y2="%d" class="axis"/>`, center, center, height-20)
	for i, g := range cp {
		y := top + i*rowH
		w := math.Abs(g.DeltaFF) / maxDelta * maxBar
		x := float64(center)
		if g.DeltaFF < 0 {
			x -= w
		}
		color := deltaColor(g.DeltaFF, macroGapStable(g))
		fmt.Fprintf(&b, `<text x="24" y="%d" class="label">%s</text>`, y+15, svgText(g.Case))
		fmt.Fprintf(&b, `<rect x="%.1f" y="%d" width="%.1f" height="16" fill="%s" rx="2"/>`, x, y+2, w, color)
		fmt.Fprintf(&b, `<text x="%d" y="%d" class="small">%+.1f</text>`, center+int(math.Copysign(w, g.DeltaFF))+signLabelOffset(g.DeltaFF), y+15, g.DeltaFF)
		fmt.Fprintf(&b, `<text x="1180" y="%d" class="small">%s vs %s</text>`, y+15, svgText(macroMeldLabel(g)), svgText(g.BestARQ))
	}
	svgFooter(&b)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func signLabelOffset(v float64) int {
	if v < 0 {
		return -54
	}
	return 8
}

func writeHeatmapSVG(path string, gaps []macroGapRow, opts macroFrontierOptions) error {
	byFacet := map[string][]macroGapRow{}
	for _, g := range gaps {
		key := fmt.Sprintf("%s loss, %s", pctName(g.Loss), burstLabel(g.Burst))
		byFacet[key] = append(byFacet[key], g)
	}
	facets := make([]string, 0, len(byFacet))
	for k := range byFacet {
		facets = append(facets, k)
	}
	sort.Strings(facets)
	mults := uniqueMults(gaps)
	rtts := uniqueRTTs(gaps)
	cellW, cellH := 82, 36
	left := 86
	facetH := 58 + len(rtts)*cellH
	width := left + len(mults)*cellW + 280
	height := 54 + len(facets)*facetH + 28
	maxDelta := maxAbsDelta(gaps)
	var b strings.Builder
	svgHeader(&b, width, height, "Frontier heatmap")
	fmt.Fprintf(&b, `<text x="24" y="32" class="title">Frame delta heatmap (Meld-auto - best ARQ)</text>`)
	for fi, facet := range facets {
		y0 := 56 + fi*facetH
		fmt.Fprintf(&b, `<text x="24" y="%d" class="facet">%s</text>`, y0, svgText(facet))
		for ci, mult := range mults {
			fmt.Fprintf(&b, `<text x="%d" y="%d" class="small center">%.2fx</text>`, left+ci*cellW+cellW/2, y0+24, mult)
		}
		for ri, rtt := range rtts {
			y := y0 + 34 + ri*cellH
			fmt.Fprintf(&b, `<text x="24" y="%d" class="small">%d ms</text>`, y+23, rtt)
			for ci, mult := range mults {
				g, ok := findGap(byFacet[facet], rtt, mult)
				x := left + ci*cellW
				fill := "#f3f4f6"
				label := "-"
				if ok {
					fill = heatColor(g.DeltaFF, maxDelta, macroGapStable(g))
					label = fmt.Sprintf("%+.0f", g.DeltaFF)
				}
				fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="%d" fill="%s" stroke="#ffffff"/>`, x, y, cellW-2, cellH-2, fill)
				fmt.Fprintf(&b, `<text x="%d" y="%d" class="small center">%s</text>`, x+cellW/2, y+23, label)
			}
		}
	}
	svgFooter(&b)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeArmFramesSVG(path string, rows []macroFrontierRow, gaps []macroGapRow, opts macroFrontierOptions) error {
	cases := append([]macroGapRow(nil), gaps...)
	sort.Slice(cases, func(i, j int) bool {
		if cases[i].TheoryMeld != cases[j].TheoryMeld {
			return cases[i].TheoryMeld
		}
		return math.Abs(cases[i].DeltaFF) > math.Abs(cases[j].DeltaFF)
	})
	if len(cases) > 12 {
		cases = cases[:12]
	}
	arms := uniqueArms(rows)
	barW := 15
	groupGap := 36
	left := 190
	top := 54
	plotH := 260
	width := left + len(cases)*(len(arms)*barW+groupGap) + 80
	height := top + plotH + 125
	maxFF := maxFFMean(rows)
	if maxFF <= 0 {
		maxFF = 1
	}
	byCaseArm := map[string]macroFrontierRow{}
	for _, r := range rows {
		if r.Failed == 0 && r.Seeds > 0 {
			byCaseArm[r.Case+"\x00"+r.Arm] = r
		}
	}
	var b strings.Builder
	svgHeader(&b, width, height, "Decoded frames by arm")
	fmt.Fprintf(&b, `<text x="24" y="32" class="title">ffprobe decoded-frame means by arm</text>`)
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>`, left, top+plotH, width-40, top+plotH)
	for i, arm := range arms {
		fmt.Fprintf(&b, `<rect x="24" y="%d" width="12" height="12" fill="%s"/>`, top+i*17, armColor(arm))
		fmt.Fprintf(&b, `<text x="42" y="%d" class="small">%s</text>`, top+11+i*17, svgText(arm))
	}
	for ci, g := range cases {
		x0 := left + ci*(len(arms)*barW+groupGap)
		for ai, arm := range arms {
			r, ok := byCaseArm[g.Case+"\x00"+arm]
			if !ok {
				continue
			}
			h := r.FFMean / maxFF * float64(plotH)
			x := x0 + ai*barW
			y := top + plotH - int(h)
			fmt.Fprintf(&b, `<rect x="%d" y="%d" width="%d" height="%d" fill="%s"/>`, x, y, barW-2, int(h), armColor(arm))
		}
		fmt.Fprintf(&b, `<text x="%d" y="%d" class="tiny" transform="rotate(45 %d,%d)">%s</text>`,
			x0, top+plotH+16, x0, top+plotH+16, svgText(g.Case))
	}
	svgFooter(&b)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func writeCostGainSVG(path string, gaps []macroGapRow, opts macroFrontierOptions) error {
	width, height := 980, 520
	left, right := 80, 40
	top, bottom := 56, 70
	plotW := width - left - right
	plotH := height - top - bottom
	maxAbsDelta := maxAbsDelta(gaps)
	maxAbsWire := 0.25
	maxRepair := 0.05
	for _, g := range gaps {
		if v := math.Abs(g.RelayByteDeltaPct); v > maxAbsWire {
			maxAbsWire = v
		}
		if g.MeldRepairOverhead > maxRepair {
			maxRepair = g.MeldRepairOverhead
		}
	}
	var b strings.Builder
	svgHeader(&b, width, height, "Cost and gain scatter")
	fmt.Fprintf(&b, `<text x="24" y="32" class="title">Frame delta versus observed wire-byte delta</text>`)
	zeroX := left + plotW/2
	zeroY := top + plotH/2
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>`, left, zeroY, width-right, zeroY)
	fmt.Fprintf(&b, `<line x1="%d" y1="%d" x2="%d" y2="%d" class="axis"/>`, zeroX, top, zeroX, height-bottom)
	fmt.Fprintf(&b, `<text x="%d" y="%d" class="small center">relay byte delta, Meld vs ARQ</text>`, zeroX, height-24)
	fmt.Fprintf(&b, `<text x="18" y="%d" class="small" transform="rotate(-90 18,%d)">ffprobe frame delta</text>`, zeroY, zeroY)
	for _, g := range gaps {
		x := float64(zeroX) + clamp(g.RelayByteDeltaPct/maxAbsWire, -1, 1)*float64(plotW)/2
		y := float64(zeroY) - clamp(g.DeltaFF/maxAbsDelta, -1, 1)*float64(plotH)/2
		r := 4.0
		if maxRepair > 0 {
			r += clamp(g.MeldRepairOverhead/maxRepair, 0, 1) * 8
		}
		color := deltaColor(g.DeltaFF, macroGapStable(g))
		fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="%.1f" fill="%s" fill-opacity="0.78"><title>%s: delta %.1f frames, wire %+.1f%%, repair %.1f%%</title></circle>`,
			x, y, r, color, svgText(g.Case), g.DeltaFF, g.RelayByteDeltaPct*100, g.MeldRepairOverhead*100)
	}
	fmt.Fprintf(&b, `<text x="%d" y="%d" class="tiny">%+.0f%%</text>`, left, height-bottom+16, -maxAbsWire*100)
	fmt.Fprintf(&b, `<text x="%d" y="%d" class="tiny">%+.0f%%</text>`, width-right-28, height-bottom+16, maxAbsWire*100)
	fmt.Fprintf(&b, `<text x="%d" y="%d" class="tiny">+%.0f frames</text>`, left+4, top+10, maxAbsDelta)
	fmt.Fprintf(&b, `<text x="%d" y="%d" class="tiny">-%.0f frames</text>`, left+4, height-bottom-6, maxAbsDelta)
	svgFooter(&b)
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func svgHeader(b *strings.Builder, width, height int, desc string) {
	fmt.Fprintf(b, `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d" role="img" aria-label="%s">
<style>
text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; fill: #111827; }
.title { font-size: 18px; font-weight: 700; }
.facet { font-size: 14px; font-weight: 700; }
.label { font-size: 11px; }
.small { font-size: 10px; }
.tiny { font-size: 9px; }
.center { text-anchor: middle; }
.axis { stroke: #6b7280; stroke-width: 1; }
</style>
<rect width="100%%" height="100%%" fill="#ffffff"/>
`, width, height, width, height, svgText(desc))
}

func svgFooter(b *strings.Builder) { b.WriteString("\n</svg>\n") }

func svgText(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&apos;")
	return r.Replace(s)
}

func burstLabel(v float64) string {
	if v < 1 {
		return "iid"
	}
	return fmt.Sprintf("burst %.0f", v)
}

func uniqueRTTs(gaps []macroGapRow) []int {
	seen := map[int]bool{}
	for _, g := range gaps {
		seen[g.RTT] = true
	}
	out := make([]int, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

func uniqueMults(gaps []macroGapRow) []float64 {
	seen := map[float64]bool{}
	for _, g := range gaps {
		seen[g.Mult] = true
	}
	out := make([]float64, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Float64s(out)
	return out
}

func uniqueArms(rows []macroFrontierRow) []string {
	seen := map[string]bool{}
	for _, r := range rows {
		if r.Failed == 0 && r.Seeds > 0 {
			seen[r.Arm] = true
		}
	}
	preferred := []string{"oracle-source", "oracle-ideal", "meld-auto", "libsrt", "libsrt-fec", "librist"}
	out := make([]string, 0, len(seen))
	for _, arm := range preferred {
		if seen[arm] {
			out = append(out, arm)
			delete(seen, arm)
		}
	}
	rest := make([]string, 0, len(seen))
	for arm := range seen {
		rest = append(rest, arm)
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func findGap(rows []macroGapRow, rtt int, mult float64) (macroGapRow, bool) {
	for _, r := range rows {
		if r.RTT == rtt && math.Abs(r.Mult-mult) < 0.001 {
			return r, true
		}
	}
	return macroGapRow{}, false
}

func maxAbsDelta(gaps []macroGapRow) float64 {
	max := 1.0
	for _, g := range gaps {
		if v := math.Abs(g.DeltaFF); v > max {
			max = v
		}
	}
	return max
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxFFMean(rows []macroFrontierRow) float64 {
	max := 0.0
	for _, r := range rows {
		if r.FFMean > max {
			max = r.FFMean
		}
	}
	return max
}

func deltaColor(v float64, stable bool) string {
	if v > 0 {
		if stable {
			return "#16a34a"
		}
		return "#86efac"
	}
	if v < 0 {
		if stable {
			return "#dc2626"
		}
		return "#fca5a5"
	}
	return "#9ca3af"
}

func heatColor(v, max float64, stable bool) string {
	if max <= 0 {
		max = 1
	}
	a := math.Abs(v) / max
	if a > 1 {
		a = 1
	}
	if !stable {
		a *= 0.55
	}
	if v >= 0 {
		return interpColor([3]int{236, 253, 245}, [3]int{22, 163, 74}, a)
	}
	return interpColor([3]int{254, 242, 242}, [3]int{220, 38, 38}, a)
}

func interpColor(lo, hi [3]int, t float64) string {
	if t < 0 {
		t = 0
	}
	if t > 1 {
		t = 1
	}
	r := int(float64(lo[0]) + (float64(hi[0]-lo[0]) * t))
	g := int(float64(lo[1]) + (float64(hi[1]-lo[1]) * t))
	b := int(float64(lo[2]) + (float64(hi[2]-lo[2]) * t))
	return fmt.Sprintf("#%02x%02x%02x", r, g, b)
}

func armColor(arm string) string {
	switch arm {
	case "oracle-source":
		return "#111827"
	case "oracle-ideal":
		return "#6b7280"
	case "meld-auto":
		return "#2563eb"
	case "libsrt":
		return "#f59e0b"
	case "libsrt-fec":
		return "#d97706"
	case "librist":
		return "#7c3aed"
	default:
		return "#0891b2"
	}
}
