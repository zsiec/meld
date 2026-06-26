package main

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

// sweepSupported reports whether the sweep can drive this arm (Meld-auto, the
// real libsrt, and the real libRIST — the iso-quality baselines).
func sweepSupported(arm string) bool {
	switch arm {
	case "meld", "meld-flat", "meld-auto", "meld-sld", "libsrt", "librist":
		return true
	}
	return false
}

// sweepArm runs one transport at (loss, rtt, budget) and returns the delivery
// fraction (delivered chunks / total). Meld runs in its default AutoGenSize,
// media-blind config — the deployable "auto" setup, not a hand-tuned one.
func sweepArm(arm string, c *chunked, loss float64, rtt, budget int, paceUs, meldMax, seed int64) float64 {
	var got map[uint32]bool
	switch arm {
	case "meld", "meld-flat", "meld-auto":
		got = runMeld(c, false, false, loss, rtt, budget, paceUs, meldMax, seed)
	case "meld-sld":
		got = runMeld(c, false, true, loss, rtt, budget, paceUs, meldMax, seed)
	case "libsrt":
		got = runLibsrt(c, loss, rtt, budget, paceUs, seed, "")
	case "librist":
		got = runLibrist(c, loss, rtt, budget, paceUs, seed)
	default:
		return 0
	}
	if len(c.chunks) == 0 {
		return 0
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
			for s := 1; s <= reps; s++ {
				ds = append(ds, sweepArm(arm, cl, loss, rtt, budget, paceUs, meldMax, int64(s)))
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
		ds = append(ds, sweepArm(arm, c, loss, rtt, budget, paceUs, meldMax, int64(s)))
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
			if measureDeliv(arm, cl, loss, rtt, hi, paceUs, meldMax, reps) < q {
				fmt.Printf("  %-8d %-10s %-11s %s\n", rtt, fmt.Sprintf(">%.0f×RTT", hiMult), "—", "(never cleared Q)")
				continue
			}
			lo := owd // budget at/below OWD cannot deliver
			for hi-lo > res {
				mid := (lo + hi) / 2
				if mid <= owd {
					lo = mid
					continue
				}
				if measureDeliv(arm, cl, loss, rtt, mid, paceUs, meldMax, reps) >= q {
					hi = mid
				} else {
					lo = mid
				}
			}
			d := measureDeliv(arm, cl, loss, rtt, hi, paceUs, meldMax, reps)
			fmt.Printf("  %-8d %-10d %-11s %.3f%%\n",
				rtt, hi, fmt.Sprintf("%.2f×", float64(hi)/float64(rtt)), d*100)
		}
	}
}
