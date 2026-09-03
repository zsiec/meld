package flow

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestEpochPolicyProbe compares the complete generation block and sliding-RLNC
// state machines under the same deterministic source, paced link, feedback path,
// and erasure realization. It is an opt-in research gate because the full matrix
// is diagnostic; production selection requires a winning region that can be
// identified from protocol measurements rather than from the test label.
func TestEpochPolicyProbe(t *testing.T) {
	if os.Getenv("MELD_EPOCH_PROBE") == "" {
		t.Skip("set MELD_EPOCH_PROBE=1 to run the profile envelope")
	}
	type channel struct {
		name  string
		burst float64
	}
	const n = 6_000
	seeds := float64(4)
	if v, err := strconv.Atoi(os.Getenv("MELD_EPOCH_SEEDS")); err == nil && v > 0 {
		seeds = float64(v)
	}
	for _, rtt := range []int64{60_000, 200_000, 400_000} {
		for _, mult := range []int64{1, 2} {
			budget := mult * rtt
			for _, ch := range []channel{{"iid", 0}, {"ge6", 6}, {"ge24", 24}, {"ge48", 48}} {
				cellName := fmt.Sprintf("rtt%d-budget%d-%s", rtt/1_000, budget/1_000, ch.name)
				if filter := os.Getenv("MELD_EPOCH_CELL"); filter != "" && filter != cellName {
					continue
				}
				var slidingDelivery, baselineDelivery, generationDelivery int
				var slidingRepair, baselineRepair, generationRepair, slidingReactive, generationReactive, autoCopies, baselineCopies, delayed, epoch uint64
				var slidingLost, slidingRecovered, generationLost, generationRecovered uint64
				var slidingPEst float64
				var slidingBurstQ8 int
				var generationPEst float64
				var generationBurstQ8 int
				var slidingInterval, generationInterval, slidingSlack, generationSlack int64
				var slidingOutages, slidingOutageSymbols uint64
				for seed := int64(1); seed <= int64(seeds); seed++ {
					cfg := DefaultConfig()
					cfg.Flow = 1
					cfg.SymbolSize = 256
					cfg.BufferMicros = budget
					cfg.MaxBitrate = 8_000_000
					makeDrop := func() func(wire.Symbol) bool {
						if ch.burst == 0 {
							return uniformDrop(uint64(seed)*7919+13, 0.10)
						}
						return geTimeDrop(seed*7919+13, 500, 0.10, ch.burst)
					}
					base := simLink{
						cfg: cfg, owdMicros: rtt / 2, srcMicros: 500, n: n,
						paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: seed,
					}

					slidingCfg := cfg
					slidingCfg.Sliding = true
					sliding := base
					sliding.cfg, sliding.sliding, sliding.drop = slidingCfg, true, makeDrop()
					autoSender := NewSlidingSender(slidingCfg)
					var autoReceiver *SlidingReceiver
					var autoReceiverCore coreReceiverT
					if os.Getenv("MELD_EPOCH_GENRX") != "" {
						autoReceiverCore = NewReceiver(slidingCfg)
					} else {
						autoReceiver = NewSlidingReceiver(slidingCfg)
						autoReceiverCore = autoReceiver
					}
					if os.Getenv("MELD_EPOCH_FORCE") != "" {
						autoSender.epochPolicy.demandQ8 = epochDemandOne
						autoSender.interMicros = base.srcMicros
					}
					sr := sliding.runCores(autoSender, autoReceiverCore)

					baseline := base
					baseline.cfg, baseline.sliding, baseline.drop = slidingCfg, true, makeDrop()
					baselineSender := NewSlidingSender(slidingCfg)
					baselineSender.disableEpochRepair = true
					br := baseline.runCores(baselineSender, NewSlidingReceiver(slidingCfg))

					generationCfg := cfg
					generationCfg.Sliding = false
					generation := base
					generation.cfg, generation.sliding = generationCfg, false
					generation.drop = makeDrop()
					generationSender, generationReceiver := NewSender(generationCfg), NewReceiver(generationCfg)
					gr := generation.runCores(generationSender, generationReceiver)
					if os.Getenv("MELD_EPOCH_VERBOSE") != "" {
						t.Logf("%s seed=%d auto=%d block-off=%d generation=%d rows=%d copies=%d wire=%d",
							cellName, seed, sr.deliveredInTime, br.deliveredInTime, gr.deliveredInTime,
							sr.sstats.RepairEpoch, sr.sstats.RepairBurstDuplicate, sr.wireBytes)
					}
					slidingDelivery += sr.deliveredInTime
					baselineDelivery += br.deliveredInTime
					generationDelivery += gr.deliveredInTime
					slidingRepair += sr.sstats.Repair
					baselineRepair += br.sstats.Repair
					generationRepair += gr.sstats.Repair
					autoCopies += sr.sstats.RepairBurstDuplicate
					baselineCopies += br.sstats.RepairBurstDuplicate
					delayed += sr.sstats.RepairOutageDiversity
					epoch += sr.sstats.RepairEpoch
					slidingReactive += sr.sstats.ReactiveRepair
					generationReactive += gr.sstats.ReactiveRepair
					slidingLost += sr.stats.Lost
					slidingRecovered += sr.stats.Recovered
					generationLost += gr.stats.Lost
					generationRecovered += gr.stats.Recovered
					slidingPEst += sr.finalPEst
					slidingBurstQ8 += sr.finalBurstQ8
					generationPEst += gr.finalPEst
					generationBurstQ8 += gr.finalBurstQ8
					if autoReceiver != nil {
						slidingInterval += autoReceiver.intervalUs
						slidingSlack += autoReceiver.slackUs
					}
					generationInterval += generationReceiver.intervalUs
					generationSlack += generationReceiver.slackUs
					slidingOutages += sr.stats.Outages
					slidingOutageSymbols += sr.stats.OutageSymbols
				}
				t.Logf("rtt=%d budget=%d channel=%s: auto delivery=%.2f%% lost=%.1f recovered=%.1f repair=%.1f reactive=%.1f copies=%.1f delayed=%.1f epoch=%.1f p=%.3f burst=%.2f fit=%.0f slack=%.0f; block-off delivery=%.2f%% repair=%.1f copies=%.1f; outages=%.1f/%0.1fsyms; generation block delivery=%.2f%% lost=%.1f recovered=%.1f repair=%.1f reactive=%.1f p=%.3f burst=%.2f fit=%.0f slack=%.0f",
					rtt/1_000, budget/1_000, ch.name,
					100*float64(slidingDelivery)/float64(n*seeds), float64(slidingLost)/seeds, float64(slidingRecovered)/seeds, float64(slidingRepair)/seeds, float64(slidingReactive)/seeds,
					float64(autoCopies)/seeds, float64(delayed)/seeds, float64(epoch)/seeds,
					slidingPEst/seeds, float64(slidingBurstQ8)/float64(seeds*burstQ8One), float64(slidingInterval)/seeds, float64(slidingSlack)/seeds,
					100*float64(baselineDelivery)/float64(n*seeds), float64(baselineRepair)/seeds, float64(baselineCopies)/seeds,
					float64(slidingOutages)/seeds, float64(slidingOutageSymbols)/seeds,
					100*float64(generationDelivery)/float64(n*seeds), float64(generationLost)/seeds, float64(generationRecovered)/seeds, float64(generationRepair)/seeds, float64(generationReactive)/seeds,
					generationPEst/seeds, float64(generationBurstQ8)/float64(seeds*burstQ8One), float64(generationInterval)/seeds, float64(generationSlack)/seeds)
			}
		}
	}
}

// TestOutageDiversityStressProbe samples more channel seeds in only the cells
// where the profile sweep observed receiver-classified outages. It is opt-in so
// the routine suite can pin the decision with a smaller deterministic gate while
// retaining the wider empirical audit.
func TestOutageDiversityStressProbe(t *testing.T) {
	if os.Getenv("MELD_OUTAGE_DIVERSITY_PROBE") == "" {
		t.Skip("set MELD_OUTAGE_DIVERSITY_PROBE=1 to run the outage-diversity stress sample")
	}
	type cell struct {
		name        string
		rtt, budget int64
		burst       float64
	}
	const (
		n     = 6_000
		seeds = 16
	)
	for _, c := range []cell{
		{"tight-ge6", 60_000, 60_000, 6},
		{"tight-ge24", 60_000, 60_000, 24},
		{"tight-ge48", 60_000, 60_000, 48},
		{"roomy-ge48", 60_000, 120_000, 48},
		{"wan-ge48", 200_000, 200_000, 48},
	} {
		if filter := os.Getenv("MELD_OUTAGE_DIVERSITY_CELL"); filter != "" && filter != c.name {
			continue
		}
		var autoDelivery, offDelivery int
		var autoBytes, offBytes uint64
		minDelta := n
		regressed := 0
		for seed := int64(1); seed <= seeds; seed++ {
			cfg := DefaultConfig()
			cfg.Flow = 1
			cfg.SymbolSize = 256
			cfg.BufferMicros = c.budget
			cfg.MaxBitrate = 8_000_000
			cfg.Sliding = true
			base := simLink{
				cfg: cfg, owdMicros: c.rtt / 2, srcMicros: 500, n: n, sliding: true,
				paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: seed,
			}
			type outageReport struct {
				highest uint32
				run     uint16
			}
			var autoReports, offReports []outageReport
			base.fbTap = func(_ clock.Timestamp, fb wire.Feedback) {
				if fb.OutageRun > 0 {
					autoReports = append(autoReports, outageReport{fb.HighestSeen, fb.OutageRun})
				}
			}
			base.drop = geTimeDrop(seed*7919+13, 500, 0.10, c.burst)
			autoSender := NewSlidingSender(cfg)
			autoSender.disableEpochRepair = true
			auto := base.runCores(autoSender, NewSlidingReceiver(cfg))
			base.fbTap = func(_ clock.Timestamp, fb wire.Feedback) {
				if fb.OutageRun > 0 {
					offReports = append(offReports, outageReport{fb.HighestSeen, fb.OutageRun})
				}
			}
			base.drop = geTimeDrop(seed*7919+13, 500, 0.10, c.burst)
			s := NewSlidingSender(cfg)
			s.disableOutageDiversity = true
			s.disableEpochRepair = true
			off := base.runCores(s, NewSlidingReceiver(cfg))
			if auto.corrupt || off.corrupt {
				t.Fatalf("rtt=%d budget=%d burst=%.0f seed=%d: false recovery", c.rtt, c.budget, c.burst, seed)
			}
			delta := auto.deliveredInTime - off.deliveredInTime
			if delta < minDelta {
				minDelta = delta
			}
			if delta < 0 {
				regressed++
			}
			if delta < 0 || os.Getenv("MELD_OUTAGE_DIVERSITY_VERBOSE") != "" {
				t.Logf("%s seed=%d delta=%d: auto outage=%d/%d reports=%v repair=%d copies=%d delayed=%d bytes=%d p=%.3f burst=%.2f; off outage=%d/%d reports=%v repair=%d copies=%d bytes=%d p=%.3f burst=%.2f",
					c.name, seed, delta,
					auto.stats.Outages, auto.stats.OutageSymbols, autoReports, auto.sstats.Repair, auto.sstats.RepairBurstDuplicate, auto.sstats.RepairOutageDiversity, auto.wireBytes, auto.finalPEst, float64(auto.finalBurstQ8)/burstQ8One,
					off.stats.Outages, off.stats.OutageSymbols, offReports, off.sstats.Repair, off.sstats.RepairBurstDuplicate, off.wireBytes, off.finalPEst, float64(off.finalBurstQ8)/burstQ8One)
			}
			autoDelivery += auto.deliveredInTime
			offDelivery += off.deliveredInTime
			autoBytes += auto.wireBytes
			offBytes += off.wireBytes
		}
		t.Logf("%s rtt=%d budget=%d burst=%.0f: delivery %.3f%% -> %.3f%% (%+d), wire %.3fMB -> %.3fMB (%+.2f%%), regressed seeds=%d/%d min=%d",
			c.name, c.rtt/1_000, c.budget/1_000, c.burst,
			100*float64(offDelivery)/float64(n*seeds), 100*float64(autoDelivery)/float64(n*seeds), autoDelivery-offDelivery,
			float64(offBytes)/1e6, float64(autoBytes)/1e6, 100*(float64(autoBytes)/float64(offBytes)-1),
			regressed, seeds, minDelta)
	}
}
