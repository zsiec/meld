package flow

// This diagnostic isolates how sliding reorder holdoff affects a bursty paced
// cell. It runs the cell single-seed, default vs hold8, and prints
// a per-feedback timeline of both ends' estimator internals so the divergence
// names the mechanism. Env-gated diagnostic, not a regression test.
//
// MELD_HOLDDIAG=1 go test -run TestHoldoffDiagTimeline -v ./internal/flow

import (
	"fmt"
	"os"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

func TestHoldoffDiagTimeline(t *testing.T) {
	if os.Getenv("MELD_HOLDDIAG") == "" {
		t.Skip("set MELD_HOLDDIAG=1 to run the holdoff isolation diagnostic")
	}
	const (
		n      = 6_000
		src    = 500
		pace   = 1 << 20
		tjit   = 2_000
		owd    = 30_000 // rtt 60ms
		budget = 90_000 // 1.5x
	)
	for _, arm := range []struct {
		name    string
		holdoff int64
		loss    float64
	}{{"default", 0, 0.10}, {"hold8", 8_000, 0.10}, {"clean", 0, 0}} {
		cfg := sweepDefaultCfg(budget)
		cfg.ReorderHoldoffMicros = arm.holdoff
		s := NewSlidingSender(cfg)
		r := NewSlidingReceiver(cfg)
		sl := simLink{
			cfg: cfg, owdMicros: owd, srcMicros: src, n: n,
			sliding: true, drop: sweepDrop(sweepCell{loss: arm.loss, burst: 12}, 0),
			paceBytesPerSec: pace, timingJitterMicros: tjit, timingSeed: 3,
		}
		var last clock.Timestamp
		sl.fbTap = func(now clock.Timestamp, fb wire.Feedback) {
			if now.Sub(last) < 100_000 {
				return // 100ms cadence keeps the timeline readable
			}
			last = now
			// f = rho/(1-pEst): the passed-through-fraction estimate used by the
			// headroom cap. rho is the breaker's EWMA arrival/offer.
			f := (float64(s.arriveRatioQ8) / 256) / (1 - s.pEst)
			if f > 1 {
				f = 1
			}
			fmt.Printf("HD|%s|t=%.2f|fbLR=%.4f|fbBQ=%d|fbCL=%d|fbDef=%d|"+
				"sPEst=%.4f|sBQ8=%d|sRate=%.3f|sCap=%.2f|sHR=%.2f|sWLB=%d|sRTT=%.0f|"+
				"f=%.3f|arrQ8=%d|rttCur=%.0f|rttPrev=%.0f|"+
				"rEWMA=%.4f|rHold=%.4f|rMBQ=%d|rExcl=%d|wHold=%.1f|wSeen=%d\n",
				arm.name, float64(now)/1e6,
				float64(fb.LossRate)/65535, fb.Burstiness, fb.CongestionLoss, fb.Deficit,
				s.pEst, s.burstQ8, s.codeRate(), s.floodCap, s.headroomCap, s.wireLossBudget,
				float64(s.rttMicros)/1000,
				f, s.arriveRatioQ8, float64(s.rttMinCur)/1000, float64(s.rttMinPrev)/1000,
				r.pEWMA, r.pHold, r.meanBurstQ8, r.lossExcl,
				float64(r.reorder.holdUs)/1000, len(r.reorder.seen))
		}
		res := sl.runCores(s, r)
		fmt.Printf("HD|%s|SUMMARY delivered=%d inTime=%d ovh=%.1f%% reactive=%d proactive=%d p99=%.0fms\n",
			arm.name, res.delivered, res.deliveredInTime, 100*res.overhead(),
			res.sstats.ReactiveRepair, res.sstats.Repair-res.sstats.ReactiveRepair,
			float64(pctlMicros(res.latencyMicros, 0.99))/1000)
	}
}
