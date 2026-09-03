package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestOutageDiversitySourceTimeGate pins the automatic scheduling decision on a
// time-correlated channel. The erasure state advances with source time, not
// transport packet count, so moving an equation cannot change the impairment
// trace it is being compared against.
func TestOutageDiversitySourceTimeGate(t *testing.T) {
	type cell struct {
		name        string
		rtt, budget int64
		burst       float64
	}
	const (
		n     = 6_000
		seeds = 4
	)
	for _, c := range []cell{
		{name: "iid-tight", rtt: 60_000, budget: 60_000},
		{name: "ge6-tight", rtt: 60_000, budget: 60_000, burst: 6},
		{name: "ge24-tight", rtt: 60_000, budget: 60_000, burst: 24},
		{name: "ge48-tight", rtt: 60_000, budget: 60_000, burst: 48},
		{name: "ge48-wan", rtt: 200_000, budget: 200_000, burst: 48},
	} {
		t.Run(c.name, func(t *testing.T) {
			var autoDelivery, offDelivery int
			var autoBytes, offBytes, delayed uint64
			for seed := int64(1); seed <= seeds; seed++ {
				cfg := DefaultConfig()
				cfg.Flow = 1
				cfg.SymbolSize = 256
				cfg.BufferMicros = c.budget
				cfg.MaxBitrate = 8_000_000
				cfg.Sliding = true
				makeDrop := func() func(wire.Symbol) bool {
					if c.burst == 0 {
						return uniformDrop(uint64(seed)*7919+13, 0.10)
					}
					return geTimeDrop(seed*7919+13, 500, 0.10, c.burst)
				}
				base := simLink{
					cfg: cfg, owdMicros: c.rtt / 2, srcMicros: 500, n: n, sliding: true,
					paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: seed,
				}
				base.drop = makeDrop()
				autoSender := NewSlidingSender(cfg)
				autoSender.disableEpochRepair = true
				auto := base.runCores(autoSender, NewSlidingReceiver(cfg))
				base.drop = makeDrop()
				s := NewSlidingSender(cfg)
				s.disableOutageDiversity = true
				s.disableEpochRepair = true
				off := base.runCores(s, NewSlidingReceiver(cfg))
				for arm, res := range map[string]simResult{"auto": auto, "off": off} {
					if res.corrupt {
						t.Fatalf("seed %d %s: false recovery", seed, arm)
					}
					assertOrdered(t, res.deliveredIDs)
				}
				autoDelivery += auto.deliveredInTime
				offDelivery += off.deliveredInTime
				autoBytes += auto.wireBytes
				offBytes += off.wireBytes
				delayed += auto.sstats.RepairOutageDiversity
			}
			t.Logf("delivery %.2f%% -> %.2f%%; wire %.3fMB -> %.3fMB; delayed=%d",
				100*float64(offDelivery)/float64(n*seeds), 100*float64(autoDelivery)/float64(n*seeds),
				float64(offBytes)/1e6, float64(autoBytes)/1e6, delayed)
			if c.burst == 0 {
				if delayed != 0 || autoDelivery != offDelivery || autoBytes != offBytes {
					t.Fatalf("iid control changed: delivery %d/%d bytes %d/%d delayed %d",
						autoDelivery, offDelivery, autoBytes, offBytes, delayed)
				}
				return
			}
			if delayed == 0 {
				// Not every finite seeded burst sample crosses the receiver's outage
				// classifier. In that case the feature must be exactly dormant.
				if autoDelivery != offDelivery || autoBytes != offBytes {
					t.Fatalf("dormant outage policy changed: delivery %d/%d bytes %d/%d",
						autoDelivery, offDelivery, autoBytes, offBytes)
				}
				return
			}
			// Scheduling the same repair credit at a different source-time slot can
			// move a small number of boundary symbols in either direction. Keep that
			// transition within one tenth of a percentage point while the broader
			// stress probe measures the distribution over more seeds.
			if minimum := offDelivery - n*seeds/1000; autoDelivery < minimum {
				t.Fatalf("outage diversity exceeded transition tolerance: %d < %d", autoDelivery, minimum)
			}
			if autoBytes > offBytes+offBytes/100 {
				t.Fatalf("outage diversity raised wire bytes by more than 1%%: %d > %d", autoBytes, offBytes)
			}
		})
	}
}
