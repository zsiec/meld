package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestDeadlineExactCrossoverEnvelope compares the last-useful-dispatch crossover
// with the prior fixed persistence policy on identical time-indexed loss traces.
// The aggregate rate ceiling is held constant, so an exact packet must replace
// other recovery rather than purchasing delivery with extra capacity.
func TestDeadlineExactCrossoverEnvelope(t *testing.T) {
	t.Parallel()
	const (
		n               = 2_000
		sourceMicros    = 1_000
		maxBitrate      = 3_200_000
		paceBytesPerSec = maxBitrate / 8
	)
	tests := []struct {
		name        string
		loss, burst float64
		rtt, budget int64
		jitter      int64
	}{
		{"iid10-rtt20-b50", 0.10, 0, 20_000, 50_000, 0},
		{"ge24-rtt20-b50", 0.10, 24, 20_000, 50_000, 0},
		{"ge96-l20-rtt20-b50", 0.20, 96, 20_000, 50_000, 0},
		{"iid10-rtt20-b70-j30", 0.10, 0, 20_000, 70_000, 30_000},
		{"ge24-rtt20-b70-j30", 0.10, 24, 20_000, 70_000, 30_000},
		{"ge96-l20-rtt20-b70-j30", 0.20, 96, 20_000, 70_000, 30_000},
		{"iid10-rtt50-b100", 0.10, 0, 50_000, 100_000, 0},
		{"ge24-rtt50-b100", 0.10, 24, 50_000, 100_000, 0},
		{"ge96-l20-rtt50-b100", 0.20, 96, 50_000, 100_000, 0},
		{"ge24-rtt50-b150-j30", 0.10, 24, 50_000, 150_000, 30_000},
		{"ge24-rtt100-b200", 0.10, 24, 100_000, 200_000, 0},
		{"ge96-l20-rtt100-b300", 0.20, 96, 100_000, 300_000, 0},
		{"ge24-rtt200-b400", 0.10, 24, 200_000, 400_000, 0},
	}
	var controlAll, candidateAll int
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var controlDelivered, candidateDelivered int
			var controlBytes, candidateBytes uint64
			var controlExact, candidateExact uint64
			var candidateHeadroom float64
			worstSeedDelta := n
			for seed := int64(1); seed <= 8; seed++ {
				run := func(disable bool) simResult {
					cfg := DefaultConfig()
					cfg.Flow = 1
					cfg.SymbolSize = 256
					cfg.BufferMicros = tt.budget
					cfg.MaxBitrate = maxBitrate
					s := NewSlidingSender(cfg)
					s.disableEpochRepair = true
					s.disableExactCrossover = disable
					var drop func(wire.Symbol) bool
					if tt.burst == 0 {
						drop = iidTimeDrop(uint32(seed), sourceMicros, tt.loss)
					} else {
						drop = geTimeDrop(seed, sourceMicros, tt.loss, tt.burst)
					}
					res := (simLink{
						cfg: cfg, owdMicros: tt.rtt / 2, srcMicros: sourceMicros, n: n,
						sliding: true, drop: drop, jitterMicros: tt.jitter,
						paceBytesPerSec: paceBytesPerSec,
					}).runCores(s, NewSlidingReceiver(cfg))
					if !disable {
						candidateHeadroom += s.sourceHeadroomRate()
					}
					return res
				}
				control := run(true)
				candidate := run(false)
				assertCoreInvariants(t, control, n, "fixed persistence")
				assertCoreInvariants(t, candidate, n, "deadline crossover")
				controlDelivered += control.deliveredInTime
				candidateDelivered += candidate.deliveredInTime
				controlBytes += control.wireBytes
				candidateBytes += candidate.wireBytes
				controlExact += control.sstats.RepairExact
				candidateExact += candidate.sstats.RepairExact
				if delta := candidate.deliveredInTime - control.deliveredInTime; delta < worstSeedDelta {
					worstSeedDelta = delta
				}
				if candidate.deliveredInTime < control.deliveredInTime {
					t.Errorf("seed %d regressed: control=%d candidate=%d delta=%d bytes=%d",
						seed, control.deliveredInTime, candidate.deliveredInTime,
						candidate.deliveredInTime-control.deliveredInTime,
						int64(candidate.wireBytes)-int64(control.wireBytes))
				}
			}
			controlAll += controlDelivered
			candidateAll += candidateDelivered
			// Compact units may change packetization by a few headers, but must not
			// purchase the result with a material increase in offered recovery.
			if candidateBytes > controlBytes+controlBytes/1000 {
				t.Errorf("material wire increase: control=%d candidate=%d", controlBytes, candidateBytes)
			}
			t.Logf("control=%d/%dB candidate=%d/%dB delta=%d bytes=%d exact=%d->%d worst_seed=%d headroom=%.3f",
				controlDelivered, controlBytes, candidateDelivered, candidateBytes,
				candidateDelivered-controlDelivered, int64(candidateBytes)-int64(controlBytes),
				controlExact, candidateExact, worstSeedDelta, candidateHeadroom/8)
		})
	}
	t.Logf("aggregate control=%d candidate=%d delta=%d", controlAll, candidateAll, candidateAll-controlAll)
	if candidateAll <= controlAll {
		t.Fatalf("deadline crossover did not improve aggregate delivery: control=%d candidate=%d",
			controlAll, candidateAll)
	}
}
