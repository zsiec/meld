package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestSlidingCongestionControlEnvelope compares sliding delay/ECN control with
// the static ceiling on identical time-indexed traces. The physical link is
// narrower than MaxBitrate but remains wider than the measured source stream,
// so a useful controller must remove recovery-induced queueing without starving
// source or exact closure.
func TestSlidingCongestionControlEnvelope(t *testing.T) {
	t.Parallel()
	const (
		n            = 2_000
		sourceMicros = 1_000
		maxBitrate   = 3_200_000
	)
	tests := []struct {
		name        string
		loss, burst float64
		rtt, budget int64
		jitter      int64
		pace        int64
	}{
		{"iid10-open-rtt50-b100", 0.10, 0, 50_000, 100_000, 0, 0},
		{"ge24-open-rtt50-b100", 0.10, 24, 50_000, 100_000, 0, 0},
		{"iid10-rtt20-b50", 0.10, 0, 20_000, 50_000, 0, 350_000},
		{"ge24-rtt20-b50", 0.10, 24, 20_000, 50_000, 0, 350_000},
		{"iid20-rtt50-b100", 0.20, 0, 50_000, 100_000, 0, 350_000},
		{"ge24-rtt50-b100", 0.10, 24, 50_000, 100_000, 0, 350_000},
		{"ge96-l20-rtt100-b200", 0.20, 96, 100_000, 200_000, 0, 350_000},
		{"ge24-rtt50-b150-j30", 0.10, 24, 50_000, 150_000, 30_000, 350_000},
	}
	var controlAll, candidateAll int
	var controlBytesAll, candidateBytesAll uint64
	var controlExactAll, candidateExactAll uint64
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var controlDelivered, candidateDelivered int
			var controlBytes, candidateBytes uint64
			var controlExact, candidateExact uint64
			worstSeedDelta := n
			for seed := int64(1); seed <= 8; seed++ {
				run := func(cc bool) simResult {
					cfg := DefaultConfig()
					cfg.Flow = 1
					cfg.SymbolSize = 256
					cfg.BufferMicros = tt.budget
					cfg.MaxBitrate = maxBitrate
					cfg.CongestionControl = cc
					var drop func(wire.Symbol) bool
					if tt.burst == 0 {
						drop = iidTimeDrop(uint32(seed), sourceMicros, tt.loss)
					} else {
						drop = geTimeDrop(seed, sourceMicros, tt.loss, tt.burst)
					}
					return (simLink{
						cfg: cfg, owdMicros: tt.rtt / 2, srcMicros: sourceMicros, n: n,
						sliding: true, drop: drop, jitterMicros: tt.jitter,
						paceBytesPerSec: tt.pace,
					}).run()
				}
				control := run(false)
				candidate := run(true)
				assertCoreInvariants(t, control, n, "static ceiling")
				assertCoreInvariants(t, candidate, n, "sliding congestion control")
				controlDelivered += control.deliveredInTime
				candidateDelivered += candidate.deliveredInTime
				controlBytes += control.wireBytes
				candidateBytes += candidate.wireBytes
				controlExact += control.sstats.RepairExact
				candidateExact += candidate.sstats.RepairExact
				if delta := candidate.deliveredInTime - control.deliveredInTime; delta < worstSeedDelta {
					worstSeedDelta = delta
				}
			}
			controlAll += controlDelivered
			candidateAll += candidateDelivered
			controlBytesAll += controlBytes
			candidateBytesAll += candidateBytes
			controlExactAll += controlExact
			candidateExactAll += candidateExact
			t.Logf("control=%d/%dB candidate=%d/%dB delta=%d bytes=%d exact=%d->%d worst_seed=%d",
				controlDelivered, controlBytes, candidateDelivered, candidateBytes,
				candidateDelivered-controlDelivered, int64(candidateBytes)-int64(controlBytes),
				controlExact, candidateExact, worstSeedDelta)
			// A timing controller necessarily changes individual packet fates. Gate
			// the probability estimate per cell instead: allow only a 0.1% sampling
			// tie, while the aggregate below must be a strict improvement.
			const seeds = 8
			noninferiority := n * seeds / 1_000
			if candidateDelivered+noninferiority < controlDelivered {
				t.Errorf("cell regressed beyond 0.1%%: control=%d candidate=%d", controlDelivered, candidateDelivered)
			}
		})
	}
	t.Logf("aggregate control=%d/%dB candidate=%d/%dB delta=%d bytes=%d",
		controlAll, controlBytesAll, candidateAll, candidateBytesAll,
		candidateAll-controlAll, int64(candidateBytesAll)-int64(controlBytesAll))
	if candidateAll <= controlAll {
		t.Fatalf("sliding congestion control did not improve aggregate delivery: control=%d candidate=%d",
			controlAll, candidateAll)
	}
	if candidateBytesAll > controlBytesAll {
		t.Fatalf("sliding congestion control increased aggregate wire bytes: control=%d candidate=%d",
			controlBytesAll, candidateBytesAll)
	}
	if candidateExactAll < controlExactAll {
		t.Fatalf("sliding congestion control starved exact closure: control=%d candidate=%d",
			controlExactAll, candidateExactAll)
	}
}
