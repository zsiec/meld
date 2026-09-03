package flow

import (
	"testing"
)

// TestCompactRepairSimulation runs the complete feedback, sender, link, and
// receiver loop with ragged source values and verifies that repair compaction is
// exercised without compromising recovery.
func TestCompactRepairSimulation(t *testing.T) {
	t.Parallel()

	const (
		n          = 1_200
		sourceStep = 1_000
	)
	cfg := DefaultConfig()
	cfg.Flow = 1
	cfg.SymbolSize = 256
	cfg.BufferMicros = 200_000
	cfg.MaxBitrate = 3_200_000

	result := (simLink{
		cfg: cfg, owdMicros: 50_000, srcMicros: sourceStep, n: n,
		sliding: true, drop: iidTimeDrop(17, sourceStep, 0.10),
		sourceSize: func(id uint32) int { return 32 + int(id%65) },
	}).runCores(NewSlidingSender(cfg), NewSlidingReceiver(cfg))
	assertCoreInvariants(t, result, n, "compact repair")
	if result.sstats.RepairCompacted == 0 || result.sstats.RepairBytesSaved == 0 {
		t.Fatalf("compact telemetry = count %d saved %d, want both nonzero",
			result.sstats.RepairCompacted, result.sstats.RepairBytesSaved)
	}
	t.Logf("delivered %d symbols with %d bytes removed across %d compact equations",
		len(result.deliveredIDs), result.sstats.RepairBytesSaved, result.sstats.RepairCompacted)
}
