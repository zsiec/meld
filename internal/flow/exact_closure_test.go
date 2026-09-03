package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

func iidTimeDrop(seed uint32, intervalMicros int64, loss float64) func(wire.Symbol) bool {
	return func(sym wire.Symbol) bool {
		slot := uint32(sym.SendTimestamp / intervalMicros)
		return coinU(uint64(seed), slot, 0x1d1d, 0, 0) < loss
	}
}

// TestAutomaticExactClosureEnvelope compares the exact-closure lane with a
// coded-only control on an identical time-indexed burst trace. The test keeps the
// aggregate wire ceiling fixed, so unit retransmissions can only replace coded
// recovery; they cannot buy a result with extra capacity.
func TestAutomaticExactClosureEnvelope(t *testing.T) {
	t.Parallel()

	const (
		n               = 3_000
		owdMicros       = 100_000
		sourceMicros    = 1_000
		budgetMicros    = 400_000
		maxBitrate      = 3_200_000
		paceBytesPerSec = maxBitrate / 8
	)
	var autoTotal, codedTotal int
	for seed := int64(1); seed <= 8; seed++ {
		run := func(disableExact bool) simResult {
			cfg := DefaultConfig()
			cfg.Flow = 1
			cfg.SymbolSize = 256
			cfg.BufferMicros = budgetMicros
			cfg.MaxBitrate = maxBitrate
			s := NewSlidingSender(cfg)
			s.disableEpochRepair = true // isolate the exact-closure lane under test
			s.disableExactRepair = disableExact
			r := NewSlidingReceiver(cfg)
			return (simLink{
				cfg: cfg, owdMicros: owdMicros, srcMicros: sourceMicros, n: n,
				sliding: true, drop: geTimeDrop(seed, sourceMicros, 0.10, 24),
				paceBytesPerSec: paceBytesPerSec,
			}).runCores(s, r)
		}
		coded := run(true)
		auto := run(false)
		assertCoreInvariants(t, coded, n, "coded-only residual")
		assertCoreInvariants(t, auto, n, "automatic exact closure")
		if auto.deliveredInTime+1 < coded.deliveredInTime {
			t.Fatalf("seed %d: exact closure regressed in-deadline delivery: coded=%d auto=%d",
				seed, coded.deliveredInTime, auto.deliveredInTime)
		}
		if auto.overhead() > coded.overhead()+0.01 {
			t.Fatalf("seed %d: exact closure bought recovery with extra capacity: coded=%.3f auto=%.3f",
				seed, coded.overhead(), auto.overhead())
		}
		t.Logf("seed %d: coded=%d auto=%d arq=%d overhead=%.1f%%/%.1f%%",
			seed, coded.deliveredInTime, auto.deliveredInTime, auto.sstats.RepairExact,
			coded.overhead()*100, auto.overhead()*100)
		codedTotal += coded.deliveredInTime
		autoTotal += auto.deliveredInTime
	}
	t.Logf("aggregate: coded=%d auto=%d delta=%d", codedTotal, autoTotal, autoTotal-codedTotal)
	if autoTotal <= codedTotal {
		t.Fatalf("exact closure did not improve aggregate delivery: coded=%d auto=%d", codedTotal, autoTotal)
	}
}

func TestAutomaticExactClosureMatrix(t *testing.T) {
	t.Parallel()

	const (
		n               = 1_500
		sourceMicros    = 1_000
		maxBitrate      = 3_200_000
		paceBytesPerSec = maxBitrate / 8
	)
	tests := []struct {
		name        string
		rtt, budget int64
		burst       float64
		expectGain  bool
	}{
		{"iid-rtt50-tight", 50_000, 100_000, 0, false},
		{"ge24-rtt50-tight", 50_000, 100_000, 24, false},
		{"ge24-rtt100-one-cycle", 100_000, 100_000, 24, false},
		{"iid-rtt200-roomy", 200_000, 400_000, 0, false},
		{"ge24-rtt200-roomy", 200_000, 400_000, 24, true},
		{"ge48-rtt200-roomy", 200_000, 400_000, 48, true},
		{"ge24-rtt400-roomy", 400_000, 800_000, 24, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var codedTotal, autoTotal int
			for seed := int64(1); seed <= 4; seed++ {
				run := func(disableExact bool) simResult {
					cfg := DefaultConfig()
					cfg.Flow = 1
					cfg.SymbolSize = 256
					cfg.BufferMicros = tt.budget
					cfg.MaxBitrate = maxBitrate
					s := NewSlidingSender(cfg)
					s.disableEpochRepair = true // isolate the exact-closure lane under test
					s.disableExactRepair = disableExact
					r := NewSlidingReceiver(cfg)
					var drop func(wire.Symbol) bool
					if tt.burst == 0 {
						drop = iidTimeDrop(uint32(seed), sourceMicros, 0.10)
					} else {
						drop = geTimeDrop(seed, sourceMicros, 0.10, tt.burst)
					}
					return (simLink{
						cfg: cfg, owdMicros: tt.rtt / 2, srcMicros: sourceMicros, n: n,
						sliding: true, drop: drop, paceBytesPerSec: paceBytesPerSec,
					}).runCores(s, r)
				}
				coded := run(true)
				auto := run(false)
				assertCoreInvariants(t, coded, n, "matrix coded residual")
				assertCoreInvariants(t, auto, n, "matrix automatic exact closure")
				codedTotal += coded.deliveredInTime
				autoTotal += auto.deliveredInTime
			}
			t.Logf("coded=%d auto=%d delta=%d", codedTotal, autoTotal, autoTotal-codedTotal)
			if tt.expectGain && autoTotal <= codedTotal {
				t.Fatalf("automatic exact closure did not improve burst delivery: coded=%d auto=%d",
					codedTotal, autoTotal)
			}
			if !tt.expectGain && autoTotal+1 < codedTotal {
				t.Fatalf("automatic policy regressed control cell: coded=%d auto=%d",
					codedTotal, autoTotal)
			}
		})
	}
}

func TestExtendedExactClosureDeepBurst(t *testing.T) {
	t.Parallel()

	const (
		n               = 2_000
		sourceMicros    = 1_000
		rttMicros       = 100_000
		budgetMicros    = 300_000
		maxBitrate      = 3_200_000
		paceBytesPerSec = maxBitrate / 8
	)
	var bitmap64Total, extendedTotal int
	var extendedExact uint64
	for seed := int64(1); seed <= 8; seed++ {
		run := func(bitmap64Only bool) simResult {
			cfg := DefaultConfig()
			cfg.Flow = 1
			cfg.SymbolSize = 256
			cfg.BufferMicros = budgetMicros
			cfg.MaxBitrate = maxBitrate
			s := NewSlidingSender(cfg)
			s.disableEpochRepair = true
			r := NewSlidingReceiver(cfg)
			r.disableExtendedClosure = bitmap64Only
			return (simLink{
				cfg: cfg, owdMicros: rttMicros / 2, srcMicros: sourceMicros, n: n,
				sliding: true, drop: geTimeDrop(seed, sourceMicros, 0.20, 96),
				paceBytesPerSec: paceBytesPerSec,
			}).runCores(s, r)
		}
		bitmap64 := run(true)
		extended := run(false)
		assertCoreInvariants(t, bitmap64, n, "64-value exact feedback")
		assertCoreInvariants(t, extended, n, "extended exact feedback")
		bitmap64Total += bitmap64.deliveredInTime
		extendedTotal += extended.deliveredInTime
		extendedExact += extended.sstats.RepairExact
	}
	t.Logf("bitmap64=%d extended=%d delta=%d exact=%d", bitmap64Total,
		extendedTotal, extendedTotal-bitmap64Total, extendedExact)
	if extendedExact == 0 {
		t.Fatal("extended closure never emitted exact repair")
	}
	if extendedTotal <= bitmap64Total {
		t.Fatalf("extended closure did not improve deep-burst delivery: bitmap64=%d extended=%d",
			bitmap64Total, extendedTotal)
	}
}
