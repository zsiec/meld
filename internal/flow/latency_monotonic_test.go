package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// geTimeDrop is a Gilbert-Elliott impairment indexed by protocol time rather
// than emitted-packet count. Every datagram emitted in the same source interval
// sees the same channel state. This is the correct coupling for a monotonicity
// proof: an optional repair packet must not advance the loss oracle and silently
// assign a different source trace to the second run.
func geTimeDrop(seed, intervalMicros int64, meanLoss, meanBurstIntervals float64) func(wire.Symbol) bool {
	step := geDrop(seed, meanLoss, meanBurstIntervals)
	var last int64 = -1
	lost := false
	return func(sym wire.Symbol) bool {
		slot := sym.SendTimestamp / intervalMicros
		for last < slot {
			lost = step(sym)
			last++
		}
		return lost
	}
}

// TestSlidingLatencyMonotonicity replays the identical source and erasure trace
// at two playout budgets in the same full-band actuator regime. More deadline
// slack may enable additional recovery, but it must never make a source symbol
// delivered by the tighter profile disappear.
func TestSlidingLatencyMonotonicity(t *testing.T) {
	const (
		n               = 2_000
		owdMicros       = 50_000
		sourceMicros    = 1_000
		maxBitrate      = 2_800_000
		paceBytesPerSec = maxBitrate / 8
	)
	for _, mode := range []struct {
		name             string
		disableCrossover bool
	}{
		{"deadline-crossover", false},
		{"fixed-persistence", true},
	} {
		for seed := int64(0); seed < 8; seed++ {
			run := func(budget int64) simResult {
				cfg := DefaultConfig()
				cfg.Flow = 1
				cfg.SymbolSize = 256
				cfg.BufferMicros = budget
				cfg.MaxBitrate = maxBitrate
				s := NewSlidingSender(cfg)
				s.disableExactCrossover = mode.disableCrossover
				return (simLink{
					cfg: cfg, owdMicros: owdMicros, srcMicros: sourceMicros, n: n,
					sliding: true, drop: geTimeDrop(seed+1, sourceMicros, 0.10, 24),
					paceBytesPerSec: paceBytesPerSec,
				}).runCores(s, NewSlidingReceiver(cfg))
			}
			tight := run(150_000)
			wide := run(200_000)
			wideSet := make(map[uint32]bool, len(wide.deliveredIDs))
			for _, id := range wide.deliveredIDs {
				wideSet[id] = true
			}
			for _, id := range tight.deliveredIDs {
				if !wideSet[id] {
					t.Errorf("%s seed %d: id %d delivered at 150ms but missing at 200ms (tight=%d wide=%d)",
						mode.name, seed, id, len(tight.deliveredIDs), len(wide.deliveredIDs))
					break
				}
			}
		}
	}
}
