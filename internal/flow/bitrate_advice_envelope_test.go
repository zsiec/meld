package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/wire"
)

// TestEncoderBitrateAdviceEnvelope models an encoder that applies the advisory
// by reducing payload bytes while preserving packet cadence.
func TestEncoderBitrateAdviceEnvelope(t *testing.T) {
	const (
		n          = 1_500
		symbolSize = 256
	)
	tests := []struct {
		name         string
		sliding      bool
		sourceMicros int64
		rtt, budget  int64
		maxBitrate   int64
		paceBytes    int64
		loss, burst  float64
		jitter       int64
		expectActive bool
	}{
		{"sliding-ample-iid10", true, 1_000, 100_000, 200_000, 8_000_000, 1_000_000, .10, 0, 0, false},
		{"sliding-tight-clean", true, 750, 100_000, 200_000, 3_000_000, 375_000, 0, 0, 0, true},
		{"sliding-tight-iid20", true, 750, 100_000, 200_000, 3_000_000, 375_000, .20, 0, 0, true},
		{"sliding-tight-ge24", true, 750, 100_000, 300_000, 3_000_000, 375_000, .10, 24, 0, true},
		{"sliding-tight-ge96-l20", true, 750, 100_000, 300_000, 3_000_000, 375_000, .20, 96, 0, true},
		{"sliding-tight-ge24-j30", true, 750, 100_000, 300_000, 3_000_000, 375_000, .10, 24, 30_000, true},
		{"generation-ample-iid10", false, 1_000, 100_000, 200_000, 8_000_000, 1_000_000, .10, 0, 0, false},
		{"generation-tight-iid10", false, 750, 100_000, 200_000, 3_000_000, 375_000, .10, 0, 0, true},
	}
	var stressedControl, stressedAdvised int
	for _, tt := range tests {
		var controlDelivered, advisedDelivered int
		var controlPayload, advisedPayload, controlWire, advisedWire uint64
		var targetSum int64
		var active int
		for seed := int64(1); seed <= 4; seed++ {
			run := func(honor bool) (simResult, int64) {
				cfg := DefaultConfig()
				cfg.Flow, cfg.SymbolSize = 1, symbolSize
				cfg.Sliding = tt.sliding
				cfg.AutoGenSize = false
				cfg.BufferMicros = tt.budget
				cfg.MaxBitrate = tt.maxBitrate
				var s coreSenderT
				if tt.sliding {
					s = NewSlidingSender(cfg)
				} else {
					s = NewSender(cfg)
				}
				var drop func(wire.Symbol) bool
				if tt.burst == 0 {
					drop = iidTimeDrop(uint32(seed), tt.sourceMicros, tt.loss)
				} else {
					drop = geTimeDrop(seed, tt.sourceMicros, tt.loss, tt.burst)
				}
				link := simLink{
					cfg: cfg, owdMicros: tt.rtt / 2, srcMicros: tt.sourceMicros, n: n,
					sliding: tt.sliding, drop: drop, paceBytesPerSec: tt.paceBytes,
					jitterMicros: tt.jitter,
				}
				if honor {
					link.sourceSize = func(uint32) int {
						var target int64
						switch x := s.(type) {
						case *SlidingSender:
							target = x.EncoderControl().TargetBitrateBps
						case *Sender:
							target = x.EncoderControl().TargetBitrateBps
						}
						if target == 0 {
							return symbolSize
						}
						n := int(target * tt.sourceMicros / 8_000_000)
						if n < 4 {
							n = 4
						}
						if n > symbolSize {
							n = symbolSize
						}
						return n
					}
				}
				var r coreReceiverT
				if tt.sliding {
					r = NewSlidingReceiver(cfg)
				} else {
					r = NewReceiver(cfg)
				}
				res := link.runCores(s, r)
				var target int64
				switch x := s.(type) {
				case *SlidingSender:
					target = x.EncoderControl().TargetBitrateBps
				case *Sender:
					target = x.EncoderControl().TargetBitrateBps
				}
				return res, target
			}
			control, _ := run(false)
			advised, target := run(true)
			if target > 0 {
				active++
				targetSum += target
			}
			controlDelivered += control.deliveredInTime
			advisedDelivered += advised.deliveredInTime
			controlPayload += control.deliveredBytes
			advisedPayload += advised.deliveredBytes
			controlWire += control.wireBytes
			advisedWire += advised.wireBytes
		}
		t.Logf("%s active=%d/4 target_mean=%d delivered=%d->%d payload=%d->%d wire=%d->%d",
			tt.name, active, targetSum/int64(max(active, 1)), controlDelivered, advisedDelivered,
			controlPayload, advisedPayload, controlWire, advisedWire)
		if tt.expectActive {
			if active != 4 {
				t.Errorf("advisory active in %d/4 seeds, want all", active)
			}
			if advisedDelivered < controlDelivered {
				t.Errorf("advisory regressed delivery: control=%d advised=%d", controlDelivered, advisedDelivered)
			}
			if advisedWire > controlWire {
				t.Errorf("advisory increased wire: control=%d advised=%d", controlWire, advisedWire)
			}
			stressedControl += controlDelivered
			stressedAdvised += advisedDelivered
		} else if active != 0 || advisedDelivered != controlDelivered || advisedPayload != controlPayload || advisedWire != controlWire {
			t.Errorf("ample-capacity advisory was not inert")
		}
	}
	if stressedAdvised <= stressedControl {
		t.Fatalf("advisory did not improve aggregate stressed delivery: %d -> %d", stressedControl, stressedAdvised)
	}
}
