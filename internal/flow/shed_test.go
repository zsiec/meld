package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
)

// TestShedTopLayerOverBudget proves the proactive temporal downscale: when the source over-drives
// the rate budget, the top temporal layer (highest-TID discardable leaf frames) is shed at the
// encoder while the base/reference layer always flows. Control: with the knob off, nothing sheds.
func TestShedTopLayerOverBudget(t *testing.T) {
	run := func(shed bool) (source, shedCnt uint64, base int) {
		const sym = 1000
		cfg := Config{Flow: 1, SymbolSize: sym, GenSize: 16, Redundancy: 0, BufferMicros: 200_000,
			MaxBitrate: 1_000_000, ShedTopLayerOverBudget: shed} // 1 Mbps budget
		s := NewSender(cfg)
		now := clock.Timestamp(0)
		// Alternate base (TID 0, reference) and leaf (TID 2, discardable), 1000 B each, every 1 ms
		// ⇒ ~8 Mbps offered into a 1 Mbps budget.
		for i := 0; i < 200; i++ {
			var fd FrameDesc
			if i%2 == 0 {
				fd = FrameDesc{Priority: 2, FrameID: uint32(i), Chunks: 1, TemporalID: 0}
				base++
			} else {
				fd = FrameDesc{Priority: 0, FrameID: uint32(i), Chunks: 1, TemporalID: 2, Discardable: true}
			}
			s.WriteFrame(now, make([]byte, sym), fd) // a full 1000 B chunk every 1 ms ⇒ ~8 Mbps offered
			now = now.Add(1_000)
		}
		st := s.Stats()
		return st.Source, st.Shed, base
	}

	source, shed, base := run(true)
	t.Logf("shed-on: Source emitted=%d, Shed=%d (base frames=%d)", source, shed, base)
	if shed == 0 {
		t.Fatal("8x over budget but nothing shed — proactive top-layer shed did not engage")
	}
	if source < uint64(base) {
		t.Fatalf("a base/reference frame was shed: Source=%d < base=%d (must never shed a reference)", source, base)
	}

	offSource, offShed, _ := run(false)
	if offShed != 0 {
		t.Fatalf("control (knob off) shed %d frames — must shed nothing", offShed)
	}
	if offSource <= source {
		t.Fatalf("control emitted %d, not more than the shedding run's %d", offSource, source)
	}
}
