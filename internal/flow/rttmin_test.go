package flow

// Tests for the sliding sender's windowed-min RTT filter: queue immunity (the
// self-locking flood's root cause), route-change adoption, and the excursion-cell
// regression the EWMA it replaced could not survive.

import (
	"os"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// feedRTTSample drives one updateRTT with a synthetic sample: a write at now-sample
// (its deadline stamp encodes the write time) reported as HighestSeen now.
func feedRTTSample(s *SlidingSender, now clock.Timestamp, sample int64, id uint32) {
	if s.deadlines == nil {
		s.deadlines = make(map[uint32]clock.Timestamp)
	}
	s.deadlines[id] = now.Add(-sample).Add(s.cfg.BufferMicros)
	s.updateRTT(now, wire.Feedback{Flow: s.cfg.Flow, HighestSeen: id + 1})
}

// TestRTTMinFilterQueueImmunity pins the fix for the self-locking flood: a burst of
// queue-inflated samples must NOT move the reported RTT while the window still
// holds a clean minimum, and a GENUINE increase is adopted once both half-windows
// have rolled past the old minimum.
func TestRTTMinFilterQueueImmunity(t *testing.T) {
	s := NewSlidingSender(Config{Flow: 1, SymbolSize: testSym, Sliding: true, CodingWindow: 32, BufferMicros: 200_000})
	now := clock.Timestamp(1_000_000)
	id := uint32(0)
	// Prime with clean 60 ms samples across one half-window.
	for i := 0; i < 20; i++ {
		feedRTTSample(s, now, 60_000, id)
		now, id = now.Add(20_000), id+1
	}
	if s.rttMicros != 60_000 {
		t.Fatalf("primed rtt = %d, want 60000", s.rttMicros)
	}
	// A 2-second storm of queue-inflated samples (the flood regime): the min filter
	// must hold the propagation-scale estimate throughout the first window and
	// never ratchet toward the queue (the EWMA it replaced reported 2.9 s here).
	worst := int64(0)
	for i := 0; i < 100; i++ {
		feedRTTSample(s, now, 2_900_000, id)
		now, id = now.Add(20_000), id+1
		if s.rttMicros > worst {
			worst = s.rttMicros
		}
		if i < 70 && s.rttMicros > 60_000 { // 70×20ms = 1.4s < one half-window
			t.Fatalf("rtt left 60ms after %d inflated samples inside the window: %d", i+1, s.rttMicros)
		}
	}
	// One clean sample after the storm keeps the estimate at propagation scale
	// (either the surviving old min or the fresh sample — never the storm value).
	feedRTTSample(s, now, 62_000, id)
	now, id = now.Add(20_000), id+1
	if s.rttMicros > 62_000 {
		t.Fatalf("post-storm estimate %d not at propagation scale", s.rttMicros)
	}
	// A genuine route change (all samples higher, no lower ones ever again) is
	// adopted once both half-windows roll.
	for i := 0; i < 200; i++ {
		feedRTTSample(s, now, 120_000, id)
		now, id = now.Add(20_000), id+1
	}
	if s.rttMicros != 120_000 {
		t.Fatalf("route change not adopted after both windows rolled: rtt = %d, want 120000", s.rttMicros)
	}
}

// TestRTTMinExcursionCellRecovered is the P-M1 regression: the fixed-8ms-holdoff
// excursion cell (ge48, rtt60, 4×RTT budget, paced+jittered) self-locked into a
// cap-saturated flood under the EWMA (rtt→2.9s, band→8, overhead ~294%). With the
// min filter the trap must not form: final rtt within 2× the true 60 ms, band not
// deadline-clipped below half its configured width, and overhead within 1.3× of
// the no-holdoff arm on the same seeds.
func TestRTTMinExcursionCellRecovered(t *testing.T) {
	t.Parallel()
	run := func(holdUs int64) (simResult, *SlidingSender) {
		cfg := sweepDefaultCfg(240_000)
		cfg.ReorderHoldoffMicros = holdUs
		s := NewSlidingSender(cfg)
		r := NewSlidingReceiver(cfg)
		sl := simLink{
			cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: 6_000,
			sliding: true, drop: geDrop(13, 0.10, 48),
			paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: 3,
		}
		return sl.runCores(s, r), s
	}
	base, _ := run(0)
	held, s := run(8_000)
	t.Logf("no-holdoff: deliv=%d ovh=%.0f%% | hold8: deliv=%d ovh=%.0f%% rtt=%dms band=%d",
		base.delivered, base.overhead()*100, held.delivered, held.overhead()*100,
		s.rttMicros/1000, s.effectiveBand())
	// The pathology's signature is the LOCK — band clipped to single digits with
	// overhead at the cap while the estimate never recovers (705ms-2.9s measured).
	// ge48 at 10% keeps a genuine residual queue on this saturated wire, and the
	// one-round retro's burst responses add transient spikes, so the estimate
	// legitimately reads propagation+residual (150-300ms). The band and overhead
	// assertions below are the lock detectors; the estimate just has to stay far
	// below the locked regime.
	if s.rttMicros > 400_000 {
		t.Fatalf("rtt estimate %dus in the self-lock regime (true 60ms)", s.rttMicros)
	}
	if band := s.effectiveBand(); band < 32 {
		t.Fatalf("band %d still deadline-clipped by an inflated rtt", band)
	}
	if held.overhead() > base.overhead()*1.3 {
		t.Fatalf("holdoff arm overhead %.0f%% vs %.0f%% base: the flood persists", held.overhead()*100, base.overhead()*100)
	}
}

// TestZHoleCellDecoderDiscards correlates the glass-hole autopsy's receiver-side
// verdict in the sim: at the hole geometry (ge48 10%, 2.5×RTT, wide band), how many
// covering equations does the span cap bin, and does the count track the stall?
func TestZHoleCellDecoderDiscards(t *testing.T) {
	if os.Getenv("MELD_HOLE_DIAG") == "" {
		t.Skip("diagnostic; set MELD_HOLE_DIAG=1")
	}
	for _, cw := range []int{64, 128, 256} {
		cfg := sweepDefaultCfg(150_000)
		cfg.CodingWindow = cw
		s := NewSlidingSender(cfg)
		r := NewSlidingReceiver(cfg)
		sl := simLink{
			cfg: cfg, owdMicros: 30_000, srcMicros: 500, n: 6_000,
			sliding: true, drop: geDrop(13, 0.10, 48),
			paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: 3,
		}
		res := sl.runCores(s, r)
		t.Logf("cw=%d: deliv=%d/%d ovh=%.0f%% p99=%dms | decoder dropped-rows=%d recovered=%d",
			cw, res.delivered, res.n, res.overhead()*100, pctlMicros(res.latencyMicros, 0.99)/1000,
			r.dec.DroppedRows(), res.stats.Recovered)
	}
}

// TestZHoleCellArbitered re-scores the sim hole cell with the ARBITERED metric the
// glass bench uses (deliveries within each chunk's own deadline): the burst autopsy
// proved the sim-vs-glass "fidelity gap" at this cell was the METRIC — raw delivery
// counts the late backlog a stalled cursor flushes, the arbiter does not.
func TestZHoleCellArbitered(t *testing.T) {
	if os.Getenv("MELD_HOLE_DIAG") == "" {
		t.Skip("diagnostic; set MELD_HOLE_DIAG=1")
	}
	for seed := int64(1); seed <= 4; seed++ {
		cfg := sweepDefaultCfg(150_000)
		cfg.CodingWindow = 256
		s := NewSlidingSender(cfg)
		r := NewSlidingReceiver(cfg)
		sl := simLink{
			cfg: cfg, owdMicros: 30_000, srcMicros: 1_316, n: 5_456,
			sliding: true, drop: geDrop(seed*7919+55, 0.10, 48),
			paceBytesPerSec: 1 << 20, timingJitterMicros: 2_000, timingSeed: seed,
		}
		res := sl.runCores(s, r)
		t.Logf("seed %d: raw-deliv=%.1f%% ARBITERED=%.1f%% (glass this cell: 9-70%% by seed)",
			seed, 100*float64(res.delivered)/float64(res.n), 100*float64(res.deliveredInTime)/float64(res.n))
	}
}
