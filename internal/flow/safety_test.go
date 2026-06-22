package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// N1 resource-safety + honest-signal tests.

// TestAdmissionCapRejectsWideWindow: a forged symbol declaring a window far wider
// than ls_max_size must be refused before any decoder is allocated.
func TestAdmissionCapRejectsWideWindow(t *testing.T) {
	cfg := testConfig()
	r := NewReceiver(cfg)
	sym := wire.Symbol{Flow: cfg.Flow, Kind: wire.Repair, WindowBase: 0, N: 60000, RepairKey: 1,
		Payload: make([]byte, testSym)}
	r.FeedSymbol(0, wire.EncodeSymbol(nil, sym))
	if r.Stats().Rejected == 0 {
		t.Fatal("wide-window symbol was not rejected")
	}
	if len(r.gens) != 0 {
		t.Fatalf("allocated %d generation decoders for a rejected symbol", len(r.gens))
	}
}

// TestAdmissionCapBoundsState: flooding distinct generations (with a hole so the
// cursor stalls and nothing is reaped) must not push the live-decoder count past the
// cap — the receiver stays a bounded finite-state machine regardless of input.
func TestAdmissionCapBoundsState(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetainedGens = 8
	r := NewReceiver(cfg)
	for i := 0; i < 500; i++ {
		base := uint32(i) * uint32(cfg.GenSize)
		// Feed base+1 (never base), so source id `base` is always missing and the
		// in-order cursor can never advance past generation 0 → nothing is reaped.
		sym := wire.Symbol{Flow: cfg.Flow, Kind: wire.Systematic, WindowBase: base,
			SrcIndex: base + 1, N: uint16(cfg.GenSize), Payload: make([]byte, testSym)}
		r.FeedSymbol(0, wire.EncodeSymbol(nil, sym))
	}
	if len(r.gens) > cfg.MaxRetainedGens {
		t.Fatalf("live decoders %d exceed cap %d", len(r.gens), cfg.MaxRetainedGens)
	}
	if r.Stats().Rejected == 0 {
		t.Fatal("expected the flood to be rejected past the cap")
	}
}

// TestHonestLossCountsPreRecovery: the wire-loss counter measures what the NETWORK
// dropped (gaps in the dense source sequence), before the decoder — so it is exact
// regardless of which symbols a repair later recovers.
func TestHonestLossCountsPreRecovery(t *testing.T) {
	cfg := testConfig()
	r := NewReceiver(cfg)
	// Deliver source ids 0,1,2 then 5,6 (a 2-long wire-loss run at 3,4).
	for _, id := range []uint32{0, 1, 2, 5, 6} {
		base := genBaseOf(id, cfg.GenSize)
		sym := wire.Symbol{Flow: cfg.Flow, Kind: wire.Systematic, WindowBase: base,
			SrcIndex: id, N: uint16(cfg.GenSize), Payload: make([]byte, testSym)}
		r.FeedSymbol(0, wire.EncodeSymbol(nil, sym))
	}
	if got := r.Stats().WireLost; got != 2 {
		t.Fatalf("WireLost = %d, want 2 (the gap at ids 3,4)", got)
	}
	// The burst estimate should have moved above the i.i.d. baseline (a run of 2).
	if r.meanBurstQ8 <= burstQ8One {
		t.Fatalf("burstiness %d did not rise above the i.i.d. baseline %d", r.meanBurstQ8, burstQ8One)
	}
}

// TestTokenBucketBoundsReflection: a sustained forged-feedback flood (every
// generation reported fully deficient) must not let the sender emit faster than the
// rate ceiling — repair is throttled, media is not.
func TestTokenBucketBoundsReflection(t *testing.T) {
	cfg := testConfig()
	cfg.BufferMicros = 2_000_000 // keep generations retained (reactive targets) for the run
	cfg.MaxBitrate = 2_000_000   // 2 Mbps ceiling
	s := NewSender(cfg)

	now := clock.Timestamp(0)
	for i := 0; i < 2*cfg.GenSize; i++ { // two full generations, retained
		s.Write(now, make([]byte, testSym))
		now = now.Add(200)
	}
	drainSend(s)

	var full [wire.MaxFeedbackGens]uint8
	for i := range full {
		full[i] = 255
	}
	const rounds = 40
	attempts := 0
	emitted := 0
	for k := 0; k < rounds; k++ { // 40 rounds × 50 ms = 2 s of forged feedback
		before := s.Stats()
		s.FeedFeedback(now, wire.Feedback{Flow: cfg.Flow, Deficit: 255, Deficits: full, DecodedLowEdge: 0})
		after := s.Stats()
		attempts += int(after.Repair+after.Throttled-before.Repair-before.Throttled) * (testSym + 29)
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			emitted += len(d)
		}
		now = now.Add(50_000)
	}
	// Token-bucket guarantee over the window since the bucket primed (clock 0): total
	// emitted ≤ burst + rate·window. Use a 2-burst margin for refill granularity.
	burst := int(cfg.MaxBitrate / 8 / 5)
	ceiling := int(float64(cfg.MaxBitrate)/8*float64(now)/1e6) + 2*burst
	if emitted > ceiling {
		t.Fatalf("emitted %d bytes exceeds the %d-byte rate ceiling", emitted, ceiling)
	}
	if s.Stats().Throttled == 0 {
		t.Fatal("no repair was throttled — the flood did not exercise the bucket")
	}
	if emitted*2 > attempts {
		t.Fatalf("bucket clipped too little: emitted %d of %d attempted bytes", emitted, attempts)
	}
}

func drainSend(s *Sender) {
	for {
		if _, ok := s.PollSend(); !ok {
			return
		}
	}
}
