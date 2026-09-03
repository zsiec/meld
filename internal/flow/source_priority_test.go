package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// TestSlidingSourceFirstCapacityGovernor proves that the default sliding profile
// charges source and every repair family to one byte budget. Source remains
// non-droppable; once it consumes the allowance, recovery is shed rather than
// being allowed to build a queue in front of later media.
func TestSlidingSourceFirstCapacityGovernor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SymbolSize = 256
	cfg.MaxBitrate = 80_000 // 10 KB/s; the deterministic bucket starts with its bounded burst
	cfg.Redundancy = 3
	s := NewSlidingSender(cfg)

	now := clock.Timestamp(1)
	const sources = 400
	for i := 0; i < sources; i++ {
		s.Write(now, make([]byte, cfg.SymbolSize))
	}

	var sourceCount int
	var sourceBytes, repairBytes int64
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Systematic {
			sourceCount++
			sourceBytes += int64(len(d))
		} else {
			repairBytes += int64(len(d))
		}
	}
	if sourceCount != sources {
		t.Fatalf("source emitted = %d, want %d", sourceCount, sources)
	}
	if s.Stats().Throttled == 0 {
		t.Fatal("recovery was not throttled after source exhausted the shared allowance")
	}
	if repairBytes > s.bucket.burst {
		t.Fatalf("repair bytes = %d, exceed initial spare allowance %d (source bytes %d)", repairBytes, s.bucket.burst, sourceBytes)
	}
}

func TestGenerationSourceFirstCapacityGovernor(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Sliding = false
	cfg.SymbolSize = 256
	cfg.GenSize = 8
	cfg.MaxBitrate = 80_000
	cfg.Redundancy = 3
	s := NewSender(cfg)

	const sources = 400
	for i := 0; i < sources; i++ {
		s.Write(1, make([]byte, cfg.SymbolSize))
	}
	s.Flush(1)
	var sourceCount, repairCount int
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Systematic {
			sourceCount++
		} else {
			repairCount++
		}
	}
	if sourceCount != sources {
		t.Fatalf("source emitted = %d, want %d", sourceCount, sources)
	}
	stats := s.Stats()
	if stats.Throttled == 0 {
		t.Fatal("generation recovery was not throttled after source exhausted the allowance")
	}
	if stats.Repair != uint64(repairCount) {
		t.Fatalf("Repair stats = %d, actual emitted repair = %d", stats.Repair, repairCount)
	}
}

func TestGenerationOptionalRepairLeavesBaseReserve(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Sliding = false
	cfg.SymbolSize = 256
	cfg.MaxBitrate = 8_000_000
	s := NewSender(cfg)
	s.now = 1
	n := symHeaderBytes + 8 + codedSymbolSize(cfg.SymbolSize)
	s.repairTokens = int64(2*n - 1)
	s.bucket.primed = true
	s.bucket.last = s.now
	s.bucket.tokens = int64(2*n - 1)
	deadline := clock.Timestamp(1_000_000)
	if s.repairAdmissible(uepCenterTier, deadline, true) {
		t.Fatal("optional repair consumed the next base equation's reserve")
	}
	if !s.repairAdmissible(uepCenterTier, deadline, false) {
		t.Fatal("base repair could not use its reserved equation")
	}
}

func TestEmptyFeedbackDoesNotAgeOutColdStart(t *testing.T) {
	cfg := DefaultConfig()
	ss := NewSlidingSender(cfg)
	gs := NewSender(cfg)
	for i := 0; i < 100; i++ {
		now := clock.Timestamp(i + 1)
		fb := wire.Feedback{Flow: cfg.Flow}
		ss.FeedFeedback(now, fb)
		gs.FeedFeedback(now, fb)
	}
	if ss.fbCount != 0 || gs.fbCount != 0 {
		t.Fatalf("empty feedback aged cold start: sliding=%d generation=%d", ss.fbCount, gs.fbCount)
	}
}

func TestSlidingRepairDeadlineAdmission(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SymbolSize = 256
	cfg.BufferMicros = 100_000
	s := NewSlidingSender(cfg)
	t0 := clock.Timestamp(1)
	s.Write(t0, make([]byte, cfg.SymbolSize))
	for {
		if _, ok := s.PollSend(); !ok {
			break
		}
	}
	s.Tick(t0.Add(cfg.BufferMicros + defaultRTTMicros/2 + 1))
	if _, ok := s.PollSend(); ok {
		t.Fatal("sender emitted repair after its last useful arrival time")
	}
	if s.Stats().DeadlineRepairSkips == 0 {
		t.Fatal("deadline admission did not account the suppressed repair")
	}
}
