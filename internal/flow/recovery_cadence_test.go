package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

func TestRecoveryCadenceIgnoresFeedbackWithoutMediaDamageStats(t *testing.T) {
	s := NewSlidingSender(Config{Flow: 1, SymbolSize: testSym, Sliding: true, BufferMicros: 100_000})
	s.FeedFeedback(clock.Timestamp(1), wire.Feedback{
		Flow:       1,
		LossRate:   8000,
		Burstiness: 48 * 256,
	})
	if got := s.EncoderControl().RecoveryCadenceFrames; got != 0 {
		t.Fatalf("RecoveryCadenceFrames = %d, want relaxed without media damage stats", got)
	}
}

func TestRecoveryCadenceRequestsTightCadenceOnLongBurstDamage(t *testing.T) {
	s := NewSlidingSender(Config{Flow: 1, SymbolSize: testSym, Sliding: true, BufferMicros: 100_000})
	s.FeedFeedback(clock.Timestamp(1), wire.Feedback{
		Flow:               1,
		LossRate:           8000,
		Burstiness:         48 * 256,
		Frames:             10,
		DecodableFrames:    10,
		Keyframes:          1,
		DecodableKeyframes: 1,
	})
	s.FeedFeedback(clock.Timestamp(2), wire.Feedback{
		Flow:               1,
		LossRate:           8000,
		Burstiness:         48 * 256,
		Frames:             20,
		DecodableFrames:    18,
		Keyframes:          1,
		DecodableKeyframes: 1,
	})
	if got := s.EncoderControl().RecoveryCadenceFrames; got != recoveryCadenceHardFrames {
		t.Fatalf("RecoveryCadenceFrames = %d, want %d", got, recoveryCadenceHardFrames)
	}
	if got := s.Stats().RecoveryCadenceFrames; got != recoveryCadenceHardFrames {
		t.Fatalf("Stats RecoveryCadenceFrames = %d, want %d", got, recoveryCadenceHardFrames)
	}
}

func TestRecoveryCadenceRelaxesAfterCleanFeedback(t *testing.T) {
	s := NewSlidingSender(Config{Flow: 1, SymbolSize: testSym, Sliding: true, BufferMicros: 100_000})
	s.FeedFeedback(clock.Timestamp(1), wire.Feedback{
		Flow:               1,
		LossRate:           8000,
		Burstiness:         48 * 256,
		Frames:             10,
		DecodableFrames:    10,
		Keyframes:          1,
		DecodableKeyframes: 1,
	})
	s.FeedFeedback(clock.Timestamp(2), wire.Feedback{
		Flow:               1,
		LossRate:           8000,
		Burstiness:         48 * 256,
		Frames:             20,
		DecodableFrames:    18,
		Keyframes:          1,
		DecodableKeyframes: 1,
	})
	if got := s.EncoderControl().RecoveryCadenceFrames; got != recoveryCadenceHardFrames {
		t.Fatalf("RecoveryCadenceFrames = %d, want active hard request", got)
	}

	frames := uint32(20)
	for i := 0; i < recoveryCadenceRelaxReports; i++ {
		frames++
		s.FeedFeedback(clock.Timestamp(3+i), wire.Feedback{
			Flow:               1,
			Burstiness:         burstQ8One,
			Frames:             frames,
			DecodableFrames:    frames - 2,
			Keyframes:          1,
			DecodableKeyframes: 1,
		})
	}
	if got := s.EncoderControl().RecoveryCadenceFrames; got != recoveryCadenceSoftFrames {
		t.Fatalf("RecoveryCadenceFrames after first quiet window = %d, want %d", got, recoveryCadenceSoftFrames)
	}
	for i := 0; i < recoveryCadenceRelaxReports; i++ {
		frames++
		s.FeedFeedback(clock.Timestamp(100+i), wire.Feedback{
			Flow:               1,
			Burstiness:         burstQ8One,
			Frames:             frames,
			DecodableFrames:    frames - 2,
			Keyframes:          1,
			DecodableKeyframes: 1,
		})
	}
	if got := s.EncoderControl().RecoveryCadenceFrames; got != 0 {
		t.Fatalf("RecoveryCadenceFrames after second quiet window = %d, want relaxed", got)
	}
}
