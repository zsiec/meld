package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/shape"
)

// TestUEPKeyframeNonInversion pins the reported UEP inversion (mediabench: UEP keyframe 62–81%
// vs flat 90–100% at equal-or-higher UEP overhead). Unequal protection gives a keyframe's
// generation a STRICTLY smaller decode-failure target than flat (targetFailureForPriority),
// i.e. strictly MORE repair, and the budget throttle sheds the LOWEST tier first — so UEP can
// never protect keyframes WORSE than flat. If it does, protection is steering the wrong way (a
// tier-mapping or sizing bug). The existing TestUEPMoneyTest only covers a tight budget where
// UEP wins; this sweeps the generous-budget regime where the bench shows the inversion.
func TestUEPKeyframeNonInversion(t *testing.T) {
	units := shape.GenerateGOP(16, 16)
	for _, cap := range []int64{0, 12_000_000, 8_000_000} { // generous → unlimited budgets
		for _, loss := range []float64{0.10, 0.20, 0.30} {
			cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, TargetFailure: 1e-3, BufferMicros: 200_000, MaxBitrate: cap}
			uK, _, uO, uOrd := runUEP(t, cfg, units, true, loss, 0x5EED)
			fK, _, fO, fOrd := runUEP(t, cfg, units, false, loss, 0x5EED)
			if !uOrd || !fOrd {
				t.Fatalf("cap=%d loss=%.0f%%: out-of-order delivery (uep=%v flat=%v)", cap, loss*100, uOrd, fOrd)
			}
			t.Logf("cap=%-10d loss=%2.0f%%: UEP key=%.3f ovhd=%3.0f%% | flat key=%.3f ovhd=%3.0f%%",
				cap, loss*100, uK, uO, fK, fO)
			// The invariant: UEP must not protect keyframes worse than flat (small epsilon for
			// loss-pattern noise). Reported as an error per-regime so every violation is visible.
			if uK < fK-0.02 {
				t.Errorf("UEP keyframe %.3f WORSE than flat %.3f (UEP ovhd %.0f%% vs flat %.0f%%) at cap=%d loss=%.0f%% — protection inverted",
					uK, fK, uO, fO, cap, loss*100)
			}
		}
	}
}
