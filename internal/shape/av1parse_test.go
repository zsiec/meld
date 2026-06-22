package shape

import "testing"

// TestGetRelativeDist checks the wrapped order-hint distance (AV1 §5.9.3), including the
// ring wraparound and the disabled (orderHintBits == 0) case.
func TestGetRelativeDist(t *testing.T) {
	cases := []struct {
		a, b, bits, want int
	}{
		{0, 0, 7, 0},
		{5, 2, 7, 3},
		{2, 5, 7, -3},
		{1, 127, 7, 2},  // 127 → 0 → 1 forward across the wrap
		{127, 1, 7, -2}, // the reverse
		{5, 2, 0, 0},    // order hints disabled
	}
	for _, c := range cases {
		if got := getRelativeDist(c.a, c.b, c.bits); got != c.want {
			t.Errorf("getRelativeDist(%d,%d,bits=%d)=%d, want %d", c.a, c.b, c.bits, got, c.want)
		}
	}
}

// TestSetFrameRefs checks the structural invariants of the set_frame_refs derivation
// (AV1 §7.8): all seven slot indices are filled and in range, and the two explicitly
// signalled references land in their named positions.
func TestSetFrameRefs(t *testing.T) {
	var refOrderHint [av1NumRefFrames]int
	// A plausible DPB: a spread of order hints across the eight slots.
	hints := []int{0, 8, 16, 4, 12, 2, 6, 10}
	for i, h := range hints {
		refOrderHint[i] = h
	}
	const lastIdx, goldIdx, orderHint, bits = 1, 3, 14, 7
	refIdx := setFrameRefs(lastIdx, goldIdx, orderHint, bits, refOrderHint)

	if refIdx[0] != lastIdx {
		t.Errorf("LAST slot = %d, want last_frame_idx %d", refIdx[0], lastIdx)
	}
	if refIdx[3] != goldIdx {
		t.Errorf("GOLDEN slot = %d, want gold_frame_idx %d", refIdx[3], goldIdx)
	}
	for i, s := range refIdx {
		if s < 0 || s >= av1NumRefFrames {
			t.Errorf("ref_frame_idx[%d] = %d, out of range [0,%d)", i, s, av1NumRefFrames)
		}
	}
}
