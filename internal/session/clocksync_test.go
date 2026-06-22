package session

import "testing"

// TestClockSyncOffset: the NTP-style offset is computed correctly, and a noisier
// (higher round-trip) probe does not override a cleaner one.
func TestClockSyncOffset(t *testing.T) {
	var cs clockSync
	// Sender clock 1_000_000 µs ahead of the receiver; 5_000 µs one-way each way.
	// t0=0 (rx) → t1=t2=1_005_000 (tx, instant turnaround) → t3=10_000 (rx).
	cs.observe(0, 1_005_000, 1_005_000, 10_000)
	if got := cs.offsetMicros(); got != 1_000_000 {
		t.Fatalf("offset %d, want 1_000_000", got)
	}
	// A return-path-delayed sample (rtt 50_000 ≫ 10_000) is rejected by the min-RTT filter.
	cs.observe(0, 1_005_000, 1_005_000, 50_000)
	if got := cs.offsetMicros(); got != 1_000_000 {
		t.Fatalf("a noisy probe changed the offset to %d", got)
	}
}
