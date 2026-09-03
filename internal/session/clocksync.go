package session

import "sync"

// clockSync estimates the offset of the remote (sender) clock relative to the local
// (receiver) clock from NTP-style two-message probes. It keeps the estimate from
// the probe with the smallest round trip — the least queuing noise, so the cleanest
// offset — and lets that best slowly inflate so a fresher probe re-anchors as the
// clocks drift apart. The receiver adds the offset to its local time to land in the
// sender's frame, so the deterministic core compares sender-stamped deadlines
// correctly cross-host without ever reading a clock itself. Safe for concurrent use.
type clockSync struct {
	mu      sync.Mutex
	offset  int64 // sender_clock − receiver_clock, microseconds
	bestRTT int64
	primed  bool
}

// observe folds one completed probe round (all microseconds): T0/T3 are the
// receiver's send/receive times, T1/T2 the sender's receive/send times. The offset
// is ((T1−T0)+(T2−T3))/2 and the round trip (T3−T0)−(T2−T1); a sample updates the
// estimate only when it is at least as clean as the running best.
func (c *clockSync) observe(t0, t1, t2, t3 int64) {
	rtt := (t3 - t0) - (t2 - t1)
	if rtt < 0 {
		rtt = 0
	}
	off := ((t1 - t0) + (t2 - t3)) / 2
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.primed || rtt <= c.bestRTT {
		c.offset, c.bestRTT, c.primed = off, rtt, true
	} else {
		c.bestRTT += c.bestRTT>>4 + 1 // forget slowly so drift can re-anchor
	}
}

// offsetMicros returns the current sender-minus-receiver clock offset (0 until the
// first probe completes, so on loopback or before sync the receiver uses its own
// clock unchanged).
func (c *clockSync) offsetMicros() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}
