// Package clock defines Meld's time type and the Clock seam between the
// deterministic core and the real world. The sans-I/O core (internal/flow) takes
// time only as explicit clock.Timestamp arguments and NEVER reads a clock itself;
// the host (internal/session) owns a RealClock and stamps every call. Tests
// substitute a manual clock to drive the core deterministically.
package clock

import "time"

// Timestamp is a monotonic time in microseconds from an arbitrary epoch. It is
// the only time unit the core understands; deadlines, RTT, and the timer wheel
// are all expressed in it.
type Timestamp int64

// Sub returns t - o in microseconds.
func (t Timestamp) Sub(o Timestamp) int64 { return int64(t - o) }

// Add returns t advanced by d microseconds.
func (t Timestamp) Add(d int64) Timestamp { return t + Timestamp(d) }

// Before reports whether t is strictly before o.
func (t Timestamp) Before(o Timestamp) bool { return t < o }

// After reports whether t is strictly after o.
func (t Timestamp) After(o Timestamp) bool { return t > o }

// Micros converts a time.Duration to whole microseconds, the Timestamp unit.
func Micros(d time.Duration) int64 { return int64(d / time.Microsecond) }

// Clock is the seam the host implements; the core never calls it.
type Clock interface {
	// Now returns the current monotonic time.
	Now() Timestamp
}

// RealClock is the production Clock: absolute Unix microseconds. It is absolute
// (not since-construction) so that two independently-constructed clocks on one host
// share an epoch. Cross-host, two hosts' clocks disagree by an unknown offset; the
// session's clock-offset handshake estimates it and translates the receiver's
// time into the sender's frame, so the core's deadline comparison stays correct.
type RealClock struct{}

// NewRealClock returns a RealClock.
func NewRealClock() *RealClock { return &RealClock{} }

// Now returns the current time in Unix microseconds.
func (c *RealClock) Now() Timestamp { return Timestamp(time.Now().UnixMicro()) }

// Offset wraps a Clock, adding a fixed microsecond offset. It models a second host
// whose clock disagrees with the first by a constant, so the cross-host clock-offset
// handshake can be exercised without two machines.
type Offset struct {
	base Clock
	off  int64
}

// NewOffset returns base's time shifted by offsetMicros.
func NewOffset(base Clock, offsetMicros int64) *Offset { return &Offset{base: base, off: offsetMicros} }

// Now returns the wrapped clock advanced by the offset.
func (o *Offset) Now() Timestamp { return o.base.Now().Add(o.off) }

// Manual is a test Clock advanced by hand; the deterministic-sim substitute for
// RealClock. It is the fake clock the Fabric simulator drives.
type Manual struct{ t Timestamp }

// NewManual returns a Manual clock set to t0.
func NewManual(t0 Timestamp) *Manual { return &Manual{t: t0} }

// Now returns the current manual time.
func (m *Manual) Now() Timestamp { return m.t }

// Set jumps the clock to t.
func (m *Manual) Set(t Timestamp) { m.t = t }

// Advance moves the clock forward by d microseconds and returns the new time.
func (m *Manual) Advance(d int64) Timestamp { m.t = m.t.Add(d); return m.t }
