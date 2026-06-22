package clock

import (
	"testing"
	"time"
)

func TestTimestampArithmetic(t *testing.T) {
	a := Timestamp(1000)
	b := a.Add(250)
	if b != 1250 {
		t.Fatalf("Add: got %d", b)
	}
	if b.Sub(a) != 250 {
		t.Fatalf("Sub: got %d", b.Sub(a))
	}
	if !a.Before(b) || !b.After(a) {
		t.Fatal("Before/After wrong")
	}
	if a.Before(a) || a.After(a) {
		t.Fatal("strictness wrong at equality")
	}
}

func TestMicros(t *testing.T) {
	if Micros(2*time.Millisecond) != 2000 {
		t.Fatalf("Micros: got %d", Micros(2*time.Millisecond))
	}
}

func TestManualClock(t *testing.T) {
	m := NewManual(100)
	if m.Now() != 100 {
		t.Fatal("initial")
	}
	if m.Advance(50) != 150 || m.Now() != 150 {
		t.Fatal("advance")
	}
	m.Set(1000)
	if m.Now() != 1000 {
		t.Fatal("set")
	}
}

func TestRealClockMonotonic(t *testing.T) {
	c := NewRealClock()
	prev := c.Now()
	for i := 0; i < 1000; i++ {
		now := c.Now()
		if now < prev {
			t.Fatalf("clock went backwards: %d < %d", now, prev)
		}
		prev = now
	}
}
