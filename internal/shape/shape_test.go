package shape

import "testing"

// deliveredAll returns a delivered-set marking every unit delivered.
func deliveredAll(units []Unit) map[uint32]bool {
	d := make(map[uint32]bool, len(units))
	for _, u := range units {
		d[u.ID] = true
	}
	return d
}

// TestDecodableClosure: a unit is decodable iff it and its whole dependency closure
// are delivered. Losing a spine unit (parameter set / IDR / base) cascades to its
// dependents; losing a discardable leaf is local.
func TestDecodableClosure(t *testing.T) {
	units := GenerateGOP(1, 8) // ps, IDR, then 7 frames
	// Everything delivered ⇒ everything decodable.
	dec := Decodable(units, deliveredAll(units))
	for _, u := range units {
		if !dec[u.ID] {
			t.Fatalf("unit %d (class %d) not decodable when all delivered", u.ID, u.Class)
		}
	}
	if r := DecodableFrameRate(units, deliveredAll(units)); r != 1 {
		t.Fatalf("frame rate %.3f, want 1.0 when all delivered", r)
	}

	// Drop the parameter set ⇒ the whole GOP is poisoned (nothing decodes).
	d := deliveredAll(units)
	d[units[0].ID] = false // units[0] is the parameter set
	if r := DecodableFrameRate(units, d); r != 0 {
		t.Fatalf("frame rate %.3f, want 0 when the parameter set is lost", r)
	}

	// Drop one base frame ⇒ it and every later spine unit (and their leaves) cascade,
	// but the IDR and the frames before the break stay decodable.
	var firstBase uint32
	for _, u := range units {
		if u.Class == ClassBase {
			firstBase = u.ID
			break
		}
	}
	d = deliveredAll(units)
	d[firstBase] = false
	dec = Decodable(units, d)
	if dec[firstBase] {
		t.Fatal("the dropped base frame should not be decodable")
	}
	var idr uint32
	for _, u := range units {
		if u.RAP {
			idr = u.ID
		}
	}
	if !dec[idr] {
		t.Fatal("the IDR (before the break) should stay decodable")
	}
	if r := DecodableFrameRate(units, d); r <= 0 || r >= 1 {
		t.Fatalf("frame rate %.3f, want a partial cascade (0 < r < 1)", r)
	}
}

// TestDecodableLeafLossIsLocal: dropping a discardable leaf removes only that frame,
// not its base or siblings.
func TestDecodableLeafLossIsLocal(t *testing.T) {
	units := GenerateGOP(1, 8)
	var leaf uint32
	found := false
	for _, u := range units {
		if u.Discardable {
			leaf = u.ID
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected at least one discardable leaf")
	}
	d := deliveredAll(units)
	d[leaf] = false
	dec := Decodable(units, d)
	if dec[leaf] {
		t.Fatal("the dropped leaf should not be decodable")
	}
	// Exactly one frame lost.
	frames, lost := 0, 0
	for _, u := range units {
		if u.Class == ClassParamSet {
			continue
		}
		frames++
		if !dec[u.ID] {
			lost++
		}
	}
	if lost != 1 {
		t.Fatalf("leaf loss cascaded: %d/%d frames lost, want 1", lost, frames)
	}
}

// TestPriorityOrdering: the tiers are ordered most-disposable (0) to protect-hardest,
// so the core can size repair monotonically in the priority byte.
func TestPriorityOrdering(t *testing.T) {
	order := []PriorityClass{ClassDisposable, ClassEnhancement, ClassBase, ClassRAP, ClassParamSet}
	for i := 1; i < len(order); i++ {
		if order[i].Wire() <= order[i-1].Wire() {
			t.Fatalf("tier %d not above %d", order[i], order[i-1])
		}
	}
	if ClassParamSet.Wire() != uint8(NumClasses)-1 {
		t.Fatalf("ClassParamSet should be the top tier (%d), got %d", uint8(NumClasses)-1, ClassParamSet.Wire())
	}
}
