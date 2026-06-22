package code

import (
	"bytes"
	"math/rand"
	"testing"
)

// winSym is the symbol size the sliding/band decoder tests use; winSrc builds n random
// source symbols of that size.
const winSym = 48

func winSrc(rng *rand.Rand, n int) [][]byte {
	s := make([][]byte, n)
	for i := range s {
		b := make([]byte, winSym)
		rng.Read(b)
		s[i] = b
	}
	return s
}

// assertWinOrdered checks that delivered source ids are strictly increasing.
func assertWinOrdered(t *testing.T, ids []uint32) {
	t.Helper()
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("delivery not strictly increasing at %d: %d then %d", i, ids[i-1], ids[i])
		}
	}
}

// bandStream runs a streaming session through a bounded coding window of b: the
// encoder (pruned to the decoder's cursor) emits one RepairWindow(b) every redEvery
// source symbols; drop(idx,kind) decides losses; a loss not recovered within b
// symbols of the frontier is skipped (its deadline). The caller's drain checks order
// and bytes; returns the delivered ids and the lost count.
func bandStream(t *testing.T, n, b, redEvery int, seed int64, drop func(idx int, kind byte) bool) (got []uint32, lost uint64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	src := winSrc(rng, n)
	enc := NewEncoder(winSym)
	d := NewBandDecoder(winSym, b, 3*b)
	var key uint16
	sysIdx, repIdx := 0, 0
	drain := func() {
		for {
			r, ok := d.Deliver()
			if !ok {
				return
			}
			if int(r.ID) >= len(src) || !bytes.Equal(r.Data, src[r.ID]) {
				t.Fatalf("delivered wrong bytes for id %d", r.ID)
			}
			got = append(got, r.ID)
		}
	}
	deadline := func() {
		for d.Highest()-d.Cursor() > uint32(b) {
			drain()
			if !d.Skip() {
				break
			}
			drain()
		}
	}
	for i := 0; i < n; i++ {
		enc.Add(src[i])
		if !drop(sysIdx, 's') {
			d.AddSystematic(uint32(i), src[i])
		}
		sysIdx++
		if (i+1)%redEvery == 0 {
			base, nn, pay := enc.RepairWindow(key, b)
			if !drop(repIdx, 'r') {
				d.AddRepair(base, nn, key, pay)
			}
			key++
			repIdx++
		}
		enc.SlideTo(d.Cursor())
		drain()
		deadline()
	}
	for round := 0; round < b+16 && d.Cursor() < uint32(n); round++ {
		base, nn, pay := enc.RepairWindow(key, b)
		d.AddRepair(base, nn, key, pay)
		key++
		enc.SlideTo(d.Cursor())
		drain()
	}
	for guard := 0; d.Cursor() < uint32(n) && guard < 4*n; guard++ {
		drain()
		if d.Cursor() < uint32(n) {
			d.Skip()
		}
		drain()
	}
	drain()
	return got, d.Lost()
}

func TestBandNoLoss(t *testing.T) {
	got, lost := bandStream(t, 200, 32, 4, 1, func(int, byte) bool { return false })
	assertWinOrdered(t, got)
	if lost != 0 || len(got) != 200 {
		t.Fatalf("delivered %d/200, lost %d", len(got), lost)
	}
}

// TestBandStreamRecoverable: 50% repair (RepairWindow 32) covers a 1-in-4 systematic
// loss within the coding window, so the band decoder recovers everything in order.
func TestBandStreamRecoverable(t *testing.T) {
	drop := func(idx int, kind byte) bool { return kind == 's' && idx%4 == 0 }
	got, lost := bandStream(t, 400, 32, 2, 7, drop)
	assertWinOrdered(t, got)
	if lost != 0 || len(got) != 400 {
		t.Fatalf("delivered %d/400, lost %d (want full, no loss)", len(got), lost)
	}
}

// TestBandStreamHeavyLoss: loss exceeds the coding budget, so some symbols are
// unrecoverable and skipped — but every delivered symbol is correct and in order,
// with full loss accounting.
func TestBandStreamHeavyLoss(t *testing.T) {
	for seed := int64(0); seed < 30; seed++ {
		rng := rand.New(rand.NewSource(seed + 100))
		drop := func(idx int, kind byte) bool { return rng.Float64() < 0.4 }
		const n = 240
		got, lost := bandStream(t, n, 24, 2, seed, drop)
		assertWinOrdered(t, got)
		if uint64(len(got))+lost != uint64(n) {
			t.Fatalf("seed %d: accounting %d+%d != %d", seed, len(got), lost, n)
		}
		if lost == 0 {
			t.Fatalf("seed %d: expected unrecoverable loss but lost=0", seed)
		}
	}
}

// TestBandSoundness: across random streams the band decoder never delivers a wrong
// symbol nor out of order, regardless of loss.
func TestBandSoundness(t *testing.T) {
	for seed := int64(0); seed < 50; seed++ {
		rng := rand.New(rand.NewSource(seed))
		p := 0.05 + rng.Float64()*0.5
		drop := func(idx int, kind byte) bool { return rng.Float64() < p }
		got, lost := bandStream(t, 200, 16+rng.Intn(32), 1+rng.Intn(3), seed+1, drop)
		assertWinOrdered(t, got)
		if uint64(len(got))+lost != 200 {
			t.Fatalf("seed %d: accounting %d+%d != 200", seed, len(got), lost)
		}
	}
}
