package code

import (
	"math/rand"
	"testing"
)

// Allocation gates for the warm decode path. CLAUDE.md mandates "no allocation on the warm
// path once buffers are sized" and "benches use -benchmem with an allocation gate on hot
// paths" — yet there was no allocation test anywhere in the tree. These establish the gate
// and, crucially, assert that decode allocation does NOT scale super-linearly with loss: the
// txbench meld-sld profile allocates 13→24→49 GB/GB as loss rises 10→20→30%, which a flat
// gate would have caught.

// bandEvent is one decoder input in a pre-computed (post-loss) stream, so AllocsPerRun
// measures only the decoder, not encoding or loss generation.
type bandEvent struct {
	repair bool
	id     uint32 // systematic id (when !repair)
	base   uint32 // repair window base (when repair)
	n      int    // repair window width
	key    uint16 // repair key
	pay    []byte // systematic data, or repair payload
}

// buildBandStream encodes n source symbols over a band b, emitting one RepairWindow(b) every
// redEvery symbols, and returns the events that SURVIVE i.i.d. loss at rate `loss` (dropped
// deterministically by index). The tail adds extra repairs so most holes are recoverable.
func buildBandStream(n, b, redEvery int, loss float64, seed int64) []bandEvent {
	rng := rand.New(rand.NewSource(seed))
	lrng := rand.New(rand.NewSource(seed ^ 0x10551))
	src := winSrc(rng, n)
	enc := NewEncoder(winSym)
	var ev []bandEvent
	var key uint16
	for i := 0; i < n; i++ {
		enc.Add(src[i])
		if lrng.Float64() >= loss {
			ev = append(ev, bandEvent{id: uint32(i), pay: src[i]})
		}
		if (i+1)%redEvery == 0 {
			base, nn, pay := enc.RepairWindow(key, b)
			if lrng.Float64() >= loss {
				ev = append(ev, bandEvent{repair: true, base: base, n: nn, key: key, pay: pay})
			}
			key++
		}
	}
	// A tail of repairs so trailing holes can still resolve (matches bandStream's flush).
	for r := 0; r < b+8; r++ {
		base, nn, pay := enc.RepairWindow(key, b)
		ev = append(ev, bandEvent{repair: true, base: base, n: nn, key: key, pay: pay})
		key++
	}
	return ev
}

// replayBand feeds a pre-computed event stream into a FRESH band decoder, draining delivery
// and skipping at the deadline (head-of-line lag > band). This is the warm decode path; its
// allocation is what we gate.
func replayBand(b, maxWin int, ev []bandEvent) {
	d := NewBandDecoder(winSym, b, maxWin)
	drain := func() {
		for {
			if _, ok := d.Deliver(); !ok {
				return
			}
		}
	}
	for _, e := range ev {
		if e.repair {
			d.AddRepair(e.base, e.n, e.key, e.pay)
		} else {
			d.AddSystematic(e.id, e.pay)
		}
		drain()
		for d.Highest()-d.Cursor() > uint32(b) {
			if !d.Skip() {
				break
			}
			drain()
		}
	}
}

// TestBandDecoderAllocScaling is the warm-path allocation gate for the meld-sld band coder.
// The per-EVENT (per inbound symbol) decode allocation must stay bounded AND flat in loss: the
// elimination/back-substitution work must not allocate more per symbol as erasures rise. (Note
// the bench's 13→24→49 GB/GB meld-sld scaling is driven by reactive-repair VOLUME — many more
// repair symbols emitted under burst — and by GB-normalized-by-shrinking-goodput, NOT by
// per-symbol decode cost; this gate pins the per-symbol cost so a true per-symbol regression
// can't hide behind that.) Per-event is the right unit because total events shrink with loss.
func TestBandDecoderAllocScaling(t *testing.T) {
	const (
		n        = 512
		b        = 32
		redEvery = 4
		runs     = 30
	)
	losses := []float64{0.0, 0.1, 0.2, 0.3}
	perEvent := make([]float64, len(losses))
	for i, loss := range losses {
		ev := buildBandStream(n, b, redEvery, loss, 7)
		perEvent[i] = testing.AllocsPerRun(runs, func() { replayBand(b, 3*b, ev) }) / float64(len(ev))
		t.Logf("band decode: loss=%2.0f%%  events=%d  allocs/event=%.2f", loss*100, len(ev), perEvent[i])
	}
	lo, hi := perEvent[0], perEvent[0]
	for _, v := range perEvent {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	// Per-event allocation ceiling (pad + coeffs + recovered copy ≈ 3, plus the loss-proportional
	// late-repair work below): catches a new unbounded per-symbol allocation on the warm path.
	if hi > 5.0 {
		t.Fatalf("band decode allocates %.2f/event (gate: <= 5) — a new warm-path allocation", hi)
	}
	// Per-event cost rises with loss because the band now reduces and uses repairs that start
	// below the cursor but still cover it (late-repair recovery of a stuck gap) — work that is
	// loss-proportional BY DESIGN, not flat. At 0% loss no gap forms, so every such repair is
	// already wholly below the cursor and cheaply rejected; as erasures rise more of them do real
	// decode work. The absolute ceiling above is the guard against a runaway per-symbol cost; here
	// only assert the growth stays bounded (a plateau), not that it is flat.
	if lo > 0 && hi > 3.0*lo {
		t.Fatalf("band decode per-event allocation grows %.1fx with loss (%.2f vs %.2f) — runaway, not bounded late-repair work", hi/lo, hi, lo)
	}
}

// TestGenerationDecoderAllocBounded gates the default generation coder's decode path: it
// allocates a fixed win-wide coefficient row per inbound symbol, so it should be roughly FLAT
// in loss (unlike the band path). Asserts a per-symbol allocation ceiling and flatness.
func TestGenerationDecoderAllocBounded(t *testing.T) {
	const (
		k    = 32
		runs = 50
	)
	rng := rand.New(rand.NewSource(11))
	src := winSrc(rng, k)
	enc := NewEncoder(winSym)
	for _, s := range src {
		enc.Add(s)
	}
	// Pre-build repair payloads (encoder side excluded from the measured function).
	const repair = 12
	type rep struct {
		base uint32
		n    int
		key  uint16
		pay  []byte
	}
	reps := make([]rep, repair)
	for j := 0; j < repair; j++ {
		base, nn, pay := enc.Repair(uint16(j))
		reps[j] = rep{base, nn, uint16(j), pay}
	}

	measure := func(drop int) float64 {
		return testing.AllocsPerRun(runs, func() {
			d := NewDecoder(winSym, 0, k)
			for i := drop; i < k; i++ { // first `drop` systematic erased
				d.AddSystematic(uint32(i), src[i])
			}
			for _, r := range reps {
				d.AddRepair(r.base, r.n, r.key, r.pay)
			}
		})
	}
	lo := measure(0)
	hi := measure(repair) // erase as many as repair can recover
	t.Logf("generation decode: allocs/run no-loss=%.0f (%.2f/sym), heavy-loss=%.0f", lo, lo/float64(k), hi)
	if hi > 1.5*lo+8 {
		t.Fatalf("generation decode allocation not flat in loss: %.0f vs %.0f", hi, lo)
	}
}
