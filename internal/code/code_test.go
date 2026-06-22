package code

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/gf"
)

const symSize = 48

// randSources returns n random source symbols of symSize bytes.
func randSources(rng *rand.Rand, n int) [][]byte {
	src := make([][]byte, n)
	for i := range src {
		s := make([]byte, symSize)
		rng.Read(s)
		src[i] = s
	}
	return src
}

// refRank computes the rank of a coefficient matrix (rows over n columns) by an
// independent dense Gauss-Jordan over GF(2^8) — the oracle the decoder is checked
// against. It deliberately shares none of the decoder's bookkeeping.
func refRank(rowsIn [][]byte, n int) int {
	rows := make([][]byte, len(rowsIn))
	for i, r := range rowsIn {
		rows[i] = append([]byte(nil), r...)
	}
	rank, pivotRow := 0, 0
	for col := 0; col < n && pivotRow < len(rows); col++ {
		sel := -1
		for r := pivotRow; r < len(rows); r++ {
			if rows[r][col] != 0 {
				sel = r
				break
			}
		}
		if sel == -1 {
			continue
		}
		rows[pivotRow], rows[sel] = rows[sel], rows[pivotRow]
		gf.MulSlice(rows[pivotRow], rows[pivotRow], gf.Inv(rows[pivotRow][col]))
		for r := 0; r < len(rows); r++ {
			if r != pivotRow && rows[r][col] != 0 {
				gf.MulAdd(rows[r], rows[pivotRow], rows[r][col])
			}
		}
		pivotRow++
		rank++
	}
	return rank
}

func unitVec(n, i int) []byte {
	v := make([]byte, n)
	v[i] = 1
	return v
}

// TestSystematicNoLoss: all systematic, no loss, shuffled order → byte-exact.
func TestSystematicNoLoss(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const n = 20
	src := randSources(rng, n)
	dec := NewDecoder(symSize, 0, n)
	order := rng.Perm(n)
	for _, i := range order {
		dec.AddSystematic(uint32(i), src[i])
	}
	if dec.NumDecoded() != n {
		t.Fatalf("decoded %d/%d", dec.NumDecoded(), n)
	}
	for i := 0; i < n; i++ {
		if !bytes.Equal(dec.data[i], src[i]) {
			t.Fatalf("symbol %d mismatch", i)
		}
	}
}

// TestRepairRecoversErasures: drop k systematic, supply k independent repair → all
// recovered byte-exact.
func TestRepairRecoversErasures(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	const n, drop = 16, 4
	src := randSources(rng, n)
	enc := NewEncoder(symSize)
	for _, s := range src {
		enc.Add(s)
	}
	dec := NewDecoder(symSize, 0, n)
	recovered := map[uint32][]byte{}
	collect := func(rs []Recovered) {
		for _, r := range rs {
			recovered[r.ID] = append([]byte(nil), r.Data...)
		}
	}
	// Deliver all but the first `drop` systematic symbols.
	for i := drop; i < n; i++ {
		collect(dec.AddSystematic(uint32(i), src[i]))
	}
	if dec.NumDecoded() != n-drop {
		t.Fatalf("pre-repair decoded %d, want %d", dec.NumDecoded(), n-drop)
	}
	// Supply `drop` repair symbols over the full window.
	for k := 0; k < drop; k++ {
		base, nn, pay := enc.Repair(uint16(k))
		collect(dec.AddRepair(base, nn, uint16(k), pay))
	}
	if dec.NumDecoded() != n {
		t.Fatalf("post-repair decoded %d/%d", dec.NumDecoded(), n)
	}
	for i := 0; i < n; i++ {
		if !bytes.Equal(recovered[uint32(i)], src[i]) {
			t.Fatalf("symbol %d mismatch after repair", i)
		}
	}
}

// TestDependentHarmless: re-feeding a known symbol surfaces nothing and does not
// inflate the rank.
func TestDependentHarmless(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const n = 8
	src := randSources(rng, n)
	dec := NewDecoder(symSize, 0, n)
	for i := 0; i < n; i++ {
		dec.AddSystematic(uint32(i), src[i])
	}
	rank := dec.Rank()
	if rs := dec.AddSystematic(3, src[3]); len(rs) != 0 {
		t.Fatalf("re-feeding recovered %d symbols", len(rs))
	}
	if dec.Rank() != rank {
		t.Fatal("rank changed on a dependent symbol")
	}
}

// TestRankOracle is the core soundness/completeness property: across many random
// erasure patterns of systematic+repair symbols, the decoder's rank equals an
// independently computed matrix rank; it recovers ALL source symbols exactly when
// (and only when) that rank is full; and it never surfaces a wrong symbol.
func TestRankOracle(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 2 + rng.Intn(24)
		src := randSources(rng, n)
		enc := NewEncoder(symSize)
		for _, s := range src {
			enc.Add(s)
		}

		// Build a pool of coded symbols: every systematic plus some repair.
		type item struct {
			coeffs []byte
			feed   func(*Decoder) []Recovered
		}
		var pool []item
		for i := 0; i < n; i++ {
			id, data := uint32(i), src[i]
			pool = append(pool, item{unitVec(n, i), func(d *Decoder) []Recovered {
				return d.AddSystematic(id, data)
			}})
		}
		nrepair := rng.Intn(n + 3)
		for k := 0; k < nrepair; k++ {
			key := uint16(k)
			base, nn, pay := enc.Repair(key)
			pool = append(pool, item{GenCoeffs(key, nn), func(d *Decoder) []Recovered {
				return d.AddRepair(base, nn, key, pay)
			}})
		}
		rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

		// Random erasure.
		dec := NewDecoder(symSize, 0, n)
		recovered := map[uint32][]byte{}
		var survivors [][]byte
		for _, it := range pool {
			if rng.Float64() < 0.35 {
				continue // dropped
			}
			survivors = append(survivors, it.coeffs)
			for _, r := range it.feed(dec) {
				recovered[r.ID] = append([]byte(nil), r.Data...)
			}
		}

		rank := refRank(survivors, n)
		if dec.Rank() != rank {
			t.Fatalf("seed %d: decoder rank %d, oracle rank %d (n=%d)", seed, dec.Rank(), rank, n)
		}
		full := rank == n
		if (dec.NumDecoded() == n) != full {
			t.Fatalf("seed %d: NumDecoded=%d n=%d but oracle rank=%d", seed, dec.NumDecoded(), n, rank)
		}
		// Soundness: every surfaced symbol is correct, regardless of rank.
		for id, data := range recovered {
			if !bytes.Equal(data, src[id]) {
				t.Fatalf("seed %d: surfaced wrong bytes for symbol %d", seed, id)
			}
		}
		// Completeness: at full rank, all symbols are present and correct.
		if full {
			for i := 0; i < n; i++ {
				if !bytes.Equal(recovered[uint32(i)], src[i]) {
					t.Fatalf("seed %d: full rank but symbol %d missing/wrong", seed, i)
				}
			}
		}
	}
}

// TestDeficit tracks the feedback signal as independent symbols arrive.
func TestDeficit(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	const n = 12
	src := randSources(rng, n)
	dec := NewDecoder(symSize, 0, n)
	for i := 0; i < n; i++ {
		if got := dec.Deficit(n); got != n-i {
			t.Fatalf("before symbol %d: deficit %d, want %d", i, got, n-i)
		}
		dec.AddSystematic(uint32(i), src[i])
	}
	if dec.Deficit(n) != 0 {
		t.Fatalf("deficit %d after full rank, want 0", dec.Deficit(n))
	}
}

// TestEncoderSlide checks window eviction and Source lookup.
func TestEncoderSlide(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	src := randSources(rng, 10)
	enc := NewEncoder(symSize)
	for _, s := range src {
		enc.Add(s)
	}
	enc.SlideTo(4)
	if enc.Base() != 4 || enc.Len() != 6 {
		t.Fatalf("after slide: base=%d len=%d", enc.Base(), enc.Len())
	}
	if _, ok := enc.Source(3); ok {
		t.Fatal("evicted symbol 3 still in window")
	}
	got, ok := enc.Source(4)
	if !ok || !bytes.Equal(got, src[4]) {
		t.Fatal("symbol 4 wrong after slide")
	}
}
