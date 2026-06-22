package gf

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestAddIsXOR checks addition is XOR and self-inverse (a+a == 0).
func TestAddIsXOR(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			if got := Add(byte(a), byte(b)); got != byte(a)^byte(b) {
				t.Fatalf("Add(%d,%d)=%d, want %d", a, b, got, byte(a)^byte(b))
			}
		}
		if Add(byte(a), byte(a)) != 0 {
			t.Fatalf("Add(%d,%d) != 0", a, a)
		}
	}
}

// TestMulIdentityAndZero checks 1 and 0 behave as the field identity/annihilator.
func TestMulIdentityAndZero(t *testing.T) {
	for a := 0; a < 256; a++ {
		if Mul(byte(a), 1) != byte(a) || Mul(1, byte(a)) != byte(a) {
			t.Fatalf("Mul identity failed for %d", a)
		}
		if Mul(byte(a), 0) != 0 || Mul(0, byte(a)) != 0 {
			t.Fatalf("Mul zero failed for %d", a)
		}
	}
}

// TestMulCommutative checks a*b == b*a exhaustively.
func TestMulCommutative(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			if Mul(byte(a), byte(b)) != Mul(byte(b), byte(a)) {
				t.Fatalf("Mul not commutative at (%d,%d)", a, b)
			}
		}
	}
}

// TestMulAssociativeAndDistributive samples the ring axioms across the field.
func TestMulAssociativeAndDistributive(t *testing.T) {
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			for c := 0; c < 256; c += 7 { // sample c to keep it O(256^2 * 37)
				ab := Mul(byte(a), byte(b))
				bc := Mul(byte(b), byte(c))
				if Mul(ab, byte(c)) != Mul(byte(a), bc) {
					t.Fatalf("not associative at (%d,%d,%d)", a, b, c)
				}
				// a*(b+c) == a*b + a*c
				lhs := Mul(byte(a), Add(byte(b), byte(c)))
				rhs := Add(Mul(byte(a), byte(b)), Mul(byte(a), byte(c)))
				if lhs != rhs {
					t.Fatalf("not distributive at (%d,%d,%d)", a, b, c)
				}
			}
		}
	}
}

// TestInvAndDiv checks Inv and Div against Mul for every nonzero element.
func TestInvAndDiv(t *testing.T) {
	for a := 1; a < 256; a++ {
		if Mul(byte(a), Inv(byte(a))) != 1 {
			t.Fatalf("Inv(%d) is not a multiplicative inverse", a)
		}
		for b := 1; b < 256; b++ {
			// Div(a,b) * b == a
			if Mul(Div(byte(a), byte(b)), byte(b)) != byte(a) {
				t.Fatalf("Div(%d,%d) wrong", a, b)
			}
			// Div(a*b, b) == a
			if Div(Mul(byte(a), byte(b)), byte(b)) != byte(a) {
				t.Fatalf("Div(Mul(%d,%d),%d) != %d", a, b, b, a)
			}
		}
	}
}

// TestInvDivZero documents the no-panic zero behavior.
func TestInvDivZero(t *testing.T) {
	if Inv(0) != 0 {
		t.Fatal("Inv(0) should return 0 (undefined, no panic)")
	}
	if Div(5, 0) != 0 || Div(0, 5) != 0 {
		t.Fatal("Div by/of zero should return 0 (no panic)")
	}
}

// TestMulAddMatchesScalar checks the vector AXPY against per-element Mul.
func TestMulAddMatchesScalar(t *testing.T) {
	src := make([]byte, 64)
	for i := range src {
		src[i] = byte(i * 3)
	}
	for _, c := range []byte{0, 1, 2, 7, 0x53, 0xFF} {
		dst := make([]byte, len(src))
		for i := range dst {
			dst[i] = byte(255 - i)
		}
		want := make([]byte, len(src))
		copy(want, dst)
		for i := range src {
			want[i] ^= Mul(c, src[i])
		}
		MulAdd(dst, src, c)
		if !bytes.Equal(dst, want) {
			t.Fatalf("MulAdd mismatch for c=%d", c)
		}
	}
}

// TestMulAddInverseRoundTrip checks MulAdd then MulAdd with the same coefficient
// cancels (since addition is its own inverse): dst ^= c*src twice == dst.
func TestMulAddInverseRoundTrip(t *testing.T) {
	src := []byte{1, 2, 3, 4, 250, 251, 252}
	dst := []byte{9, 8, 7, 6, 5, 4, 3}
	orig := append([]byte(nil), dst...)
	MulAdd(dst, src, 0x8C)
	MulAdd(dst, src, 0x8C)
	if !bytes.Equal(dst, orig) {
		t.Fatal("MulAdd applied twice did not cancel")
	}
}

// TestMulSliceNormalizes checks MulSlice(_, row, Inv(p)) makes the pivot 1.
func TestMulSliceNormalizes(t *testing.T) {
	row := []byte{0x53, 0x10, 0x00, 0x9A}
	pivot := row[0]
	out := make([]byte, len(row))
	MulSlice(out, row, Inv(pivot))
	if out[0] != 1 {
		t.Fatalf("normalized pivot = %d, want 1", out[0])
	}
}

// TestMulAddByteExactVsGolden checks the exported MulAdd (the SIMD path on arm64) is
// byte-for-byte identical to the scalar golden mulAddScalar for EVERY coefficient, across
// lengths that exercise sub-vector tails, exact vectors, and many vectors, and at unaligned
// starts — the project rule that a SIMD path must match the scalar golden byte-for-byte.
func TestMulAddByteExactVsGolden(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	lengths := []int{0, 1, 7, 15, 16, 17, 31, 32, 33, 48, 63, 64, 65, 127, 256, 1316}
	mk := func(n, off int) []byte { // an unaligned, randomly-filled slice of length n
		b := make([]byte, n+off)
		rng.Read(b)
		return b[off : off+n]
	}
	for c := 0; c < 256; c++ {
		for _, n := range lengths {
			for _, off := range []int{0, 1, 3, 8} {
				src := mk(n, off)
				simd := mk(n, off)
				gold := append([]byte(nil), simd...)
				MulAdd(simd, src, byte(c))
				mulAddScalar(gold, src, byte(c))
				if !bytes.Equal(simd, gold) {
					t.Fatalf("MulAdd != golden for c=%d n=%d off=%d", c, n, off)
				}
			}
		}
	}
}

// FuzzMulAddVsGolden checks MulAdd matches the scalar golden on arbitrary lengths and data
// (the first byte is the coefficient), complementing the exhaustive-coefficient test above.
func FuzzMulAddVsGolden(f *testing.F) {
	f.Add([]byte{0x53, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			return
		}
		c := data[0]
		src := data[1:]
		simd := make([]byte, len(src))
		rand.New(rand.NewSource(int64(len(src)))).Read(simd)
		gold := append([]byte(nil), simd...)
		MulAdd(simd, src, c)
		mulAddScalar(gold, src, c)
		if !bytes.Equal(simd, gold) {
			t.Fatalf("MulAdd != golden for c=%d len=%d", c, len(src))
		}
	})
}

func BenchmarkMulAdd1316(b *testing.B) {
	src := make([]byte, 1316)
	dst := make([]byte, 1316)
	b.SetBytes(1316)
	for i := 0; i < b.N; i++ {
		MulAdd(dst, src, 0x53)
	}
}

// BenchmarkMulAddScalar1316 benchmarks the scalar golden at the same size, so the SIMD
// speedup is visible side by side.
func BenchmarkMulAddScalar1316(b *testing.B) {
	src := make([]byte, 1316)
	dst := make([]byte, 1316)
	b.SetBytes(1316)
	for i := 0; i < b.N; i++ {
		mulAddScalar(dst, src, 0x53)
	}
}
