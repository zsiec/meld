//go:build amd64

package gf

import (
	"bytes"
	"math/rand"
	"testing"
)

// TestMulAddAVX2ByteExact forces the AVX2 path on and checks it is byte-for-byte identical
// to the scalar golden across every coefficient, length (sub-vector tails, exact vectors,
// many vectors), and pointer alignment. Forcing is needed because Rosetta 2 (the
// Apple-Silicon x86 translator the cross-arch test runs under) EXECUTES the AVX2 instructions
// but does not advertise AVX2 in CPUID — so the runtime gate would otherwise skip the path
// and the comparison would be scalar-vs-scalar. On native AVX2 hardware the path is already
// live; this just guarantees it is also exercised on a translating host. (A genuinely
// non-AVX2 x86 — pre-2013 — would fault here; no such machine is a realistic test host.)
func TestMulAddAVX2ByteExact(t *testing.T) {
	old := hasAVX2
	hasAVX2 = true
	defer func() { hasAVX2 = old }()

	rng := rand.New(rand.NewSource(2))
	lengths := []int{0, 1, 7, 15, 31, 32, 33, 48, 63, 64, 65, 127, 256, 1316}
	mk := func(n, off int) []byte {
		b := make([]byte, n+off)
		rng.Read(b)
		return b[off : off+n]
	}
	for c := 0; c < 256; c++ {
		for _, n := range lengths {
			for _, off := range []int{0, 1, 3, 8} {
				src := mk(n, off)
				avx := mk(n, off)
				gold := append([]byte(nil), avx...)
				MulAdd(avx, src, byte(c))
				mulAddScalar(gold, src, byte(c))
				if !bytes.Equal(avx, gold) {
					t.Fatalf("AVX2 MulAdd != golden for c=%d n=%d off=%d", c, n, off)
				}
			}
		}
	}
	t.Logf("native CPUID reports AVX2=%v (forced on for this test)", old)
}
