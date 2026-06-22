//go:build amd64

package gf

// hasAVX2 records whether the CPU advertises AVX2 (with OS YMM-state support), checked once.
// It gates the SIMD path: an x86 without AVX2 (pre-2013, or a VM that masks it) falls back to
// the scalar golden rather than executing an unsupported instruction.
var hasAVX2 = cpuHasAVX2()

// cpuHasAVX2 reports CPUID/XGETBV support for AVX2 + OS-saved YMM state.
//
//go:noescape
func cpuHasAVX2() bool

// mulAddAVX2 computes dst[i] ^= c*src[i] for n bytes (n a multiple of 32) via the AVX2
// split-table multiply: lo/hi point to the 16-byte mulLo[c]/mulHi[c] tables, broadcast to
// both 128-bit lanes. Implemented in gf_amd64.s.
//
//go:noescape
func mulAddAVX2(dst, src *byte, n int, lo, hi *byte)

// MulAdd computes dst[i] ^= c * src[i] for i in [0, len(src)) — the GF(2^8) AXPY. On an
// AVX2 CPU the 32-byte-aligned bulk runs through the AVX2 split-table multiply (two VPSHUFB
// per 32 bytes); the sub-vector tail (and a non-AVX2 CPU) uses the scalar golden, so the
// result is byte-for-byte identical to mulAddScalar. len(dst) must be >= len(src); a zero
// coefficient is a no-op.
func MulAdd(dst, src []byte, c byte) {
	if c == 0 {
		return
	}
	n := len(src)
	if n == 0 {
		return
	}
	if hasAVX2 {
		bulk := n &^ 31 // round down to a multiple of 32
		if bulk > 0 {
			mulAddAVX2(&dst[0], &src[0], bulk, &mulLo[c][0], &mulHi[c][0])
		}
		if bulk < n {
			mulAddScalar(dst[bulk:n], src[bulk:n], c)
		}
		return
	}
	mulAddScalar(dst, src, c)
}
