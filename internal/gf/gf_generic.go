//go:build !arm64 && !amd64

package gf

// MulAdd computes dst[i] ^= c * src[i] for i in [0, len(src)) — the GF(2^8) AXPY that
// accumulates a coefficient-scaled symbol into another. len(dst) must be >= len(src). A
// zero coefficient is a no-op. arm64 (NEON) and amd64 (AVX2) ship SIMD paths; every other
// arch uses this scalar golden — a GFNI amd64 path is a hardware-tested follow-on.
func MulAdd(dst, src []byte, c byte) {
	if c == 0 {
		return
	}
	mulAddScalar(dst, src, c)
}
