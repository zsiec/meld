//go:build arm64

package gf

// mulAddNEON computes dst[i] ^= c*src[i] for n bytes (n a multiple of 16) using the NEON
// split-table multiply: lo/hi point to the 16-byte mulLo[c]/mulHi[c] tables. Implemented in
// gf_arm64.s.
//
//go:noescape
func mulAddNEON(dst, src *byte, n int, lo, hi *byte)

// MulAdd computes dst[i] ^= c * src[i] for i in [0, len(src)) — the GF(2^8) AXPY. On arm64
// the 16-byte-aligned bulk runs through the NEON split-table multiply (one vector TBL per
// nibble); the sub-vector tail falls back to the scalar golden, so the result is byte-for-
// byte identical to mulAddScalar. len(dst) must be >= len(src); a zero coefficient is a
// no-op.
func MulAdd(dst, src []byte, c byte) {
	if c == 0 {
		return
	}
	n := len(src)
	if n == 0 {
		return
	}
	bulk := n &^ 15 // round down to a multiple of 16
	if bulk > 0 {
		mulAddNEON(&dst[0], &src[0], bulk, &mulLo[c][0], &mulHi[c][0])
	}
	if bulk < n {
		mulAddScalar(dst[bulk:n], src[bulk:n], c)
	}
}
