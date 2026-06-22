// Package gf implements arithmetic over GF(2^8), the Galois field underpinning
// Meld's network coding (internal/code). The field uses the primitive polynomial
// 0x11D (x^8 + x^4 + x^3 + x^2 + 1) with generator 2 — the same field as
// Reed-Solomon / QR codes — so coefficients are interoperable with standard
// erasure-coding references.
//
// Addition and subtraction are both XOR. Multiplication, division, and inversion
// use precomputed log/antilog tables; a full 256x256 product table makes the
// vector operation MulAdd (dst ^= c*src, the network-coding hot path) a per-byte
// table lookup. The package is pure and deterministic: no clock, no I/O, and no
// mutable global state after package initialization.
//
// Division and inversion of zero are undefined (zero has no multiplicative
// inverse); callers must not invoke them on a zero divisor. They return 0 rather
// than panicking, honoring Meld's no-panic-in-library rule, but the result is not
// meaningful.
package gf

const (
	// poly is the primitive polynomial 0x11D used to reduce products back into
	// the field (x^8 + x^4 + x^3 + x^2 + 1).
	poly = 0x11D
	// order is the size of the multiplicative group (2^8 - 1). Exponents are
	// reduced modulo this.
	order = 255
)

var (
	// expTbl[i] = 2^i in GF(2^8). Doubled to length 2*order so Mul can index
	// logTbl[a]+logTbl[b] (max 254+254=508) without a modulo.
	expTbl [2 * order]byte
	// logTbl[expTbl[i]] = i. logTbl[0] is unused (the logarithm of 0 is
	// undefined) and left zero.
	logTbl [256]byte
	// mulTbl[a][b] = a*b in GF(2^8). 64 KiB; makes Mul and the MulAdd/MulSlice
	// inner loops a single table lookup per byte.
	mulTbl [256][256]byte
	// mulLo[c]/mulHi[c] are the per-coefficient split (nibble) tables the SIMD MulAdd
	// uses: for a byte b = (hi<<4)|lo, c*b = mulHi[c][hi] ^ mulLo[c][lo], so a 16-byte
	// vector table lookup (NEON TBL / x86 PSHUFB) multiplies a whole register at once.
	// mulLo[c][v] = c*v and mulHi[c][v] = c*(v<<4), for v in 0..15. 8 KiB total.
	mulLo [256][16]byte
	mulHi [256][16]byte
)

func init() {
	// Generate the cyclic multiplicative group <2>. The 255 nonzero elements are
	// exactly 2^0..2^254; 2 is primitive for poly 0x11D so this hits them all.
	x := 1
	for i := 0; i < order; i++ {
		expTbl[i] = byte(x)
		expTbl[i+order] = byte(x)
		logTbl[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= poly
		}
	}
	for a := 1; a < 256; a++ {
		la := int(logTbl[a])
		row := &mulTbl[a]
		for b := 1; b < 256; b++ {
			row[b] = expTbl[la+int(logTbl[b])]
		}
	}
	// row/column 0 stay zero (a*0 = 0*b = 0).
	// Derive the SIMD split tables from the full product table.
	for c := 0; c < 256; c++ {
		for v := 0; v < 16; v++ {
			mulLo[c][v] = mulTbl[c][v]    // c * v        (low nibble contribution)
			mulHi[c][v] = mulTbl[c][v<<4] // c * (v << 4) (high nibble contribution)
		}
	}
}

// Add returns a + b in GF(2^8), which is XOR. Subtraction is identical, so this
// also serves as Sub.
func Add(a, b byte) byte { return a ^ b }

// Mul returns a * b in GF(2^8).
func Mul(a, b byte) byte { return mulTbl[a][b] }

// Inv returns the multiplicative inverse of a (a != 0), i.e. the y with a*y = 1.
// Inv(0) is undefined and returns 0.
func Inv(a byte) byte {
	if a == 0 {
		return 0
	}
	return expTbl[order-int(logTbl[a])]
}

// Div returns a / b in GF(2^8) (b != 0). Div by zero is undefined and returns 0.
func Div(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return expTbl[int(logTbl[a])-int(logTbl[b])+order]
}

// mulAddScalar is the portable byte-at-a-time GF(2^8) AXPY (dst[i] ^= c*src[i]) and the
// GOLDEN reference every SIMD MulAdd must match byte-for-byte. The exported MulAdd dispatches
// to a SIMD implementation where one exists (gf_arm64.go) and to this otherwise
// (gf_generic.go); MulAdd's SIMD path uses this for the sub-vector tail.
func mulAddScalar(dst, src []byte, c byte) {
	t := &mulTbl[c]
	for i, s := range src {
		dst[i] ^= t[s]
	}
}

// MulSlice computes dst[i] = c * src[i] for i in [0, len(src)). len(dst) must be
// >= len(src). With c = Inv(p) this normalizes a row so its pivot becomes 1.
func MulSlice(dst, src []byte, c byte) {
	t := &mulTbl[c]
	for i, s := range src {
		dst[i] = t[s]
	}
}
