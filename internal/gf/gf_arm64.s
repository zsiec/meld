//go:build arm64

#include "textflag.h"

// func mulAddNEON(dst, src *byte, n int, lo, hi *byte)
//
// dst[i] ^= c*src[i] for n bytes (n a multiple of 16). lo/hi are the 16-byte split tables
// for coefficient c: for a byte b = (hi4<<4)|lo4, c*b = hi[hi4] ^ lo[lo4]. NEON TBL does the
// 16-entry lookup of a whole register at once, so each iteration multiplies 16 bytes.
TEXT ·mulAddNEON(SB), NOSPLIT, $0-40
	MOVD dst+0(FP), R0
	MOVD src+8(FP), R1
	MOVD n+16(FP), R2
	MOVD lo+24(FP), R3
	MOVD hi+32(FP), R4

	VLD1 (R3), [V0.B16]            // lo table:  c*0 .. c*15
	VLD1 (R4), [V1.B16]            // hi table:  c*(0<<4) .. c*(15<<4)
	MOVD $0x0f, R5
	VDUP R5, V2.B16               // 0x0f x16 (low-nibble mask)

loop:
	VLD1 (R1), [V3.B16]           // 16 source bytes
	ADD  $16, R1, R1
	VAND V2.B16, V3.B16, V4.B16   // low nibbles
	VUSHR $4, V3.B16, V5.B16      // high nibbles (0..15, no mask needed)
	VTBL V4.B16, [V0.B16], V6.B16 // c * low-nibble
	VTBL V5.B16, [V1.B16], V7.B16 // c * high-nibble
	VEOR V7.B16, V6.B16, V6.B16   // c * src
	VLD1 (R0), [V16.B16]          // current dst
	VEOR V16.B16, V6.B16, V16.B16 // dst ^= c*src
	VST1 [V16.B16], (R0)
	ADD  $16, R0, R0
	SUB  $16, R2, R2
	CBNZ R2, loop
	RET
