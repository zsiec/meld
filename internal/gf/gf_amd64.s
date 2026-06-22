//go:build amd64

#include "textflag.h"

DATA mask0f<>+0(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA mask0f<>+8(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA mask0f<>+16(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA mask0f<>+24(SB)/8, $0x0f0f0f0f0f0f0f0f
GLOBL mask0f<>(SB), RODATA|NOPTR, $32

// func cpuHasAVX2() bool
// CPUID.1:ECX OSXSAVE(27)+AVX(28), XGETBV XCR0 SSE(1)+AVX(2), CPUID.7:EBX AVX2(5).
TEXT ·cpuHasAVX2(SB), NOSPLIT, $0-1
	MOVL $1, AX
	CPUID
	ANDL $0x18000000, CX // OSXSAVE | AVX
	CMPL CX, $0x18000000
	JNE  no
	MOVL $0, CX
	XGETBV               // -> EDX:EAX = XCR0
	ANDL $0x6, AX        // XMM | YMM state saved by the OS
	CMPL AX, $0x6
	JNE  no
	MOVL $7, AX
	MOVL $0, CX
	CPUID
	ANDL $0x20, BX       // AVX2 (EBX bit 5)
	CMPL BX, $0x20
	JNE  no
	MOVB $1, ret+0(FP)
	RET
no:
	MOVB $0, ret+0(FP)
	RET

// func mulAddAVX2(dst, src *byte, n int, lo, hi *byte)  ; n a multiple of 32.
//
// For a byte b = (hi4<<4)|lo4, c*b = hi[hi4] ^ lo[lo4]. VPSHUFB does a 16-entry per-128-bit-
// lane lookup, so the 16-byte tables are broadcast to both lanes and one VPSHUFB per nibble
// multiplies 32 bytes. AVX2 has no byte-granularity shift, so the high nibble (VPSRLW $4)
// is masked back to 0..15 before its lookup.
TEXT ·mulAddAVX2(SB), NOSPLIT, $0-40
	MOVQ dst+0(FP), AX
	MOVQ src+8(FP), BX
	MOVQ n+16(FP), CX
	MOVQ lo+24(FP), DX
	MOVQ hi+32(FP), SI
	VBROADCASTI128 (DX), Y0       // lo table in both lanes
	VBROADCASTI128 (SI), Y1       // hi table in both lanes
	VMOVDQU        mask0f<>(SB), Y2 // 0x0f x32
loop:
	VMOVDQU (BX), Y3              // 32 source bytes
	VPAND   Y2, Y3, Y4           // low nibbles
	VPSRLW  $4, Y3, Y5           // high nibbles (word shift...)
	VPAND   Y2, Y5, Y5           // ...masked to 0..15
	VPSHUFB Y4, Y0, Y6           // c * low-nibble
	VPSHUFB Y5, Y1, Y7           // c * high-nibble
	VPXOR   Y7, Y6, Y6           // c * src
	VMOVDQU (AX), Y8             // current dst
	VPXOR   Y8, Y6, Y8           // dst ^= c*src
	VMOVDQU Y8, (AX)
	ADDQ    $32, BX
	ADDQ    $32, AX
	SUBQ    $32, CX
	JNZ     loop
	VZEROUPPER
	RET
