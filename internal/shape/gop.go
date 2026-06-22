package shape

import "math/bits"

// GenerateGOP returns a deterministic synthetic GOP stream for exercising unequal
// protection without a real codec: `gops` groups of `gopLen` frames (gopLen a power of
// 2), each group a parameter set → IDR → a hierarchical-B temporal pyramid.
//
// The temporal pyramid is LEAF-HEAVY, the real contribution-video shape: a thin base
// layer (tid 0) carries the motion spine, and most frames are higher-temporal-layer
// enhancement/disposable leaves. The dependency shape is the one UEP exists for:
//
//	paramSet → IDR → base → base → base → ...     (the spine; a loss CASCADES the GOP)
//	                  │       │       │
//	                  └─ enhancement / disposable leaves ─┘   (a loss is LOCAL: one frame)
//
// Every leaf references the nearest preceding base frame, so losing a base poisons the
// whole sub-GOP hanging off it while losing a leaf drops only itself. Because the base
// spine is a small fraction of the bytes, a tight budget steered onto it keeps a
// watchable reduced-frame-rate picture decodable where a flat budget — spread across the
// many leaves — lets the spine break and the GOP collapse. That contrast is the metric
// DecodableFrameRate measures.
//
// Tier mapping by temporal id: tid 0 → ClassBase, tid 1 → ClassEnhancement, tid ≥ 2 →
// ClassDisposable; the IDR is ClassRAP and the parameter set ClassParamSet.
func GenerateGOP(gops, gopLen int) []Unit {
	const (
		paramSize = 80    // parameter sets are tiny — the cheapest high-leverage bytes
		idrSize   = 3_000 // the intra anchor (kept modest so the leaf layers dominate the byte budget)
		baseSize  = 2_000 // base-layer reference frames
		enhSize   = 1_200 // mid temporal layer
		dispSize  = 600   // top temporal layer (smallest, but the most numerous)
	)
	if gopLen < 2 {
		gopLen = 2
	}
	tl := bits.Len(uint(gopLen)) - 1 // temporal layers (gopLen = 2^tl)
	var units []Unit
	var id uint32
	for g := 0; g < gops; g++ {
		ps := id
		id++
		units = append(units, Unit{ID: ps, Class: ClassParamSet, Confidence: Signaled, Size: paramSize})

		idr := id
		id++
		units = append(units, Unit{ID: idr, Class: ClassRAP, RAP: true, Picture: true, Confidence: Signaled, Size: idrSize, RefersTo: []uint32{ps}})

		prevBase := idr
		for p := 1; p < gopLen; p++ {
			// Hierarchical-B temporal id: position p sits at layer tl-1-ntz(p), so the
			// base layer (tid 0) lands only on the most-divisible positions — leaf-heavy.
			tid := tl - 1 - bits.TrailingZeros(uint(p))
			if tid < 0 {
				tid = 0
			}
			uid := id
			id++
			switch tid {
			case 0:
				units = append(units, Unit{ID: uid, Class: ClassBase, Picture: true, TemporalID: 0, Confidence: Signaled, Size: baseSize, RefersTo: []uint32{prevBase}})
				prevBase = uid
			case 1:
				units = append(units, Unit{ID: uid, Class: ClassEnhancement, Picture: true, TemporalID: 1, Discardable: true, Confidence: Signaled, Size: enhSize, RefersTo: []uint32{prevBase}})
			default:
				units = append(units, Unit{ID: uid, Class: ClassDisposable, Picture: true, TemporalID: uint8(tid), Discardable: true, Confidence: Signaled, Size: dispSize, RefersTo: []uint32{prevBase}})
			}
		}
	}
	return units
}
