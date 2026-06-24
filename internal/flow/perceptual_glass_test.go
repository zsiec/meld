package flow

import (
	"os"
	"os/exec"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// glassPerceptual streams a real shaped bitstream through Sender/Receiver over an i.i.d.-loss
// channel, mirroring glassDeliver, but classifies each access unit by what the RECEIVER actually
// handed downstream: fully delivered (every chunk), CORRUPT (some chunks but not all — a partial AU
// a decoder would choke on), or lost (nothing). It honours cfg.FrameAtomic and passes the real
// per-unit media descriptor (priority, temporal id, refs, discardable). Returns the whole-unit
// delivery map (for reassembly/ffprobe) and the corrupt-AU count.
func glassPerceptual(t *testing.T, cfg Config, shaped []shape.Shaped, lossP float64, seed uint64) (unitDelivered map[uint32]bool, corrupt int) {
	t.Helper()
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	now := clock.Timestamp(0)
	delivered := map[uint32]bool{}
	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				continue
			}
			var key uint32
			if sym.Kind == wire.Systematic {
				key = sym.SrcIndex
			} else {
				key = (sym.WindowBase*100003 + uint32(sym.RepairKey)) | (1 << 31)
			}
			if coinU(seed, uint32(sym.Kind), key) < lossP {
				continue
			}
			r.FeedSymbol(now, d)
		}
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
		for {
			_, d, ok := r.PollDeliver()
			if !ok {
				break
			}
			delivered[chunkID(d)] = true
		}
	}

	var nextID uint32
	chunksPer := make([]int, len(shaped))
	firstChunk := make([]uint32, len(shaped))
	for ui, sh := range shaped {
		nch := (len(sh.Payload) + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		chunksPer[ui] = nch
		firstChunk[ui] = nextID
		fd := FrameDesc{Priority: sh.Unit.Class.Wire(), FrameID: sh.Unit.ID, RefFrameIDs: sh.Unit.RefersTo,
			Chunks: uint16(nch), RAP: sh.Unit.RAP, Discardable: sh.Unit.Discardable, TemporalID: sh.Unit.TemporalID}
		for c := 0; c < nch; c++ {
			s.WriteFrame(now, makeChunkN(nextID), fd)
			nextID++
			pump()
			now = now.Add(testTick)
			s.Tick(now)
			r.Tick(now)
			pump()
		}
		s.Flush(now)
		pump()
	}
	settle := int(cfg.BufferMicros/testTick) + 8*cfg.GenSize
	for k := 0; k < settle; k++ {
		now = now.Add(testTick)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	now = now.Add(cfg.BufferMicros + int64(nextID)*testTick)
	r.Tick(now)
	pump()

	unitDelivered = make(map[uint32]bool, len(shaped))
	for ui, sh := range shaped {
		got := 0
		for c := 0; c < chunksPer[ui]; c++ {
			if delivered[firstChunk[ui]+uint32(c)] {
				got++
			}
		}
		unitDelivered[sh.Unit.ID] = got == chunksPer[ui]
		if got > 0 && got < chunksPer[ui] {
			corrupt++ // a partial access unit reached the decoder — visual garbage
		}
	}
	return unitDelivered, corrupt
}

// TestGlassFrameAtomicNoCorruption is the headline perceptual proof on REAL media: frame-atomic
// delivery (#15) never hands a decoder a partial access unit, so the receiver-visible stream is
// always cleanly decodable — "deliver something visually right, never a corrupt frame." The naive
// baseline (frame-atomic off, every other parameter equal) leaks many partial AUs on a multi-chunk
// HEVC stream under heavy loss; atomic suppresses them ENTIRELY and decodes MORE of the picture.
// ffprobe confirms the decodable counts against the shaper's model on both arms.
//
// The metric is the DECODABLE set — the transitive-dependency closure a real decoder renders, which
// ffprobe confirms exactly — not raw whole-unit arrivals. (Raw arrivals would credit the naive arm
// with late, orphaned frames this deadline-blind harness drains at the end; those render as garbage
// or not at all, so they are not perceptually real.) Both arms drain identically, so the comparison
// is fair: atomic wins because coherent whole-frame delivery plus early drop of a doomed frame keeps
// reference chains intact, whereas the naive arm's corruption and scattered partial delivery break
// them — a delivered B-frame whose reference was corrupted is not decodable.
func TestGlassFrameAtomicNoCorruption(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb.h265")
	if err != nil {
		t.Skip("no HEVC sample")
	}
	haveFF := false
	if _, err := exec.LookPath("ffprobe"); err == nil {
		haveFF = true
	}
	shaped := shape.NewHEVCShaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}
	// Heavy loss + real multi-chunk frames + a tight redundancy: the regime where a non-atomic
	// receiver leaks partial AUs. Both arms differ ONLY in FrameAtomic — same UEP, budget, channel —
	// so the corruption delta is frame-atomic's alone.
	base := Config{Flow: 1, SymbolSize: 64, GenSize: 24, Redundancy: 0.10, BufferMicros: 80_000}
	const lossP = 0.40

	var atomicCorrupt, naiveCorrupt, atomicDec, naiveDec, confirmed int
	for seed := uint64(1); seed <= 8; seed++ {
		sd := seed * 0x9E3779B1
		aCfg, nCfg := base, base
		aCfg.FrameAtomic, nCfg.FrameAtomic = true, false

		aDeliv, aCorr := glassPerceptual(t, aCfg, shaped, lossP, sd)
		nDeliv, nCorr := glassPerceptual(t, nCfg, shaped, lossP, sd)
		atomicCorrupt += aCorr
		naiveCorrupt += nCorr

		aH, aPics := reassemble(shaped, units, aDeliv)
		nH, nPics := reassemble(shaped, units, nDeliv)
		atomicDec += aPics
		naiveDec += nPics
		if haveFF {
			for _, c := range []struct {
				h []byte
				n int
			}{{aH, aPics}, {nH, nPics}} {
				if c.n == 0 {
					continue
				}
				if got := ffprobeFrames(t, c.h); got != c.n {
					t.Fatalf("seed %d: ffprobe decoded %d frames, model predicted %d", seed, got, c.n)
				}
				confirmed++
			}
		}
		t.Logf("seed %d: atomic{corrupt=%d decodable=%d} | naive{corrupt=%d decodable=%d}", seed, aCorr, aPics, nCorr, nPics)
	}
	t.Logf("TOTALS: atomic corrupt=%d decodable=%d | naive corrupt=%d decodable=%d (%d ffprobe-confirmed)",
		atomicCorrupt, atomicDec, naiveCorrupt, naiveDec, confirmed)

	// #15's guarantee: a partial access unit is NEVER delivered.
	if atomicCorrupt != 0 {
		t.Fatalf("frame-atomic delivered %d partial (corrupt) access units — the whole-or-nothing guarantee broke", atomicCorrupt)
	}
	// The baseline must actually exhibit the corruption frame-atomic prevents, or the test proves nothing.
	if naiveCorrupt < 5 {
		t.Fatalf("the non-atomic baseline leaked only %d partial AUs at loss=%.0f%% — regime too easy to demonstrate the win", naiveCorrupt, 100*lossP)
	}
	// Coherent delivery preserves the dependency DAG: atomic decodes at least as many frames as the
	// naive baseline (here materially more), because whole-frame delivery + early drop keeps reference
	// chains intact while the baseline's corruption breaks them.
	if atomicDec < naiveDec {
		t.Fatalf("frame-atomic decoded FEWER frames (%d) than the naive baseline (%d) — coherence win regressed", atomicDec, naiveDec)
	}
	if haveFF && confirmed == 0 {
		t.Fatal("no non-empty decodable streams were ffprobe-confirmed — loosen the loss/budget")
	}
}
