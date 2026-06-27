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

// TestGlassFrameAtomicNoCorruption is the opt-in perceptual proof on REAL media: frame-atomic
// delivery (#15) never hands a decoder a partial access unit, so the receiver-visible stream is
// cleanly gapped instead of partially corrupt. The non-atomic baseline (every other parameter
// equal) can leak partial AUs on a multi-chunk HEVC stream under heavy loss, although stronger
// repair may now recover or drop those units cleanly before the non-atomic receiver exposes a
// fragment. This is a decoder hygiene tradeoff, not a byte-stream default: all-or-nothing delivery
// intentionally drops recoverable bytes, so DefaultConfig keeps FrameAtomic off and media
// applications opt in when clean frame gaps are preferable. ffprobe confirms the decodable counts
// against the shaper's model on both arms.
//
// The metric is the DECODABLE set — the transitive-dependency closure a real decoder renders, which
// ffprobe confirms exactly — not raw whole-unit arrivals. (Raw arrivals would credit the naive arm
// with late, orphaned frames this deadline-blind harness drains at the end; those render as garbage
// or not at all, so they are not perceptually real.) Both arms drain identically, so the comparison
// is fair for validating the hygiene property; it intentionally does not assert a delivery win for
// atomic mode.
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
	if haveFF && confirmed == 0 {
		t.Fatal("no non-empty decodable streams were ffprobe-confirmed — loosen the loss/budget")
	}
}

// TestGlassTemporalLayered exercises the media-awareness pipeline on REAL temporal-layer-tagged
// HEVC — bbb_temporal.h265, encoded with x265 --temporal-layers 4 so the bitstream actually carries
// nuh_temporal_id 0..3 (the stock clips are all tid 0, so this is the only real vehicle for the
// temporal mechanisms). It pins three true, load-bearing facts: the shaper reads a genuine 4-layer
// hierarchy; the dependency model stays ffprobe-EXACT on temporal-layered media (no-loss decode);
// and the frame-atomic whole-or-nothing guarantee (#15) holds on this clip too — zero corrupt AUs,
// ffprobe-confirmed decodable counts.
//
// What it does NOT assert, honestly: that temporal-depth UEP (#17) or the temporal shed (#16) BEAT
// flat protection here. Measured on this clip, #17's increment over plain UEP is within noise
// (decodable keyframe rate uep+tid ~= uep ~= flat), and the sender shed (#16) cannot even deplete
// its token bucket on a clip this small (it is rate-validated in shed_test.go instead). The bench is
// the arbiter: on a decodable-frame metric the temporal-protection refinements are marginal, which
// matches the design read that #17 only bites deep (tid>=3) hierarchies. #15, by contrast, is a
// clear real-media win (see TestGlassFrameAtomicNoCorruption).
func TestGlassTemporalLayered(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb_temporal.h265")
	if err != nil {
		t.Skip("no temporal-layered HEVC sample")
	}
	shaped := shape.NewHEVCShaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}

	// A genuine multi-temporal-layer stream: every layer 0..3 must be present, or the clip is not
	// exercising the temporal path at all.
	tids := map[uint8]bool{}
	for _, u := range units {
		if u.Picture {
			tids[u.TemporalID] = true
		}
	}
	for tid := uint8(0); tid <= 3; tid++ {
		if !tids[tid] {
			t.Fatalf("clip is not multi-layer: temporal layer %d absent (saw %v)", tid, tids)
		}
	}

	haveFF := false
	if _, err := exec.LookPath("ffprobe"); err == nil {
		haveFF = true
	}

	// The dependency model is EXACT on temporal-layered HEVC: with no loss the real decoder produces
	// precisely the predicted slices.
	if haveFF {
		full := map[uint32]bool{}
		for _, u := range units {
			full[u.ID] = true
		}
		h, slices := reassemble(shaped, units, full)
		if got := ffprobeFrames(t, h); got != slices {
			t.Fatalf("no-loss: ffprobe decoded %d frames, model predicted %d on temporal-layered media", got, slices)
		}
	}

	// Under loss, frame-atomic delivery never hands the decoder a partial AU on this clip either, and
	// ffprobe confirms the decodable set the model predicts.
	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0.15, BufferMicros: 100_000, FrameAtomic: true}
	const lossP = 0.30
	var corrupt, confirmed, decoded int
	for seed := uint64(1); seed <= 6; seed++ {
		deliv, c := glassPerceptual(t, cfg, shaped, lossP, seed*0x9E3779B1)
		corrupt += c
		h, pics := reassemble(shaped, units, deliv)
		decoded += pics
		if haveFF && pics > 0 {
			if got := ffprobeFrames(t, h); got != pics {
				t.Fatalf("seed %d: ffprobe decoded %d frames, model predicted %d", seed, got, pics)
			}
			confirmed++
		}
	}
	t.Logf("temporal-layered: TID layers=%v | frame-atomic corrupt=%d decodable=%d (%d ffprobe-confirmed)", tids, corrupt, decoded, confirmed)
	if corrupt != 0 {
		t.Fatalf("frame-atomic delivered %d partial AUs on temporal-layered media — the guarantee broke", corrupt)
	}
	if haveFF && confirmed == 0 {
		t.Fatal("no non-empty decodable streams were ffprobe-confirmed — loosen the loss/budget")
	}
}
