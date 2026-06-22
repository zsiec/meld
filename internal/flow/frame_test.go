package flow

import (
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// runFrameFlow streams a synthetic GOP through Sender.WriteFrame (carrying the per-unit
// frame descriptor) and a Receiver over an i.i.d.-loss channel, then returns the
// receiver's PARSE-FREE FrameStats alongside the oracle keyframe/frame rates computed
// from which units actually fully delivered. The two should agree: the receiver
// reconstructs decodability from the wire descriptors without parsing the codec.
func runFrameFlow(t *testing.T, cfg Config, units []shape.Unit, lossP float64, seed uint64) (fs FrameStats, oracleKF, oracleFR float64) {
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
	chunksPer := make([]int, len(units))
	for ui, u := range units {
		nch := (u.Size + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		chunksPer[ui] = nch
		fd := FrameDesc{Priority: u.Class.Wire(), FrameID: u.ID, RefFrameIDs: u.RefersTo, Chunks: uint16(nch), RAP: u.RAP, Discardable: u.Discardable}
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

	unitDelivered := make(map[uint32]bool, len(units))
	var idCursor uint32
	for ui, u := range units {
		all := true
		for c := 0; c < chunksPer[ui]; c++ {
			if !delivered[idCursor] {
				all = false
			}
			idCursor++
		}
		unitDelivered[u.ID] = all
	}
	return r.FrameStats(), shape.DecodableKeyframeRate(units, unitDelivered), shape.DecodableFrameRate(units, unitDelivered)
}

// TestReceiverFrameStatsNoLoss: with no loss the receiver reports every keyframe (and
// every frame) decodable, parse-free from the wire descriptors.
func TestReceiverFrameStatsNoLoss(t *testing.T) {
	units := shape.GenerateGOP(8, 16)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 200_000}
	fs, _, _ := runFrameFlow(t, cfg, units, 0, 1)
	if fs.Keyframes != 8 {
		t.Fatalf("receiver saw %d keyframes, want 8 (one IDR per GOP)", fs.Keyframes)
	}
	if fs.DecodableKeyframes != fs.Keyframes || fs.DecodableFrames != fs.Frames {
		t.Fatalf("no loss but not all decodable: %+v", fs)
	}
}

// TestReceiverFrameStatsTracksOracle: under heavy loss the receiver's parse-free
// keyframe-decodability rate matches the oracle computed from actual delivery, and some
// keyframes are lost (a meaningful comparison).
func TestReceiverFrameStatsTracksOracle(t *testing.T) {
	units := shape.GenerateGOP(16, 16)
	cfg := Config{Flow: 1, SymbolSize: 256, GenSize: 16, Redundancy: 0, BufferMicros: 60_000, MaxBitrate: 6_000_000}
	for seed := uint64(1); seed <= 4; seed++ {
		fs, oracleKF, _ := runFrameFlow(t, cfg, units, 0.45, seed*0x9E3779B1)
		if fs.Keyframes == 0 {
			t.Fatalf("seed %d: no keyframes resolved", seed)
		}
		recvKF := float64(fs.DecodableKeyframes) / float64(fs.Keyframes)
		t.Logf("seed %d: receiver keyframe %.3f (%d/%d) vs oracle %.3f", seed, recvKF, fs.DecodableKeyframes, fs.Keyframes, oracleKF)
		if recvKF >= 1.0 {
			t.Fatalf("seed %d: expected some keyframe loss under 45%% loss, got %.3f", seed, recvKF)
		}
		if d := recvKF - oracleKF; d < -0.12 || d > 0.12 {
			t.Fatalf("seed %d: receiver keyframe rate %.3f diverges from oracle %.3f", seed, recvKF, oracleKF)
		}
	}
}
