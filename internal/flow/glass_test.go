package flow

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/shape"
	"github.com/zsiec/meld/internal/wire"
)

// glassDeliver streams a real shaped bitstream through Sender(WriteFrame)/Receiver over
// an i.i.d.-loss channel and returns which access units fully delivered (all chunks).
// Chunks are id-stamped (the transport only decides delivery; the bytes reassembled for
// the decoder are the originals), uep selects the per-unit tier vs the flat base tier.
func glassDeliver(t *testing.T, cfg Config, shaped []shape.Shaped, uep bool, lossP float64, seed uint64) map[uint32]bool {
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
	for ui, sh := range shaped {
		nch := (len(sh.Payload) + cfg.SymbolSize - 1) / cfg.SymbolSize
		if nch < 1 {
			nch = 1
		}
		chunksPer[ui] = nch
		fd := FrameDesc{Priority: sh.Unit.Class.Wire(), FrameID: sh.Unit.ID, RefFrameIDs: sh.Unit.RefersTo, Chunks: uint16(nch), RAP: sh.Unit.RAP, Discardable: sh.Unit.Discardable}
		if !uep {
			fd.Priority = uint8(uepCenterTier)
		}
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

	unitDelivered := make(map[uint32]bool, len(shaped))
	var idCursor uint32
	for ui, sh := range shaped {
		all := true
		for c := 0; c < chunksPer[ui]; c++ {
			if !delivered[idCursor] {
				all = false
			}
			idCursor++
		}
		unitDelivered[sh.Unit.ID] = all
	}
	return unitDelivered
}

// reassemble concatenates, in stream order, the Annex-B NAL bytes of the units that are
// DECODABLE given the delivered set — the stream a decoder would actually receive.
func reassemble(shaped []shape.Shaped, units []shape.Unit, delivered map[uint32]bool) ([]byte, int) {
	dec := shape.Decodable(units, delivered)
	var out []byte
	slices := 0
	for _, sh := range shaped {
		if !dec[sh.Unit.ID] {
			continue
		}
		out = append(out, 0, 0, 0, 1)
		out = append(out, sh.Payload...)
		if sh.Unit.Picture {
			slices++ // a coded picture (the decoder emits a frame for it)
		}
	}
	return out, slices
}

// reassembleAV1 rebuilds a decodable AV1 low-overhead OBU stream from the units a surviving
// displayed picture actually needs: every Picture that is decodable, plus the transitive
// closure of its references (its hidden alt-refs, its key frame, the sequence header). It
// concatenates each such OBU (which already carries its own size field) and inserts a
// temporal_delimiter_obu at each temporal-unit boundary — a sequence header, or a frame that
// follows another frame — since the shaper drops delimiters. Restricting to the needed
// closure drops orphan units (e.g. a sequence header whose GOP's key frame was lost), which
// would otherwise leave a frameless temporal unit that a strict decoder rejects. Returns the
// stream and the displayed-picture count, which a real decoder must reproduce exactly.
func reassembleAV1(shaped []shape.Shaped, units []shape.Unit, delivered map[uint32]bool) ([]byte, int) {
	dec := shape.Decodable(units, delivered)
	byID := make(map[uint32]shape.Unit, len(units))
	for _, u := range units {
		byID[u.ID] = u
	}
	needed := make(map[uint32]bool, len(units))
	var mark func(id uint32)
	mark = func(id uint32) {
		if needed[id] {
			return
		}
		u, ok := byID[id]
		if !ok {
			return
		}
		needed[id] = true
		for _, r := range u.RefersTo {
			mark(r)
		}
	}
	for _, u := range units {
		if u.Picture && dec[u.ID] {
			mark(u.ID)
		}
	}

	td := []byte{0x12, 0x00} // temporal_delimiter_obu: type 2, has_size, size 0
	var out []byte
	pics, tuHasFrame, first := 0, false, true
	for _, sh := range shaped {
		if !needed[sh.Unit.ID] || len(sh.Payload) == 0 {
			continue
		}
		typ := (sh.Payload[0] >> 3) & 0xF
		isFrame := typ == 3 || typ == 6 // frame_header_obu or frame_obu
		isSeq := typ == 1
		if first || isSeq || (isFrame && tuHasFrame) {
			out = append(out, td...)
			tuHasFrame = false
		}
		first = false
		out = append(out, sh.Payload...)
		if isFrame {
			tuHasFrame = true
		}
		if sh.Unit.Picture {
			pics++
		}
	}
	return out, pics
}

// ffprobeFrames decodes an Annex-B stream and returns the frame count, or -1 if ffprobe
// is unavailable or errors.
func ffprobeFrames(t *testing.T, h264 []byte) int {
	return ffprobeFramesExt(t, h264, "h264")
}

// ffprobeFramesExt is ffprobeFrames with an explicit container extension so ffprobe selects
// the right demuxer (e.g. "obu" for a raw AV1 OBU stream, "h264"/"h265" for Annex-B).
func ffprobeFramesExt(t *testing.T, data []byte, ext string) int {
	t.Helper()
	bin, err := exec.LookPath("ffprobe")
	if err != nil {
		return -1
	}
	f, err := os.CreateTemp(t.TempDir(), "glass-*."+ext)
	if err != nil {
		return -1
	}
	if _, err := f.Write(data); err != nil {
		return -1
	}
	f.Close()
	out, err := exec.Command(bin, "-v", "error", "-count_frames", "-select_streams", "v:0",
		"-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", f.Name()).Output()
	if err != nil {
		return -1
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return -1
	}
	return n
}

// TestGlassToGlassReal is the WP6 glass-to-glass: a real H.264 clip is shaped, streamed
// through Meld over loss, and the receiver-visible DECODABLE set is reassembled and
// handed to a real decoder (ffprobe). The decoder must produce exactly the predicted
// number of frames — confirming the shaper's dependency model against ffmpeg on real
// media — and unequal protection must keep at least as many frames decodable as flat.
func TestGlassToGlassReal(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb.h264")
	if err != nil {
		t.Skip("no real H.264 sample")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}
	shaped := shape.NewAVCShaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}

	// Sanity: with no loss every unit delivers, the reassembled stream is the whole clip,
	// and ffprobe decodes every coded slice — the pipeline (shape → reassemble → decode).
	full := map[uint32]bool{}
	for _, u := range units {
		full[u.ID] = true
	}
	h264, slices := reassemble(shaped, units, full)
	if got := ffprobeFrames(t, h264); got != slices {
		t.Fatalf("no-loss: ffprobe decoded %d frames, want %d (the coded slices)", got, slices)
	}

	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0.4, BufferMicros: 150_000}
	const lossP = 0.30
	var uepTot, flatTot, confirmed int
	for seed := uint64(1); seed <= 4; seed++ {
		uepH, uepSlices := reassemble(shaped, units, glassDeliver(t, cfg, shaped, true, lossP, seed*0x9E3779B1))
		flatH, flatSlices := reassemble(shaped, units, glassDeliver(t, cfg, shaped, false, lossP, seed*0x9E3779B1))
		t.Logf("seed %d: UEP decodable slices=%d | flat decodable slices=%d", seed, uepSlices, flatSlices)
		// The real decoder confirms the model: it decodes exactly the predicted slices
		// (checked only when the stream is non-empty — ffprobe needs an IDR to decode).
		for _, c := range []struct {
			h []byte
			n int
		}{{uepH, uepSlices}, {flatH, flatSlices}} {
			if c.n == 0 {
				continue
			}
			if got := ffprobeFrames(t, c.h); got != c.n {
				t.Fatalf("seed %d: ffprobe decoded %d frames, the model predicted %d", seed, got, c.n)
			}
			confirmed++
		}
		uepTot += uepSlices
		flatTot += flatSlices
	}
	if confirmed == 0 {
		t.Fatal("no non-empty decodable streams were confirmed — loosen the loss/budget")
	}
	// At an equal budget over a real stream, UEP keeps at least as many frames decodable.
	t.Logf("totals: UEP %d frames vs flat %d (%d ffprobe-confirmed streams)", uepTot, flatTot, confirmed)
	if uepTot < flatTot {
		t.Fatalf("UEP decoded fewer real frames (%d) than flat (%d)", uepTot, flatTot)
	}
}

// TestGlassUEPBFrames surfaces the unequal-protection gap glass-to-glass on a REAL
// B-frame stream: a Big Buck Bunny clip encoded with hierarchical B-frames (so ~half the
// frames are non-reference, disposable). At a TIGHT, equal budget UEP steers the budget
// to the IDR + reference spine and sheds the disposable B-frames, keeping materially more
// of the picture DECODABLE than flat protection — which spreads the same budget and lets
// the spine cascade. Decodability is the shaper's dependency model, now EXACT for
// bidirectional B-frames: each B references the two anchors that bracket it in display
// order (POC, §8.2.1), so a real decoder (ffprobe) decodes exactly the predicted set.
func TestGlassUEPBFrames(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb_bframes.h264")
	if err != nil {
		t.Skip("no B-frame H.264 sample")
	}
	shaped := shape.NewAVCShaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}
	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0, BufferMicros: 80_000, MaxBitrate: 2_500_000}
	const lossP = 0.30
	haveFFprobe := false
	if _, err := exec.LookPath("ffprobe"); err == nil {
		haveFFprobe = true
	}
	var uepKey, flatKey float64
	var confirmed int
	for seed := uint64(1); seed <= 6; seed++ {
		uepDeliv := glassDeliver(t, cfg, shaped, true, lossP, seed*0x9E3779B1)
		flatDeliv := glassDeliver(t, cfg, shaped, false, lossP, seed*0x9E3779B1)
		uKF := shape.DecodableKeyframeRate(units, uepDeliv)
		fKF := shape.DecodableKeyframeRate(units, flatDeliv)
		uepKey += uKF
		flatKey += fKF
		line := ""
		if haveFFprobe {
			// Exact decodability check: with POC bracketing every B-frame in the decodable
			// set has BOTH anchors present, so the real decoder emits exactly the predicted
			// number of pictures — no missing-forward-reference gaps (the old approximation).
			uepH, uepSlices := reassemble(shaped, units, uepDeliv)
			flatH, flatSlices := reassemble(shaped, units, flatDeliv)
			for _, c := range []struct {
				h []byte
				n int
			}{{uepH, uepSlices}, {flatH, flatSlices}} {
				if c.n == 0 {
					continue
				}
				if got := ffprobeFrames(t, c.h); got != c.n {
					t.Fatalf("seed %d: ffprobe decoded %d frames, the model predicted %d (B-frame bracketing not exact)", seed, got, c.n)
				}
				confirmed++
			}
			line = " | ffprobe frames UEP=" + strconv.Itoa(uepSlices) + " flat=" + strconv.Itoa(flatSlices)
		}
		t.Logf("seed %d: decodable-keyframe UEP=%.3f | flat=%.3f%s", seed, uKF, fKF, line)
	}
	t.Logf("mean decodable-keyframe rate: UEP %.3f vs flat %.3f", uepKey/6, flatKey/6)
	if haveFFprobe && confirmed == 0 {
		t.Fatal("no non-empty decodable streams were confirmed — loosen the loss/budget")
	}
	// On the real B-frame stream at a tight equal budget, UEP keeps more GOP anchors alive.
	if uepKey <= flatKey {
		t.Fatalf("UEP (%.3f) did not keep more keyframes decodable than flat (%.3f) on the real B-frame stream", uepKey, flatKey)
	}
}

// TestGlassHEVCExact is the H.265 glass-to-glass: a real HEVC clip encoded with a REGULAR
// hierarchical-B GOP is shaped, streamed through Meld over loss, and the receiver-visible
// DECODABLE set is reassembled and decoded by ffprobe. As with AVC, the HEVC shaper
// resolves each B-picture to its two bracketing anchors (parsed POC, §8.3.1); for a regular
// hierarchical-B structure those anchors ARE the actual references, so the dependency model
// is EXACT: ffprobe decodes precisely the predicted picture set, across protection modes
// and seeds, with no missing-reference gaps. (An adaptive encoder that picks farther
// references is approximated, not modelled exactly — the same scope as the AVC shaper.)
func TestGlassHEVCExact(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb.h265")
	if err != nil {
		t.Skip("no HEVC sample")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}
	shaped := shape.NewHEVCShaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}
	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0.4, BufferMicros: 150_000}
	const lossP = 0.20
	var confirmed int
	for seed := uint64(1); seed <= 6; seed++ {
		uepH, uepPics := reassemble(shaped, units, glassDeliver(t, cfg, shaped, true, lossP, seed*0x9E3779B1))
		flatH, flatPics := reassemble(shaped, units, glassDeliver(t, cfg, shaped, false, lossP, seed*0x9E3779B1))
		for _, c := range []struct {
			h []byte
			n int
		}{{uepH, uepPics}, {flatH, flatPics}} {
			if c.n == 0 {
				continue
			}
			if got := ffprobeFrames(t, c.h); got != c.n {
				t.Fatalf("seed %d: ffprobe decoded %d HEVC frames, the model predicted %d (POC bracketing not exact)", seed, got, c.n)
			}
			confirmed++
		}
		t.Logf("seed %d: decodable pictures UEP=%d flat=%d (ffprobe-confirmed)", seed, uepPics, flatPics)
	}
	if confirmed == 0 {
		t.Fatal("no non-empty decodable HEVC streams were confirmed — loosen the loss/budget")
	}
}

// TestGlassAV1Exact is the AV1 glass-to-glass: a real low-overhead OBU stream (with hidden
// alt-ref frames and show_existing_frame) is shaped, streamed through Meld over loss, and the
// receiver-visible DECODABLE set is reassembled into a valid OBU stream and decoded by
// ffprobe (libdav1d). The AV1 shaper resolves each inter frame's references EXACTLY through
// its eight reference slots (ref_frame_idx, with set_frame_refs for short signaling), tracks
// hidden references separately from displayed pictures, and resolves show_existing_frame to
// the slot it displays — so the real decoder emits precisely the predicted displayed-picture
// count, across protection modes and seeds.
func TestGlassAV1Exact(t *testing.T) {
	data, err := os.ReadFile("../shape/testdata/bbb.obu")
	if err != nil {
		t.Skip("no AV1 sample")
	}
	if _, err := exec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe not available")
	}
	shaped := shape.NewAV1Shaper().Shape(data)
	units := make([]shape.Unit, len(shaped))
	for i, sh := range shaped {
		units[i] = sh.Unit
	}
	cfg := Config{Flow: 1, SymbolSize: 128, GenSize: 16, Redundancy: 0.4, BufferMicros: 150_000}
	const lossP = 0.15
	var confirmed int
	for seed := uint64(1); seed <= 6; seed++ {
		uepH, uepPics := reassembleAV1(shaped, units, glassDeliver(t, cfg, shaped, true, lossP, seed*0x9E3779B1))
		flatH, flatPics := reassembleAV1(shaped, units, glassDeliver(t, cfg, shaped, false, lossP, seed*0x9E3779B1))
		for _, c := range []struct {
			h []byte
			n int
		}{{uepH, uepPics}, {flatH, flatPics}} {
			if c.n == 0 {
				continue
			}
			if got := ffprobeFramesExt(t, c.h, "obu"); got != c.n {
				t.Fatalf("seed %d: ffprobe decoded %d AV1 frames, the model predicted %d (slot model not exact)", seed, got, c.n)
			}
			confirmed++
		}
		t.Logf("seed %d: decodable pictures UEP=%d flat=%d (ffprobe-confirmed)", seed, uepPics, flatPics)
	}
	if confirmed == 0 {
		t.Fatal("no non-empty decodable AV1 streams were confirmed — loosen the loss/budget")
	}
}
