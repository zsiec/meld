// Command glassbench is a glass-to-glass media-decodability comparison with ffprobe as
// an external decoder check. It streams the same real AVC, HEVC, or AV1
// clip through candidate transports over one shared impairment model (loss + one-way delay,
// matched latency budget) — Meld with media-aware unequal protection (WriteFrame/UEP),
// Meld media-blind (Write/flat), real libSRT, and real libRIST — then reassembles each
// receiver's DELIVERED set into an Annex-B stream and asks ffprobe how many frames
// actually decode. The metric is picture-level QoE (decodable frames / keyframes), not
// byte delivery: the point is that a lost keyframe poisons a whole GOP, so WHICH packets
// survive matters as much as how many. Meld (in-module) can use internal/shape for the
// dependency model; the C stacks use the same byte-level harness as the SRT/RIST benchmark.
package main

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zsiec/meld"
	"github.com/zsiec/meld/internal/shape"
)

const seqHdr = 4 // bytes of sequence number prefixed to each chunk

type elementaryFormat uint8

const (
	formatAVC elementaryFormat = iota
	formatHEVC
	formatAV1
)

func (f elementaryFormat) name() string {
	switch f {
	case formatHEVC:
		return "hevc"
	case formatAV1:
		return "av1"
	default:
		return "avc"
	}
}

func (f elementaryFormat) ffprobeDemuxer() string {
	switch f {
	case formatHEVC:
		return "hevc"
	case formatAV1:
		return "obu"
	default:
		return "h264"
	}
}

func (f elementaryFormat) tempSuffix() string {
	switch f {
	case formatHEVC:
		return ".h265"
	case formatAV1:
		return ".obu"
	default:
		return ".h264"
	}
}

func (f elementaryFormat) usesAnnexB() bool { return f != formatAV1 }

func formatForClip(path string) (elementaryFormat, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".h264", ".264", ".avc":
		return formatAVC, nil
	case ".h265", ".265", ".hevc":
		return formatHEVC, nil
	case ".obu", ".av1":
		return formatAV1, nil
	default:
		return 0, fmt.Errorf("unsupported elementary-stream extension %q (want AVC .h264, HEVC .h265, or AV1 .obu)", filepath.Ext(path))
	}
}

func shaperForFormat(format elementaryFormat, avcOpts shape.AVCOptions) shape.Shaper {
	switch format {
	case formatHEVC:
		return shape.NewHEVCShaper()
	case formatAV1:
		return shape.NewAV1Shaper()
	default:
		return shape.NewAVCShaperWithOptions(avcOpts)
	}
}

// chunked is the source: the clip split into transport chunks plus the unit graph the
// arbiter scores against. chunkUnit[seq] is the unit a chunk belongs to; unitChunks[id]
// is the set of chunk seqs that make up a unit (delivered iff all arrive).
type chunked struct {
	chunks     [][]byte // [seqHdr | video bytes], one per transport packet
	units      []shape.Unit
	shaped     []shape.Shaped
	unitChunks map[uint32][]uint32
	chunkSize  int
	format     elementaryFormat
}

func chunkClip(path string, chunkSize int, avcOpts shape.AVCOptions) (*chunked, error) {
	format, err := formatForClip(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	shaped := shaperForFormat(format, avcOpts).Shape(data)
	if len(shaped) == 0 {
		return nil, fmt.Errorf("%s shaper found no transportable units in %s", format.name(), path)
	}
	c := &chunked{
		units: make([]shape.Unit, len(shaped)), shaped: shaped,
		unitChunks: map[uint32][]uint32{}, chunkSize: chunkSize, format: format,
	}
	var seq uint32
	for i, sh := range shaped {
		c.units[i] = sh.Unit
		body := sh.Payload
		if len(body) == 0 {
			body = []byte{0}
		}
		for off := 0; off < len(body); off += chunkSize {
			end := off + chunkSize
			if end > len(body) {
				end = len(body)
			}
			pkt := make([]byte, seqHdr+(end-off))
			binary.BigEndian.PutUint32(pkt[:seqHdr], seq)
			copy(pkt[seqHdr:], body[off:end])
			c.chunks = append(c.chunks, pkt)
			c.unitChunks[sh.Unit.ID] = append(c.unitChunks[sh.Unit.ID], seq)
			seq++
		}
	}
	return c, nil
}

const x264CadenceIntraRefresh = "intra-refresh"

func transcodeX264CadenceCRF(path string, interval, crf int) (string, func(), error) {
	if interval <= 0 {
		return path, func() {}, nil
	}
	f, err := os.CreateTemp("", "glass-x264-*.h264")
	if err != nil {
		return "", nil, err
	}
	out := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(out)
		return "", nil, err
	}
	params := []string{
		fmt.Sprintf("keyint=%d", interval),
		fmt.Sprintf("min-keyint=%d", interval),
		"scenecut=0",
		"repeat-headers=1",
		"intra-refresh=1",
		"b-pyramid=none",
	}
	args := []string{"-y", "-v", "error", "-f", "h264", "-i", path, "-an", "-c:v", "libx264", "-preset", "veryfast"}
	if crf > 0 {
		args = append(args, "-crf", strconv.Itoa(crf))
	}
	args = append(args, "-x264-params", strings.Join(params, ":"), "-f", "h264", out)
	cmd := exec.Command("ffmpeg", args...)
	if b, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(out)
		msg := strings.TrimSpace(string(b))
		if msg == "" {
			msg = err.Error()
		}
		return "", nil, fmt.Errorf("ffmpeg x264 intra-refresh transcode: %s", msg)
	}
	return out, func() { _ = os.Remove(out) }, nil
}

type boundedX264Result struct {
	Path    string
	Cleanup func()
	CRF     int
	PSNR    float64
	OK      bool
	Reason  string
}

func transcodeX264CadenceBounded(path string, interval int, maxBytes int64, minPSNR float64) (boundedX264Result, error) {
	if interval <= 0 {
		return boundedX264Result{Path: path, Cleanup: func() {}, OK: true}, nil
	}
	if maxBytes <= 0 {
		out, cleanup, err := transcodeX264CadenceCRF(path, interval, 0)
		if err != nil {
			return boundedX264Result{}, err
		}
		psnr, err := averagePSNR(path, out)
		if err != nil {
			cleanup()
			return boundedX264Result{}, err
		}
		if minPSNR > 0 && psnr < minPSNR {
			cleanup()
			return boundedX264Result{PSNR: psnr, Reason: "psnr_floor"}, nil
		}
		return boundedX264Result{Path: out, Cleanup: cleanup, PSNR: psnr, OK: true}, nil
	}

	const minCRF = 18
	const maxCRF = 51
	var best boundedX264Result
	for lo, hi := minCRF, maxCRF; lo <= hi; {
		crf := (lo + hi) / 2
		out, cleanup, err := transcodeX264CadenceCRF(path, interval, crf)
		if err != nil {
			if best.Cleanup != nil {
				best.Cleanup()
			}
			return boundedX264Result{}, err
		}
		st, err := os.Stat(out)
		if err != nil {
			cleanup()
			if best.Cleanup != nil {
				best.Cleanup()
			}
			return boundedX264Result{}, err
		}
		if st.Size() <= maxBytes {
			if best.Cleanup != nil {
				best.Cleanup()
			}
			best = boundedX264Result{Path: out, Cleanup: cleanup, CRF: crf, OK: true}
			hi = crf - 1
		} else {
			cleanup()
			lo = crf + 1
		}
	}
	if !best.OK {
		return boundedX264Result{Reason: "byte_cap"}, nil
	}
	psnr, err := averagePSNR(path, best.Path)
	if err != nil {
		best.Cleanup()
		return boundedX264Result{}, err
	}
	best.PSNR = psnr
	if minPSNR > 0 && psnr < minPSNR {
		best.Cleanup()
		return boundedX264Result{PSNR: psnr, Reason: "psnr_floor"}, nil
	}
	return best, nil
}

func averagePSNR(ref, dist string) (float64, error) {
	cmd := exec.Command("ffmpeg", "-v", "info", "-f", "h264", "-i", ref, "-f", "h264", "-i", dist, "-lavfi", "psnr", "-f", "null", "-")
	b, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("ffmpeg psnr: %s", strings.TrimSpace(string(b)))
	}
	out := string(b)
	idx := strings.LastIndex(out, "average:")
	if idx < 0 {
		return 0, fmt.Errorf("ffmpeg psnr: average PSNR not found")
	}
	rest := out[idx+len("average:"):]
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return 0, fmt.Errorf("ffmpeg psnr: malformed average PSNR")
	}
	if strings.EqualFold(fields[0], "inf") {
		return 999, nil
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, fmt.Errorf("ffmpeg psnr average %q: %w", fields[0], err)
	}
	return v, nil
}

// deliveredUnits maps a delivered chunk-seq set to the units fully delivered (all chunks).
func (c *chunked) deliveredUnits(seqs map[uint32]bool) map[uint32]bool {
	out := map[uint32]bool{}
	for id, cs := range c.unitChunks {
		all := true
		for _, s := range cs {
			if !seqs[s] {
				all = false
				break
			}
		}
		out[id] = all
	}
	return out
}

// reassembleDecodable builds the model-filtered elementary stream of DECODABLE units
// (delivered + dependency closure) and returns it plus the displayed-picture count the
// decoder should reproduce. This is a model sanity helper, not the benchmark arbiter.
func (c *chunked) reassembleDecodable(delivered map[uint32]bool) ([]byte, int) {
	dec := shape.Decodable(c.units, delivered)
	var out []byte
	pics := 0
	for _, sh := range c.shaped {
		if !dec[sh.Unit.ID] {
			continue
		}
		if c.format.usesAnnexB() {
			out = append(out, 0, 0, 0, 1)
		}
		out = append(out, sh.Payload...)
		if sh.Unit.Picture {
			pics++
		}
	}
	return out, pics
}

// reassembleDelivered builds the elementary stream from the raw delivered chunk set,
// in source order. An Annex-B unit with any delivered chunk gets one start code; AV1
// low-overhead OBUs retain their own size-delimited framing. Partial units are
// intentionally passed to ffprobe instead of being filtered through Meld's dependency
// oracle.
func (c *chunked) reassembleDelivered(seqs map[uint32]bool) []byte {
	var out []byte
	for _, sh := range c.shaped {
		started := false
		for _, seq := range c.unitChunks[sh.Unit.ID] {
			if !seqs[seq] {
				continue
			}
			if !started && c.format.usesAnnexB() {
				out = append(out, 0, 0, 0, 1)
			}
			started = true
			pkt := c.chunks[seq]
			if len(pkt) > seqHdr {
				out = append(out, pkt[seqHdr:]...)
			}
		}
	}
	return out
}

// ffprobeFrames asks ffprobe how many frames the source codec actually decodes.
func (c *chunked) ffprobeFrames(stream []byte) (int, error) {
	if len(stream) == 0 {
		return 0, nil
	}
	f, err := os.CreateTemp("", "glass-*"+c.format.tempSuffix())
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.Write(stream); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	out, err := exec.Command("ffprobe", "-v", "error", "-f", c.format.ffprobeDemuxer(), "-count_frames", "-select_streams", "v:0",
		"-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", f.Name()).Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return 0, nil // invalid/undecodable stream, not a benchmark infrastructure failure
		}
		return 0, err
	}
	s := strings.TrimSpace(string(out))
	if s == "" || s == "N/A" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// --- impairment relay: forward media follows the selected loss model; delay applies both ways ---

func freeUDP() int {
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return 0
	}
	p := c.LocalAddr().(*net.UDPAddr).Port
	_ = c.Close()
	return p
}

// geBurstPkts, when >= 1, switches dropper from i.i.d. to a Gilbert-Elliott
// 2-state channel whose MARGINAL loss equals the requested loss and whose mean
// bad-run length is geBurstPkts SOURCE-packet time quanta. Channel state advances
// with source time, not emitted transport packet count, so an arm cannot shorten
// a fade by injecting repair or retransmissions. 0 = i.i.d. (default).
var geBurstPkts float64

// meldTgtFail / meldRed override the Meld proactive provisioning (0 / <0 = default),
// for the low-loss latency-floor experiment.
var (
	meldTgtFail       float64
	meldRed           float64 = -1
	meldGenSize       int
	meldNoAuto        bool
	meldNoReorder     bool
	meldNoDecay       bool
	meldReactiveShift bool
)

// meldHeadroom enables the experimental headroom-aware proactive sizing (A/B arm).
var (
	meldHeadroom            bool
	meldCongestionControl   bool
	jitterDur               time.Duration
	relayForwardBytesPerSec int64
)

// deadlineArbiter scores every arm against the same hard per-chunk playout deadline
// (sendTime + budget), instead of "frames eventually delivered" — see docs/bench.md
// "Deadline Semantics" for why ARQ latency-window output otherwise inflates burst cells.
var deadlineArbiter = new(bool)

// Relay counters instrument what the impairment relay actually observes. Forward
// means sender-side UDP traffic toward the receiver-side endpoint; reverse means
// receiver feedback toward the live sender address.
var (
	relayEnq, relaySent                              atomic.Int64
	relayDropped                                     atomic.Int64
	relayEnqBytes, relaySentBytes, relayDroppedBytes atomic.Int64
	relayReverseEnq, relayReverseSent                atomic.Int64
	relayReverseEnqBytes, relayReverseSentBytes      atomic.Int64
)

type relayMetrics struct {
	ForwardEnqueued  int64
	ForwardSent      int64
	ForwardDropped   int64
	ForwardEnqueuedB int64
	ForwardSentB     int64
	ForwardDroppedB  int64
	ReverseEnqueued  int64
	ReverseSent      int64
	ReverseEnqueuedB int64
	ReverseSentB     int64
}

func resetRelayMetrics() {
	relayEnq.Store(0)
	relaySent.Store(0)
	relayDropped.Store(0)
	relayEnqBytes.Store(0)
	relaySentBytes.Store(0)
	relayDroppedBytes.Store(0)
	relayReverseEnq.Store(0)
	relayReverseSent.Store(0)
	relayReverseEnqBytes.Store(0)
	relayReverseSentBytes.Store(0)
}

func snapshotRelayMetrics() relayMetrics {
	return relayMetrics{
		ForwardEnqueued:  relayEnq.Load(),
		ForwardSent:      relaySent.Load(),
		ForwardDropped:   relayDropped.Load(),
		ForwardEnqueuedB: relayEnqBytes.Load(),
		ForwardSentB:     relaySentBytes.Load(),
		ForwardDroppedB:  relayDroppedBytes.Load(),
		ReverseEnqueued:  relayReverseEnq.Load(),
		ReverseSent:      relayReverseSent.Load(),
		ReverseEnqueuedB: relayReverseEnqBytes.Load(),
		ReverseSentB:     relayReverseSentBytes.Load(),
	}
}

var benchProcUserMicros, benchProcSystemMicros, benchProcMaxRSSKB atomic.Int64

type processMetrics struct {
	UserMs   float64
	SystemMs float64
	MaxRSSKB int64
}

func resetBenchProcMetrics() {
	benchProcUserMicros.Store(0)
	benchProcSystemMicros.Store(0)
	benchProcMaxRSSKB.Store(0)
}

func snapshotBenchProcMetrics() processMetrics {
	return processMetrics{
		UserMs:   float64(benchProcUserMicros.Load()) / 1000,
		SystemMs: float64(benchProcSystemMicros.Load()) / 1000,
		MaxRSSKB: benchProcMaxRSSKB.Load(),
	}
}

func recordBenchProcMetrics(ps *os.ProcessState) {
	if ps == nil {
		return
	}
	benchProcUserMicros.Add(ps.UserTime().Microseconds())
	benchProcSystemMicros.Add(ps.SystemTime().Microseconds())
	if ru, ok := ps.SysUsage().(*syscall.Rusage); ok {
		rss := int64(ru.Maxrss)
		if runtime.GOOS == "darwin" {
			rss /= 1024
		}
		for {
			cur := benchProcMaxRSSKB.Load()
			if rss <= cur || benchProcMaxRSSKB.CompareAndSwap(cur, rss) {
				break
			}
		}
	}
}

// sldWindow is the Meld sliding CodingWindow used by runMeld when sliding is on
// (0 = the coder's default). A package global so the sweep/probe can vary it
// without threading it through every runner signature.
var sldWindow int

func dropper(loss float64, seed, paceUs int64) func() bool {
	rng := rand.New(rand.NewSource(seed))
	if geBurstPkts >= 1 && loss > 0 && loss < 1 {
		if paceUs <= 0 {
			paceUs = 1
		}
		return geDropperAt(loss, geBurstPkts, rng,
			time.Duration(paceUs)*time.Microsecond, time.Now)
	}
	return func() bool { return loss > 0 && rng.Float64() < loss }
}

// geDropperAt evolves the Gilbert state on source-time quanta rather than once
// per transport datagram. Extra repair/retransmission packets therefore cannot
// consume a bad run faster and silently turn a fixed network trace into a shorter
// source-time burst. Every packet sent in the same quantum sees the same path
// state, matching a time-correlated first-mile outage.
func geDropperAt(loss, meanBurst float64, rng *rand.Rand, quantum time.Duration, now func() time.Time) func() bool {
	r := 1.0 / meanBurst
	p := r * loss / (1 - loss)
	var start time.Time
	lastTick := int64(-1)
	bad := false
	drop := false
	return func() bool {
		at := now()
		if start.IsZero() {
			start = at
		}
		tick := int64(at.Sub(start) / quantum)
		if tick < lastTick {
			tick = lastTick
		}
		for lastTick < tick {
			drop = bad
			if bad {
				if rng.Float64() < r {
					bad = false
				}
			} else if rng.Float64() < p {
				bad = true
			}
			lastTick++
		}
		return drop
	}
}

// freeEven finds a free even port whose +1 neighbor is also free — RIST main profile uses
// the even/odd pair for RTP media + RTCP (NACK feedback), so both must be relayed.
func freeEven() int {
	for try := 0; try < 200; try++ {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			continue
		}
		port := c.LocalAddr().(*net.UDPAddr).Port
		_ = c.Close()
		if port%2 != 0 {
			continue
		}
		if c2, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port + 1}); e == nil {
			_ = c2.Close()
			return port
		}
	}
	return 0
}

// delayed is one packet held in the relay's delay line until its release time.
type delayed struct {
	at         time.Time
	b          []byte
	to         *net.UDPAddr // reverse path's live client addr; nil ⇒ fixed downstream
	serialized bool         // forward-link capacity has already assigned this packet a finish time
}

func serializedFinish(ready time.Time, size int, bytesPerSec int64, freeAt time.Time) time.Time {
	if bytesPerSec <= 0 || size <= 0 {
		return ready
	}
	start := ready
	if freeAt.After(start) {
		start = freeAt
	}
	d := time.Duration((int64(size)*int64(time.Second) + bytesPerSec - 1) / bytesPerSec)
	return start.Add(d)
}

// delayHeap orders held packets by release time, so injected jitter reorders them WITHOUT one timer
// per packet — a single forwarder drains it, so a big-frame burst cannot cluster N simultaneous timer
// fires (the artifact that dropped packets on the clip's big I-frame even at 0% configured loss).
type delayHeap []delayed

func (h delayHeap) Len() int            { return len(h) }
func (h delayHeap) Less(i, j int) bool  { return h[i].at.Before(h[j].at) }
func (h delayHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x interface{}) { *h = append(*h, x.(delayed)) }
func (h *delayHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// relayOn forwards datagrams between a public port (the client/sender side) and a fixed downstream
// (the receiver), dropping per `drop` and delaying each by owd (+ forward jitter). Generous socket
// buffers absorb bursts the relay has not drained, and a single time-ordered forwarder per direction
// releases packets at their scheduled time (rather than one timer per packet) so the clip's big-frame
// burst cannot cluster simultaneous timer fires — the artifact that dropped packets at 0% loss.
func relayOn(listenPort int, downstream string, drop func() bool, owd time.Duration, jitterSeed int64, trace *seedTrace) (int, func()) {
	var dropPacket func([]byte) bool
	if drop != nil {
		dropPacket = func([]byte) bool { return drop() }
	}
	return relayOnFiltered(listenPort, downstream, dropPacket, owd, jitterSeed, trace)
}

// relayOnFiltered is relayOn with a protocol-aware loss predicate. Delay is
// still applied to every datagram; the predicate lets a benchmark keep session
// admission reliable while impairing media, parity, and retransmissions.
func relayOnFiltered(listenPort int, downstream string, drop func([]byte) bool, owd time.Duration, jitterSeed int64, trace *seedTrace) (int, func()) {
	down, err := net.ResolveUDPAddr("udp", downstream)
	if err != nil {
		return 0, func() {}
	}
	pub, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: listenPort})
	if err != nil {
		return 0, func() {}
	}
	srv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		_ = pub.Close()
		return 0, func() {}
	}
	_ = pub.SetReadBuffer(8 << 20)
	_ = pub.SetWriteBuffer(8 << 20)
	_ = srv.SetReadBuffer(8 << 20)
	_ = srv.SetWriteBuffer(8 << 20)
	var client struct {
		sync.Mutex
		a *net.UDPAddr
	}
	if jitterSeed == 0 {
		jitterSeed = 1
	}
	rng := rand.New(rand.NewSource(jitterSeed))
	forwarder := func(out *net.UDPConn, fixedTo *net.UDPAddr) chan<- delayed {
		in := make(chan delayed, 1<<16)
		go func() {
			h := &delayHeap{}
			var wireFreeAt time.Time
			timer := time.NewTimer(time.Hour)
			if !timer.Stop() {
				<-timer.C
			}
			for {
				var wait <-chan time.Time
				if h.Len() > 0 {
					if d := time.Until((*h)[0].at); d <= 0 {
						p := heap.Pop(h).(delayed)
						if fixedTo != nil && relayForwardBytesPerSec > 0 && !p.serialized {
							wireFreeAt = serializedFinish(p.at, len(p.b), relayForwardBytesPerSec, wireFreeAt)
							p.at, p.serialized = wireFreeAt, true
							heap.Push(h, p)
							continue
						}
						to := fixedTo
						if to == nil {
							to = p.to
						}
						if to != nil {
							if _, err := out.WriteToUDP(p.b, to); err != nil {
								continue
							}
							if fixedTo != nil {
								relaySent.Add(1)
								relaySentBytes.Add(int64(len(p.b)))
							} else {
								relayReverseSent.Add(1)
								relayReverseSentBytes.Add(int64(len(p.b)))
							}
						}
						continue
					} else {
						timer.Reset(d)
						wait = timer.C
					}
				}
				select {
				case p, ok := <-in:
					if !ok {
						return
					}
					heap.Push(h, p)
				case <-wait:
				}
			}
		}()
		return in
	}
	fwdCh := forwarder(srv, down) // forward (media+repair) → fixed downstream receiver
	revCh := forwarder(pub, nil)  // reverse (feedback) → the live client addr stamped per packet
	go func() {
		b := make([]byte, 2048)
		for {
			n, a, e := pub.ReadFromUDP(b)
			if e != nil {
				close(fwdCh)
				return
			}
			client.Lock()
			client.a = a
			client.Unlock()
			dropped := drop != nil && drop(b[:n])
			d := owd
			if jitterDur > 0 {
				d += time.Duration(rng.Int63n(int64(jitterDur)))
			}
			if trace != nil {
				trace.recordRelay(b[:n], dropped, d)
			}
			if dropped {
				relayDropped.Add(1)
				relayDroppedBytes.Add(int64(n))
				continue
			}
			relayEnq.Add(1)
			relayEnqBytes.Add(int64(n))
			fwdCh <- delayed{at: time.Now().Add(d), b: append([]byte(nil), b[:n]...)}
		}
	}()
	go func() {
		b := make([]byte, 2048)
		for {
			n, _, e := srv.ReadFromUDP(b)
			if e != nil {
				close(revCh)
				return
			}
			client.Lock()
			a := client.a
			client.Unlock()
			if a != nil {
				packet := b[:n]
				relayReverseEnq.Add(1)
				relayReverseEnqBytes.Add(int64(len(packet)))
				if trace != nil {
					trace.recordFeedback(packet) // burst autopsy: sender-observed feedback stream
				}
				revCh <- delayed{at: time.Now().Add(owd), b: append([]byte(nil), packet...), to: a}
			}
		}
	}()
	return pub.LocalAddr().(*net.UDPAddr).Port, func() {
		_ = pub.Close()
		_ = srv.Close()
	}
}

func arqLatencyMs(rttMs, mult, floorMs int) int {
	if l := mult * rttMs; l > floorMs {
		return l
	}
	return floorMs
}

// --- Meld arm (public API, real UDP) ---

type meldArmConfig struct {
	name               string
	uep                bool
	frame              bool
	frameAtomic        bool
	sliding            bool
	disableFramePolicy bool
	repairCeiling      bool
	outageOff          bool
}

func meldArm(name string) (meldArmConfig, bool) {
	switch name {
	case "meld", "meld-flat", "meld-flat-unit":
		return meldArmConfig{name: name}, true
	case "meld-auto":
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true}, true
	case "meld-outage-off":
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true, outageOff: true}, true
	case "meld-uep-unit":
		return meldArmConfig{name: name, uep: true}, true
	case "meld-flat-frame":
		return meldArmConfig{name: name, frame: true}, true
	case "meld-uep", "meld-uep-frame":
		return meldArmConfig{name: name, uep: true, frame: true}, true
	case "meld-uep-frame-atomic":
		return meldArmConfig{name: name, uep: true, frame: true, frameAtomic: true}, true
	case "meld-uep-frame-noatomic":
		return meldArmConfig{name: name, uep: true, frame: true, disableFramePolicy: true}, true
	case "meld-sld":
		return meldArmConfig{name: name, sliding: true}, true
	case "meld-sld-uep":
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true}, true
	case "meld-repair-ceiling":
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true, repairCeiling: true}, true
	}
	return meldArmConfig{}, false
}

func isMeldArm(name string) bool {
	_, ok := meldArm(name)
	return ok
}

type meldRunResult struct {
	got       map[uint32]bool
	txStats   meld.SenderStats
	rxStats   meld.ReceiverStats
	relayEnq  int64
	relaySent int64
}

func runMeldNamed(c *chunked, name string, loss float64, rttMs, budgetMs int, paceUs, maxBps, seed int64) meldRunResult {
	return runMeldNamedTrace(c, name, loss, rttMs, budgetMs, paceUs, maxBps, seed, nil)
}

func runMeldNamedTrace(c *chunked, name string, loss float64, rttMs, budgetMs int, paceUs, maxBps, seed int64, trace *seedTrace) meldRunResult {
	arm, ok := meldArm(name)
	if !ok {
		return meldRunResult{}
	}
	return runMeldArm(c, arm, loss, rttMs, budgetMs, paceUs, maxBps, seed, trace)
}

func meldFrameDesc(sh shape.Shaped, chunks int, uep bool) meld.FrameDesc {
	fd := meld.FrameDesc{
		FrameID:         sh.Unit.ID,
		RefFrameIDs:     sh.Unit.RefersTo,
		Chunks:          uint16(chunks),
		TemporalID:      sh.Unit.TemporalID,
		RAP:             sh.Unit.RAP,
		RecoveryRefresh: sh.Unit.RecoveryRefresh,
		Discardable:     sh.Unit.Discardable,
		NonPicture:      !sh.Unit.Picture,
	}
	if uep {
		fd.Priority = sh.Unit.Class.Wire()
	} else {
		fd.Priority = 2 // base tier (media-blind)
	}
	return fd
}

func runMeldArm(c *chunked, arm meldArmConfig, loss float64, rttMs, budgetMs int, paceUs, maxBps, seed int64, trace *seedTrace) meldRunResult {
	owd := time.Duration(rttMs/2) * time.Millisecond
	cfg := meld.DefaultConfig()
	cfg.SymbolSize = seqHdr + c.chunkSize
	cfg.BufferMicros = int64(budgetMs) * 1000
	cfg.Sliding = arm.sliding
	if arm.frameAtomic {
		cfg.FrameAtomic = true
	}
	if arm.disableFramePolicy {
		cfg.FrameAtomic = false
		cfg.EvictBrokenFrames = false
	}
	if meldTgtFail > 0 {
		cfg.TargetFailure = meldTgtFail // tighter ⇒ more proactive (cover the tail, skip the reactive round)
	}
	if meldRed >= 0 {
		cfg.Redundancy = meldRed // proactive floor override
	}
	if meldGenSize > 0 {
		cfg.GenSize = meldGenSize
	}
	if meldNoAuto {
		cfg.AutoGenSize = false // pin the generation width (test the fill-latency lever)
	}
	if meldNoReorder {
		cfg.AutoReorderHoldoff = false // disable the self-tuning reorder window (A/B the default-on)
	}
	if meldNoDecay {
		cfg.ProactiveDecay = false // disable the margin/floor decay (A/B the default-on)
	}
	if meldReactiveShift {
		cfg.SlidingReactiveShift = true // opt-in sliding reactive-offload bundle (A/B arm)
	}
	if meldHeadroom {
		cfg.HeadroomAwareSizing = true // opt-in affordable-rate ceiling on the proactive sizer (A/B arm)
	}
	if meldCongestionControl {
		cfg.CongestionControl = true // A/B the delay/ECN total-rate controller on Meld arms
	}
	if arm.sliding {
		// Band-form sliding coder: repair is fungible across a wide window, so a
		// concentrated burst is covered without a round trip (vs the generation
		// coder, where a burst overwhelms one generation). CodingWindow is the max
		// band width (the burst span it can recover); the sender adapts the effective
		// span down to fit the budget. sldWindow=0 ⇒ the coder's default.
		cfg.Sliding = true
		cfg.CodingWindow = sldWindow
	}
	if maxBps > 0 {
		cfg.MaxBitrate = maxBps // a realistic link cap creates the scarcity UEP allocates within
	}
	if arm.repairCeiling {
		cfg.TargetFailure = 1e-12
		cfg.Redundancy = 1.0
		cfg.RepairWithinBudget = false
	}
	if arm.outageOff {
		cfg.OutageAware = false
	}
	resetRelayMetrics()
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		if os.Getenv("GLASSDBG") != "" {
			fmt.Fprintf(os.Stderr, "meld NewReceiver: %v\n", err)
		}
		return meldRunResult{}
	}
	port, stop := relayOn(0, rx.LocalAddr(), dropper(loss, seed, int64(paceUs)), owd, seed, trace)
	defer stop()
	tx, err := meld.NewSender(fmt.Sprintf("127.0.0.1:%d", port), cfg)
	if err != nil {
		if os.Getenv("GLASSDBG") != "" {
			fmt.Fprintf(os.Stderr, "meld NewSender: %v\n", err)
		}
		_ = rx.Close()
		return meldRunResult{}
	}
	got := map[uint32]bool{}
	var arrivals map[uint32]time.Time
	if *deadlineArbiter || trace != nil {
		// Meld enforces the per-chunk deadline internally; recording arrivals at the
		// same pipeline position as the ARQ sinks makes the arbiter a self-honesty
		// check here rather than a semantics change. A seed trace records them too
		// (the burst autopsy's per-chunk landing times).
		arrivals = map[uint32]time.Time{}
	}
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		buf := make([]byte, cfg.SymbolSize)
		for {
			if err := rx.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
				close(done)
				return
			}
			n, e := rx.Read(buf)
			if e != nil {
				close(done)
				return
			}
			if n >= seqHdr {
				seq := binary.BigEndian.Uint32(buf[:seqHdr])
				mu.Lock()
				got[seq] = true
				if arrivals != nil {
					if _, seen := arrivals[seq]; !seen {
						arrivals[seq] = time.Now()
					}
				}
				mu.Unlock()
			}
		}
	}()
	// chunk seq -> the unit's descriptor, so WriteFrame carries the media metadata.
	descOf := map[uint32]meld.FrameDesc{}
	for _, sh := range c.shaped {
		fd := meldFrameDesc(sh, len(c.unitChunks[sh.Unit.ID]), arm.uep)
		for _, s := range c.unitChunks[sh.Unit.ID] {
			descOf[s] = fd
		}
	}
	start := time.Now()
	var writeWall, maxWriteWall time.Duration
	var slowWrites int
	for i, pkt := range c.chunks {
		seq := binary.BigEndian.Uint32(pkt[:seqHdr])
		writeStart := time.Now()
		var writeErr error
		switch {
		case arm.frame:
			_, writeErr = tx.WriteFrame(pkt, descOf[seq])
		case arm.uep:
			_, writeErr = tx.WriteUnit(pkt, descOf[seq].Priority)
		default:
			_, writeErr = tx.WriteUnit(pkt, 2)
		}
		if writeErr != nil {
			_ = tx.Close()
			_ = rx.Close()
			<-done
			return meldRunResult{}
		}
		writeDur := time.Since(writeStart)
		writeWall += writeDur
		if writeDur > maxWriteWall {
			maxWriteWall = writeDur
		}
		if writeDur > time.Millisecond {
			slowWrites++
		}
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
	feedElapsed := time.Since(start)
	tx.Flush()
	time.Sleep(owd + time.Duration(budgetMs+400)*time.Millisecond)
	txStats, rxStats := tx.Stats(), rx.Stats()
	relayStats := snapshotRelayMetrics()
	_ = tx.Close()
	_ = rx.Close()
	<-done
	mu.Lock()
	if trace != nil {
		trace.recordArrivals(arrivals, start, paceUs, budgetMs)
	}
	if *deadlineArbiter {
		applyDeadlineArbiter(arm.name, got, arrivals, start, paceUs, budgetMs)
	}
	mu.Unlock()
	if os.Getenv("GLASSDBG") != "" {
		var miss [10]int
		total := len(c.chunks)
		for i := 0; i < total; i++ {
			if !got[uint32(i)] {
				miss[i*10/total]++
			}
		}
		fmt.Fprintf(os.Stderr, "[dbg arm=%s frame=%v uep=%v atomic=%v noframepolicy=%v sld=%v] tx src=%d source_wire_mean=%d repair=%d(reactive=%d throttled=%d deadline_skips=%d tightens=%d) | relay enq=%d sent=%d bytes=%d dropped-bytes=%d | rx deliv=%d recov=%d lost=%d evicted=%d | got=%d/%d | miss-by-decile=%v\n",
			arm.name, arm.frame, arm.uep, cfg.FrameAtomic, arm.disableFramePolicy, cfg.Sliding,
			txStats.Source, txStats.SourceWireBytesMean, txStats.Repair, txStats.ReactiveRepair, txStats.Throttled, txStats.DeadlineRepairSkips, txStats.HeadroomTightens,
			relayStats.ForwardEnqueued, relayStats.ForwardSent, relayStats.ForwardSentB, relayStats.ForwardDroppedB,
			rxStats.Delivered, rxStats.Recovered, rxStats.Lost, rxStats.Evicted, len(got), total, miss)
		fmt.Fprintf(os.Stderr, "[attr arm=%s] proactive=%d burstdup=%d outage_diversity=%d block_mds=%d blocks=%d demand_q8=%d correlation_q8=%d memory_q8=%d fixed_mix_q8=%d cold=%d singleton=%d sparse=%d deficit=%d arq=%d compact=%d saved_bytes=%d (repair=%d)\n",
			arm.name, txStats.RepairProactive, txStats.RepairBurstDuplicate,
			txStats.RepairOutageDiversity, txStats.RepairEpoch, txStats.EpochBlocks,
			txStats.EpochDemandQ8, txStats.EpochCorrelationQ8, txStats.EpochMemoryQ8, txStats.EpochShareQ8,
			txStats.RepairProactiveCold,
			txStats.RepairSingleton, txStats.RepairSparse, txStats.RepairDeficit, txStats.RepairExact,
			txStats.RepairCompacted, txStats.RepairBytesSaved, txStats.Repair)
		fmt.Fprintf(os.Stderr, "[source arm=%s] feed_elapsed=%v scheduled=%v slip=%v write_wall=%v max_write=%v slow_writes=%d\n", arm.name, feedElapsed,
			time.Duration(len(c.chunks))*time.Duration(paceUs)*time.Microsecond,
			feedElapsed-time.Duration(len(c.chunks))*time.Duration(paceUs)*time.Microsecond,
			writeWall, maxWriteWall, slowWrites)
	}
	return meldRunResult{got: got, txStats: txStats, rxStats: rxStats, relayEnq: relayStats.ForwardEnqueued, relaySent: relayStats.ForwardSent}
}

// --- C-stack arms: real subprocesses driven through the same relay ---

// udpSink collects delivered chunk seqs; when at is non-nil it also records each
// seq's FIRST arrival instant (the equal-deadline arbiter's receive clock — srt/rist
// deliver in order, so the first datagram at the sink IS that chunk's delivery).
func udpSink(port int, got map[uint32]bool, at map[uint32]time.Time, mu *sync.Mutex) *net.UDPConn {
	conn, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	go func() {
		b := make([]byte, 2048)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(4 * time.Second))
			n, _, e := conn.ReadFromUDP(b)
			if e != nil {
				if ne, ok := e.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
			if n >= seqHdr {
				seq := binary.BigEndian.Uint32(b[:seqHdr])
				mu.Lock()
				got[seq] = true
				if at != nil {
					if _, seen := at[seq]; !seen {
						at[seq] = time.Now()
					}
				}
				mu.Unlock()
			}
		}
	}()
	return conn
}

// feed paces the chunks into the sender tool and returns the schedule anchor:
// chunk seq s left the source at (returned start) + s·paceUs.
func feed(port int, chunks [][]byte, paceUs int64) (time.Time, error) {
	fc, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = fc.Close() }()
	start := time.Now()
	for i, pkt := range chunks {
		if _, err := fc.Write(pkt); err != nil {
			return time.Time{}, err
		}
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
	return start, nil
}

// arbLatency returns the ARQ-arm latency setting for a playout budget. Under the
// equal-deadline arbiter the transport must RELEASE data before the deadline:
// srt-live-transmit's TSBPD (and ristreceiver's recovery buffer) release at
// ingest + latency ON THE RECEIVER'S CLOCK, whose view of ingest trails send time
// by the one-way transit — measured arrivals sit at send + latency + owd + ε, so
// latency == budget − headroom alone scores ~owd late at EVERY budget (the 0%
// C-anchor artifact; min/med lateness ≈ owd + 0-10 ms across buf 150-600). An
// operator deploying against a hard send-time deadline budgets for transit:
// latency = budget − owd − 20 ms output headroom (TSBPD/buffer release jitter
// measured ±8 ms; 10 ms straddled the deadline and dropped a third of libsrt's
// on-time-capable chunks), floored at SRT's conventional 20 ms minimum. This is
// the same physics the Meld arm pays internally (its deadline stamps are
// send-time + budget, covering transit).
func arbLatency(budgetMs, owdMs int) int {
	l := budgetMs - owdMs - 20
	if l < 20 {
		l = 20
	}
	return l
}

// applyDeadlineArbiter drops every delivered seq whose first arrival missed its
// playout deadline — sendStart + seq·paceUs + budget, the SAME hard per-chunk
// deadline the Meld arms enforce internally (docs/bench.md "Deadline Semantics":
// without this, ARQ arms are scored on frames EVENTUALLY delivered, and burst
// cells compare scoring semantics rather than transports). Chunks with no
// recorded arrival (nil map entry) are kept — the arbiter never invents loss.
// Reports how many it dropped.
func applyDeadlineArbiter(arm string, got map[uint32]bool, at map[uint32]time.Time, sendStart time.Time, paceUs int64, budgetMs int) int {
	if at == nil || sendStart.IsZero() {
		return 0
	}
	budget := time.Duration(budgetMs) * time.Millisecond
	late := 0
	for seq := range got {
		arr, ok := at[seq]
		if !ok {
			continue
		}
		deadline := sendStart.Add(time.Duration(seq)*time.Duration(paceUs)*time.Microsecond + budget)
		if arr.After(deadline) {
			delete(got, seq)
			late++
		}
	}
	if late > 0 && os.Getenv("GLASSDBG") != "" {
		lates := make([]time.Duration, 0, len(at))
		for seq, arr := range at {
			deadline := sendStart.Add(time.Duration(seq)*time.Duration(paceUs)*time.Microsecond + budget)
			lates = append(lates, arr.Sub(deadline))
		}
		sort.Slice(lates, func(i, j int) bool { return lates[i] < lates[j] })
		fmt.Fprintf(os.Stderr, "[arbiter arm=%s] dropped %d past-deadline chunks (kept %d); lateness min/med/max = %v / %v / %v\n",
			arm, late, len(got), lates[0], lates[len(lates)/2], lates[len(lates)-1])
	}
	return late
}

type benchProc struct {
	cmd  *exec.Cmd
	done chan struct{}
	mu   sync.Mutex
	err  error
}

func startBenchProc(cmd *exec.Cmd) (*benchProc, error) {
	if cmd.Stderr == nil {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &benchProc{cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		recordBenchProcMetrics(cmd.ProcessState)
		p.mu.Lock()
		p.err = err
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}

func (p *benchProc) waitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *benchProc) exited() (error, bool) {
	select {
	case <-p.done:
		return p.waitErr(), true
	default:
		return nil, false
	}
}

func (p *benchProc) stop() {
	if p == nil {
		return
	}
	if _, ok := p.exited(); ok {
		return
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}

func runLibsrt(c *chunked, loss float64, rttMs, latMs int, paceUs, seed int64, fec string) map[uint32]bool {
	owd := time.Duration(rttMs/2) * time.Millisecond
	rport, sink := freeUDP(), freeUDP()
	got := map[uint32]bool{}
	budgetMs := latMs // the cell budget; the arm's latency setting derives from it below
	var arrivals map[uint32]time.Time
	if *deadlineArbiter {
		arrivals = map[uint32]time.Time{}
		latMs = arbLatency(budgetMs, rttMs/2)
	}
	var mu sync.Mutex
	sc := udpSink(sink, got, arrivals, &mu)
	defer func() { _ = sc.Close() }()
	pf := ""
	if fec != "" {
		pf = "&packetfilter=" + fec
	}
	recv := exec.Command("srt-live-transmit",
		fmt.Sprintf("srt://:%d?mode=listener&latency=%d%s", rport, latMs, pf),
		fmt.Sprintf("udp://127.0.0.1:%d", sink))
	quietBenchProc(recv)
	recvP, err := startBenchProc(recv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "libsrt recv:", err)
		return nil
	}
	defer recvP.stop()
	time.Sleep(1000 * time.Millisecond)
	if err, ok := recvP.exited(); ok {
		fmt.Fprintln(os.Stderr, "libsrt recv exited:", err)
		return nil
	}
	drop := dropper(loss, seed, paceUs)
	var impair atomic.Bool
	feedp, stop := relayOn(0, fmt.Sprintf("127.0.0.1:%d", rport), func() bool {
		return impair.Load() && drop()
	}, owd, seed, nil)
	defer stop()
	feedInPort := freeUDP()
	send := exec.Command("srt-live-transmit",
		fmt.Sprintf("udp://:%d", feedInPort),
		fmt.Sprintf("srt://127.0.0.1:%d?mode=caller&latency=%d%s", feedp, latMs, pf))
	quietBenchProc(send)
	sendP, err := startBenchProc(send)
	if err != nil {
		fmt.Fprintln(os.Stderr, "libsrt send:", err)
		return nil
	}
	defer sendP.stop()
	time.Sleep(1500*time.Millisecond + owd)
	if err, ok := sendP.exited(); ok {
		fmt.Fprintln(os.Stderr, "libsrt send exited:", err)
		return nil
	}
	impair.Store(true)
	feedStart, err := feed(feedInPort, c.chunks, paceUs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "libsrt feed:", err)
		return nil
	}
	time.Sleep(owd + time.Duration(latMs+800)*time.Millisecond)
	if err, ok := sendP.exited(); ok {
		fmt.Fprintln(os.Stderr, "libsrt send exited after feed:", err)
		return nil
	}
	if err, ok := recvP.exited(); ok {
		fmt.Fprintln(os.Stderr, "libsrt recv exited after feed:", err)
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	applyDeadlineArbiter("libsrt", got, arrivals, feedStart, paceUs, budgetMs)
	return got
}

func runLibrist(c *chunked, loss float64, rttMs, latMs int, paceUs, seed int64) map[uint32]bool {
	owd := time.Duration(rttMs/2) * time.Millisecond
	home, _ := os.UserHomeDir()
	tools := home + "/dev/librist/build/tools"
	env := append(os.Environ(), "DYLD_LIBRARY_PATH="+home+"/dev/librist/build")
	rport, feedIn, sink := freeEven(), freeUDP(), freeUDP()
	got := map[uint32]bool{}
	budgetMs := latMs // the cell budget; the arm's latency setting derives from it below
	var arrivals map[uint32]time.Time
	if *deadlineArbiter {
		arrivals = map[uint32]time.Time{}
		latMs = arbLatency(budgetMs, rttMs/2)
	}
	var mu sync.Mutex
	sc := udpSink(sink, got, arrivals, &mu)
	defer func() { _ = sc.Close() }()
	recv := exec.Command(tools+"/ristreceiver", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("rist://@127.0.0.1:%d", rport), "-o", fmt.Sprintf("udp://127.0.0.1:%d", sink))
	recv.Env = env
	quietBenchProc(recv)
	recvP, err := startBenchProc(recv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "librist recv:", err)
		return nil
	}
	defer recvP.stop()
	time.Sleep(1200 * time.Millisecond)
	if err, ok := recvP.exited(); ok {
		fmt.Fprintln(os.Stderr, "librist recv exited:", err)
		return nil
	}
	relayE := freeEven()
	drop := dropper(loss, seed, paceUs)
	var impair atomic.Bool
	_, sA := relayOn(relayE, fmt.Sprintf("127.0.0.1:%d", rport), func() bool {
		return impair.Load() && drop()
	}, owd, seed, nil) // RTP media (dropped after admission)
	_, sB := relayOn(relayE+1, fmt.Sprintf("127.0.0.1:%d", rport+1), nil, owd, seed+1, nil) // RTCP/NACK (clean)
	defer sA()
	defer sB()
	send := exec.Command(tools+"/ristsender", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("udp://@127.0.0.1:%d", feedIn), "-o", fmt.Sprintf("rist://127.0.0.1:%d", relayE))
	send.Env = env
	quietBenchProc(send)
	sendP, err := startBenchProc(send)
	if err != nil {
		fmt.Fprintln(os.Stderr, "librist send:", err)
		return nil
	}
	defer sendP.stop()
	time.Sleep(1500*time.Millisecond + owd)
	if err, ok := sendP.exited(); ok {
		fmt.Fprintln(os.Stderr, "librist send exited:", err)
		return nil
	}
	impair.Store(true)
	feedStart, err := feed(feedIn, c.chunks, paceUs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "librist feed:", err)
		return nil
	}
	time.Sleep(owd + time.Duration(latMs+800)*time.Millisecond)
	if err, ok := sendP.exited(); ok {
		fmt.Fprintln(os.Stderr, "librist send exited after feed:", err)
		return nil
	}
	if err, ok := recvP.exited(); ok {
		fmt.Fprintln(os.Stderr, "librist recv exited after feed:", err)
		return nil
	}
	mu.Lock()
	defer mu.Unlock()
	applyDeadlineArbiter("librist", got, arrivals, feedStart, paceUs, budgetMs)
	return got
}

func quietBenchProc(cmd *exec.Cmd) {
	if os.Getenv("GLASSDBG") == "" {
		cmd.Stderr = io.Discard
	}
}

type score struct {
	frameRate, keyRate float64
}

type benchRun struct {
	seqs    map[uint32]bool
	meld    *meldRunResult
	trace   *seedTrace
	metrics armRunMetrics
}

func grade(c *chunked, seqs map[uint32]bool) (score, []byte, int) {
	du := c.deliveredUnits(seqs)
	_, pics := c.reassembleDecodable(du)
	return score{
		frameRate: shape.DecodableFrameRate(c.units, du),
		keyRate:   shape.DecodableKeyframeRate(c.units, du),
	}, c.reassembleDelivered(seqs), pics
}

type missingSummary struct {
	firstMissingUnit    int64
	firstMissingPicture int64
	firstMissingKey     int64
	firstBrokenUnit     int64
	firstBrokenRef      int64
}

func missingSummaryFor(c *chunked, seqs map[uint32]bool) missingSummary {
	out := missingSummary{-1, -1, -1, -1, -1}
	delivered := c.deliveredUnits(seqs)
	dec := shape.Decodable(c.units, delivered)
	for _, sh := range c.shaped {
		id := sh.Unit.ID
		if !delivered[id] {
			if out.firstMissingUnit < 0 {
				out.firstMissingUnit = int64(id)
			}
			if sh.Unit.Picture && out.firstMissingPicture < 0 {
				out.firstMissingPicture = int64(id)
			}
			if sh.Unit.RAP && out.firstMissingKey < 0 {
				out.firstMissingKey = int64(id)
			}
			continue
		}
		if !dec[id] && out.firstBrokenUnit < 0 {
			out.firstBrokenUnit = int64(id)
			for _, ref := range sh.Unit.RefersTo {
				if !dec[ref] {
					out.firstBrokenRef = int64(ref)
					break
				}
			}
		}
	}
	return out
}

func printSeedDiag(c *chunked, arm string, rep int, seed int64, ff int, sc score, seqs map[uint32]bool, m *meldRunResult) {
	if os.Getenv("GLASSDBG") == "" {
		return
	}
	ms := missingSummaryFor(c, seqs)
	chunks := len(seqs)
	txSrc, txRepair, txReactive, txThrottled := uint64(0), uint64(0), uint64(0), uint64(0)
	repairExact, repairBurstDuplicate, repairOutageDiversity, repairEpoch := uint64(0), uint64(0), uint64(0), uint64(0)
	epochBlocks := uint64(0)
	epochDemandQ8, epochCorrelationQ8, epochMemoryQ8, epochFixedMixQ8 := uint16(0), uint16(0), uint16(0), uint16(0)
	repairCompacted, repairBytesSaved := uint64(0), uint64(0)
	rxDelivered, rxRecovered, rxLost, rxEvicted := uint64(0), uint64(0), uint64(0), uint64(0)
	relayEnq, relaySent := int64(-1), int64(-1)
	if m != nil {
		txSrc, txRepair, txReactive, txThrottled = m.txStats.Source, m.txStats.Repair, m.txStats.ReactiveRepair, m.txStats.Throttled
		repairExact, repairBurstDuplicate = m.txStats.RepairExact, m.txStats.RepairBurstDuplicate
		repairOutageDiversity = m.txStats.RepairOutageDiversity
		repairEpoch = m.txStats.RepairEpoch
		epochBlocks = m.txStats.EpochBlocks
		epochDemandQ8 = m.txStats.EpochDemandQ8
		epochCorrelationQ8 = m.txStats.EpochCorrelationQ8
		epochMemoryQ8 = m.txStats.EpochMemoryQ8
		epochFixedMixQ8 = m.txStats.EpochShareQ8
		repairCompacted, repairBytesSaved = m.txStats.RepairCompacted, m.txStats.RepairBytesSaved
		rxDelivered, rxRecovered, rxLost, rxEvicted = m.rxStats.Delivered, m.rxStats.Recovered, m.rxStats.Lost, m.rxStats.Evicted
		relayEnq, relaySent = m.relayEnq, m.relaySent
	}
	fmt.Fprintf(os.Stderr,
		"glassseed arm=%s rep=%d seed=%d ff=%d frame_pct=%.3f key_pct=%.3f chunks=%d/%d tx_src=%d tx_repair=%d tx_reactive=%d tx_throttled=%d repair_exact=%d repair_burst_duplicate=%d repair_outage_diversity=%d repair_epoch=%d epoch_blocks=%d epoch_demand_q8=%d epoch_correlation_q8=%d epoch_memory_q8=%d epoch_share_q8=%d repair_compacted=%d repair_bytes_saved=%d relay_enq=%d relay_sent=%d rx_delivered=%d rx_recovered=%d rx_lost=%d rx_evicted=%d first_missing_unit=%d first_missing_picture=%d first_missing_key=%d first_broken_unit=%d first_broken_ref=%d\n",
		arm, rep, seed, ff, sc.frameRate, sc.keyRate, chunks, len(c.chunks),
		txSrc, txRepair, txReactive, txThrottled, repairExact, repairBurstDuplicate, repairOutageDiversity, repairEpoch,
		epochBlocks, epochDemandQ8, epochCorrelationQ8, epochMemoryQ8, epochFixedMixQ8,
		repairCompacted, repairBytesSaved, relayEnq, relaySent,
		rxDelivered, rxRecovered, rxLost, rxEvicted,
		ms.firstMissingUnit, ms.firstMissingPicture, ms.firstMissingKey, ms.firstBrokenUnit, ms.firstBrokenRef)
}

type clipSummary struct {
	TotalPics  int
	TotalKey   int
	Disposable int
	FFFrames   int
}

func summarizeClip(c *chunked) clipSummary {
	var s clipSummary
	for _, u := range c.units {
		if u.Picture {
			s.TotalPics++
		}
		if u.RAP {
			s.TotalKey++
		}
		if u.Discardable {
			s.Disposable++
		}
	}
	fullFF, err := c.ffprobeFrames(c.reassembleDelivered(allChunkSeqs(c)))
	if err == nil {
		s.FFFrames = fullFF
	}
	return s
}

func parseClipList(list, fallback string) []string {
	list = strings.TrimSpace(list)
	if list == "" {
		return []string{fallback}
	}
	out := make([]string, 0)
	seen := map[string]bool{}
	for _, part := range strings.Split(list, ",") {
		path := strings.TrimSpace(part)
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		out = append(out, path)
	}
	if len(out) == 0 {
		return []string{fallback}
	}
	return out
}

func sourceIDForClip(path string) string {
	base := filepath.Base(path)
	base = strings.ToLower(base)
	var b strings.Builder
	lastDash := false
	for _, r := range base {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	id := strings.Trim(b.String(), "-")
	if id == "" {
		return "source"
	}
	return id
}

func writePublishSourcesIndex(outDir string, suite publishSuite, clips []string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Publish Source Matrix\n\n")
	fmt.Fprintf(&b, "Suite: `%s`\n\n", suite.Name)
	fmt.Fprintf(&b, "Each source runs in its own subdirectory so source-specific raw artifacts remain independent.\n\n")
	fmt.Fprintf(&b, "| source | codec | clip | artifacts |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- |\n")
	for _, clip := range clips {
		id := sourceIDForClip(clip)
		format, _ := formatForClip(clip)
		fmt.Fprintf(&b, "| `%s` | `%s` | `%s` | [`%s/FRONTIER.md`](%s/FRONTIER.md), [`%s/FAIRNESS.md`](%s/FAIRNESS.md) |\n",
			id, format.name(), clip, id, id, id, id)
	}
	return os.WriteFile(filepath.Join(outDir, "SOURCES.md"), []byte(b.String()), 0o644)
}

func main() {
	clip := flag.String("clip", "internal/shape/testdata/bbb_bframes.h264", "AVC .h264, HEVC .h265, or AV1 low-overhead .obu elementary stream")
	publishClips := flag.String("publishclips", "", "comma-separated AVC, HEVC, or AV1 elementary streams for -publishsuite; each source gets its own report subdirectory")
	loss := flag.Float64("loss", 0.2, "forward loss fraction")
	rtt := flag.Int("rtt", 100, "RTT ms")
	mult := flag.Int("rttmult", 1, "budget = max(floor, mult*rtt)")
	floorMs := flag.Int("buf", 100, "budget floor ms")
	mbps := flag.Float64("mbps", 8, "stream bitrate Mbps (paces chunks)")
	maxMbps := flag.Float64("maxmbps", 0, "meld MaxBitrate cap Mbps (0=default ~100); set near source rate to create UEP scarcity")
	wireMbps := flag.Float64("wirembps", 0, "shared forward-link capacity Mbps for every arm (0=unbounded)")
	reps := flag.Int("reps", 4, "seeds per arm")
	chunkSize := flag.Int("chunk", 1316, "video bytes per chunk")
	arms := flag.String("arms", "meld-auto,libsrt,librist", "comma list")
	sweep := flag.Bool("sweep", false, "iso-quality min-latency mode: find each transport's B_min vs RTT for quality bar -q")
	q := flag.Float64("q", 0.999, "quality bar: minimum delivery fraction (sweep mode)")
	rtts := flag.String("rtts", "20,50,100,200,400", "RTTs (ms) to sweep (sweep mode)")
	streamK := flag.Int("streamk", 4, "source repetition multiplier for sweep and ad hoc macro modes; named publish suites choose an RTT-normalized horizon")
	macroFrontier := flag.Bool("macrofrontier", false, "macro frontier mode: sweep ffprobe-decoded frames across loss/burst/RTT/latency")
	publishSuiteFlag := flag.String("publishsuite", "", "named publication benchmark suite: "+strings.Join(publishSuiteNames(), ", "))
	frontierLosses := flag.String("frontierlosses", "0.05,0.10", "macro frontier forward loss fractions")
	frontierBursts := flag.String("frontierbursts", "0,24", "macro frontier GE mean burst lengths in source-packet time quanta (0=i.i.d.)")
	frontierMults := flag.String("frontiermults", "1,1.5,2,3", "macro frontier latency budgets as RTT multipliers")
	frontierTop := flag.Int("frontiertop", 8, "macro frontier rows per summary section")
	frontierShards := flag.Int("frontiershards", 1, "deterministically partition macro/publish cells into this many independently mergeable shards")
	frontierShard := flag.Int("frontiershard", 0, "zero-based macro/publish shard to execute")
	mergeFrontier := flag.String("mergefrontier", "", "merge and strictly audit completed shard directories into -reportdir")
	geburst := flag.Float64("geburst", 0, "GE mean burst length in source-packet time quanta (0=i.i.d.); marginal loss stays -loss")
	atxrtt := flag.Float64("atxrtt", 0, "probe mode: measure delivery at this fixed ×RTT budget (no bisection), printing per-seed spread")
	sldwin := flag.Int("sldwin", 0, "Meld sliding CodingWindow override (max band width); 0 = automatic protocol default")
	tgtfail := flag.Float64("tgtfail", 0, "Meld TargetFailure override (0 = default 1e-3); tighter = more proactive")
	red := flag.Float64("red", -1, "Meld Redundancy floor override (<0 = default)")
	gensize := flag.Int("gensize", 0, "Meld GenSize override (0 = default)")
	noauto := flag.Bool("noauto", false, "disable Meld AutoGenSize (pin GenSize)")
	noreorder := flag.Bool("noreorder", false, "disable Meld AutoReorderHoldoff (on by default)")
	nodecay := flag.Bool("nodecay", false, "disable Meld ProactiveDecay (on by default; A/B the margin/floor decay)")
	reactiveshift := flag.Bool("reactiveshift", false, "enable Meld SlidingReactiveShift (experimental sliding reactive-offload bundle)")
	headroom := flag.Bool("headroom", false, "enable Meld HeadroomAwareSizing (experimental affordable-rate ceiling on proactive repair)")
	cc := flag.Bool("cc", false, "enable Meld delay/ECN congestion control (A/B all Meld arms)")
	sourceConstrained := flag.Bool("sourceconstrained", false, "model a constrained encoder/source: drop AVC SEI positively identified as non-recovery; default preserves SEI")
	sourceDropDisposable := flag.Bool("sourcedropdisposable", false, "constrained AVC source model: also drop non-reference disposable pictures")
	autoEncoderCadence := flag.Bool("autoencoder", false, "macro frontier: model Meld encoder recovery-cadence actuator; meld-auto may use bounded x264 source variants")
	autoEncoderInterval := flag.Int("autoencoderinterval", 0, "macro frontier autoencoder: recovery interval in frames (0=mode default)")
	autoEncoderByteCap := flag.Float64("autoencoderbytecap", 0, "macro frontier autoencoder: max Meld source bytes as a multiple of baseline source bytes (0=unbounded)")
	autoEncoderPSNRMin := flag.Float64("autoencoderpsnrmin", 0, "macro frontier autoencoder: minimum average PSNR dB vs baseline source (0=disabled)")
	jitterMs := flag.Int("jitter", 0, "max per-datagram forward jitter ms (injects reorder)")
	deadlineArbiter = flag.Bool("deadlinearbiter", false, "equal-deadline scoring: drop chunks arriving past sendTime+budget for ALL arms (docs/bench.md \"Deadline Semantics\")")
	reportDir := flag.String("reportdir", "", "write ladder report artifacts to this directory")
	reportCaseName := flag.String("reportcase", "", "case name for report artifacts (default derived from loss/burst/RTT)")
	flag.Parse()
	var selectedPublishSuite publishSuite
	if *publishSuiteFlag != "" {
		var ok bool
		selectedPublishSuite, ok = publishSuiteByName(*publishSuiteFlag)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown publish suite %q (known: %s)\n", *publishSuiteFlag, strings.Join(publishSuiteNames(), ", "))
			os.Exit(1)
		}
		if normalized := publicationChunkSize(*chunkSize); normalized != *chunkSize {
			fmt.Fprintf(os.Stderr, "publish suite: common payload ceiling selects -chunk %d (requested %d)\n", normalized, *chunkSize)
			*chunkSize = normalized
		}
	}
	if *mergeFrontier != "" {
		if selectedPublishSuite.Name == "" {
			fmt.Fprintln(os.Stderr, "merge frontier: -mergefrontier requires -publishsuite")
			os.Exit(1)
		}
		if *reportDir == "" {
			fmt.Fprintln(os.Stderr, "merge frontier: -mergefrontier requires -reportdir")
			os.Exit(1)
		}
		meldMax := int64(*maxMbps * 1e6)
		mergeOpts := macroFrontierOptions{
			SuiteName:        selectedPublishSuite.Name,
			SuiteDescription: selectedPublishSuite.Description,
			Losses:           append([]float64(nil), selectedPublishSuite.Losses...),
			Bursts:           append([]float64(nil), selectedPublishSuite.Bursts...),
			RTTs:             append([]int(nil), selectedPublishSuite.RTTs...),
			Mults:            append([]float64(nil), selectedPublishSuite.Mults...),
			JitterPlanes:     append([]int(nil), selectedPublishSuite.Jitters...),
			Arms:             append([]string(nil), selectedPublishSuite.Arms...),
			Reps:             *reps,
			FloorMs:          *floorMs,
			MeldMax:          meldMax,
			Mbps:             *mbps,
			WireMbps:         *wireMbps,
			ChunkSize:        *chunkSize,
			OutDir:           *reportDir,
			TopN:             *frontierTop,
			JitterMs:         *jitterMs,
			ShardCount:       *frontierShards,
		}
		clips := parseClipList(*publishClips, *clip)
		if err := mergeMacroFrontierShards(*mergeFrontier, *reportDir, selectedPublishSuite, mergeOpts, clips); err != nil {
			fmt.Fprintln(os.Stderr, "merge frontier:", err)
			os.Exit(1)
		}
		return
	}
	if *wireMbps > 0 {
		relayForwardBytesPerSec = int64(*wireMbps * 1e6 / 8)
	}
	jitterDur = time.Duration(*jitterMs) * time.Millisecond
	meldNoReorder = *noreorder
	meldNoDecay = *nodecay
	meldReactiveShift = *reactiveshift
	meldHeadroom = *headroom
	meldCongestionControl = *cc
	geBurstPkts = *geburst
	sldWindow = *sldwin
	meldTgtFail = *tgtfail
	meldRed = *red
	meldGenSize = *gensize
	meldNoAuto = *noauto

	clipPath := *clip
	avcOpts := shape.AVCOptions{
		SourceConstrained:      *sourceConstrained || *sourceDropDisposable,
		DropDisposablePictures: *sourceDropDisposable,
	}
	c, err := chunkClip(clipPath, *chunkSize, avcOpts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clip:", err)
		os.Exit(1)
	}
	clipStats := summarizeClip(c)
	budgetMs := arqLatencyMs(*rtt, *mult, *floorMs)
	paceUs := int64(float64((*chunkSize+seqHdr)*8) / (*mbps * 1e6) * 1e6)
	if paceUs < 1 {
		paceUs = 1
	}
	meldMax := int64(*maxMbps * 1e6)
	macroOptsFor := func(path string, stats clipSummary, outDir string) macroFrontierOptions {
		format, _ := formatForClip(path)
		return macroFrontierOptions{
			Losses:              parseFloatList(*frontierLosses),
			Bursts:              parseFloatList(*frontierBursts),
			RTTs:                parseRTTs(*rtts),
			Mults:               parseFloatList(*frontierMults),
			Arms:                strings.Split(*arms, ","),
			Reps:                *reps,
			FloorMs:             *floorMs,
			PaceUs:              paceUs,
			MeldMax:             meldMax,
			Mbps:                *mbps,
			WireMbps:            *wireMbps,
			ChunkSize:           *chunkSize,
			TotalPics:           stats.TotalPics,
			OutDir:              outDir,
			TopN:                *frontierTop,
			JitterMs:            *jitterMs,
			ShardCount:          *frontierShards,
			ShardIndex:          *frontierShard,
			SourceID:            sourceIDForClip(path),
			SourceClip:          path,
			SourceCodec:         format.name(),
			SourceRepeats:       *streamK,
			SourceFFFrames:      stats.FFFrames,
			AVCOpts:             avcOpts,
			AutoEncoderCadence:  *autoEncoderCadence,
			AutoEncoderInterval: *autoEncoderInterval,
			AutoEncoderByteCap:  *autoEncoderByteCap,
			AutoEncoderPSNRMin:  *autoEncoderPSNRMin,
		}
	}
	if *publishSuiteFlag != "" {
		suite := selectedPublishSuite
		clips := parseClipList(*publishClips, clipPath)
		if len(clips) > 1 || clips[0] != clipPath {
			if *reportDir == "" {
				fmt.Fprintln(os.Stderr, "publish suite: -publishclips requires -reportdir")
				os.Exit(1)
			}
			if err := os.MkdirAll(*reportDir, 0o755); err != nil {
				fmt.Fprintln(os.Stderr, "publish suite:", err)
				os.Exit(1)
			}
			for _, path := range clips {
				pc, err := chunkClip(path, *chunkSize, avcOpts)
				if err != nil {
					fmt.Fprintf(os.Stderr, "clip %s: %v\n", path, err)
					os.Exit(1)
				}
				outDir := filepath.Join(*reportDir, sourceIDForClip(path))
				if err := runPublishSuite(pc, suite, macroOptsFor(path, summarizeClip(pc), outDir)); err != nil {
					fmt.Fprintf(os.Stderr, "publish suite %s: %v\n", path, err)
					os.Exit(1)
				}
			}
			if err := writePublishSourcesIndex(*reportDir, suite, clips); err != nil {
				fmt.Fprintln(os.Stderr, "publish suite:", err)
				os.Exit(1)
			}
			return
		}
		if err := runPublishSuite(c, suite, macroOptsFor(clipPath, clipStats, *reportDir)); err != nil {
			fmt.Fprintln(os.Stderr, "publish suite:", err)
			os.Exit(1)
		}
		return
	}
	if *macroFrontier {
		if err := runMacroFrontier(c, macroOptsFor(clipPath, clipStats, *reportDir)); err != nil {
			fmt.Fprintln(os.Stderr, "macro frontier:", err)
			os.Exit(1)
		}
		return
	}
	if *sweep {
		if *atxrtt > 0 {
			runProbe(c, *loss, paceUs, meldMax, *mbps, parseRTTs(*rtts), *reps, *atxrtt, *streamK, strings.Split(*arms, ","))
		} else {
			runSweep(c, *loss, paceUs, meldMax, *mbps, parseRTTs(*rtts), *reps, *q, *streamK, strings.Split(*arms, ","))
		}
		return
	}
	var report *benchReport
	if *reportDir != "" {
		name := strings.TrimSpace(*reportCaseName)
		if name == "" {
			name = makeCaseName(*loss, *geburst, *rtt, *mult, *jitterMs)
		}
		var err error
		report, err = newBenchReport(*reportDir, reportCase{
			Name:                name,
			Clip:                *clip,
			Loss:                *loss,
			GEBurst:             *geburst,
			GEBurstClock:        "source_time",
			RTTMs:               *rtt,
			RTTMult:             *mult,
			BufferMs:            *floorMs,
			BudgetMs:            budgetMs,
			BitrateMbps:         *mbps,
			MaxMbps:             *maxMbps,
			WireMbps:            *wireMbps,
			ChunkSize:           *chunkSize,
			Reps:                *reps,
			Arms:                *arms,
			SldWindow:           *sldwin,
			AutoEncoderCadence:  *autoEncoderCadence,
			AutoEncoderInterval: *autoEncoderInterval,
			AutoEncoderByteCap:  *autoEncoderByteCap,
			AutoEncoderPSNRMin:  *autoEncoderPSNRMin,
			JitterMs:            *jitterMs,
			SourceConstrained:   avcOpts.SourceConstrained,
			DropDisposable:      avcOpts.DropDisposablePictures,
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "report:", err)
			os.Exit(1)
		}
	}
	// no-loss sanity: ffprobe must decode all pictures from the full clip
	fullSeqs := map[uint32]bool{}
	for i := range c.chunks {
		fullSeqs[uint32(i)] = true
	}
	fullH := c.reassembleDelivered(fullSeqs)
	_, fullPics := c.reassembleDecodable(c.deliveredUnits(fullSeqs))
	fullFF, err := c.ffprobeFrames(fullH)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ffprobe full clip:", err)
		os.Exit(1)
	}
	if fullFF != fullPics {
		fmt.Fprintf(os.Stderr, "ffprobe full clip decoded %d frames, model predicts %d\n", fullFF, fullPics)
		os.Exit(1)
	}
	fmt.Printf("# glassbench: %s — %d units, %d pictures, %d keyframes, %d DISPOSABLE units, %d chunks (%dB)\n",
		*clip, len(c.units), clipStats.TotalPics, clipStats.TotalKey, clipStats.Disposable, len(c.chunks), *chunkSize)
	fmt.Printf("# ffprobe(full clip) = %d frames (decodable-set model predicts %d)\n", fullFF, fullPics)
	fmt.Printf("# loss %.0f%%, RTT %dms, budget %dms (%dxRTT), %.0f Mbps, %d seeds; arbiter=ffprobe\n",
		*loss*100, *rtt, budgetMs, *mult, *mbps, *reps)
	if avcOpts.SourceConstrained {
		fmt.Printf("# source constrained: AVC non-recovery SEI shed before transport\n")
	}
	if avcOpts.DropDisposablePictures {
		fmt.Printf("# source constrained: AVC non-reference pictures shed before transport\n")
	}
	fmt.Printf("# metric: ffprobe-decoded frames (mean), and the model's decodable frame%% / keyframe%%\n\n")
	fmt.Printf("%-24s  ff-frames  frame%%  keyframe%%\n", "arm")

	run := func(name string, fn func(seed int64, trace *seedTrace) benchRun) {
		var ffSum int
		var frSum, kfSum float64
		var repairSum, reactiveSum uint64
		ok := 0
		failed := 0
		for s := 1; s <= *reps; s++ {
			seed := benchmarkSeed(s)
			var trace *seedTrace
			if report != nil {
				trace = report.newTrace(name, s, seed)
			}
			res := fn(seed, trace)
			seqs := res.seqs
			if seqs == nil {
				failed++
				continue
			}
			sc, stream, _ := grade(c, seqs)
			ff, err := c.ffprobeFrames(stream)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s seed %d ffprobe: %v\n", name, s, err)
				failed++
				continue
			}
			printSeedDiag(c, name, s, seed, ff, sc, seqs, res.meld)
			if res.meld != nil {
				repairSum += res.meld.txStats.Repair
				reactiveSum += res.meld.txStats.ReactiveRepair
			}
			if report != nil {
				if err := report.addSeed(c, name, s, seed, ff, sc, seqs, res.meld, res.trace); err != nil {
					fmt.Fprintln(os.Stderr, "report seed:", err)
					failed++
					continue
				}
			}
			ffSum += ff
			frSum += sc.frameRate
			kfSum += sc.keyRate
			ok++
		}
		addReportResult := func() {
			if report == nil {
				return
			}
			row := reportResult{Case: report.cas.Name, Arm: name, Failed: failed, Seeds: ok}
			if ok > 0 {
				row.FFMean = float64(ffSum) / float64(ok)
				row.FramePctMean = frSum / float64(ok)
				row.KeyPctMean = kfSum / float64(ok)
				row.RepairMean = float64(repairSum) / float64(ok)
				row.ReactiveMean = float64(reactiveSum) / float64(ok)
			}
			report.addResult(row)
		}
		if failed > 0 || ok == 0 {
			addReportResult()
			fmt.Printf("%-24s  FAILED (%d/%d seeds failed)\n", name, failed, *reps)
			return
		}
		addReportResult()
		fmt.Printf("%-24s  %7.1f   %5.1f%%   %6.1f%%\n",
			name, float64(ffSum)/float64(ok), 100*frSum/float64(ok), 100*kfSum/float64(ok))
	}

	want := map[string]bool{}
	for _, a := range strings.Split(*arms, ",") {
		want[strings.TrimSpace(a)] = true
	}
	order := []string{
		"meld", "meld-auto", "meld-outage-off", "meld-flat", "meld-flat-unit",
		"meld-uep-unit", "meld-flat-frame", "meld-uep", "meld-uep-frame", "meld-uep-frame-atomic", "meld-uep-frame-noatomic",
		"meld-sld", "meld-sld-uep", "meld-repair-ceiling",
		"oracle-source", "oracle-ideal",
		"libsrt", "libsrt-fec", "librist",
	}
	sort.SliceStable(order, func(i, j int) bool { return false })
	for _, a := range order {
		if !want[a] {
			continue
		}
		if isMeldArm(a) {
			run(a, func(seed int64, trace *seedTrace) benchRun {
				res := runMeldNamedTrace(c, a, *loss, *rtt, budgetMs, paceUs, meldMax, seed, trace)
				return benchRun{seqs: res.got, meld: &res, trace: trace}
			})
			continue
		}
		switch a {
		case "oracle-source":
			run("oracle-source", func(seed int64, trace *seedTrace) benchRun {
				return benchRun{seqs: allChunkSeqs(c), trace: trace}
			})
		case "oracle-ideal":
			run("oracle-ideal", func(seed int64, trace *seedTrace) benchRun {
				return benchRun{seqs: idealDeadlineSeqs(c, *rtt, budgetMs), trace: trace}
			})
		case "libsrt":
			run("libsrt", func(seed int64, trace *seedTrace) benchRun {
				return benchRun{seqs: runLibsrt(c, *loss, *rtt, budgetMs, paceUs, seed, ""), trace: trace}
			})
		case "libsrt-fec":
			run("libsrt-fec", func(seed int64, trace *seedTrace) benchRun {
				return benchRun{seqs: runLibsrt(c, *loss, *rtt, budgetMs, paceUs, seed, "fec,cols:10,rows:5,arq:onreq"), trace: trace}
			})
		case "librist":
			run("librist", func(seed int64, trace *seedTrace) benchRun {
				return benchRun{seqs: runLibrist(c, *loss, *rtt, budgetMs, paceUs, seed), trace: trace}
			})
		}
	}
	if report != nil {
		if err := report.write(); err != nil {
			fmt.Fprintln(os.Stderr, "report:", err)
			os.Exit(1)
		}
	}
}
