// Command glassbench is a glass-to-glass media-decodability comparison with ffprobe as
// an external decoder check. It streams the same real H.264
// clip through four transports over one shared impairment relay (loss + one-way delay,
// matched latency budget) — Meld with media-aware unequal protection (WriteFrame/UEP),
// Meld media-blind (Write/flat), real libSRT, and real libRIST — then reassembles each
// receiver's DELIVERED set into an Annex-B stream and asks ffprobe how many frames
// actually decode. The metric is picture-level QoE (decodable frames / keyframes), not
// byte delivery: the point is that a lost keyframe poisons a whole GOP, so WHICH packets
// survive matters as much as how many. Meld (in-module) can use internal/shape for the
// dependency model; the C stacks are driven exactly as in the cref byte-level harness.
package main

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
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

// chunked is the source: the clip split into transport chunks plus the unit graph the
// arbiter scores against. chunkUnit[seq] is the unit a chunk belongs to; unitChunks[id]
// is the set of chunk seqs that make up a unit (delivered iff all arrive).
type chunked struct {
	chunks     [][]byte // [seqHdr | video bytes], one per transport packet
	units      []shape.Unit
	shaped     []shape.Shaped
	unitChunks map[uint32][]uint32
	chunkSize  int
}

func chunkClip(path string, chunkSize int, avcOpts shape.AVCOptions) (*chunked, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	shaped := shape.NewAVCShaperWithOptions(avcOpts).Shape(data)
	c := &chunked{units: make([]shape.Unit, len(shaped)), shaped: shaped,
		unitChunks: map[uint32][]uint32{}, chunkSize: chunkSize}
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
		os.Remove(out)
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
		os.Remove(out)
		msg := strings.TrimSpace(string(b))
		if msg == "" {
			msg = err.Error()
		}
		return "", nil, fmt.Errorf("ffmpeg x264 intra-refresh transcode: %s", msg)
	}
	return out, func() { os.Remove(out) }, nil
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

// reassembleDecodable builds the model-filtered Annex-B stream of DECODABLE units
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
		out = append(out, 0, 0, 0, 1)
		out = append(out, sh.Payload...)
		if sh.Unit.Picture {
			pics++
		}
	}
	return out, pics
}

// reassembleDelivered builds the Annex-B stream from the raw delivered chunk set, in
// source order. A unit with any delivered chunk gets one start code, then only the
// delivered byte ranges; partial units are intentionally passed to ffprobe as partial
// units instead of being filtered through Meld's dependency oracle.
func (c *chunked) reassembleDelivered(seqs map[uint32]bool) []byte {
	var out []byte
	for _, sh := range c.shaped {
		started := false
		for _, seq := range c.unitChunks[sh.Unit.ID] {
			if !seqs[seq] {
				continue
			}
			if !started {
				out = append(out, 0, 0, 0, 1)
				started = true
			}
			pkt := c.chunks[seq]
			if len(pkt) > seqHdr {
				out = append(out, pkt[seqHdr:]...)
			}
		}
	}
	return out
}

// ffprobeFrames asks ffprobe how many frames the stream actually decodes.
func ffprobeFrames(h264 []byte) (int, error) {
	if len(h264) == 0 {
		return 0, nil
	}
	f, err := os.CreateTemp("", "glass-*.h264")
	if err != nil {
		return 0, err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(h264); err != nil {
		f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	out, err := exec.Command("ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
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

// --- impairment relay (forward media dropped per dropper, owd both ways) — cref-identical ---

func freeUDP() int {
	c, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	p := c.LocalAddr().(*net.UDPAddr).Port
	c.Close()
	return p
}

// geBurstPkts, when >= 1, switches dropper from i.i.d. to a Gilbert-Elliott
// 2-state channel whose MARGINAL loss equals the requested loss and whose mean
// bad-run (burst) length is geBurstPkts packets — the realistic bursty first mile
// where a fade outlives ARQ's retransmit window. 0 = i.i.d. (default).
var geBurstPkts float64

// meldTgtFail / meldRed override the Meld proactive provisioning (0 / <0 = default),
// for the low-loss latency-floor experiment.
var meldTgtFail float64
var meldRed float64 = -1
var meldGenSize int
var meldNoAuto bool
var meldNoReorder bool
var meldNoDecay bool
var meldReactiveShift bool

// meldHeadroom enables the experimental headroom-aware proactive sizing (A/B arm).
var meldHeadroom bool
var jitterDur time.Duration

// deadlineArbiter scores every arm against the same hard per-chunk playout deadline
// (sendTime + budget), instead of "frames eventually delivered" — see docs/bench.md
// "Deadline Semantics" for why ARQ latency-window output otherwise inflates burst cells.
var deadlineArbiter = new(bool)

// Relay counters instrument what the impairment relay actually observes. Forward
// means sender-side UDP traffic toward the receiver-side endpoint; reverse means
// receiver feedback toward the live sender address.
var relayEnq, relaySent atomic.Int64
var relayDropped atomic.Int64
var relayEnqBytes, relaySentBytes, relayDroppedBytes atomic.Int64
var relayReverseEnq, relayReverseSent atomic.Int64
var relayReverseEnqBytes, relayReverseSentBytes atomic.Int64

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

func dropper(loss float64, seed int64) func() bool {
	rng := rand.New(rand.NewSource(seed))
	if geBurstPkts >= 1 && loss > 0 && loss < 1 {
		// meanBurst = 1/r, marginal = p/(p+r); bad state always drops (h=1,k=0).
		r := 1.0 / geBurstPkts
		p := r * loss / (1 - loss)
		bad := false
		return func() bool {
			drop := bad
			if bad {
				if rng.Float64() < r {
					bad = false
				}
			} else if rng.Float64() < p {
				bad = true
			}
			return drop
		}
	}
	return func() bool { return loss > 0 && rng.Float64() < loss }
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
		c.Close()
		if port%2 != 0 {
			continue
		}
		if c2, e := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port + 1}); e == nil {
			c2.Close()
			return port
		}
	}
	return 0
}

// delayed is one packet held in the relay's delay line until its release time.
type delayed struct {
	at time.Time
	b  []byte
	to *net.UDPAddr // reverse path's live client addr; nil ⇒ fixed downstream
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
	down, _ := net.ResolveUDPAddr("udp", downstream)
	pub, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: listenPort})
	if err != nil {
		return 0, func() {}
	}
	srv, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	pub.SetReadBuffer(8 << 20)
	pub.SetWriteBuffer(8 << 20)
	srv.SetReadBuffer(8 << 20)
	srv.SetWriteBuffer(8 << 20)
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
			timer := time.NewTimer(time.Hour)
			if !timer.Stop() {
				<-timer.C
			}
			for {
				var wait <-chan time.Time
				if h.Len() > 0 {
					if d := time.Until((*h)[0].at); d <= 0 {
						p := heap.Pop(h).(delayed)
						to := fixedTo
						if to == nil {
							to = p.to
						}
						if to != nil {
							out.WriteToUDP(p.b, to)
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
			dropped := drop != nil && drop()
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
				relayReverseEnq.Add(1)
				relayReverseEnqBytes.Add(int64(n))
				if trace != nil {
					trace.recordFeedback(b[:n]) // burst autopsy: decoded feedback stream
				}
				revCh <- delayed{at: time.Now().Add(owd), b: append([]byte(nil), b[:n]...), to: a}
			}
		}
	}()
	return pub.LocalAddr().(*net.UDPAddr).Port, func() { pub.Close(); srv.Close() }
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
	outageAware        bool
	outageOff          bool
}

func meldArm(name string) (meldArmConfig, bool) {
	switch name {
	case "meld", "meld-flat", "meld-flat-unit":
		return meldArmConfig{name: name}, true
	case "meld-auto":
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true}, true
	case "meld-outage":
		// meld-auto with two-regime outage composure explicitly ON. Now that
		// Config.OutageAware is default-on this equals meld-auto; kept for grids that
		// pre-date the default. meld-outage-off is the A/B arm (the outage-blind
		// estimators the default replaced).
		return meldArmConfig{name: name, uep: true, frame: true, sliding: true, outageAware: true}, true
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

func runMeld(c *chunked, uep, sliding bool, loss float64, rttMs, budgetMs int, paceUs, maxBps, seed int64) map[uint32]bool {
	arm := meldArmConfig{uep: uep, frame: uep, sliding: sliding}
	return runMeldArm(c, arm, loss, rttMs, budgetMs, paceUs, maxBps, seed, nil).got
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
	if arm.outageAware {
		cfg.OutageAware = true
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
	port, stop := relayOn(0, rx.LocalAddr(), dropper(loss, seed), owd, seed, trace)
	defer stop()
	tx, err := meld.NewSender(fmt.Sprintf("127.0.0.1:%d", port), cfg)
	if err != nil {
		if os.Getenv("GLASSDBG") != "" {
			fmt.Fprintf(os.Stderr, "meld NewSender: %v\n", err)
		}
		rx.Close()
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
			rx.SetReadDeadline(time.Now().Add(15 * time.Second))
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
	for i, pkt := range c.chunks {
		seq := binary.BigEndian.Uint32(pkt[:seqHdr])
		switch {
		case arm.frame:
			tx.WriteFrame(pkt, descOf[seq])
		case arm.uep:
			tx.WriteUnit(pkt, descOf[seq].Priority)
		default:
			tx.WriteUnit(pkt, 2)
		}
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
	tx.Flush()
	time.Sleep(owd + time.Duration(budgetMs+400)*time.Millisecond)
	txStats, rxStats := tx.Stats(), rx.Stats()
	relayStats := snapshotRelayMetrics()
	tx.Close()
	rx.Close()
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
		fmt.Fprintf(os.Stderr, "[dbg arm=%s frame=%v uep=%v atomic=%v noframepolicy=%v sld=%v] tx src=%d repair=%d(reactive=%d throttled=%d tightens=%d) | relay enq=%d sent=%d | rx deliv=%d recov=%d lost=%d evicted=%d | got=%d/%d | miss-by-decile=%v\n",
			arm.name, arm.frame, arm.uep, cfg.FrameAtomic, arm.disableFramePolicy, cfg.Sliding,
			txStats.Source, txStats.Repair, txStats.ReactiveRepair, txStats.Throttled, txStats.HeadroomTightens,
			relayStats.ForwardEnqueued, relayStats.ForwardSent,
			rxStats.Delivered, rxStats.Recovered, rxStats.Lost, rxStats.Evicted, len(got), total, miss)
		fmt.Fprintf(os.Stderr, "[attr arm=%s] proactive=%d cold=%d singleton=%d sparse=%d deficit=%d (repair=%d)\n",
			arm.name, txStats.RepairProactive, txStats.RepairProactiveCold, txStats.RepairSingleton,
			txStats.RepairSparse, txStats.RepairDeficit, txStats.Repair)
	}
	return meldRunResult{got: got, txStats: txStats, rxStats: rxStats, relayEnq: relayStats.ForwardEnqueued, relaySent: relayStats.ForwardSent}
}

// --- C-stack arms (real subprocess + relay) — cref-identical drive ---

// udpSink collects delivered chunk seqs; when at is non-nil it also records each
// seq's FIRST arrival instant (the equal-deadline arbiter's receive clock — srt/rist
// deliver in order, so the first datagram at the sink IS that chunk's delivery).
func udpSink(port int, got map[uint32]bool, at map[uint32]time.Time, mu *sync.Mutex) *net.UDPConn {
	conn, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	go func() {
		b := make([]byte, 2048)
		for {
			conn.SetReadDeadline(time.Now().Add(4 * time.Second))
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
func feed(port int, chunks [][]byte, paceUs int64) time.Time {
	fc, _ := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	defer fc.Close()
	start := time.Now()
	for i, pkt := range chunks {
		fc.Write(pkt)
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
	return start
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
	cmd.Stderr = os.Stderr
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

func runLibsrt(c *chunked, loss float64, rttMs, latMs int, paceUs int64, seed int64, fec string) map[uint32]bool {
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
	defer sc.Close()
	pf := ""
	if fec != "" {
		pf = "&packetfilter=" + fec
	}
	recv := exec.Command("srt-live-transmit",
		fmt.Sprintf("srt://:%d?mode=listener&latency=%d%s", rport, latMs, pf),
		fmt.Sprintf("udp://127.0.0.1:%d", sink))
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
	feedp, stop := relayOn(0, fmt.Sprintf("127.0.0.1:%d", rport), dropper(loss, seed), owd, seed, nil)
	defer stop()
	feedInPort := freeUDP()
	send := exec.Command("srt-live-transmit",
		fmt.Sprintf("udp://:%d", feedInPort),
		fmt.Sprintf("srt://127.0.0.1:%d?mode=caller&latency=%d%s", feedp, latMs, pf))
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
	feedStart := feed(feedInPort, c.chunks, paceUs)
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

func runLibrist(c *chunked, loss float64, rttMs, latMs int, paceUs int64, seed int64) map[uint32]bool {
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
	defer sc.Close()
	recv := exec.Command(tools+"/ristreceiver", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("rist://@127.0.0.1:%d", rport), "-o", fmt.Sprintf("udp://127.0.0.1:%d", sink))
	recv.Env = env
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
	_, sA := relayOn(relayE, fmt.Sprintf("127.0.0.1:%d", rport), dropper(loss, seed), owd, seed, nil) // RTP media (dropped)
	_, sB := relayOn(relayE+1, fmt.Sprintf("127.0.0.1:%d", rport+1), nil, owd, seed+1, nil)           // RTCP/NACK (clean)
	defer sA()
	defer sB()
	send := exec.Command(tools+"/ristsender", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("udp://@127.0.0.1:%d", feedIn), "-o", fmt.Sprintf("rist://127.0.0.1:%d", relayE))
	send.Env = env
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
	feedStart := feed(feedIn, c.chunks, paceUs)
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

type score struct {
	frames, keyframes  int
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
	rxDelivered, rxRecovered, rxLost, rxEvicted := uint64(0), uint64(0), uint64(0), uint64(0)
	relayEnq, relaySent := int64(-1), int64(-1)
	if m != nil {
		txSrc, txRepair, txReactive, txThrottled = m.txStats.Source, m.txStats.Repair, m.txStats.ReactiveRepair, m.txStats.Throttled
		rxDelivered, rxRecovered, rxLost, rxEvicted = m.rxStats.Delivered, m.rxStats.Recovered, m.rxStats.Lost, m.rxStats.Evicted
		relayEnq, relaySent = m.relayEnq, m.relaySent
	}
	fmt.Fprintf(os.Stderr,
		"glassseed arm=%s rep=%d seed=%d ff=%d frame_pct=%.3f key_pct=%.3f chunks=%d/%d tx_src=%d tx_repair=%d tx_reactive=%d tx_throttled=%d relay_enq=%d relay_sent=%d rx_delivered=%d rx_recovered=%d rx_lost=%d rx_evicted=%d first_missing_unit=%d first_missing_picture=%d first_missing_key=%d first_broken_unit=%d first_broken_ref=%d\n",
		arm, rep, seed, ff, sc.frameRate, sc.keyRate, chunks, len(c.chunks),
		txSrc, txRepair, txReactive, txThrottled, relayEnq, relaySent,
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
	fullFF, err := ffprobeFrames(c.reassembleDelivered(allChunkSeqs(c)))
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
	ext := filepath.Ext(base)
	if ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
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
	fmt.Fprintf(&b, "| source | clip | artifacts |\n")
	fmt.Fprintf(&b, "| --- | --- | --- |\n")
	for _, clip := range clips {
		id := sourceIDForClip(clip)
		fmt.Fprintf(&b, "| `%s` | `%s` | [`%s/FRONTIER.md`](%s/FRONTIER.md), [`%s/FAIRNESS.md`](%s/FAIRNESS.md) |\n",
			id, clip, id, id, id, id)
	}
	return os.WriteFile(filepath.Join(outDir, "SOURCES.md"), []byte(b.String()), 0o644)
}

func main() {
	clip := flag.String("clip", "internal/shape/testdata/bbb_bframes.h264", "Annex-B H.264 clip")
	publishClips := flag.String("publishclips", "", "comma-separated Annex-B H.264 clips for -publishsuite; each source gets its own report subdirectory")
	loss := flag.Float64("loss", 0.2, "forward loss fraction")
	rtt := flag.Int("rtt", 100, "RTT ms")
	mult := flag.Int("rttmult", 1, "budget = max(floor, mult*rtt)")
	floorMs := flag.Int("buf", 100, "budget floor ms")
	mbps := flag.Float64("mbps", 8, "stream bitrate Mbps (paces chunks)")
	maxMbps := flag.Float64("maxmbps", 0, "meld MaxBitrate cap Mbps (0=default ~100); set near source rate to create UEP scarcity")
	reps := flag.Int("reps", 4, "seeds per arm")
	chunkSize := flag.Int("chunk", 1316, "video bytes per chunk")
	arms := flag.String("arms", "meld-auto,libsrt,librist", "comma list")
	sweep := flag.Bool("sweep", false, "iso-quality min-latency mode: find each transport's B_min vs RTT for quality bar -q")
	q := flag.Float64("q", 0.999, "quality bar: minimum delivery fraction (sweep mode)")
	rtts := flag.String("rtts", "20,50,100,200,400", "RTTs (ms) to sweep (sweep mode)")
	streamK := flag.Int("streamk", 4, "stream-length multiplier so delivery%% resolves a tight bar (sweep mode)")
	macroFrontier := flag.Bool("macrofrontier", false, "macro frontier mode: sweep ffprobe-decoded frames across loss/burst/RTT/latency")
	publishSuite := flag.String("publishsuite", "", "named publication benchmark suite: "+strings.Join(publishSuiteNames(), ", "))
	frontierLosses := flag.String("frontierlosses", "0.05,0.10", "macro frontier forward loss fractions")
	frontierBursts := flag.String("frontierbursts", "0,24", "macro frontier GE mean burst lengths in packets (0=i.i.d.)")
	frontierMults := flag.String("frontiermults", "1,1.5,2,3", "macro frontier latency budgets as RTT multipliers")
	frontierTop := flag.Int("frontiertop", 8, "macro frontier rows per summary section")
	geburst := flag.Float64("geburst", 0, "GE mean burst length in packets (0=i.i.d.); marginal loss stays -loss")
	atxrtt := flag.Float64("atxrtt", 0, "probe mode: measure delivery at this fixed ×RTT budget (no bisection), printing per-seed spread")
	sldwin := flag.Int("sldwin", 256, "Meld sliding CodingWindow (max band width); 0 = coder default")
	tgtfail := flag.Float64("tgtfail", 0, "Meld TargetFailure override (0 = default 1e-3); tighter = more proactive")
	red := flag.Float64("red", -1, "Meld Redundancy floor override (<0 = default)")
	gensize := flag.Int("gensize", 0, "Meld GenSize override (0 = default)")
	noauto := flag.Bool("noauto", false, "disable Meld AutoGenSize (pin GenSize)")
	noreorder := flag.Bool("noreorder", false, "disable Meld AutoReorderHoldoff (on by default)")
	nodecay := flag.Bool("nodecay", false, "disable Meld ProactiveDecay (on by default; A/B the margin/floor decay)")
	reactiveshift := flag.Bool("reactiveshift", false, "enable Meld SlidingReactiveShift (experimental sliding reactive-offload bundle)")
	headroom := flag.Bool("headroom", false, "enable Meld HeadroomAwareSizing (experimental affordable-rate ceiling on proactive repair)")
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
	jitterDur = time.Duration(*jitterMs) * time.Millisecond
	meldNoReorder = *noreorder
	meldNoDecay = *nodecay
	meldReactiveShift = *reactiveshift
	meldHeadroom = *headroom
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
			ChunkSize:           *chunkSize,
			TotalPics:           stats.TotalPics,
			OutDir:              outDir,
			TopN:                *frontierTop,
			JitterMs:            *jitterMs,
			SourceID:            sourceIDForClip(path),
			SourceClip:          path,
			SourceFFFrames:      stats.FFFrames,
			AVCOpts:             avcOpts,
			AutoEncoderCadence:  *autoEncoderCadence,
			AutoEncoderInterval: *autoEncoderInterval,
			AutoEncoderByteCap:  *autoEncoderByteCap,
			AutoEncoderPSNRMin:  *autoEncoderPSNRMin,
		}
	}
	if *publishSuite != "" {
		suite, ok := publishSuiteByName(*publishSuite)
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown publish suite %q (known: %s)\n", *publishSuite, strings.Join(publishSuiteNames(), ", "))
			os.Exit(1)
		}
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
			RTTMs:               *rtt,
			RTTMult:             *mult,
			BufferMs:            *floorMs,
			BudgetMs:            budgetMs,
			BitrateMbps:         *mbps,
			MaxMbps:             *maxMbps,
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
	fullFF, err := ffprobeFrames(fullH)
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
			seed := int64(s)*7919 + 13
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
			sc, h264, _ := grade(c, seqs)
			ff, err := ffprobeFrames(h264)
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
		"meld", "meld-auto", "meld-outage", "meld-outage-off", "meld-flat", "meld-flat-unit",
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
