// Command glassbench is a glass-to-glass media-decodability comparison judged by a
// NON-BIASED arbiter (ffprobe), not Meld's own model. It streams the same real H.264
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
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

func chunkClip(path string, chunkSize int) (*chunked, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	shaped := shape.NewAVCShaper().Shape(data)
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

// reassemble builds the Annex-B stream of the DECODABLE units (delivered + dependency
// closure) and returns it plus the displayed-picture count the decoder should reproduce.
func (c *chunked) reassemble(delivered map[uint32]bool) ([]byte, int) {
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

// ffprobeFrames asks the non-biased arbiter how many frames the stream actually decodes.
func ffprobeFrames(h264 []byte) int {
	f, err := os.CreateTemp("", "glass-*.h264")
	if err != nil {
		return -1
	}
	defer os.Remove(f.Name())
	f.Write(h264)
	f.Close()
	out, err := exec.Command("ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0",
		"-show_entries", "stream=nb_read_frames", "-of", "csv=p=0", f.Name()).Output()
	if err != nil {
		return -1
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return n
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
var jitterDur time.Duration

// relayEnq/relaySent instrument the forward path of the relay (enqueued vs actually written) to
// isolate relay drops from receiver behavior under GLASSDBG.
var relayEnq, relaySent atomic.Int64

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
func relayOn(listenPort int, downstream string, drop func() bool, owd time.Duration) (int, func()) {
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
	rng := rand.New(rand.NewSource(int64(pub.LocalAddr().(*net.UDPAddr).Port)))
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
			if drop != nil && drop() {
				continue
			}
			d := owd
			if jitterDur > 0 {
				d += time.Duration(rng.Int63n(int64(jitterDur)))
			}
			relayEnq.Add(1)
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

func runMeld(c *chunked, uep, sliding bool, loss float64, rttMs, budgetMs int, paceUs, maxBps, seed int64) map[uint32]bool {
	owd := time.Duration(rttMs/2) * time.Millisecond
	cfg := meld.DefaultConfig()
	cfg.SymbolSize = seqHdr + c.chunkSize
	cfg.BufferMicros = int64(budgetMs) * 1000
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
	if sliding {
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
	rx, err := meld.NewReceiver("127.0.0.1:0", cfg)
	if err != nil {
		return nil
	}
	relayEnq.Store(0)
	relaySent.Store(0)
	port, stop := relayOn(0, rx.LocalAddr(), dropper(loss, seed), owd)
	defer stop()
	tx, err := meld.NewSender(fmt.Sprintf("127.0.0.1:%d", port), cfg)
	if err != nil {
		rx.Close()
		return nil
	}
	got := map[uint32]bool{}
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
				mu.Lock()
				got[binary.BigEndian.Uint32(buf[:seqHdr])] = true
				mu.Unlock()
			}
		}
	}()
	// chunk seq -> the unit's descriptor, so WriteFrame carries the media metadata.
	descOf := map[uint32]meld.FrameDesc{}
	for _, sh := range c.shaped {
		fd := meld.FrameDesc{FrameID: sh.Unit.ID, RefFrameIDs: sh.Unit.RefersTo,
			Chunks: uint16(len(c.unitChunks[sh.Unit.ID])), RAP: sh.Unit.RAP, Discardable: sh.Unit.Discardable}
		if uep {
			fd.Priority = sh.Unit.Class.Wire()
		} else {
			fd.Priority = 2 // base tier (media-blind)
		}
		for _, s := range c.unitChunks[sh.Unit.ID] {
			descOf[s] = fd
		}
	}
	start := time.Now()
	for i, pkt := range c.chunks {
		seq := binary.BigEndian.Uint32(pkt[:seqHdr])
		if uep {
			tx.WriteFrame(pkt, descOf[seq])
		} else {
			tx.WriteUnit(pkt, 2)
		}
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
	tx.Flush()
	time.Sleep(owd + time.Duration(budgetMs+400)*time.Millisecond)
	tx.Close()
	rx.Close()
	<-done
	if os.Getenv("GLASSDBG") != "" {
		ts, rs := tx.Stats(), rx.Stats()
		var miss [10]int
		total := len(c.chunks)
		for i := 0; i < total; i++ {
			if !got[uint32(i)] {
				miss[i*10/total]++
			}
		}
		fmt.Fprintf(os.Stderr, "[dbg sld=%v] tx src=%d repair=%d(reactive=%d) | relay enq=%d sent=%d | rx deliv=%d recov=%d lost=%d evicted=%d | got=%d/%d | miss-by-decile=%v\n",
			cfg.Sliding, ts.Source, ts.Repair, ts.ReactiveRepair, relayEnq.Load(), relaySent.Load(), rs.Delivered, rs.Recovered, rs.Lost, rs.Evicted, len(got), total, miss)
	}
	return got
}

// --- C-stack arms (real subprocess + relay) — cref-identical drive ---

func udpSink(port int, got map[uint32]bool, mu *sync.Mutex) *net.UDPConn {
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
				mu.Lock()
				got[binary.BigEndian.Uint32(b[:seqHdr])] = true
				mu.Unlock()
			}
		}
	}()
	return conn
}

func feed(port int, chunks [][]byte, paceUs int64) {
	fc, _ := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	defer fc.Close()
	start := time.Now()
	for i, pkt := range chunks {
		fc.Write(pkt)
		if d := time.Duration(i+1)*time.Duration(paceUs)*time.Microsecond - time.Since(start); d > 0 {
			time.Sleep(d)
		}
	}
}

func runLibsrt(c *chunked, loss float64, rttMs, latMs int, paceUs int64, seed int64, fec string) map[uint32]bool {
	owd := time.Duration(rttMs/2) * time.Millisecond
	rport, sink := freeUDP(), freeUDP()
	got := map[uint32]bool{}
	var mu sync.Mutex
	sc := udpSink(sink, got, &mu)
	defer sc.Close()
	pf := ""
	if fec != "" {
		pf = "&packetfilter=" + fec
	}
	recv := exec.Command("srt-live-transmit",
		fmt.Sprintf("srt://:%d?mode=listener&latency=%d%s", rport, latMs, pf),
		fmt.Sprintf("udp://127.0.0.1:%d", sink))
	if recv.Start() != nil {
		return nil
	}
	time.Sleep(1000 * time.Millisecond)
	feedp, stop := relayOn(0, fmt.Sprintf("127.0.0.1:%d", rport), dropper(loss, seed), owd)
	defer stop()
	feedInPort := freeUDP()
	send := exec.Command("srt-live-transmit",
		fmt.Sprintf("udp://:%d", feedInPort),
		fmt.Sprintf("srt://127.0.0.1:%d?mode=caller&latency=%d%s", feedp, latMs, pf))
	if send.Start() != nil {
		return nil
	}
	time.Sleep(1500*time.Millisecond + owd)
	feed(feedInPort, c.chunks, paceUs)
	time.Sleep(owd + time.Duration(latMs+800)*time.Millisecond)
	send.Process.Kill()
	send.Wait()
	recv.Process.Kill()
	recv.Wait()
	return got
}

func runLibrist(c *chunked, loss float64, rttMs, latMs int, paceUs int64, seed int64) map[uint32]bool {
	owd := time.Duration(rttMs/2) * time.Millisecond
	home, _ := os.UserHomeDir()
	tools := home + "/dev/librist/build/tools"
	env := append(os.Environ(), "DYLD_LIBRARY_PATH="+home+"/dev/librist/build")
	rport, feedIn, sink := freeEven(), freeUDP(), freeUDP()
	got := map[uint32]bool{}
	var mu sync.Mutex
	sc := udpSink(sink, got, &mu)
	defer sc.Close()
	recv := exec.Command(tools+"/ristreceiver", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("rist://@127.0.0.1:%d", rport), "-o", fmt.Sprintf("udp://127.0.0.1:%d", sink))
	recv.Env = env
	if recv.Start() != nil {
		return nil
	}
	time.Sleep(1200 * time.Millisecond)
	relayE := freeEven()
	_, sA := relayOn(relayE, fmt.Sprintf("127.0.0.1:%d", rport), dropper(loss, seed), owd) // RTP media (dropped)
	_, sB := relayOn(relayE+1, fmt.Sprintf("127.0.0.1:%d", rport+1), nil, owd)             // RTCP/NACK (clean)
	defer sA()
	defer sB()
	send := exec.Command(tools+"/ristsender", "-p", "0", "-b", strconv.Itoa(latMs),
		"-i", fmt.Sprintf("udp://@127.0.0.1:%d", feedIn), "-o", fmt.Sprintf("rist://127.0.0.1:%d", relayE))
	send.Env = env
	if send.Start() != nil {
		return nil
	}
	time.Sleep(1500*time.Millisecond + owd)
	feed(feedIn, c.chunks, paceUs)
	time.Sleep(owd + time.Duration(latMs+800)*time.Millisecond)
	send.Process.Kill()
	send.Wait()
	recv.Process.Kill()
	recv.Wait()
	return got
}

type score struct {
	frames, keyframes  int
	frameRate, keyRate float64
}

func grade(c *chunked, seqs map[uint32]bool) (score, []byte, int) {
	du := c.deliveredUnits(seqs)
	h264, pics := c.reassemble(du)
	return score{
		frameRate: shape.DecodableFrameRate(c.units, du),
		keyRate:   shape.DecodableKeyframeRate(c.units, du),
	}, h264, pics
}

func main() {
	clip := flag.String("clip", "internal/shape/testdata/bbb_bframes.h264", "Annex-B H.264 clip")
	loss := flag.Float64("loss", 0.2, "forward loss fraction")
	rtt := flag.Int("rtt", 100, "RTT ms")
	mult := flag.Int("rttmult", 1, "budget = max(floor, mult*rtt)")
	floorMs := flag.Int("buf", 100, "budget floor ms")
	mbps := flag.Float64("mbps", 8, "stream bitrate Mbps (paces chunks)")
	maxMbps := flag.Float64("maxmbps", 0, "meld MaxBitrate cap Mbps (0=default ~100); set near source rate to create UEP scarcity")
	reps := flag.Int("reps", 4, "seeds per arm")
	chunkSize := flag.Int("chunk", 1316, "video bytes per chunk")
	arms := flag.String("arms", "meld-uep,meld-flat,libsrt,libsrt-fec,librist", "comma list")
	sweep := flag.Bool("sweep", false, "iso-quality min-latency mode: find each transport's B_min vs RTT for quality bar -q")
	q := flag.Float64("q", 0.999, "quality bar: minimum delivery fraction (sweep mode)")
	rtts := flag.String("rtts", "20,50,100,200,400", "RTTs (ms) to sweep (sweep mode)")
	streamK := flag.Int("streamk", 4, "stream-length multiplier so delivery%% resolves a tight bar (sweep mode)")
	geburst := flag.Float64("geburst", 0, "GE mean burst length in packets (0=i.i.d.); marginal loss stays -loss")
	atxrtt := flag.Float64("atxrtt", 0, "probe mode: measure delivery at this fixed ×RTT budget (no bisection), printing per-seed spread")
	sldwin := flag.Int("sldwin", 256, "Meld sliding CodingWindow (max band width); 0 = coder default")
	tgtfail := flag.Float64("tgtfail", 0, "Meld TargetFailure override (0 = default 1e-3); tighter = more proactive")
	red := flag.Float64("red", -1, "Meld Redundancy floor override (<0 = default)")
	gensize := flag.Int("gensize", 0, "Meld GenSize override (0 = default)")
	noauto := flag.Bool("noauto", false, "disable Meld AutoGenSize (pin GenSize)")
	noreorder := flag.Bool("noreorder", false, "disable Meld AutoReorderHoldoff (on by default)")
	jitterMs := flag.Int("jitter", 0, "max per-datagram forward jitter ms (injects reorder)")
	flag.Parse()
	jitterDur = time.Duration(*jitterMs) * time.Millisecond
	meldNoReorder = *noreorder
	geBurstPkts = *geburst
	sldWindow = *sldwin
	meldTgtFail = *tgtfail
	meldRed = *red
	meldGenSize = *gensize
	meldNoAuto = *noauto

	c, err := chunkClip(*clip, *chunkSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clip:", err)
		os.Exit(1)
	}
	totalPics := 0
	totalKey := 0
	disposable := 0
	for _, u := range c.units {
		if u.Picture {
			totalPics++
		}
		if u.RAP {
			totalKey++
		}
		if u.Discardable {
			disposable++
		}
	}
	budgetMs := arqLatencyMs(*rtt, *mult, *floorMs)
	paceUs := int64(float64((*chunkSize+seqHdr)*8) / (*mbps * 1e6) * 1e6)
	if paceUs < 1 {
		paceUs = 1
	}
	meldMax := int64(*maxMbps * 1e6)
	if *sweep {
		if *atxrtt > 0 {
			runProbe(c, *loss, paceUs, meldMax, *mbps, parseRTTs(*rtts), *reps, *atxrtt, *streamK, strings.Split(*arms, ","))
		} else {
			runSweep(c, *loss, paceUs, meldMax, *mbps, parseRTTs(*rtts), *reps, *q, *streamK, strings.Split(*arms, ","))
		}
		return
	}
	// no-loss sanity: ffprobe must decode all pictures from the full clip
	full := map[uint32]bool{}
	for _, u := range c.units {
		full[u.ID] = true
	}
	fullH, fullPics := c.reassemble(full)
	fmt.Printf("# glassbench: %s — %d units, %d pictures, %d keyframes, %d DISPOSABLE units, %d chunks (%dB)\n",
		*clip, len(c.units), totalPics, totalKey, disposable, len(c.chunks), *chunkSize)
	fmt.Printf("# ffprobe(full clip) = %d frames (decodable-set model predicts %d)\n", ffprobeFrames(fullH), fullPics)
	fmt.Printf("# loss %.0f%%, RTT %dms, budget %dms (%dxRTT), %.0f Mbps, %d seeds; arbiter=ffprobe\n",
		*loss*100, *rtt, budgetMs, *mult, *mbps, *reps)
	fmt.Printf("# metric: ffprobe-decoded frames (mean), and the model's decodable frame%% / keyframe%%\n\n")
	fmt.Printf("%-12s  ff-frames  frame%%  keyframe%%\n", "arm")

	run := func(name string, fn func(seed int64) map[uint32]bool) {
		var ffSum int
		var frSum, kfSum float64
		ok := 0
		for s := 1; s <= *reps; s++ {
			seqs := fn(int64(s)*7919 + 13)
			if seqs == nil {
				continue
			}
			sc, h264, _ := grade(c, seqs)
			ff := ffprobeFrames(h264)
			if ff < 0 {
				ff = 0
			}
			ffSum += ff
			frSum += sc.frameRate
			kfSum += sc.keyRate
			ok++
		}
		if ok == 0 {
			fmt.Printf("%-12s  FAILED\n", name)
			return
		}
		fmt.Printf("%-12s  %7.1f   %5.1f%%   %6.1f%%\n",
			name, float64(ffSum)/float64(ok), 100*frSum/float64(ok), 100*kfSum/float64(ok))
	}

	want := map[string]bool{}
	for _, a := range strings.Split(*arms, ",") {
		want[strings.TrimSpace(a)] = true
	}
	order := []string{"meld-uep", "meld-flat", "libsrt", "libsrt-fec", "librist"}
	sort.SliceStable(order, func(i, j int) bool { return false })
	for _, a := range order {
		if !want[a] {
			continue
		}
		switch a {
		case "meld-uep":
			run("meld-uep", func(seed int64) map[uint32]bool {
				return runMeld(c, true, false, *loss, *rtt, budgetMs, paceUs, meldMax, seed)
			})
		case "meld-flat":
			run("meld-flat", func(seed int64) map[uint32]bool {
				return runMeld(c, false, false, *loss, *rtt, budgetMs, paceUs, meldMax, seed)
			})
		case "meld-sld":
			run("meld-sld", func(seed int64) map[uint32]bool {
				return runMeld(c, false, true, *loss, *rtt, budgetMs, paceUs, meldMax, seed)
			})
		case "libsrt":
			run("libsrt", func(seed int64) map[uint32]bool { return runLibsrt(c, *loss, *rtt, budgetMs, paceUs, seed, "") })
		case "libsrt-fec":
			run("libsrt-fec", func(seed int64) map[uint32]bool {
				return runLibsrt(c, *loss, *rtt, budgetMs, paceUs, seed, "fec,cols:10,rows:5,arq:onreq")
			})
		case "librist":
			run("librist", func(seed int64) map[uint32]bool { return runLibrist(c, *loss, *rtt, budgetMs, paceUs, seed) })
		}
	}
}
