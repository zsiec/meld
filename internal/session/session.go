// Package session is Meld's goroutine host: the thin, dumb pump that wraps the
// deterministic sans-I/O core (internal/flow) in real UDP I/O, a clock, and a
// tick cadence. It owns the sockets and goroutines; it contains no protocol logic
// — every protocol decision is the core's, surfaced as drained datagrams the host
// puts on the wire and inbound datagrams the host feeds in.
//
// Concurrency: a single mutex serializes all access to the (non-concurrent) flow
// core. I/O happens outside the lock — datagrams are collected under the lock and
// transmitted after releasing it — so a slow socket never blocks the core.
//
// Encryption (opt-in via a *SecurityConfig) lives entirely here, not in the core: the
// host runs the hybrid post-quantum handshake (internal/crypto) before any media and
// AEAD-seals each source symbol at the Write boundary / opens it after delivery
// (encrypt-then-code). The core stays crypto-blind — it never imports internal/crypto
// and only reports the delivered symbol's source id so the host can derive the nonce.
package session

import (
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/crypto"
	"github.com/zsiec/meld/internal/flow"
	"github.com/zsiec/meld/internal/wire"
)

// tickInterval drives the core's time-based work (deadline eviction at the
// receiver, the redundancy controller / keepalives at the sender).
const tickInterval = 5 * time.Millisecond

// The encryption layer (SecurityConfig, the ratcheting sealState/openState keying, the
// handshake constants) lives in secure.go and is shared by the single-path and multipath
// hosts. The core stays crypto-blind — it never imports internal/crypto and only reports
// each delivered symbol's source id so the host can derive the nonce.

// coreSender and coreReceiver are the flow-core method sets the host drives. flow
// provides two implementations — the generation coder and the band-form sliding
// coder (Config.Sliding) — selected here so the rest of the host is identical.
type coreSender interface {
	Write(now clock.Timestamp, data []byte)
	WriteUnit(now clock.Timestamp, data []byte, priority uint8)
	WriteFrame(now clock.Timestamp, data []byte, fd flow.FrameDesc)
	FeedFeedback(now clock.Timestamp, fb wire.Feedback)
	Tick(now clock.Timestamp)
	Flush(now clock.Timestamp)
	PollSend() ([]byte, bool)
	Stats() flow.SenderStats
	EncoderControl() flow.EncoderControl
	// RateBudgetBitsPerSec is the send-rate budget the host pacer releases within (the
	// congestion controller's output, or the static ceiling). Read-only — the pacer
	// never sets it; it only reshapes timing within it.
	RateBudgetBitsPerSec() int64
}

type coreReceiver interface {
	FeedSymbol(now clock.Timestamp, datagram []byte)
	FeedSymbolECN(now clock.Timestamp, datagram []byte, ecn flow.ECN)
	Tick(now clock.Timestamp)
	PollDeliver() (uint32, []byte, bool)
	PollSend() ([]byte, bool)
	Stats() flow.ReceiverStats
	FrameStats() flow.FrameStats
}

func newCoreSender(cfg flow.Config) coreSender {
	if cfg.Sliding {
		return flow.NewSlidingSender(cfg)
	}
	return flow.NewSender(cfg)
}

func newCoreReceiver(cfg flow.Config) coreReceiver {
	if cfg.Sliding {
		return flow.NewSlidingReceiver(cfg)
	}
	return flow.NewReceiver(cfg)
}

func usesL4SMarking(cfg flow.Config) bool {
	return cfg.CongestionControl && !cfg.Sliding
}

func usesECNReceive(cfg flow.Config) bool {
	return !cfg.Sliding
}

func mtuProbeBodySize(size int, ctl controlState) int {
	if ctl.active {
		size -= crypto.ControlOverhead
		if size < 0 {
			return 0
		}
	}
	return size
}

const (
	minRecvBufferSize     = 2048
	defaultMaxUDPDatagram = 64 << 10
	maxWireSymbolOverhead = 1200 // base header + SendTimestamp + max frame descriptor extension slack
)

func recvBufferSize(cfg flow.Config) int {
	size := defaultMaxUDPDatagram
	if cfg.SymbolSize+maxWireSymbolOverhead > size {
		size = cfg.SymbolSize + maxWireSymbolOverhead
	}
	if cfg.MaxProbeMTU > size {
		size = cfg.MaxProbeMTU
	}
	if size < minRecvBufferSize {
		return minRecvBufferSize
	}
	return size
}

// Sender is the UDP transmit host. It dials a remote, runs a flow.Sender, and
// exposes a blocking Write. Safe for one writer plus the internal goroutines.
type Sender struct {
	sub       Substrate
	clk       clock.Clock
	cfg       flow.Config
	mu        sync.Mutex
	flow      coreSender
	pace      *hostPacer  // host transmit pacer (nil when Config.Pace is off)
	mtu       *pmtudState // per-path DPLPMTUD state machine (nil when Config.ProbeMTU is off)
	mtuNonce  uint32      // correlation nonce of the outstanding MTU probe (guarded by mu)
	done      chan struct{}
	closeOnce sync.Once

	// Encryption (the handshake initiator + the ratcheting send keying). Cleartext when
	// the embedded sealState has a nil sec. Guarded by mu.
	sealState
	hsInit *crypto.Initiator // pending handshake
	hsMsg1 []byte            // cached framed message 1, resent until established
	hsDone chan struct{}     // closed when the secure channel is up
}

// NewSender dials remote (host:port) and starts the transmit host. A non-nil active sec
// runs the encryption handshake before returning; NewSender blocks until the secure
// channel is established or the handshake times out.
func NewSender(remote string, cfg flow.Config, sec *SecurityConfig) (*Sender, error) {
	sub, err := dialUDP(remote)
	if err != nil {
		return nil, err
	}
	return newSender(sub, cfg, clock.NewRealClock(), sec)
}

// NewSenderOver builds a coded sender over a caller-provided Substrate instead of dialing UDP. The
// host supplies the datagram transport — a WebTransport session, an in-WASM bridge to the browser's
// transport, an in-process pipe — so the same coder runs anywhere a datagram pipe exists.
func NewSenderOver(sub Substrate, cfg flow.Config, sec *SecurityConfig) (*Sender, error) {
	return newSender(sub, cfg, clock.NewRealClock(), sec)
}

// pmtudConfigFromCfg builds the DPLPMTUD state-machine config from the public flow.Config
// (the ceiling from MaxProbeMTU; base and timers from defaults).
func pmtudConfigFromCfg(cfg flow.Config) pmtudConfig {
	return pmtudConfig{Max: cfg.MaxProbeMTU}
}

// newSender builds the transmit host on sub with a given clock and starts its goroutines.
// The substrate seam lets a test drive the host over an in-memory pipe (no real socket).
func newSender(sub Substrate, cfg flow.Config, clk clock.Clock, sec *SecurityConfig) (*Sender, error) {
	return newSenderCfg(sub, cfg, clk, sec, pmtudConfigFromCfg(cfg))
}

// newSenderCfg is newSender with an explicit DPLPMTUD config, so a test can inject fast
// probe timers without exposing them on the public Config.
func newSenderCfg(sub Substrate, cfg flow.Config, clk clock.Clock, sec *SecurityConfig, pcfg pmtudConfig) (*Sender, error) {
	if err := validateSubstrate(sub); err != nil {
		return nil, err
	}
	if err := sec.validate(cfg.SymbolSize); err != nil {
		sub.Close()
		return nil, err
	}
	s := &Sender{
		sub: sub, clk: clk, cfg: cfg, flow: newCoreSender(cfg), done: make(chan struct{}),
		sealState: newSealState(sec, cfg.SymbolSize, cfg.Flow),
	}
	if usesL4SMarking(cfg) {
		_ = setECN(sub) // mark outgoing data ECT(1) (L4S) so an AQM can CE-mark rather than drop
	}
	if cfg.ProbeMTU {
		// Per-path DPLPMTUD: discover the path MTU and detect black holes. Set the socket's
		// Don't-Fragment bit (best-effort) so probes test the path unfragmented.
		s.mtu = newPMTUD(pcfg)
		_ = setDontFragment(sub)
	}
	if cfg.Pace {
		// The pacer releases at a rate slaved to the core's budget (never its own), with a
		// small smoothing burst and a deadline-derived backpressure limit. Started before the
		// loops so tick/feedback can re-slave it; idle until the first media Write.
		s.pace = newHostPacer(clk, s.flow.RateBudgetBitsPerSec()/8,
			paceBurstMicros(cfg), paceQueueLimitMicros(cfg),
			func(d []byte) (int, error) {
				if err := writeDatagram(s.sub, d, nil); err != nil {
					return 0, err
				}
				return len(d), nil
			})
	}
	if sec.active() {
		s.hsDone = make(chan struct{})
		init, err := crypto.NewInitiator(s.psk, beU32(cfg.Flow), sec.epochSize())
		if err != nil {
			sub.Close()
			return nil, err
		}
		msg1, err := init.WriteMessage1()
		if err != nil {
			sub.Close()
			return nil, err
		}
		s.hsInit = init
		s.hsMsg1 = wire.EncodeHandshakeInit(nil, msg1)
		_ = writeDatagram(s.sub, s.hsMsg1, nil) // first attempt; tickLoop retries until established
	}
	go s.recvLoop()
	go s.tickLoop()
	if s.sec != nil {
		select {
		case <-s.hsDone:
		case <-time.After(handshakeTimeout):
			s.Close()
			return nil, errHandshakeTimeout
		}
	}
	return s, nil
}

// Write hands one media chunk to the flow at the base protection tier and transmits
// the resulting symbols. When encrypted, the chunk is AEAD-sealed first.
func (s *Sender) Write(p []byte) (int, error) {
	select {
	case <-s.done:
		return 0, net.ErrClosed
	default:
	}
	s.mu.Lock()
	data, err := s.seal(p)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.flow.Write(s.clk.Now(), data)
	out := drainSend(s.flow)
	s.mu.Unlock()
	return len(p), s.sendOut(out)
}

// WriteUnit hands one media chunk to the flow carrying a protection tier (the priority
// the media shaper assigns) and transmits the resulting symbols.
func (s *Sender) WriteUnit(p []byte, priority uint8) (int, error) {
	select {
	case <-s.done:
		return 0, net.ErrClosed
	default:
	}
	s.mu.Lock()
	data, err := s.seal(p)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.flow.WriteUnit(s.clk.Now(), data, priority)
	out := drainSend(s.flow)
	s.mu.Unlock()
	return len(p), s.sendOut(out)
}

// WriteFrame hands one media chunk to the flow carrying the full access-unit descriptor
// (protection tier + dependency) and transmits the resulting symbols.
func (s *Sender) WriteFrame(p []byte, fd flow.FrameDesc) (int, error) {
	select {
	case <-s.done:
		return 0, net.ErrClosed
	default:
	}
	s.mu.Lock()
	data, err := s.seal(p)
	if err != nil {
		s.mu.Unlock()
		return 0, err
	}
	s.flow.WriteFrame(s.clk.Now(), data, fd)
	out := drainSend(s.flow)
	s.mu.Unlock()
	return len(p), s.sendOut(out)
}

// Flush closes the open generation (end of stream) and transmits its repair. When
// pacing, the repair is enqueued (drained by the pace loop / the Close flush), not
// blocked on — end-of-stream backpressure would only delay teardown.
func (s *Sender) Flush() {
	s.mu.Lock()
	s.flow.Flush(s.clk.Now())
	out := drainSend(s.flow)
	s.mu.Unlock()
	if s.pace != nil {
		s.pace.offer(out)
		return
	}
	s.transmit(out)
}

// Stats returns the sender's emission counters.
func (s *Sender) Stats() flow.SenderStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flow.Stats()
}

// EncoderControl returns Meld's current advisory encoder-control request.
func (s *Sender) EncoderControl() flow.EncoderControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flow.EncoderControl()
}

// Close flushes the tail, stops the goroutines, and closes the substrate. With pacing,
// the tail repair is drained to the wire (flushClose) before the substrate is shut, so
// end-of-stream protection is not lost to teardown.
func (s *Sender) Close() error {
	s.Flush()                                // enqueue/transmit the tail repair
	s.closeOnce.Do(func() { close(s.done) }) // stop recv/tick loops — no more enqueues
	if s.pace != nil {
		s.pace.flushClose() // drain the queue to the substrate at full speed
		s.pace.stop()       // then halt the pace loop
	}
	return s.sub.Close()
}

// sendOut puts the core's drained datagrams on the wire: through the pacer (smoothed +
// backpressured) when enabled, else transmitted immediately (the pre-pacer behaviour).
func (s *Sender) sendOut(out [][]byte) error {
	if s.pace != nil {
		return s.pace.put(out)
	}
	return s.transmit(out)
}

func (s *Sender) transmit(out [][]byte) error {
	for _, d := range out {
		if err := writeDatagram(s.sub, d, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sender) recvLoop() {
	buf := make([]byte, recvBufferSize(s.cfg))
	for {
		n, _, err := s.sub.ReadFrom(buf)
		if err != nil {
			return
		}
		t, e := wire.PeekType(buf[:n])
		if e != nil {
			continue
		}
		switch {
		case wire.IsFeedback(t):
			// On an encrypted flow, authenticate the feedback (and reject replays) under the
			// control key before it can retire recovery state — an unauthenticated or replayed
			// feedback is a forgeable DoS.
			s.mu.Lock()
			d, ok := s.ctl.open(buf[:n])
			if !ok {
				s.mu.Unlock()
				continue
			}
			fb, e := wire.DecodeFeedback(d)
			if e != nil {
				s.mu.Unlock()
				continue
			}
			s.flow.FeedFeedback(s.clk.Now(), fb)
			budget := s.flow.RateBudgetBitsPerSec()
			s.mu.Unlock()
			if s.pace != nil {
				s.pace.setRate(budget / 8) // congestion budget moved — re-slave the pacer
			}
		case wire.IsHandshakeResp(t):
			s.handleHandshakeResp(buf[:n])
		case wire.IsHandshakeCookie(t):
			s.handleCookieReply(buf[:n])
		case wire.IsClockProbe(t):
			// Echo the probe's T0 with the sender's receive (T1) and send (T2) times, so the
			// receiver can recover the clock offset (N4). On an encrypted flow the probe is
			// authenticated and the echo sealed, so a forged or replayed probe cannot skew the
			// offset; open and seal happen under the lock so the sequence/replay state is safe.
			recv := int64(s.clk.Now())
			s.mu.Lock()
			d, ok := s.ctl.open(buf[:n])
			if !ok {
				s.mu.Unlock()
				continue
			}
			p, e := wire.DecodeClockProbe(d)
			if e != nil {
				s.mu.Unlock()
				continue
			}
			echo, ok := s.ctl.seal(wire.EncodeClockEcho(nil, wire.ClockEcho{T0: p.T0, T1: recv, T2: int64(s.clk.Now())}))
			s.mu.Unlock()
			if ok {
				_ = writeDatagram(s.sub, echo, nil)
			}
		case wire.IsMTUProbeAck(t):
			// A DPLPMTUD probe was confirmed by the peer. Authenticate it, then feed the
			// matching-nonce ack to the state machine (the nonce rejects a stale ack from a
			// previous probe of the same size — load-bearing for black-hole detection).
			s.mu.Lock()
			d, ok := s.ctl.open(buf[:n])
			if !ok {
				s.mu.Unlock()
				continue
			}
			if nonce, size, e := wire.DecodeMTUProbeAck(d); e == nil && s.mtu != nil && nonce == s.mtuNonce {
				s.mtu.onAck(s.clk.Now(), int(size))
			}
			s.mu.Unlock()
		}
	}
}

// handleHandshakeResp completes the initiator handshake from message 2: it establishes
// the session, builds the epoch sealer, and unblocks NewSender. A failed message (wrong
// PSK, tamper) is ignored so a retry can still succeed.
func (s *Sender) handleHandshakeResp(datagram []byte) {
	if s.sec == nil {
		return
	}
	payload, err := wire.DecodeHandshake(datagram)
	if err != nil {
		return
	}
	s.mu.Lock()
	ok := s.completeResp(payload, s.hsInit) // installs sendSecret + control keys on success
	s.mu.Unlock()
	if ok {
		close(s.hsDone) // exactly once: completeResp returns ok only on the establishing call
	}
}

// handleCookieReply answers a responder's under-load cookie: it decrypts the cookie and
// re-arms message 1 with the cookie-derived mac2, which tickLoop then resends.
func (s *Sender) handleCookieReply(datagram []byte) {
	if s.sec == nil {
		return
	}
	payload, err := wire.DecodeHandshake(datagram)
	if err != nil {
		return
	}
	cookie, err := crypto.OpenCookieReply(s.psk, payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	if s.sendSecret != nil || s.hsInit == nil {
		s.mu.Unlock()
		return
	}
	if m1, err := s.hsInit.WriteMessage1WithCookie(cookie); err == nil {
		s.hsMsg1 = wire.EncodeHandshakeInit(nil, m1)
	}
	msg := s.hsMsg1
	s.mu.Unlock()
	_ = writeDatagram(s.sub, msg, nil)
}

func (s *Sender) tickLoop() {
	tk := time.NewTicker(tickInterval)
	defer tk.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-tk.C:
			s.mu.Lock()
			if s.sec != nil && s.sendSecret == nil {
				msg := s.hsMsg1 // snapshot under the lock; handleCookieReply may rewrite it
				s.mu.Unlock()
				_ = writeDatagram(s.sub, msg, nil) // resend handshake message 1 until established
				continue
			}
			now := s.clk.Now()
			s.flow.Tick(now)
			out := drainSend(s.flow)
			budget := s.flow.RateBudgetBitsPerSec()
			var probe []byte
			if s.mtu != nil {
				// DPLPMTUD: send the next probe the state machine asks for, padded to the
				// candidate size and sealed/authenticated like the other control datagrams.
				if size, send := s.mtu.tick(now); send {
					s.mtuNonce++
					if p, ok := s.ctl.seal(wire.EncodeMTUProbe(nil, s.mtuNonce, mtuProbeBodySize(size, s.ctl))); ok {
						probe = p
					}
				}
			}
			s.mu.Unlock()
			if s.pace != nil {
				s.pace.setRate(budget / 8) // re-slave to the current budget
				s.pace.offer(out)          // tick-driven repair/keepalive: non-blocking
			} else {
				s.transmit(out)
			}
			if probe != nil {
				_ = writeDatagram(s.sub, probe, nil) // control plane: bypass the pacer
			}
		}
	}
}

// PathMTU returns the discovered path PLPMTU in bytes (the UDP-payload size DPLPMTUD has
// confirmed the path passes), or 0 when probing is disabled. Phase 1 reports it; symbol
// sizing does not yet consume it.
func (s *Sender) PathMTU() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mtu == nil {
		return 0
	}
	return s.mtu.PLPMTU()
}

// PathMTUBlackHoles returns how many path black holes DPLPMTUD has detected (0 when
// probing is disabled) — a path silently shrinking below the discovered PLPMTU.
func (s *Sender) PathMTUBlackHoles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mtu == nil {
		return 0
	}
	return s.mtu.BlackHoles()
}

// Receiver is the UDP receive host. It binds a local address, runs a
// flow.Receiver, returns feedback to the peer, and exposes a blocking Read.
type Receiver struct {
	sub  Substrate
	clk  clock.Clock
	cfg  flow.Config // retained so a re-handshake can rebuild the core for the new session
	mu   sync.Mutex
	flow coreReceiver
	peer net.Addr
	// emitQ hands drained delivery batches to deliverCh in DRAIN ORDER without holding
	// any lock across the blocking channel send: each drainer (recvLoop, tickLoop)
	// appends its batch under mu — the same critical section that drained the core, so
	// queue order is drain order — and a single elected emitter flushes the queue FIFO
	// with NO lock held while it blocks on a slow app. (A dedicated emit mutex held
	// across the send was tried first: with deliverCh full it froze every mu path
	// behind the consumer, and deadlocked an app that calls Stats() from its Read
	// goroutine — mu → emitMu → deliverCh → mu.)
	emitQ     [][][]byte
	emitting  bool
	cs        clockSync // cross-host clock-offset estimate (N4)
	probeTick int       // tick counter pacing the clock-offset probes
	deliverCh chan []byte
	done      chan struct{}
	closeOnce sync.Once

	// Encryption (the handshake responder + the ratcheting receive keying). Cleartext
	// when the embedded openState has a nil sec. Opening happens under mu so the keyer is
	// never raced.
	openState
	// statAccum carries the delivery counters of cores retired by a re-handshake, so Stats()
	// stays monotonic across a core rebuild (added to the live core's stats).
	statAccum flow.ReceiverStats
	// hsReplyAddr is the source of the last (re)handshake we replied to fresh — where a
	// retransmit's cached reply is resent, so it goes to the real sender, never a spoofed source.
	hsReplyAddr net.Addr

	dlmu   sync.Mutex
	readDL time.Time
}

// NewReceiver binds bind (host:port; :0 for an ephemeral port) and starts the
// receive host. A non-nil active sec arms the encryption responder; the handshake
// completes when the sender connects (NewReceiver does not block).
func NewReceiver(bind string, cfg flow.Config, sec *SecurityConfig) (*Receiver, error) {
	sub, err := listenUDP(bind)
	if err != nil {
		return nil, err
	}
	r, err := newReceiver(sub, cfg, clock.NewRealClock(), sec)
	if err != nil {
		sub.Close()
		return nil, err
	}
	return r, nil
}

// NewReceiverOver builds a coded receiver over a caller-provided Substrate instead of binding a UDP
// socket — the receive-side counterpart of NewSenderOver, for running meld over WebTransport, a WASM
// bridge, or any host-owned datagram transport.
func NewReceiverOver(sub Substrate, cfg flow.Config, sec *SecurityConfig) (*Receiver, error) {
	return newReceiver(sub, cfg, clock.NewRealClock(), sec)
}

// newReceiver builds the receive host on sub with a given clock and starts its
// goroutines. The clock seam lets a test inject an offset clock to exercise the
// cross-host handshake (N4) without two machines.
func newReceiver(sub Substrate, cfg flow.Config, clk clock.Clock, sec *SecurityConfig) (*Receiver, error) {
	if err := validateSubstrate(sub); err != nil {
		return nil, err
	}
	if err := sec.validate(cfg.SymbolSize); err != nil {
		return nil, err
	}
	os, err := newOpenState(sec, cfg.Flow)
	if err != nil {
		return nil, err
	}
	r := &Receiver{
		sub:       sub,
		clk:       clk,
		cfg:       cfg,
		flow:      newCoreReceiver(cfg),
		deliverCh: make(chan []byte, 1<<15),
		done:      make(chan struct{}),
		openState: os,
	}
	if usesECNReceive(cfg) {
		_ = setECN(sub) // enable per-datagram TOS reception so the CE codepoint reaches FeedSymbolECN
	}
	go r.recvLoop()
	go r.tickLoop()
	return r, nil
}

// coreNow translates the receiver's local time into the sender's clock frame by the
// estimated offset, so the deterministic core compares sender-stamped deadlines
// correctly cross-host. On loopback / before the first probe the offset is 0.
func (r *Receiver) coreNow() clock.Timestamp { return r.clk.Now().Add(r.cs.offsetMicros()) }

// LocalAddr returns the bound address (useful after binding :0).
func (r *Receiver) LocalAddr() string { return r.sub.LocalAddr().String() }

// Read returns the next in-order delivered media chunk into p, blocking until one
// is available, the read deadline passes, or the receiver is closed.
func (r *Receiver) Read(p []byte) (int, error) {
	var timeout <-chan time.Time
	r.dlmu.Lock()
	dl := r.readDL
	r.dlmu.Unlock()
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		tm := time.NewTimer(d)
		defer tm.Stop()
		timeout = tm.C
	}
	select {
	case c := <-r.deliverCh:
		n := copy(p, c)
		if r.ptPool != nil {
			// Encrypted session: c is a pooled decrypt buffer (openAll). The plaintext has been
			// copied into the caller's p, so the buffer can be recycled for the next decrypt.
			r.ptPool.Put(c)
		}
		return n, nil
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	case <-r.done:
		return 0, io.EOF
	}
}

// SetReadDeadline sets the deadline applied by subsequent Read calls.
func (r *Receiver) SetReadDeadline(t time.Time) error {
	r.dlmu.Lock()
	r.readDL = t
	r.dlmu.Unlock()
	return nil
}

// Stats returns the receiver's delivery counters.
func (r *Receiver) Stats() flow.ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return addStats(r.statAccum, r.flow.Stats())
}

// FrameStats returns the receiver's parse-free media-frame decodability snapshot (WP6).
func (r *Receiver) FrameStats() flow.FrameStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flow.FrameStats()
}

// Close stops the goroutines and closes the substrate.
func (r *Receiver) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return r.sub.Close()
}

func (r *Receiver) recvLoop() {
	buf := make([]byte, recvBufferSize(r.cfg))
	oob := make([]byte, 128) // per-datagram TOS/traffic-class control message (the ECN codepoint)
	for {
		n, addr, ecn, err := readECN(r.sub, buf, oob)
		if err != nil {
			return
		}
		t, e := wire.PeekType(buf[:n])
		if e != nil {
			continue
		}
		switch {
		case wire.IsClockEcho(t):
			// Complete a probe round: T3 is now (receiver clock). On an encrypted flow the
			// echo is authenticated and replay-checked under the lock, so a forged or replayed
			// one cannot skew the offset / deadline frame. The echo does NOT adopt the source as
			// the endpoint: the peer is learned from the handshake and authenticated media, so a
			// replayed echo (one lost before we saw it) cannot redirect the return path.
			t3 := int64(r.clk.Now())
			r.mu.Lock()
			if d, ok := r.ctl.open(buf[:n]); ok {
				if echo, e := wire.DecodeClockEcho(d); e == nil {
					r.cs.observe(echo.T0, echo.T1, echo.T2, t3)
				}
			}
			r.mu.Unlock()
		case wire.IsHandshakeInit(t):
			r.handleHandshakeInit(buf[:n], addr)
		case wire.IsMTUProbe(t):
			// Echo a DPLPMTUD probe back to its source so the sender learns the path passed it.
			// Stateless (no core involvement), authenticated/sealed like the clock echo. The
			// The ACK reports what ACTUALLY arrived on the wire. On encrypted flows the opened
			// body is shorter than buf[:n] by the control trailer, so use n after DecodeMTUProbe
			// validates the nonce/type.
			r.mu.Lock()
			d, ok := r.ctl.open(buf[:n])
			if !ok {
				r.mu.Unlock()
				continue
			}
			nonce, _, e := wire.DecodeMTUProbe(d)
			var ack []byte
			if e == nil {
				ack, ok = r.ctl.seal(wire.EncodeMTUProbeAck(nil, nonce, uint16(n)))
			}
			r.mu.Unlock()
			if e == nil && ok {
				_ = writeDatagram(r.sub, ack, addr)
			}
		case wire.IsSymbol(t):
			r.feedSymbol(buf[:n], addr, ecn)
		}
	}
}

// handleHandshakeInit answers a message 1: a first handshake establishes the session, a
// retransmit re-sends the cached reply (only to the established peer — no reflection), and a
// new handshake from a restarted sender stages a PENDING session that respondInit replies to
// but does not commit; it is promoted later by feedSymbol once a symbol opens under it.
func (r *Receiver) handleHandshakeInit(datagram []byte, addr net.Addr) {
	if r.sec == nil {
		return
	}
	payload, err := wire.DecodeHandshake(datagram)
	if err != nil {
		return
	}
	r.mu.Lock()
	wasEstablished := r.established
	res := r.respondInit(payload, peerID(addr))
	var to net.Addr
	if res.send != nil {
		if res.toPeer {
			// A live-session retransmit answers the current authenticated peer, or — before any
			// data has been authenticated — the source that established the session (remembered
			// below), NEVER this datagram's source. So a lost first reply still reaches the real
			// sender (no deadlock) while a replay from a spoofed source cannot reflect message 2.
			to = r.peer
			if to == nil {
				to = r.hsReplyAddr
			}
		} else {
			to = addr // a fresh establish, a pending re-handshake, or a cookie reply: answer the source
		}
		// Remember the establishing source ONLY on a first-contact establish (established
		// flipped false→true here) — never on a pending re-handshake or a cookie reply, which
		// leave the live session in place. Otherwise a re-handshake / cookie from a different
		// source X would overwrite hsReplyAddr, and the LIVE session's retransmit (its peer not
		// yet learned) would resolve to X — deadlocking the real sender and reflecting message 2.
		if !wasEstablished && r.established {
			r.hsReplyAddr = addr
		}
	}
	r.mu.Unlock()
	if res.send != nil && to != nil {
		_ = writeDatagram(r.sub, res.send, to)
	}
}

// feedSymbol feeds one inbound symbol to the core and emits/acks the result. On an
// encrypted flow it drops symbols until the handshake has established the opener; it also
// promotes a pending re-handshake the instant a symbol authenticates under its keys.
func (r *Receiver) feedSymbol(datagram []byte, addr net.Addr, ecn flow.ECN) {
	r.mu.Lock()
	if r.sec != nil && !r.established {
		r.mu.Unlock()
		return // not yet established — cannot open
	}
	// The wire-symbol decode and the re-handshake trial paths (promotion, the straggler guard)
	// are encryption-only; a cleartext flow skips them so the warm path decodes the symbol just
	// once (the core's FeedSymbol decodes it).
	var sym wire.Symbol
	var sysSym bool
	if r.sec != nil {
		var derr error
		sym, derr = wire.DecodeSymbol(datagram) // decode once; reused by the trial paths below
		sysSym = derr == nil && sym.Kind == wire.Systematic
		now := int64(r.clk.Now())
		// Commit-after-confirm: if a re-handshake is pending and THIS symbol opens under it, the
		// restarted sender is real — promote it and rebuild the core (its ids restart at 0), reset
		// the clock offset for the new frame, fold the retiring core's counters into the total, and
		// arm the straggler-screen window.
		if sysSym && r.tryPromote(sym) {
			r.statAccum = addStats(r.statAccum, r.flow.Stats())
			r.flow = newCoreReceiver(r.cfg)
			r.cs, r.probeTick = clockSync{}, 0 // re-anchor the clock offset; probeTick 0 ⇒ probe next tick
			r.guardUntilMicros = now + guardDurationMicros
		} else if now < r.guardUntilMicros && sysSym && r.staleStraggler(sym) {
			// During the guard window, drop an old-session straggler before it reaches the rebuilt
			// core and poisons an id; everything else passes through.
			r.mu.Unlock()
			return
		}
	}
	r.flow.FeedSymbolECN(r.coreNow(), datagram, ecn) // sender-frame time; ecn is the CE signal for the CC
	out := r.openAll(drainDeliver(r.flow))
	// Adopt the source as the feedback endpoint. A cleartext flow has no auth to gate on. An
	// encrypted flow at BOOTSTRAP (no peer learned yet) binds on either an authenticated
	// systematic (even if it is out of order and not yet deliverable) or the first authentic
	// delivery. Waiting only for delivery can deadlock generation-mode recovery: a lost cursor
	// symbol blocks delivery, feedback has no peer, and reactive repair never starts. Once a peer
	// is set, a ROAM rebinds only when THIS specific systematic opens under the live keys — never
	// on the global delivery signal, which a forged cursor-id symbol from a spoofed source could
	// ride to hijack feedback.
	switch {
	case r.sec == nil:
		r.peer = addr
	case r.peer == nil:
		if len(out) > 0 || (sysSym && r.authenticates(sym)) {
			r.peer = addr
		}
	case !sameAddr(addr, r.peer) && sysSym && r.authenticates(sym):
		r.peer = addr
	}
	peer := r.peer
	fbs := r.ctl.sealBatch(drainSend(r.flow))
	r.enqueueEmit(out) // releases mu; delivers in drain order, no lock held across the send
	r.sendFeedback(fbs, peer)
}

// clockProbeEvery paces the offset probes: one per this many ticks (≈200 ms at the
// 5 ms tick), enough to track drift without flooding. cookieRotateEvery refreshes the
// mac2 cookie secret (and resets the load window) on a coarser cadence (~1 s) — long
// enough that a cookie survives the initiator's retry round trip.
const (
	clockProbeEvery   = 40
	cookieRotateEvery = 200
)

func (r *Receiver) tickLoop() {
	tk := time.NewTicker(tickInterval)
	defer tk.Stop()
	for {
		select {
		case <-r.done:
			return
		case <-tk.C:
			r.mu.Lock()
			r.flow.Tick(r.coreNow())
			out := r.openAll(drainDeliver(r.flow))
			fbs := r.ctl.sealBatch(drainSend(r.flow))
			peer := r.peer
			r.probeTick++
			var probe []byte
			if peer != nil && r.probeTick%clockProbeEvery == 1 {
				if sealed, ok := r.ctl.seal(wire.EncodeClockProbe(nil, wire.ClockProbe{T0: int64(r.clk.Now())})); ok {
					probe = sealed
				}
			}
			if r.probeTick%cookieRotateEvery == 0 {
				r.rotateCookie()
			}
			r.enqueueEmit(out) // releases mu; delivers in drain order, no lock held across the send
			r.sendFeedback(fbs, peer)
			if probe != nil {
				_ = writeDatagram(r.sub, probe, peer)
			}
		}
	}
}

// enqueueEmit queues one drained batch for in-order delivery and, unless another
// drainer already owns the emitter role, flushes the queue itself. Called with mu
// HELD; returns with mu released. Exactly one goroutine flushes at a time, FIFO, so
// delivery order matches drain order; the flusher holds no lock during the blocking
// sends, so a slow app stalls only delivery — ticks, feedback, and Stats() keep
// flowing, and the app may safely call Stats() from its Read goroutine.
func (r *Receiver) enqueueEmit(out [][]byte) {
	if len(out) > 0 {
		r.emitQ = append(r.emitQ, out)
	}
	if r.emitting || len(r.emitQ) == 0 {
		r.mu.Unlock()
		return
	}
	r.emitting = true
	for len(r.emitQ) > 0 {
		batch := r.emitQ[0]
		r.emitQ[0] = nil
		r.emitQ = r.emitQ[1:]
		r.mu.Unlock()
		r.emit(batch)
		r.mu.Lock()
	}
	r.emitQ, r.emitting = nil, false
	r.mu.Unlock()
}

// emit delivers already-opened chunks to the application (opening happens under the lock
// in openAll, so the keyer is never raced).
func (r *Receiver) emit(chunks [][]byte) {
	for _, c := range chunks {
		select {
		case r.deliverCh <- c:
		case <-r.done:
			return
		}
	}
}

// sendFeedback writes already-sealed feedback datagrams (sealed under the lock by the
// caller) to the authenticated peer. A nil peer (not yet learned) drops them.
func (r *Receiver) sendFeedback(sealed [][]byte, peer net.Addr) {
	if peer == nil {
		return
	}
	for _, fb := range sealed {
		_ = writeDatagram(r.sub, fb, peer)
	}
}

// peerID is the source-address identity bound into the anti-amplification cookie: the IP
// only (not the port), so a NAT source-port remap between the cookie reply and the retry
// does not invalidate a legitimate cookie. Return-routability still holds — an off-path
// attacker cannot receive the reply at the victim's IP.
func peerID(addr net.Addr) []byte {
	if ua, ok := addr.(*net.UDPAddr); ok && ua.IP != nil {
		// Canonicalize to the 16-byte form so the same address presented as 4-byte IPv4 on
		// one datagram and 16-byte v4-in-v6 on another (dual-stack sockets) yields the same
		// cookie identity — otherwise a legitimate cookie retry could be rejected under load.
		if ip16 := ua.IP.To16(); ip16 != nil {
			return ip16
		}
	}
	return []byte(addr.String())
}

// sameAddr reports whether two source addresses are the same UDP endpoint (IP + port), used
// to tell a NAT roam from a steady peer without a per-symbol string allocation.
func sameAddr(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == b
	}
	ua, aok := a.(*net.UDPAddr)
	ub, bok := b.(*net.UDPAddr)
	if aok && bok {
		return ua.Port == ub.Port && ua.IP.Equal(ub.IP)
	}
	return a.String() == b.String()
}

// addStats sums two ReceiverStats field-wise, so a host can carry counters across a core
// rebuild (a re-handshake) and keep Stats() monotonic.
func addStats(a, b flow.ReceiverStats) flow.ReceiverStats {
	return flow.ReceiverStats{
		Delivered:  a.Delivered + b.Delivered,
		Lost:       a.Lost + b.Lost,
		Recovered:  a.Recovered + b.Recovered,
		Duplicates: a.Duplicates + b.Duplicates,
		WireLost:   a.WireLost + b.WireLost,
		Rejected:   a.Rejected + b.Rejected,
		Evicted:    a.Evicted + b.Evicted,
	}
}

// delivered is one in-order delivered source symbol drained from the core: its source
// id (the AEAD nonce input) and payload.
type delivered struct {
	id   uint32
	data []byte
}

// drainSend collects all pending transmit datagrams from a flow half.
func drainSend(f interface {
	PollSend() ([]byte, bool)
}) [][]byte {
	var out [][]byte
	for {
		d, ok := f.PollSend()
		if !ok {
			return out
		}
		out = append(out, d)
	}
}

// drainDeliver collects all pending delivered source symbols (id + chunk) from a receiver.
func drainDeliver(r interface {
	PollDeliver() (uint32, []byte, bool)
}) []delivered {
	var out []delivered
	for {
		id, d, ok := r.PollDeliver()
		if !ok {
			return out
		}
		out = append(out, delivered{id, d})
	}
}
