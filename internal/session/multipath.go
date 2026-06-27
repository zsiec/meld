package session

import (
	"bytes"
	"errors"
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

// ErrNoPaths is returned when a multipath host is built without any path addresses.
var ErrNoPaths = errors.New("meld: session: need at least one path")

// ErrPathCountMismatch is returned when the flow path count and socket count disagree.
var ErrPathCountMismatch = errors.New("meld: session: multipath path count mismatch")

// ErrSlidingMultipathUnsupported is returned when the band-form sliding profile is
// requested through the multipath host. Sliding symbols do not stamp PathID today.
var ErrSlidingMultipathUnsupported = errors.New("meld: session: sliding multipath is not implemented")

const feedbackDuplicateWindowMicros = 20_000

// MultipathSender is the multi-socket transmit host: it runs an N-path flow.Sender
// (flow.Config.Paths = N) and routes each emitted symbol to the socket of the path the
// core's scheduler stamped it for (wire.Symbol.PathID). It owns one dialed UDP socket
// per path; everything else mirrors the single-path Sender (it is the same dumb pump,
// just demultiplexing the send queue by path). Feedback and clock echoes arrive on any
// path and feed the one shared core. Encryption (the embedded sealState + the handshake)
// rides path 0; the per-symbol seal is path-agnostic (the core decodes from the union).
type MultipathSender struct {
	subs      []Substrate // one per path, index == PathID
	clk       clock.Clock
	cfg       flow.Config
	mu        sync.Mutex
	flow      coreSender
	pace      *hostPacer
	mtu       []*pmtudState
	mtuNonce  []uint32
	done      chan struct{}
	closeOnce sync.Once

	sealState
	hsInit *crypto.Initiator
	hsMsg1 []byte
	hsDone chan struct{}

	lastFeedback   []byte
	lastFeedbackAt clock.Timestamp
}

// NewMultipathSender dials one UDP socket per remote (remotes[i] is path i) and starts
// the transmit host. cfg.Paths must match len(remotes). A non-nil active sec runs the
// encryption handshake (on path 0) before returning.
func NewMultipathSender(remotes []string, cfg flow.Config, sec *SecurityConfig) (*MultipathSender, error) {
	if cfg.Sliding {
		return nil, ErrSlidingMultipathUnsupported
	}
	if len(remotes) < 1 {
		return nil, ErrNoPaths
	}
	if cfg.Paths != len(remotes) {
		return nil, ErrPathCountMismatch
	}
	if err := sec.validate(cfg.SymbolSize); err != nil {
		return nil, err
	}
	subs := make([]Substrate, 0, len(remotes))
	for _, r := range remotes {
		sub, err := dialUDP(r)
		if err != nil {
			closeConns(subs)
			return nil, err
		}
		subs = append(subs, sub)
	}
	if usesL4SMarking(cfg) {
		for _, sub := range subs {
			_ = setECN(sub) // mark each path's outgoing data ECT(1) (L4S)
		}
	}
	s := &MultipathSender{
		subs: subs, clk: clock.NewRealClock(), cfg: cfg, flow: newCoreSender(cfg), done: make(chan struct{}),
		sealState: newSealState(sec, cfg.SymbolSize, cfg.Flow),
	}
	if cfg.ProbeMTU {
		s.mtu = make([]*pmtudState, len(subs))
		s.mtuNonce = make([]uint32, len(subs))
		for i, sub := range subs {
			s.mtu[i] = newPMTUD(pmtudConfigFromCfg(cfg))
			_ = setDontFragment(sub)
		}
	}
	if cfg.Pace {
		s.pace = newHostPacer(s.clk, s.flow.RateBudgetBitsPerSec()/8,
			paceBurstMicros(cfg), paceQueueLimitMicros(cfg),
			func(d []byte) (int, error) {
				if err := s.writeRoutedDatagram(d); err != nil {
					return 0, err
				}
				return len(d), nil
			})
	}
	if sec.active() {
		s.hsDone = make(chan struct{})
		init, err := crypto.NewInitiator(s.psk, beU32(cfg.Flow), sec.epochSize())
		if err != nil {
			closeConns(subs)
			return nil, err
		}
		msg1, err := init.WriteMessage1()
		if err != nil {
			closeConns(subs)
			return nil, err
		}
		s.hsInit = init
		s.hsMsg1 = wire.EncodeHandshakeInit(nil, msg1)
		s.broadcast(s.hsMsg1) // first attempt on EVERY path; tickLoop retries until established
	}
	for i := range subs {
		go s.recvLoop(i)
	}
	go s.tickLoop()
	if sec.active() {
		select {
		case <-s.hsDone:
		case <-time.After(handshakeTimeout):
			s.Close()
			return nil, errHandshakeTimeout
		}
	}
	return s, nil
}

// Write hands one media chunk to the flow at the base protection tier and transmits the
// resulting symbols, each on the socket of the path the scheduler placed it on. When
// encrypted, the chunk is AEAD-sealed first (path-agnostic).
func (s *MultipathSender) Write(p []byte) (int, error) {
	return s.write(p, func(now clock.Timestamp, data []byte) { s.flow.Write(now, data) })
}

// WriteUnit hands one media chunk carrying a protection tier (unequal protection, WP6).
func (s *MultipathSender) WriteUnit(p []byte, priority uint8) (int, error) {
	return s.write(p, func(now clock.Timestamp, data []byte) { s.flow.WriteUnit(now, data, priority) })
}

// WriteFrame hands one media chunk carrying the full access-unit descriptor (WP6).
func (s *MultipathSender) WriteFrame(p []byte, fd flow.FrameDesc) (int, error) {
	return s.write(p, func(now clock.Timestamp, data []byte) { s.flow.WriteFrame(now, data, fd) })
}

// write seals (when encrypted), hands the chunk to the core via push, and transmits.
func (s *MultipathSender) write(p []byte, push func(clock.Timestamp, []byte)) (int, error) {
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
	push(s.clk.Now(), data)
	out := drainSend(s.flow)
	s.mu.Unlock()
	return len(p), s.sendOut(out)
}

// Flush closes the open generation (end of stream) and transmits its repair.
func (s *MultipathSender) Flush() {
	s.mu.Lock()
	s.flow.Flush(s.clk.Now())
	out := drainSend(s.flow)
	s.mu.Unlock()
	_ = s.sendOut(out)
}

// Stats returns the sender's emission counters.
func (s *MultipathSender) Stats() flow.SenderStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flow.Stats()
}

// EncoderControl returns Meld's current advisory encoder-control request.
func (s *MultipathSender) EncoderControl() flow.EncoderControl {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flow.EncoderControl()
}

// PathMTUs returns each path's discovered PLPMTU in bytes. Entries are 0 when PMTUD is off.
func (s *MultipathSender) PathMTUs() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int, len(s.subs))
	for i, p := range s.mtu {
		if p != nil {
			out[i] = p.PLPMTU()
		}
	}
	return out
}

// PathMTU returns the path-set minimum PLPMTU in bytes, or 0 when PMTUD is off.
func (s *MultipathSender) PathMTU() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return pathSetMin(s.mtu)
}

// PathMTUBlackHoles returns the aggregate number of DPLPMTUD black-hole events.
func (s *MultipathSender) PathMTUBlackHoles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, p := range s.mtu {
		if p != nil {
			total += p.BlackHoles()
		}
	}
	return total
}

// Close flushes the tail, stops the goroutines, and closes every path substrate.
func (s *MultipathSender) Close() error {
	s.Flush()
	s.closeOnce.Do(func() { close(s.done) })
	if s.pace != nil {
		s.pace.flushClose()
		s.pace.stop()
	}
	closeConns(s.subs)
	return nil
}

// broadcast writes a datagram on every path. The handshake rides every path (not just
// path 0) so a path that is dead or lossy at connection time cannot stall establishment —
// whichever path is healthy carries it, and the receiver answers on the path it arrived on.
func (s *MultipathSender) broadcast(msg []byte) {
	for _, sub := range s.subs {
		_ = writeDatagram(sub, msg, nil)
	}
}

func (s *MultipathSender) sendOut(out [][]byte) error {
	if s.pace != nil {
		return s.pace.put(out)
	}
	return s.transmit(out)
}

func (s *MultipathSender) pathForDatagram(d []byte) int {
	if sym, err := wire.DecodeSymbol(d); err == nil && int(sym.PathID) < len(s.subs) {
		return int(sym.PathID)
	}
	return 0
}

func (s *MultipathSender) writeRoutedDatagram(d []byte) error {
	return writeDatagram(s.subs[s.pathForDatagram(d)], d, nil)
}

// transmit routes each datagram to the substrate of its stamped path. A symbol whose
// PathID is out of range (or that fails to decode) falls back to path 0, so a routing
// surprise degrades to single-path rather than dropping media.
func (s *MultipathSender) transmit(out [][]byte) error {
	for _, d := range out {
		if err := s.writeRoutedDatagram(d); err != nil {
			return err
		}
	}
	return nil
}

// recvLoop services one path substrate: it feeds back feedback reports, answers clock
// probes (N4), and completes the encryption handshake (path 0), exactly like the
// single-path host but per path.
func (s *MultipathSender) recvLoop(path int) {
	sub := s.subs[path]
	buf := make([]byte, recvBufferSize(s.cfg))
	for {
		n, _, err := sub.ReadFrom(buf)
		if err != nil {
			return
		}
		t, e := wire.PeekType(buf[:n])
		if e != nil {
			continue
		}
		switch {
		case wire.IsFeedback(t):
			now := s.clk.Now()
			s.mu.Lock()
			if !s.ctl.active && s.duplicateCleartextFeedback(now, buf[:n]) {
				s.mu.Unlock()
				continue
			}
			d, ok := s.ctl.open(buf[:n]) // authenticate + replay-check feedback on an encrypted flow
			if !ok {
				s.mu.Unlock()
				continue
			}
			fb, e := wire.DecodeFeedback(d)
			if e != nil {
				s.mu.Unlock()
				continue
			}
			if !s.ctl.active {
				s.rememberCleartextFeedback(now, buf[:n])
			}
			s.flow.FeedFeedback(now, fb)
			s.mu.Unlock()
		case wire.IsHandshakeResp(t):
			s.handleHandshakeResp(buf[:n])
		case wire.IsHandshakeCookie(t):
			s.handleCookieReply(buf[:n])
		case wire.IsClockProbe(t):
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
				_ = writeDatagram(sub, echo, nil)
			}
		case wire.IsMTUProbeAck(t):
			s.mu.Lock()
			d, ok := s.ctl.open(buf[:n])
			if ok && path < len(s.mtu) && s.mtu[path] != nil {
				if nonce, size, e := wire.DecodeMTUProbeAck(d); e == nil && nonce == s.mtuNonce[path] {
					s.mtu[path].onAck(s.clk.Now(), int(size))
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *MultipathSender) duplicateCleartextFeedback(now clock.Timestamp, d []byte) bool {
	return s.lastFeedbackAt != 0 &&
		now.Sub(s.lastFeedbackAt) < feedbackDuplicateWindowMicros &&
		bytes.Equal(s.lastFeedback, d)
}

func (s *MultipathSender) rememberCleartextFeedback(now clock.Timestamp, d []byte) {
	s.lastFeedback = append(s.lastFeedback[:0], d...)
	s.lastFeedbackAt = now
}

// handleHandshakeResp completes the initiator handshake from message 2 and unblocks the
// constructor; the per-epoch sealer is built on demand by seal().
func (s *MultipathSender) handleHandshakeResp(datagram []byte) {
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

// handleCookieReply answers a responder's under-load cookie by re-arming message 1 (on
// path 0) with the cookie-derived mac2; tickLoop resends it.
func (s *MultipathSender) handleCookieReply(datagram []byte) {
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
	s.broadcast(msg) // resend the cookie-bearing message 1 on every path
}

func (s *MultipathSender) tickLoop() {
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
				s.broadcast(msg) // resend handshake message 1 on every path until established
				continue
			}
			now := s.clk.Now()
			s.flow.Tick(now)
			out := drainSend(s.flow)
			budget := s.flow.RateBudgetBitsPerSec()
			type pathProbe struct {
				path int
				data []byte
			}
			var probes []pathProbe
			for i, mtu := range s.mtu {
				if mtu == nil {
					continue
				}
				if size, send := mtu.tick(now); send {
					s.mtuNonce[i]++
					if p, ok := s.ctl.seal(wire.EncodeMTUProbe(nil, s.mtuNonce[i], mtuProbeBodySize(size, s.ctl))); ok {
						probes = append(probes, pathProbe{path: i, data: p})
					}
				}
			}
			s.mu.Unlock()
			if s.pace != nil {
				s.pace.setRate(budget / 8)
				s.pace.offer(out)
			} else {
				s.transmit(out)
			}
			for _, p := range probes {
				_ = writeDatagram(s.subs[p.path], p.data, nil)
			}
		}
	}
}

// MultipathReceiver is the multi-socket receive host: it binds one UDP socket per path
// and feeds every inbound symbol into the one shared flow.Receiver, which reads the
// path from wire.Symbol.PathID and decodes from the union across paths. Feedback is
// returned on every path's learned peer (idempotent and tiny, so duplicating it keeps
// the control loop alive if a path dies); the clock and encryption handshakes (N4 /
// docs/encryption.md) run on path 0. The per-symbol open (embedded openState) is
// path-agnostic.
type MultipathReceiver struct {
	subs      []Substrate
	clk       clock.Clock
	mu        sync.Mutex
	flow      coreReceiver
	peers     []net.Addr // learned peer per path
	cs        clockSync
	probeTick int
	deliverCh chan []byte
	done      chan struct{}
	closeOnce sync.Once

	cfg flow.Config // retained so a re-handshake can rebuild the core for the new session
	openState
	// statAccum carries retired cores' counters so Stats() stays monotonic across a
	// re-handshake core rebuild.
	statAccum flow.ReceiverStats
	// hsReplyAddr is the source of the last (re)handshake we replied to fresh — where a
	// retransmit's cached reply is resent, so it reaches the real sender, never a spoofed source.
	hsReplyAddr net.Addr

	dlmu   sync.Mutex
	readDL time.Time

	// dropHook, when non-nil, drops an inbound datagram on the given path before it
	// reaches the core — a test seam for injecting per-path / correlated loss over real
	// sockets. Always nil in production.
	dropHook func(path int, b []byte) bool
}

// NewMultipathReceiver binds one socket per bind address (binds[i] is path i; use ":0"
// for an ephemeral port) and starts the receive host.
func NewMultipathReceiver(binds []string, cfg flow.Config, sec *SecurityConfig) (*MultipathReceiver, error) {
	if cfg.Sliding {
		return nil, ErrSlidingMultipathUnsupported
	}
	if cfg.Paths != len(binds) {
		return nil, ErrPathCountMismatch
	}
	subs, err := bindAll(binds)
	if err != nil {
		return nil, err
	}
	r, err := newMultipathReceiver(subs, cfg, clock.NewRealClock(), nil, sec)
	if err != nil {
		closeConns(subs)
		return nil, err
	}
	return r, nil
}

// bindAll resolves and listens on each bind address, returning all substrates or closing
// any opened so far on the first error.
func bindAll(binds []string) ([]Substrate, error) {
	if len(binds) < 1 {
		return nil, ErrNoPaths
	}
	subs := make([]Substrate, 0, len(binds))
	for _, b := range binds {
		sub, err := listenUDP(b)
		if err != nil {
			closeConns(subs)
			return nil, err
		}
		subs = append(subs, sub)
	}
	return subs, nil
}

// newMultipathReceiver builds the host on subs with a given clock (the seam mirrors
// newReceiver, for the cross-host offset test) and an optional inbound drop hook (a
// test seam for injecting per-path loss; nil in production), set BEFORE the goroutines
// start so it is never read concurrently with a write, then starts the goroutines.
func newMultipathReceiver(subs []Substrate, cfg flow.Config, clk clock.Clock, dropHook func(int, []byte) bool, sec *SecurityConfig) (*MultipathReceiver, error) {
	if cfg.Sliding {
		return nil, ErrSlidingMultipathUnsupported
	}
	if len(subs) < 1 {
		return nil, ErrNoPaths
	}
	if cfg.Paths != len(subs) {
		return nil, ErrPathCountMismatch
	}
	for _, sub := range subs {
		if err := validateSubstrate(sub); err != nil {
			return nil, err
		}
	}
	if err := sec.validate(cfg.SymbolSize); err != nil {
		return nil, err
	}
	os, err := newOpenState(sec, cfg.Flow)
	if err != nil {
		return nil, err
	}
	r := &MultipathReceiver{
		subs:      subs,
		clk:       clk,
		cfg:       cfg,
		flow:      newCoreReceiver(cfg),
		peers:     make([]net.Addr, len(subs)),
		deliverCh: make(chan []byte, 1<<15),
		done:      make(chan struct{}),
		dropHook:  dropHook,
		openState: os,
	}
	if usesECNReceive(cfg) {
		for _, sub := range subs {
			_ = setECN(sub) // enable per-datagram TOS reception so CE marks reach FeedSymbolECN
		}
	}
	for i := range subs {
		go r.recvLoop(i)
	}
	go r.tickLoop()
	return r, nil
}

// LocalAddrs returns the bound address of each path substrate (useful after binding :0).
func (r *MultipathReceiver) LocalAddrs() []string {
	addrs := make([]string, len(r.subs))
	for i, s := range r.subs {
		addrs[i] = s.LocalAddr().String()
	}
	return addrs
}

// coreNow translates local time into the sender's frame by the estimated clock offset
// (N4), so the deterministic core compares sender-stamped deadlines correctly.
func (r *MultipathReceiver) coreNow() clock.Timestamp { return r.clk.Now().Add(r.cs.offsetMicros()) }

// Read returns the next in-order delivered media chunk, blocking until one is
// available, the read deadline passes, or the receiver is closed.
func (r *MultipathReceiver) Read(p []byte) (int, error) {
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
			r.ptPool.Put(c) // encrypted session: recycle the decrypt buffer after copying it out
		}
		return n, nil
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	case <-r.done:
		return 0, io.EOF
	}
}

// SetReadDeadline sets the deadline applied by subsequent Read calls.
func (r *MultipathReceiver) SetReadDeadline(t time.Time) error {
	r.dlmu.Lock()
	r.readDL = t
	r.dlmu.Unlock()
	return nil
}

// Stats returns the receiver's delivery counters.
func (r *MultipathReceiver) Stats() flow.ReceiverStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return addStats(r.statAccum, r.flow.Stats())
}

// FrameStats returns the receiver's parse-free media-frame decodability snapshot (WP6).
func (r *MultipathReceiver) FrameStats() flow.FrameStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flow.FrameStats()
}

// Close stops the goroutines and closes every path substrate.
func (r *MultipathReceiver) Close() error {
	r.closeOnce.Do(func() { close(r.done) })
	closeConns(r.subs)
	return nil
}

func (r *MultipathReceiver) recvLoop(path int) {
	sub := r.subs[path]
	buf := make([]byte, recvBufferSize(r.cfg))
	oob := make([]byte, 128) // per-datagram TOS/traffic-class control message (the ECN codepoint)
	for {
		n, addr, ecn, err := readECN(sub, buf, oob)
		if err != nil {
			return
		}
		t, e := wire.PeekType(buf[:n])
		if e != nil {
			continue
		}
		switch {
		case wire.IsClockEcho(t):
			t3 := int64(r.clk.Now())
			r.mu.Lock()
			if d, ok := r.ctl.open(buf[:n]); ok { // authenticate + replay-check the echo
				if echo, e := wire.DecodeClockEcho(d); e == nil {
					r.cs.observe(echo.T0, echo.T1, echo.T2, t3)
				}
			}
			r.mu.Unlock()
		case wire.IsHandshakeInit(t):
			r.handleHandshakeInit(buf[:n], addr, path)
		case wire.IsMTUProbe(t):
			r.mu.Lock()
			d, ok := r.ctl.open(buf[:n])
			nonce, _, e := wire.DecodeMTUProbe(d)
			var ack []byte
			if ok && e == nil {
				ack, ok = r.ctl.seal(wire.EncodeMTUProbeAck(nil, nonce, uint16(n)))
			}
			r.mu.Unlock()
			if ok && e == nil {
				_ = writeDatagram(sub, ack, addr)
			}
		case wire.IsSymbol(t):
			if r.dropHook != nil && r.dropHook(path, buf[:n]) {
				if r.sec == nil { // cleartext test seam: still learn the peer so feedback flows
					r.mu.Lock()
					r.peers[path] = addr
					r.mu.Unlock()
				}
				continue
			}
			r.feedSymbol(buf[:n], addr, path, ecn)
		}
	}
}

// handleHandshakeInit answers a message 1 on the path it arrived on. A first handshake
// establishes; a retransmit of the live session resends the cached reply to an authenticated
// peer (anti-reflection); a restarted sender's new handshake stages a PENDING session that is
// promoted later by feedSymbol once a symbol opens under it (commit-after-confirm).
func (r *MultipathReceiver) handleHandshakeInit(datagram []byte, addr net.Addr, path int) {
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
			// A live-session retransmit answers an authenticated peer, or — before any data —
			// the source that established the session, NEVER this datagram's source. So a lost
			// first reply still reaches the real sender (no deadlock) and a replay from a spoofed
			// source cannot reflect message 2.
			to = firstPeer(r.peers)
			if to == nil {
				to = r.hsReplyAddr
			}
		} else {
			to = addr
		}
		// Remember the establishing source ONLY on a first-contact establish — never on a pending
		// re-handshake or a cookie reply (see the single-path note in session.go): a re-handshake
		// from a different source must not overwrite hsReplyAddr and misdirect the live session's
		// retransmit. One hsReplyAddr is shared across paths, so the establishing source is the
		// right fallback for any path's live-session retransmit.
		if !wasEstablished && r.established {
			r.hsReplyAddr = addr
		}
	}
	r.mu.Unlock()
	if res.send != nil && to != nil {
		_ = writeDatagram(r.subs[path], res.send, to)
	}
}

// feedSymbol feeds one inbound symbol to the shared core and emits/acks the result. On an
// encrypted flow it drops symbols until established, and promotes a pending re-handshake the
// instant a symbol authenticates under its keys.
func (r *MultipathReceiver) feedSymbol(datagram []byte, addr net.Addr, path int, ecn flow.ECN) {
	r.mu.Lock()
	if r.sec != nil && !r.established {
		r.mu.Unlock()
		return
	}
	// The decode and the re-handshake trial paths are encryption-only; a cleartext flow skips
	// them so the warm path decodes the symbol just once (the core's FeedSymbol decodes it).
	var sym wire.Symbol
	var sysSym bool
	if r.sec != nil {
		var derr error
		sym, derr = wire.DecodeSymbol(datagram) // decode once; reused by the trial paths below
		sysSym = derr == nil && sym.Kind == wire.Systematic
		now := int64(r.clk.Now())
		if sysSym && r.tryPromote(sym) { // commit-after-confirm: a restarted sender's first real symbol
			r.statAccum = addStats(r.statAccum, r.flow.Stats())
			r.flow = newCoreReceiver(r.cfg)
			r.cs, r.probeTick = clockSync{}, 0 // re-anchor the clock offset; probeTick 0 ⇒ probe next tick
			r.guardUntilMicros = now + guardDurationMicros
		} else if now < r.guardUntilMicros && sysSym && r.staleStraggler(sym) {
			// During the guard window, drop an old-session straggler before it poisons the rebuilt
			// core; everything else passes through.
			r.mu.Unlock()
			return
		}
	}
	r.flow.FeedSymbolECN(r.coreNow(), datagram, ecn) // PathID is in the symbol; sender-frame time; ecn → CC
	out := r.openAll(drainDeliver(r.flow))
	// Adopt the source as THIS path's feedback endpoint (see the single-path note in session.go):
	// a cleartext flow binds unconditionally; an encrypted flow binds at BOOTSTRAP (no peer on
	// this path yet) on an authenticated systematic even if delivery is still cursor-blocked, or
	// on the first authentic delivery. On a ROAM it rebinds only when THIS systematic opens under
	// the live keys, never on the global delivery signal a spoofed source could ride.
	switch {
	case r.sec == nil:
		r.peers[path] = addr
	case r.peers[path] == nil:
		if len(out) > 0 || (sysSym && r.authenticates(sym)) {
			r.peers[path] = addr
		}
	case !sameAddr(addr, r.peers[path]) && sysSym && r.authenticates(sym):
		r.peers[path] = addr
	}
	peers := append([]net.Addr(nil), r.peers...)
	fbs := r.ctl.sealBatch(drainSend(r.flow))
	r.mu.Unlock()
	r.emit(out)
	r.sendFeedback(fbs, peers)
}

func (r *MultipathReceiver) tickLoop() {
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
			peers := append([]net.Addr(nil), r.peers...)
			r.probeTick++
			// Probe the FIRST path with a known peer (not hard-pinned to path 0): if path 0 is
			// dead at startup but another path established, clock sync still begins.
			probePath := -1
			for i, p := range peers {
				if p != nil {
					probePath = i
					break
				}
			}
			var probe []byte
			if probePath >= 0 && r.probeTick%clockProbeEvery == 1 {
				if sealed, ok := r.ctl.seal(wire.EncodeClockProbe(nil, wire.ClockProbe{T0: int64(r.clk.Now())})); ok {
					probe = sealed
				}
			}
			if r.probeTick%cookieRotateEvery == 0 {
				r.rotateCookie()
			}
			r.mu.Unlock()
			r.emit(out)
			r.sendFeedback(fbs, peers)
			if probe != nil {
				_ = writeDatagram(r.subs[probePath], probe, peers[probePath])
			}
		}
	}
}

// emit delivers already-opened chunks to the application (opening happens under the lock
// in openAll, so the keyer is never raced).
func (r *MultipathReceiver) emit(chunks [][]byte) {
	for _, c := range chunks {
		select {
		case r.deliverCh <- c:
		case <-r.done:
			return
		}
	}
}

// firstPeer returns the first known (authenticated) peer across the paths, or nil if none —
// any of them reaches the real sender, so a live-session retransmit reply is never swallowed
// by a nil per-path peer (and never reflected to a spoofable source).
func firstPeer(peers []net.Addr) net.Addr {
	for _, p := range peers {
		if p != nil {
			return p
		}
	}
	return nil
}

// sendFeedback writes each already-sealed feedback datagram (sealed under the lock by the
// caller) on every path whose peer is known, so a single dead path does not stall the
// control loop (feedback is idempotent state).
func (r *MultipathReceiver) sendFeedback(sealed [][]byte, peers []net.Addr) {
	for _, fb := range sealed {
		for i, peer := range peers {
			if peer != nil {
				_ = writeDatagram(r.subs[i], fb, peer)
			}
		}
	}
}
