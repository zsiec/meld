package flow

import (
	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// This file is the band-form sliding-window coder (the low-latency profile,
// Config.Sliding). It mirrors the generation Sender/Receiver method set so the host
// drives it identically, but codes over one elastic window with a bounded band
// (code.RepairWindow / code.BandDecoder): repair is continuous and fungible across
// the band, symbols are delivered the instant they decode (no per-generation close),
// and decode is O(band²) per symbol. The same redundancy controller drives it —
// feed-forward repairForTarget at the loss estimate + reactive symbolsForDeficit on
// the window degree-of-freedom deficit.

// SlidingSender is the band-form transmit half. Same methods as the generation
// Sender; selected by Config.Sliding.
type SlidingSender struct {
	cfg          Config
	b            int   // configured MAX band (decode-cost cap); the effective span adapts below it
	interMicros  int64 // EWMA of the inter-write interval (source cadence)
	enc          *code.Encoder
	repairKey    uint16
	credit       float64
	pEst         float64
	burstQ8      int // estimated mean loss-run length, Q8 (from feedback; 256 = i.i.d.)
	rttMicros    int64
	lastWrite    clock.Timestamp
	lastRepair   clock.Timestamp
	lastReactive clock.Timestamp
	deadlines    map[uint32]clock.Timestamp
	sendQ        [][]byte
	stats        SenderStats

	// codeRate memo: the proactive set-point depends only on (effectiveBand, pEst,
	// burstQ8); recomputing the GE-tail search per source symbol made the controller
	// O(symbols) instead of O(feedback) and detonated CPU/alloc under bursts. Cache it and
	// recompute only when an input actually moves.
	crBand    int
	crPEst    float64
	crBurstQ8 int
	crRate    float64
	crValid   bool
}

// RateBudgetBitsPerSec returns the send-rate budget the host pacer should release within.
// The sliding coder has no delay-based controller, so this is the static MaxBitrate
// ceiling (the same value the generation coder falls back to when CC is off).
func (s *SlidingSender) RateBudgetBitsPerSec() int64 { return s.cfg.maxBitrate() }

func NewSlidingSender(cfg Config) *SlidingSender {
	return &SlidingSender{
		cfg:       cfg,
		b:         cfg.codingWindow(),
		enc:       code.NewEncoder(cfg.SymbolSize),
		rttMicros: defaultRTTMicros,
		burstQ8:   burstQ8One,
		deadlines: make(map[uint32]clock.Timestamp),
	}
}

// Band-sizing (deadline-aware). A symbol's whole coding span must arrive before its
// deadline: span_time (effectiveBand × inter-write interval) + one-way propagation
// (~rtt/2) + a guard must fit the buffer, or the trailing edge of the span lands at
// the deadline and the deadline-skip beats recovery (a fixed-wide band is why the
// sliding profile stalled at WAN RTT). So the effective coded span shrinks from the
// configured max as the budget tightens or the RTT grows — small at WAN, full at LAN
// — which the measurement harness confirms is the per-regime sweet spot. The larger
// the span the lower the overhead for a given protection (variance margin shrinks
// with block length), so it rides as wide as the deadline allows.
const (
	bandGuardMicros = 20_000 // slack beyond rtt/2 (≈ one feedback interval)
	minBand         = 8      // floor: a too-narrow band loses coding efficiency
)

// effectiveBand returns the coded span to use now: the configured max band, reduced
// to what fits the deadline budget after one-way propagation and a guard.
func (s *SlidingSender) effectiveBand() int {
	b := s.b
	if s.interMicros <= 0 || s.cfg.BufferMicros <= 0 {
		return b
	}
	budget := s.cfg.BufferMicros - s.rttMicros/2 - bandGuardMicros
	if budget < s.interMicros {
		budget = s.interMicros // at least one symbol of span
	}
	if fit := int(budget / s.interMicros); fit < b {
		b = fit
	}
	if b < minBand {
		b = minBand
	}
	return b
}

// Write emits one source symbol systematic, appends it to the window, and emits
// credit-paced proactive repair over the trailing band.
func (s *SlidingSender) Write(now clock.Timestamp, data []byte) {
	if s.lastWrite != 0 {
		if gap := now.Sub(s.lastWrite); gap > 0 {
			if s.interMicros == 0 {
				s.interMicros = gap
			} else {
				s.interMicros += (gap - s.interMicros) / 8
			}
		}
	}
	s.lastWrite = now
	id := s.enc.Add(data)
	dl := now.Add(s.cfg.BufferMicros)
	s.deadlines[id] = dl
	src, _ := s.enc.Source(id)
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Systematic, WindowBase: id, SrcIndex: id, N: 1, Deadline: int64(dl), Payload: src})
	s.stats.Source++
	for s.credit += s.codeRate(); s.credit >= 1; s.credit-- {
		s.emitRepair(now, false)
	}
}

// WriteUnit satisfies the media-aware write contract; the sliding profile codes one
// continuous fungible band rather than per-generation, so per-unit unequal protection
// is a later refinement — priority is accepted and ignored for now (Write semantics).
func (s *SlidingSender) WriteUnit(now clock.Timestamp, data []byte, _ uint8) {
	s.Write(now, data)
}

// WriteFrame satisfies the media-aware write contract; the sliding profile does not yet
// carry the frame descriptor, so the dependency hints are accepted and ignored.
func (s *SlidingSender) WriteFrame(now clock.Timestamp, data []byte, _ FrameDesc) {
	s.Write(now, data)
}

// Flush protects the tail by emitting enough band-repair to cover its estimated loss.
func (s *SlidingSender) Flush(now clock.Timestamp) {
	n := s.enc.Len()
	if eb := s.effectiveBand(); n > eb {
		n = eb
	}
	if n == 0 {
		return
	}
	r := repairForTarget(n, s.pEst, s.cfg.targetFailure(), maxRepairFactor)
	if floor := s.cfg.repairFloor(n); r < floor {
		r = floor
	}
	for ; r > 0; r-- {
		s.emitRepair(now, false)
	}
}

// FeedFeedback refreshes the loss/RTT estimates, prunes the window to the decoded
// low-edge, and sends reactive band-repair on a degree-of-freedom deficit.
func (s *SlidingSender) FeedFeedback(now clock.Timestamp, fb wire.Feedback) {
	if fb.Flow != s.cfg.Flow {
		return
	}
	s.updateRTT(now, fb)
	s.pEst = float64(fb.LossRate) / 65535
	if s.burstQ8 = int(fb.Burstiness); s.burstQ8 < burstQ8One {
		s.burstQ8 = burstQ8One // mean loss-run length for the burst-aware sizer (N2)
	}
	if fb.DecodedLowEdge > s.enc.Base() {
		for id := s.enc.Base(); id < fb.DecodedLowEdge; id++ {
			delete(s.deadlines, id)
		}
		s.enc.SlideTo(fb.DecodedLowEdge)
	}
	if fb.Deficit > 0 {
		s.reactive(now, int(fb.Deficit))
	}
}

// Tick idle-flushes a band-repair when no source has arrived for a while.
func (s *SlidingSender) Tick(now clock.Timestamp) {
	if s.enc.Len() > 0 && now.Sub(s.lastWrite) >= flushIdleMicros && now.Sub(s.lastRepair) >= flushIdleMicros {
		s.emitRepair(now, false)
	}
}

// PollSend returns the next datagram to transmit, or nil/false.
func (s *SlidingSender) PollSend() ([]byte, bool) {
	if len(s.sendQ) == 0 {
		return nil, false
	}
	d := s.sendQ[0]
	s.sendQ = s.sendQ[1:]
	return d, true
}

// Stats returns a snapshot of what has been emitted.
func (s *SlidingSender) Stats() SenderStats { return s.stats }

func (s *SlidingSender) codeRate() float64 {
	b := s.effectiveBand()
	// The set-point depends only on (effectiveBand, pEst, burstQ8) — delta is constant —
	// so return the memo when none has moved. pEst/burstQ8 change only on feedback and the
	// band only as the source cadence / RTT drift, so the expensive GE-tail search runs once
	// per change instead of once per source symbol (the per-symbol cadence was the sliding
	// profile's CPU/alloc pathology). The cached value is byte-identical to recomputing it.
	if s.crValid && b == s.crBand && s.pEst == s.crPEst && s.burstQ8 == s.crBurstQ8 {
		return s.crRate
	}
	delta := s.cfg.targetFailure()
	r := repairForTarget(b, s.pEst, delta, maxRepairFactor)
	if ge := repairForGE(b, int(s.pEst*1e6), s.burstQ8, delta, maxRepairFactor); ge > r {
		r = ge // burst-aware set-point (N2): size for the GE tail on a bursty channel
	}
	rate := float64(r) / float64(b)
	if rate < s.cfg.Redundancy {
		rate = s.cfg.Redundancy
	}
	s.crBand, s.crPEst, s.crBurstQ8, s.crRate, s.crValid = b, s.pEst, s.burstQ8, rate, true
	return rate
}

func (s *SlidingSender) reactive(now clock.Timestamp, deficit int) {
	if s.enc.Len() == 0 {
		return
	}
	interval := s.rttMicros
	if interval < minReactiveIntervalMicros {
		interval = minReactiveIntervalMicros
	} else if interval > maxReactiveIntervalMicros {
		interval = maxReactiveIntervalMicros
	}
	if s.lastReactive != 0 && now.Sub(s.lastReactive) < interval {
		return
	}
	s.lastReactive = now
	// Size the batch to the loss the in-flight window actually experienced, not the global
	// EWMA (which reports 0 through warmup and lags a burst). Over a band of b source symbols
	// protected at the current code rate, being `deficit` short implies ≈ (b·rate + deficit) of
	// the b·(1+rate) sent were lost — an accurate, lag-free per-window erasure rate.
	p := s.pEst
	if b := float64(s.effectiveBand()); b > 0 {
		proactive := b * s.codeRate()
		if pGen := (proactive + float64(deficit)) / (b + proactive); pGen > p {
			p = pGen
		}
	}
	for i := symbolsForDeficit(deficit, p, s.cfg.targetFailure(), maxRepairFactor); i > 0; i-- {
		s.emitRepair(now, true)
	}
}

func (s *SlidingSender) updateRTT(now clock.Timestamp, fb wire.Feedback) {
	if fb.HighestSeen == 0 {
		return
	}
	if dl, ok := s.deadlines[fb.HighestSeen-1]; ok {
		if sample := now.Sub(dl.Add(-s.cfg.BufferMicros)); sample > 0 {
			s.rttMicros = s.rttMicros - s.rttMicros/8 + sample/8
		}
	}
}

func (s *SlidingSender) emitRepair(now clock.Timestamp, reactive bool) {
	if s.enc.Len() == 0 {
		return
	}
	key := s.repairKey
	s.repairKey++
	base, n, pay := s.enc.RepairWindow(key, s.effectiveBand())
	dl := s.deadlines[base+uint32(n)-1]
	s.emit(wire.Symbol{Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(n), RepairKey: key, Deadline: int64(dl), Payload: pay})
	s.lastRepair = now
	s.stats.Repair++
	if reactive {
		s.stats.ReactiveRepair++
	}
}

func (s *SlidingSender) emit(sym wire.Symbol) {
	s.sendQ = append(s.sendQ, wire.EncodeSymbol(nil, sym))
}

// SlidingReceiver is the band-form receive half. Same methods as the generation
// Receiver; selected by Config.Sliding.
type SlidingReceiver struct {
	cfg        Config
	dec        *code.BandDecoder
	directRecv map[uint32]bool
	lateDrops  uint64
	lastFB     clock.Timestamp
	fedOnce    bool
	deliverQ   []deliveredSym
	sendQ      [][]byte
	stats      ReceiverStats

	// Deadline extrapolation (symbols carry source_time + buffer).
	haveRef    bool
	refID      uint32
	refDL      clock.Timestamp
	intervalUs int64
	refSamples int

	// Channel-erasure-rate estimator.
	lossStarted bool
	lossBase    uint32
	lossHighest uint32
	lossRecv    int
	pEWMA       float64
	pHold       float64

	// Forward-gap walk for the mean loss-run length (N2 burst-aware sizing); mirrors
	// the generation receiver.
	haveExpect  bool
	expectNext  uint32
	meanBurstQ8 uint32
}

func NewSlidingReceiver(cfg Config) *SlidingReceiver {
	return &SlidingReceiver{
		cfg:         cfg,
		dec:         code.NewBandDecoder(cfg.SymbolSize, cfg.codingWindow(), slidingMaxWin),
		directRecv:  make(map[uint32]bool),
		intervalUs:  1,
		meanBurstQ8: burstQ8One,
	}
}

// FeedSymbol decodes and absorbs one inbound symbol datagram, delivering ready
// symbols in order.
func (r *SlidingReceiver) FeedSymbol(now clock.Timestamp, datagram []byte) {
	sym, err := wire.DecodeSymbol(datagram)
	if err != nil || sym.Flow != r.cfg.Flow {
		return
	}
	dl := clampDeadline(now, clock.Timestamp(sym.Deadline), r.cfg.BufferMicros)
	switch sym.Kind {
	case wire.Systematic:
		id := sym.SrcIndex
		if !r.admit(id) {
			r.stats.Rejected++
			return
		}
		// Count the network arrival for the erasure estimate independent of the
		// deadline/cursor — a symbol received late was not dropped by the channel, so
		// counting it as loss would inflate the redundancy controller (see the
		// generation receiver). Each id's systematic arrives once.
		r.observeLoss(id)
		if id < r.dec.Cursor() || r.directRecv[id] {
			r.stats.Duplicates++
		} else {
			r.directRecv[id] = true
		}
		r.updateRef(id, dl)
		r.dec.AddSystematic(id, sym.Payload)
	case wire.Repair:
		n := int(sym.N)
		if n <= 0 || uint64(sym.WindowBase)+uint64(n) > 1<<32 {
			return // non-positive width, or a window that wraps the id space (forged)
		}
		hi := sym.WindowBase + uint32(n) - 1
		if !r.admit(hi) {
			r.stats.Rejected++
			return
		}
		r.updateRef(hi, dl)
		r.dec.AddRepair(sym.WindowBase, n, sym.RepairKey, sym.Payload)
	}
	r.pump(now)
	r.maybeFeedback(now)
}

// admit bounds the frontier jump a single datagram can force on the band decoder. The
// decoder's grow() advances the frontier one id at a time (delivering or skipping each), so a
// forged id far beyond the window would stall it for O(id-cursor) iterations — a one-packet
// receiver hang. This mirrors the generation receiver's admit() horizon: an id within the
// delivery window is cheap; one more than a full window past the frontier can never be
// delivered in-window, so it is refused. coverID is the highest source id the symbol touches.
func (r *SlidingReceiver) admit(coverID uint32) bool {
	if coverID < r.dec.Cursor() {
		return true // duplicate of an already delivered/skipped id — no frontier growth
	}
	if h := r.dec.Highest(); coverID >= h && coverID-h > uint32(slidingMaxWin) {
		return false
	}
	return true
}

// Tick advances time, enforcing deadline skips and periodic feedback.
func (r *SlidingReceiver) Tick(now clock.Timestamp) {
	r.pump(now)
	r.maybeFeedback(now)
}

// PollDeliver returns the next in-order delivered source symbol (id + chunk), or
// 0/nil/false. The id is the host's AEAD nonce input (epoch‖src_index).
func (r *SlidingReceiver) PollDeliver() (uint32, []byte, bool) {
	if len(r.deliverQ) == 0 {
		return 0, nil, false
	}
	d := r.deliverQ[0]
	r.deliverQ = r.deliverQ[1:]
	return d.id, d.data, true
}

// PollSend returns the next feedback datagram, or nil/false.
func (r *SlidingReceiver) PollSend() ([]byte, bool) {
	if len(r.sendQ) == 0 {
		return nil, false
	}
	d := r.sendQ[0]
	r.sendQ = r.sendQ[1:]
	return d, true
}

// Stats returns delivery outcomes.
func (r *SlidingReceiver) Stats() ReceiverStats {
	s := r.stats
	s.Lost = r.dec.Lost() + r.lateDrops
	return s
}

// FrameStats satisfies the media-aware receiver contract; the sliding profile does not
// yet carry the frame descriptor, so it reports no frame-level decodability.
func (r *SlidingReceiver) FrameStats() FrameStats { return FrameStats{} }

func (r *SlidingReceiver) pump(now clock.Timestamp) {
	for {
		r.drainDeliver(now)
		c := r.dec.Cursor()
		if c >= r.dec.Highest() {
			return
		}
		dl, ok := r.deadline(c)
		if !ok || !now.After(dl) {
			return
		}
		if !r.dec.Skip() {
			return
		}
		delete(r.directRecv, c)
	}
}

func (r *SlidingReceiver) drainDeliver(now clock.Timestamp) {
	for {
		rec, ok := r.dec.Deliver()
		if !ok {
			return
		}
		if dl, ok := r.deadline(rec.ID); ok && now.After(dl) {
			r.lateDrops++
			delete(r.directRecv, rec.ID)
			continue
		}
		r.deliverQ = append(r.deliverQ, deliveredSym{rec.ID, append([]byte(nil), rec.Data...)})
		r.stats.Delivered++
		if !r.directRecv[rec.ID] {
			r.stats.Recovered++
		}
		delete(r.directRecv, rec.ID)
	}
}

func (r *SlidingReceiver) maybeFeedback(now clock.Timestamp) {
	if r.fedOnce && now.Sub(r.lastFB) < feedbackIntervalMicros {
		return
	}
	r.fedOnce = true
	r.lastFB = now
	def := r.dec.Deficit()
	if def > 0xFFFF {
		def = 0xFFFF
	}
	r.sendQ = append(r.sendQ, wire.EncodeFeedback(nil, wire.Feedback{
		Flow:           r.cfg.Flow,
		DecodedLowEdge: r.dec.Cursor(),
		HighestSeen:    r.dec.Highest(),
		Deficit:        uint16(def),
		LossRate:       uint16(r.lossEstimate() * 65535),
		Burstiness:     uint16(r.meanBurstQ8),
	}))
}

func (r *SlidingReceiver) updateRef(id uint32, dl clock.Timestamp) {
	if !r.haveRef {
		r.haveRef, r.refID, r.refDL = true, id, dl
		return
	}
	if id > r.refID {
		span := int64(id - r.refID)
		if sample := dl.Sub(r.refDL) / span; sample > 0 {
			r.intervalUs = r.intervalUs - r.intervalUs/4 + sample/4
			if r.intervalUs > maxIntervalMicros {
				r.intervalUs = maxIntervalMicros // bound the extrapolation multiplier (forged stamps)
			}
			r.refSamples++
		}
		r.refID, r.refDL = id, dl
	}
}

func (r *SlidingReceiver) deadline(id uint32) (clock.Timestamp, bool) {
	if !r.haveRef || r.refSamples == 0 {
		return 0, false
	}
	return r.refDL.Add((int64(id) - int64(r.refID)) * r.intervalUs), true
}

func (r *SlidingReceiver) observeLoss(id uint32) {
	// Forward-gap walk: a first-arrival id past the expected one is a loss run of that
	// length; smooth the mean run length in Q8 (signed EWMA, per-run-capped) for the
	// burst-aware sizer (N2). Mirrors the generation receiver's walkGap.
	if !r.haveExpect {
		r.haveExpect, r.expectNext = true, id+1
	} else if id >= r.expectNext {
		if run := id - r.expectNext; run > 0 {
			s := int64(run)
			if s > burstSampleCap {
				s = burstSampleCap
			}
			mb := int64(r.meanBurstQ8) + ((s<<8)-int64(r.meanBurstQ8))>>burstEWMAShift
			if mb < burstQ8One {
				mb = burstQ8One
			}
			r.meanBurstQ8 = uint32(mb)
		}
		r.expectNext = id + 1
	}
	if !r.lossStarted {
		r.lossStarted, r.lossBase, r.lossHighest, r.lossRecv = true, id, id, 1
		return
	}
	if id < r.lossBase {
		return
	}
	if id > r.lossHighest {
		r.lossHighest = id
	}
	r.lossRecv++
	win := lossWindowMin
	if b := r.cfg.codingWindow(); b > win {
		win = b // ~one band; not 8*b, which can exceed a short stream
	}
	span := int(r.lossHighest-r.lossBase) + 1
	if span < win {
		return
	}
	loss := 1 - float64(r.lossRecv)/float64(span)
	if loss < 0 {
		loss = 0
	}
	r.pEWMA += (loss - r.pEWMA) / float64(int(1)<<lossEWMAShift)
	if hold := r.pHold * lossHoldDecay; loss > hold {
		r.pHold = loss
	} else {
		r.pHold = hold
	}
	r.lossBase, r.lossRecv = r.lossHighest+1, 0
}

func (r *SlidingReceiver) lossEstimate() float64 {
	p := r.pEWMA
	if r.pHold > p {
		p = r.pHold
	}
	if p < 0 {
		return 0
	}
	if p > 0.95 {
		return 0.95
	}
	return p
}
