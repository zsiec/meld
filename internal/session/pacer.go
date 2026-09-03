package session

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/flow"
	"github.com/zsiec/meld/internal/wire"
)

// Pacer defaults (host-owned; the core's flow.Config carries the optional overrides).
const (
	defaultPaceBurstMicros = 5_000   // 5 ms of smoothing burst credit
	defaultPaceLimitMicros = 150_000 // fallback backpressure limit when no deadline is set
)

// paceBurstMicros is the smoothing-burst window: Config.PaceBurstMicros or the default.
func paceBurstMicros(cfg flow.Config) int64 {
	if cfg.PaceBurstMicros > 0 {
		return cfg.PaceBurstMicros
	}
	return defaultPaceBurstMicros
}

// paceQueueLimitMicros is the backpressure queue-time limit: Config.PaceQueueLimitMicros,
// else 3/4 of the deadline budget (leaving ~1/4 headroom for downstream latency so paced
// datagrams stay inside the deadline), else the fallback.
func paceQueueLimitMicros(cfg flow.Config) int64 {
	if cfg.PaceQueueLimitMicros > 0 {
		return cfg.PaceQueueLimitMicros
	}
	if cfg.BufferMicros > 0 {
		return cfg.BufferMicros * 3 / 4
	}
	return defaultPaceLimitMicros
}

// This file is the host transmit pacer (the "source budget contract" arm). It sits
// between the flow core's drained datagrams and the socket, and does ONE job the core
// cannot: shape WHEN already-decided datagrams hit the wire, and apply backpressure to
// the media source. It is NOT a congestion controller — its release rate is slaved
// read-only to the core's flow.Sender.RateBudgetBitsPerSec() (the delay-control budget, or the
// static ceiling when CC is off). Adding a second, independently-controlled rate here is
// the "two loops fighting over one rate" anti-pattern (docs/substrate.md); the pacer only
// reshapes timing within the budget the core already set.
//
// Why the host and not the core: the sans-I/O core is non-blocking by construction —
// media is non-droppable, so the core's token bucket lets media drive the bucket negative
// rather than refuse it. The core therefore cannot bound the SOURCE rate; only the host,
// at the Write() boundary, can (by blocking the writer). The core's bucket still sheds
// REPAIR by protection tier; the pacer is purely additive over what the core admitted.
//
// Design rules, covered by the host-pacer tests:
//   - release at the BUDGET, never the encoder average (pacing at the average stretches a
//     keyframe across its avg-rate airtime, well past the deadline);
//   - the smoothing burst-credit (a few ms) and the queue-time limit (≈ the deadline) are
//     SEPARATE knobs — conflating them lets the bucket hoard a whole queue-window of credit
//     in a quiet gap and dump it on the next keyframe, re-creating the microburst;
//   - the queue-time limit is the DEADLINE minus expected downstream latency, not a
//     WebRTC-style multi-second bound (a datagram draining after its deadline is dead);
//   - never drop media: source and repair are FIFO within class, source drains first,
//     and overload is handled by backpressure at Write(), not by dropping queued media.

// minBurstBytes floors the smoothing reservoir so a single MTU-sized datagram always fits
// even at a tiny budget (mirrors the core token bucket's burst floor).
const minBurstBytes = 1 << 12

// pacerState is the PURE, clockless decision core: a leaky bucket over a
// source-priority queue of opaque datagrams. FIFO order is preserved within the
// source and recovery classes, but fresh source is inserted ahead of queued repair.
// Time enters only as explicit clock.Timestamp arguments — it never reads a clock,
// so it is unit-testable deterministically. The goroutine wrapper (hostPacer) stamps
// it with the host clock.
type pacerState struct {
	rateBytesPerSec int64 // slaved to the core budget; >0 always (host floors it)
	burstBytes      int64 // token reservoir cap = rate × burstMicros, floored
	tokens          int64 // aggregate source+repair credit
	sourceTokens    int64 // independent source credit: repair debt cannot stall fresh media
	last            clock.Timestamp
	primed          bool
	queue           [][]byte
	queuedBytes     int64
	sourceCount     int   // source datagrams occupy queue[:sourceCount]
	exactCount      int   // exact repairs occupy queue[sourceCount:sourceCount+exactCount]
	sourceBytes     int64 // source bytes ahead of the next source admission
}

// setRate re-slaves the bucket to a new budget (bytes/sec) and recomputes the burst
// reservoir from burstMicros. A rate CUT clamps the token level down immediately so a
// stale reservoir of credit cannot defeat the new, lower budget.
func (p *pacerState) setRate(bytesPerSec, burstMicros int64) {
	if bytesPerSec < 1 {
		bytesPerSec = 1
	}
	p.rateBytesPerSec = bytesPerSec
	burst := bytesPerSec * burstMicros / 1_000_000
	if burst < minBurstBytes {
		burst = minBurstBytes
	}
	p.burstBytes = burst
	if p.tokens > burst {
		p.tokens = burst
	}
	if p.sourceTokens > burst {
		p.sourceTokens = burst
	}
}

// enqueue appends one datagram to the FIFO. The pacer copies nothing — callers hand it
// buffers they no longer mutate (the core allocates a fresh datagram per emit).
func (p *pacerState) enqueue(d []byte) {
	p.queue = append(p.queue, d)
	p.queuedBytes += int64(len(d))
}

// enqueueExact inserts an exact repair behind all source but ahead of fungible
// recovery. FIFO is preserved inside the exact class. It spends the same shared
// tokens as every other repair; this changes deadline order, not rate.
func (p *pacerState) enqueueExact(d []byte) {
	at := p.sourceCount + p.exactCount
	p.queue = append(p.queue, nil)
	copy(p.queue[at+1:], p.queue[at:])
	p.queue[at] = d
	p.exactCount++
	p.queuedBytes += int64(len(d))
}

// enqueueSource inserts fresh source after older source but before all recovery
// traffic. This is the host half of the source-first capacity contract: a repair
// burst can consume spare wire tokens, but cannot create a standing queue in front
// of media that arrives later.
func (p *pacerState) enqueueSource(d []byte) {
	p.queue = append(p.queue, nil)
	copy(p.queue[p.sourceCount+1:], p.queue[p.sourceCount:])
	p.queue[p.sourceCount] = d
	p.sourceCount++
	p.sourceBytes += int64(len(d))
	p.queuedBytes += int64(len(d))
}

// refill advances the bucket to now, capping credit at the burst reservoir.
func (p *pacerState) refill(now clock.Timestamp) {
	if !p.primed {
		p.primed, p.last = true, now
		return
	}
	if dt := now.Sub(p.last); dt > 0 {
		credit := p.rateBytesPerSec * dt / 1_000_000
		p.tokens += credit
		if p.tokens > p.burstBytes {
			p.tokens = p.burstBytes
		}
		p.sourceTokens += credit
		if p.sourceTokens > p.burstBytes {
			p.sourceTokens = p.burstBytes
		}
		p.last = now
	}
}

// due refills to now and pops every datagram the budget currently permits, in FIFO order.
// One over-budget datagram may drive tokens negative (so a keyframe larger than the burst
// is never stuck — it sends and pays the credit back over the next intervals), then the
// loop stops until credit recovers. Returns the datagrams to put on the wire.
func (p *pacerState) due(now clock.Timestamp) [][]byte {
	p.refill(now)
	var out [][]byte
	for len(p.queue) > 0 {
		isSource := p.sourceCount > 0
		if (isSource && p.sourceTokens <= 0) || (!isSource && p.tokens <= 0) {
			break
		}
		d := p.queue[0]
		copy(p.queue[0:], p.queue[1:])
		p.queue = p.queue[:len(p.queue)-1]
		if p.sourceCount > 0 {
			p.sourceCount--
			p.sourceBytes -= int64(len(d))
			p.sourceTokens -= int64(len(d))
		} else if p.exactCount > 0 {
			p.exactCount--
		}
		p.queuedBytes -= int64(len(d))
		p.tokens -= int64(len(d))
		out = append(out, d)
	}
	return out
}

// drainAll pops the entire queue regardless of credit — the end-of-stream flush, where
// pacing the tail is pointless and the bytes just need to get out.
func (p *pacerState) drainAll() [][]byte {
	out := make([][]byte, len(p.queue))
	copy(out, p.queue)
	p.queue = nil
	p.queuedBytes = 0
	p.sourceCount = 0
	p.exactCount = 0
	p.sourceBytes = 0
	return out
}

// untilNextMicros reports how long until the next datagram may be sent: 0 if one is due
// now, a negative value if the queue is empty (sleep until signalled), else the time for
// credit to climb back above zero. The result is the precise wake-up the pacer loop sleeps
// for, so it neither busy-spins nor lags the budget.
func (p *pacerState) untilNextMicros() int64 {
	if len(p.queue) == 0 {
		return -1
	}
	credit := p.tokens
	if p.sourceCount > 0 {
		credit = p.sourceTokens
	}
	if credit > 0 {
		return 0
	}
	// credit must reach > 0; deficit is -tokens, plus one to cross zero.
	need := -credit + 1
	us := need * 1_000_000 / p.rateBytesPerSec
	if us < minPaceSleepMicros {
		us = minPaceSleepMicros
	}
	return us
}

func (p *pacerState) drainMicrosForBytes(bytes int64) int64 {
	if p.rateBytesPerSec < 1 {
		return 1 << 62
	}
	return bytes * 1_000_000 / p.rateBytesPerSec
}

// sourceDrainMicrosAfter is the delay a newly admitted source datagram can see.
// Queued repair is excluded because enqueueSource places the new source ahead of it.
func (p *pacerState) sourceDrainMicrosAfter(d []byte) int64 {
	return p.drainMicrosForBytes(p.sourceBytes + int64(len(d)))
}

func isSystematicDatagram(d []byte) bool {
	t, err := wire.PeekType(d)
	if err != nil {
		return true // fail source-safe: an opaque host datagram must not be starved
	}
	return wire.IsSystematic(t)
}

func isExactRepairDatagram(d []byte) bool {
	t, err := wire.PeekType(d)
	return err == nil && wire.IsUnitRepair(t)
}

// minPaceSleepMicros floors the pacer's wake-up interval so it never busy-spins on a
// nearly-exhausted bucket; coarse enough to be cheap, fine enough to pace 100 Mbps+
// smoothly (≈ a few datagrams per wake at the ceiling).
const minPaceSleepMicros = 250

// hostPacer wraps pacerState with the goroutine machinery the host owns: a release loop
// that sleeps until the next datagram is due (or it is signalled), the socket write, and
// the condition variable Write() blocks on for backpressure. The pure state stays
// clock-free; hostPacer is the only part that touches a real clock and timers.
type hostPacer struct {
	mu          sync.Mutex
	cond        *sync.Cond // broadcast when the backlog drains, to wake blocked Writers
	st          pacerState
	limitMicros int64 // queue-time backpressure threshold (≈ deadline − downstream latency)
	burstMicros int64
	clk         clock.Clock
	write       func([]byte) (int, error) // the substrate write
	sig         chan struct{}             // wakes the loop on enqueue / rate change / flush
	done        chan struct{}
	drained     chan struct{} // closed when a flush has emptied the queue
	flushing    bool
	writeErr    error // first async substrate write error, surfaced to Write/Close
	closeOnce   sync.Once
}

// newHostPacer builds a pacer releasing at the given initial budget, with burst/limit
// knobs, and starts its release loop. write is the substrate sink.
func newHostPacer(clk clock.Clock, initialBudgetBytesPerSec, burstMicros, limitMicros int64, write func([]byte) (int, error)) *hostPacer {
	hp := &hostPacer{
		clk:         clk,
		burstMicros: burstMicros,
		limitMicros: limitMicros,
		write:       write,
		sig:         make(chan struct{}, 1),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
	}
	hp.cond = sync.NewCond(&hp.mu)
	hp.st.setRate(initialBudgetBytesPerSec, burstMicros)
	hp.st.tokens = hp.st.burstBytes       // start full: an initial burst (≤ burst window) goes promptly
	hp.st.sourceTokens = hp.st.burstBytes // source has its own non-repairable reservation
	go hp.loop()
	return hp
}

// offer enqueues datagrams WITHOUT backpressure — for the tick-driven internal path
// (repair / keepalive), which must never block the tick cadence. The core has already
// metered this traffic; the dominant source backpressure is on put() at the Write
// boundary.
func (hp *hostPacer) offer(datagrams [][]byte) {
	if len(datagrams) == 0 {
		return
	}
	hp.mu.Lock()
	for _, d := range datagrams {
		if isSystematicDatagram(d) {
			hp.st.enqueueSource(d)
		} else if isExactRepairDatagram(d) {
			hp.st.enqueueExact(d)
		} else {
			hp.st.enqueue(d)
		}
	}
	hp.mu.Unlock()
	hp.wake()
}

// setRate re-slaves the release rate to a fresh budget (host calls this after feedback /
// on tick). It wakes the loop so a rate increase takes effect immediately.
func (hp *hostPacer) setRate(bytesPerSec int64) {
	hp.mu.Lock()
	hp.st.setRate(bytesPerSec, hp.burstMicros)
	hp.mu.Unlock()
	hp.wake()
}

// put enqueues datagrams for paced transmission, applying backpressure: if the standing
// queue would exceed the queue-time limit it blocks the caller until the backlog drains
// enough to admit the next SOURCE datagram (or the pacer closes). Recovery is
// already bounded by the core and is admitted without blocking the application;
// later source is inserted ahead of it. This is the source budget contract — the
// only place media can be slowed, since the core never refuses it.
// Returns the first async substrate write error, if any.
func (hp *hostPacer) put(datagrams [][]byte) error {
	return hp.putClassified(datagrams, false)
}

// putFlow is the protocol-aware source path. Unlike the generic put helper used
// by pacer tests and non-flow callers, it classifies encoded symbols so recovery
// cannot block or queue ahead of source.
func (hp *hostPacer) putFlow(datagrams [][]byte) error {
	return hp.putClassified(datagrams, true)
}

func (hp *hostPacer) putClassified(datagrams [][]byte, classify bool) error {
	if len(datagrams) == 0 {
		return hp.err()
	}
	hp.mu.Lock()
	for _, d := range datagrams {
		isSource := !classify || isSystematicDatagram(d)
		for isSource && hp.st.sourceCount > 0 && hp.st.sourceDrainMicrosAfter(d) > hp.limitMicros {
			select {
			case <-hp.done:
				hp.mu.Unlock()
				return net.ErrClosed
			default:
			}
			hp.cond.Wait()
		}
		if isSource {
			hp.st.enqueueSource(d)
		} else if isExactRepairDatagram(d) {
			hp.st.enqueueExact(d)
		} else {
			hp.st.enqueue(d)
		}
	}
	hp.mu.Unlock()
	hp.wake()
	return hp.err()
}

// loop releases due datagrams and sleeps until the next is due or it is signalled.
func (hp *hostPacer) loop() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		hp.mu.Lock()
		now := hp.clk.Now()
		var out [][]byte
		if hp.flushing {
			out = hp.st.drainAll()
		} else {
			out = hp.st.due(now)
		}
		wait := hp.st.untilNextMicros()
		flushing := hp.flushing
		hp.cond.Broadcast() // a drain frees backpressure room
		hp.mu.Unlock()

		for _, d := range out {
			n, err := hp.write(d)
			if err == nil && n != len(d) {
				err = io.ErrShortWrite
			}
			if err != nil {
				hp.setErr(err)
				break
			}
		}

		if flushing {
			hp.mu.Lock()
			empty := len(hp.st.queue) == 0
			hp.mu.Unlock()
			if empty {
				close(hp.drained)
				return
			}
			continue // keep draining as fast as the substrate accepts
		}

		select {
		case <-hp.done:
			return
		case <-hp.sig:
			continue // re-evaluate (new data, new rate)
		default:
		}
		if wait < 0 { // queue empty — sleep until signalled
			select {
			case <-hp.done:
				return
			case <-hp.sig:
			}
			continue
		}
		resetTimer(timer, time.Duration(wait)*time.Microsecond)
		select {
		case <-hp.done:
			return
		case <-hp.sig:
		case <-timer.C:
		}
	}
}

// flushClose drains everything queued at full speed (end of stream), then stops the loop.
// Bounded: the queue holds only finite buffered media. Safe to call once.
func (hp *hostPacer) flushClose() {
	hp.mu.Lock()
	hp.flushing = true
	already := false
	select {
	case <-hp.drained:
		already = true
	default:
	}
	hp.mu.Unlock()
	if already {
		return
	}
	hp.wake()
	select {
	case <-hp.drained:
	case <-hp.done:
	}
}

// stop halts the loop without waiting to drain (hard close).
func (hp *hostPacer) stop() {
	hp.closeOnce.Do(func() { close(hp.done) })
	hp.mu.Lock()
	hp.cond.Broadcast() // release any blocked Writers
	hp.mu.Unlock()
}

func (hp *hostPacer) wake() {
	select {
	case hp.sig <- struct{}{}:
	default:
	}
}

func (hp *hostPacer) setErr(err error) {
	hp.mu.Lock()
	if hp.writeErr == nil {
		hp.writeErr = err
	}
	hp.mu.Unlock()
}

func (hp *hostPacer) err() error {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return hp.writeErr
}

// resetTimer safely resets a possibly-fired timer to d.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
