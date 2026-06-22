package session

import (
	"net"
	"sync"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/flow"
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
// read-only to the core's flow.Sender.RateBudgetBitsPerSec() (the Copa/N3 budget, or the
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
// Design rules, each validated by the host-pacer probe (see PACER_FINDINGS in the
// experiment branch):
//   - release at the BUDGET, never the encoder average (pacing at the average stretches a
//     keyframe across its avg-rate airtime, well past the deadline);
//   - the smoothing burst-credit (a few ms) and the queue-time limit (≈ the deadline) are
//     SEPARATE knobs — conflating them lets the bucket hoard a whole queue-window of credit
//     in a quiet gap and dump it on the next keyframe, re-creating the microburst;
//   - the queue-time limit is the DEADLINE minus expected downstream latency, not a
//     WebRTC-style multi-second bound (a datagram draining after its deadline is dead);
//   - never drop media: the queue is FIFO and drains in order; overload is handled by
//     backpressure at Write(), not by dropping queued datagrams.

// minBurstBytes floors the smoothing reservoir so a single MTU-sized datagram always fits
// even at a tiny budget (mirrors the core token bucket's burst floor).
const minBurstBytes = 1 << 12

// pacerState is the PURE, clockless decision core: a leaky bucket over a FIFO of opaque
// datagrams. Time enters only as explicit clock.Timestamp arguments — it never reads a
// clock, so it is unit-testable deterministically. The goroutine wrapper (hostPacer)
// stamps it with the host clock.
type pacerState struct {
	rateBytesPerSec int64 // slaved to the core budget; >0 always (host floors it)
	burstBytes      int64 // token reservoir cap = rate × burstMicros, floored
	tokens          int64 // available credit; may go briefly negative (one over-budget datagram, paid down)
	last            clock.Timestamp
	primed          bool
	queue           [][]byte
	queuedBytes     int64
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
}

// enqueue appends one datagram to the FIFO. The pacer copies nothing — callers hand it
// buffers they no longer mutate (the core allocates a fresh datagram per emit).
func (p *pacerState) enqueue(d []byte) {
	p.queue = append(p.queue, d)
	p.queuedBytes += int64(len(d))
}

// refill advances the bucket to now, capping credit at the burst reservoir.
func (p *pacerState) refill(now clock.Timestamp) {
	if !p.primed {
		p.primed, p.last = true, now
		return
	}
	if dt := now.Sub(p.last); dt > 0 {
		p.tokens += p.rateBytesPerSec * dt / 1_000_000
		if p.tokens > p.burstBytes {
			p.tokens = p.burstBytes
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
	for len(p.queue) > 0 && p.tokens > 0 {
		d := p.queue[0]
		p.queue = p.queue[1:]
		p.queuedBytes -= int64(len(d))
		p.tokens -= int64(len(d))
		out = append(out, d)
	}
	return out
}

// drainAll pops the entire queue regardless of credit — the end-of-stream flush, where
// pacing the tail is pointless and the bytes just need to get out.
func (p *pacerState) drainAll() [][]byte {
	out := p.queue
	p.queue = nil
	p.queuedBytes = 0
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
	if p.tokens > 0 {
		return 0
	}
	// credit must reach > 0; deficit is -tokens, plus one to cross zero.
	need := -p.tokens + 1
	us := need * 1_000_000 / p.rateBytesPerSec
	if us < minPaceSleepMicros {
		us = minPaceSleepMicros
	}
	return us
}

// queueDrainMicros is the time to drain the current backlog at the budget rate — the
// standing queue delay the pacer is about to impose. Write() compares it to the queue-time
// limit to decide backpressure. Conservative (ignores any positive credit).
func (p *pacerState) queueDrainMicros() int64 {
	if p.rateBytesPerSec < 1 {
		return 1 << 62
	}
	return p.queuedBytes * 1_000_000 / p.rateBytesPerSec
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
	hp.st.tokens = hp.st.burstBytes // start full: an initial burst (≤ burst window) goes promptly
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
		hp.st.enqueue(d)
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
// queue already exceeds the queue-time limit it blocks the caller until the backlog drains
// under the limit (or the pacer closes). This is the source budget contract — the only
// place media can be slowed, since the core never refuses it. Returns the first async
// substrate write error, if any.
func (hp *hostPacer) put(datagrams [][]byte) error {
	if len(datagrams) == 0 {
		return hp.err()
	}
	hp.mu.Lock()
	for hp.st.queueDrainMicros() > hp.limitMicros {
		select {
		case <-hp.done:
			hp.mu.Unlock()
			return net.ErrClosed
		default:
		}
		hp.cond.Wait()
	}
	for _, d := range datagrams {
		hp.st.enqueue(d)
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
			if _, err := hp.write(d); err != nil {
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
