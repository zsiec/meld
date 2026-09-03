package session

import (
	"sync"
	"testing"
	"time"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// dg builds a datagram of n bytes whose first byte tags its sequence (for order checks).
func dg(seq, n int) []byte {
	b := make([]byte, n)
	if n > 0 {
		b[0] = byte(seq)
	}
	return b
}

// step drives a pacerState from t0 for durMicros at dtMicros steps, returning everything
// released and the largest single-step release (the microburst the pacer let through).
func step(p *pacerState, t0, durMicros, dtMicros int64) (out [][]byte, maxStepBytes int64) {
	now := clock.Timestamp(t0)
	for elapsed := int64(0); elapsed <= durMicros; elapsed += dtMicros {
		rel := p.due(now)
		var b int64
		for _, d := range rel {
			b += int64(len(d))
		}
		if b > maxStepBytes {
			maxStepBytes = b
		}
		out = append(out, rel...)
		now = now.Add(dtMicros)
	}
	return out, maxStepBytes
}

// TestPacerState_RateAdherence: a backlog drains at ≈ the configured rate, and no single
// fine step releases more than the burst reservoir (smoothing — the microburst is gone).
func TestPacerState_RateAdherence(t *testing.T) {
	const rate = 2_000_000 // 2 MB/s
	const burstUs = 5_000
	var p pacerState
	p.setRate(rate, burstUs)
	for i := 0; i < 2000; i++ { // 2000 × 1316 ≈ 2.6 MB backlog
		p.enqueue(dg(i, 1316))
	}
	// drain for 1 s at 1 ms steps.
	out, maxStep := step(&p, 0, 1_000_000, 1_000)
	var bytes int64
	for _, d := range out {
		bytes += int64(len(d))
	}
	gotRate := bytes // bytes in ~1 s
	if gotRate < rate*9/10 || gotRate > rate*11/10 {
		t.Errorf("drained %d B in 1s, want ≈%d (±10%%)", gotRate, rate)
	}
	// no step may exceed the burst reservoir plus one MTU of overshoot.
	if cap := int64(rate*burstUs/1_000_000) + 1316; maxStep > cap {
		t.Errorf("microburst: max single-step release %d B exceeds burst+MTU %d B", maxStep, cap)
	}
}

// TestPacerState_OversizedNeverStuck: a single datagram larger than the burst reservoir is
// still released (tokens go negative and pay back), never dropped or stuck — the keyframe
// pay-down property.
func TestPacerState_OversizedNeverStuck(t *testing.T) {
	var p pacerState
	p.setRate(1_000_000, 5_000) // burst = 5000 B
	big := dg(1, 20_000)        // 4× the burst
	p.enqueue(big)
	out, _ := step(&p, 0, 100_000, 1_000) // 100 ms is ample at 1 MB/s
	if len(out) != 1 || len(out[0]) != 20_000 {
		t.Fatalf("oversized datagram not released exactly once: got %d datagrams", len(out))
	}
	if p.queuedBytes != 0 || len(p.queue) != 0 {
		t.Fatalf("queue not empty after draining oversized datagram: %d bytes", p.queuedBytes)
	}
}

// TestPacerState_NeverDropsInOrder: every enqueued datagram is released exactly once, in
// FIFO order, with byte totals conserved (the pacer never drops media).
func TestPacerState_NeverDropsInOrder(t *testing.T) {
	var p pacerState
	p.setRate(5_000_000, 5_000)
	const n = 300
	var inBytes int64
	for i := 0; i < n; i++ {
		d := dg(i, 500+i) // varying sizes
		inBytes += int64(len(d))
		p.enqueue(d)
	}
	out, _ := step(&p, 0, 2_000_000, 500)
	if len(out) != n {
		t.Fatalf("released %d/%d datagrams", len(out), n)
	}
	var outBytes int64
	for _, d := range out {
		outBytes += int64(len(d))
	}
	// sizes are strictly increasing (500+i), so released lengths must be non-decreasing if
	// FIFO order held.
	for i := 1; i < len(out); i++ {
		if len(out[i]) < len(out[i-1]) {
			t.Fatalf("out of order at %d: len %d after %d", i, len(out[i]), len(out[i-1]))
		}
	}
	if outBytes != inBytes {
		t.Fatalf("byte total not conserved: out %d != in %d", outBytes, inBytes)
	}
}

// TestPacerState_SourcePriority proves the central scheduler invariant: later
// source jumps queued recovery, while FIFO order remains stable within each class.
func TestPacerState_SourcePriority(t *testing.T) {
	var p pacerState
	p.setRate(10_000_000, 5_000)
	p.enqueue(dg(10, 510))
	p.enqueueSource(dg(1, 501))
	p.enqueue(dg(11, 511))
	p.enqueueSource(dg(2, 502))

	out, _ := step(&p, 0, 1_000_000, 500)
	if len(out) != 4 {
		t.Fatalf("released %d/4 datagrams", len(out))
	}
	want := []byte{1, 2, 10, 11}
	for i, d := range out {
		if d[0] != want[i] {
			t.Fatalf("release[%d] tag = %d, want %d", i, d[0], want[i])
		}
	}
}

func TestPacerState_SourceThenExactThenCodedPriority(t *testing.T) {
	var p pacerState
	p.setRate(10_000_000, 5_000)
	p.enqueue(dg(10, 510))
	p.enqueueExact(dg(20, 520))
	p.enqueueSource(dg(1, 501))
	p.enqueue(dg(11, 511))
	p.enqueueExact(dg(21, 521))
	p.enqueueSource(dg(2, 502))

	out, _ := step(&p, 0, 1_000_000, 500)
	if len(out) != 6 {
		t.Fatalf("released %d/6 datagrams", len(out))
	}
	want := []byte{1, 2, 20, 21, 10, 11}
	for i, d := range out {
		if d[0] != want[i] {
			t.Fatalf("release[%d] tag = %d, want %d", i, d[0], want[i])
		}
	}
}

func TestPacerState_RepairDebtCannotStallSource(t *testing.T) {
	var p pacerState
	p.setRate(1_000_000, 5_000)
	p.tokens = -20_000
	p.sourceTokens = p.burstBytes
	p.enqueue(dg(10, 510))
	p.enqueueSource(dg(1, 501))

	out := p.due(0)
	if len(out) != 1 || out[0][0] != 1 {
		t.Fatalf("repair debt released %v, want only source tag 1", datagramTags(out))
	}
	if len(p.queue) != 1 || p.queue[0][0] != 10 {
		t.Fatalf("queued tags = %v, want repair tag 10", datagramTags(p.queue))
	}
}

func datagramTags(ds [][]byte) []byte {
	tags := make([]byte, len(ds))
	for i, d := range ds {
		if len(d) > 0 {
			tags[i] = d[0]
		}
	}
	return tags
}

func TestHostPacerRepairAdmissionDoesNotBlockSourceWriter(t *testing.T) {
	hp := &hostPacer{
		limitMicros: 1,
		sig:         make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	hp.cond = sync.NewCond(&hp.mu)
	hp.st.setRate(1, 5_000)
	repair := wire.EncodeSymbol(nil, wire.Symbol{
		Kind: wire.Repair, N: 1, Payload: make([]byte, 256),
	})

	done := make(chan error, 1)
	go func() { done <- hp.putFlow([][]byte{repair, repair}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("put repair: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("repair admission blocked the source-facing writer")
	}

	hp.mu.Lock()
	defer hp.mu.Unlock()
	if hp.st.sourceCount != 0 || len(hp.st.queue) != 2 {
		t.Fatalf("queued source/total = %d/%d, want 0/2", hp.st.sourceCount, len(hp.st.queue))
	}
}

// TestPacerState_Determinism: identical inputs ⇒ identical release schedule (pure, no
// clock read), the property that makes the pacer unit-testable.
func TestPacerState_Determinism(t *testing.T) {
	build := func() *pacerState {
		p := &pacerState{}
		p.setRate(3_000_000, 5_000)
		for i := 0; i < 500; i++ {
			p.enqueue(dg(i, 800+(i%7)*30))
		}
		return p
	}
	a, ma := step(build(), 0, 1_500_000, 700)
	b, mb := step(build(), 0, 1_500_000, 700)
	if len(a) != len(b) || ma != mb {
		t.Fatalf("non-deterministic: len %d/%d maxStep %d/%d", len(a), len(b), ma, mb)
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			t.Fatalf("non-deterministic at %d: %d vs %d", i, len(a[i]), len(b[i]))
		}
	}
}

// TestPacerState_RateCutClampsCredit: cutting the rate shrinks the burst reservoir and
// clamps hoarded credit down immediately, so a stale reservoir cannot defeat the new,
// lower budget.
func TestPacerState_RateCutClampsCredit(t *testing.T) {
	var p pacerState
	p.setRate(10_000_000, 5_000) // burst = 50_000 B
	now := clock.Timestamp(0)
	p.refill(now)              // prime
	p.refill(now.Add(100_000)) // fill 100 ms worth → capped at burst 50_000
	if p.tokens != p.burstBytes {
		t.Fatalf("expected tokens at burst cap %d, got %d", p.burstBytes, p.tokens)
	}
	p.setRate(1_000_000, 5_000) // cut 10×: burst = 5_000 B
	if p.tokens > p.burstBytes {
		t.Fatalf("credit not clamped after rate cut: tokens %d > burst %d", p.tokens, p.burstBytes)
	}
}

// TestPacerState_BurstFloor: a tiny budget still admits an MTU-sized datagram (the burst
// reservoir is floored), so the pacer never deadlocks at low rate.
func TestPacerState_BurstFloor(t *testing.T) {
	var p pacerState
	p.setRate(100, 5_000) // absurdly small rate; burst would be 0 without the floor
	if p.burstBytes < minBurstBytes {
		t.Fatalf("burst not floored: %d < %d", p.burstBytes, minBurstBytes)
	}
	p.enqueue(dg(1, 1316))
	out, _ := step(&p, 0, 200_000_000, 1_000_000) // plenty of (virtual) time
	if len(out) != 1 {
		t.Fatalf("MTU datagram never released at tiny rate: %d", len(out))
	}
}

func TestHostPacerPutBlocksBeforeCrossingQueueLimit(t *testing.T) {
	hp := &hostPacer{
		limitMicros: 1_000_000,
		sig:         make(chan struct{}, 1),
		done:        make(chan struct{}),
	}
	hp.cond = sync.NewCond(&hp.mu)
	hp.st.setRate(1_000, 5_000) // 1000 bytes/sec

	done := make(chan error, 1)
	go func() {
		done <- hp.put([][]byte{dg(1, 500), dg(2, 600)})
	}()

	deadline := time.After(200 * time.Millisecond)
	for {
		hp.mu.Lock()
		queued := len(hp.st.queue)
		hp.mu.Unlock()
		if queued == 1 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("put returned before blocking at the limit: %v", err)
		case <-deadline:
			t.Fatalf("put did not enqueue first datagram before blocking; queued=%d", queued)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	select {
	case err := <-done:
		t.Fatalf("put returned with over-limit second datagram admitted: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	hp.mu.Lock()
	hp.st.drainAll()
	hp.cond.Broadcast()
	hp.mu.Unlock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("put after drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("put did not resume after queue drained")
	}
	hp.mu.Lock()
	defer hp.mu.Unlock()
	if got := len(hp.st.queue); got != 1 {
		t.Fatalf("queued after resume = %d, want 1 second datagram", got)
	}
}

// TestHostPacer_BackpressureAndDrain exercises the goroutine wrapper end to end: a burst
// far over the queue-time limit makes put() block (backpressure), the loop drains it to
// the sink at the budget, every datagram arrives exactly once in order, and flushClose
// empties the tail. Real clock + timers; timing-tolerant assertions.
func TestHostPacer_BackpressureAndDrain(t *testing.T) {
	var mu sync.Mutex
	var got [][]byte
	write := func(d []byte) (int, error) {
		mu.Lock()
		got = append(got, append([]byte(nil), d...))
		mu.Unlock()
		return len(d), nil
	}
	// 8 Mbps = 1 MB/s, 40 ms queue-time limit, 5 ms burst.
	hp := newHostPacer(clock.NewRealClock(), 1_000_000, 5_000, 40_000, write)
	defer hp.stop()

	const n = 400
	in := make([][]byte, n)
	for i := range in {
		in[i] = dg(i, 1316) // ~526 KB total ≈ 526 ms at 1 MB/s ≫ 40 ms limit ⇒ must backpressure
	}

	writeStart := time.Now()
	for _, d := range in {
		if err := hp.put([][]byte{d}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	writeDur := time.Since(writeStart) // time the WRITER spent — backpressure paces it
	hp.flushClose()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != n {
		t.Fatalf("delivered %d/%d datagrams", len(got), n)
	}
	for i, d := range got {
		if d[0] != byte(i) {
			t.Fatalf("out of order at %d: tag %d", i, d[0])
		}
	}
	// 526 KB at 1 MB/s with a 40 ms queue cap: the writer cannot push it all in well under
	// the drain time — backpressure must have held it. (Robust to scheduler jitter: the
	// floor is far below the ~480 ms true drain time, far above an unbackpressured blast.)
	if writeDur < 100*time.Millisecond {
		t.Errorf("write loop took only %v — backpressure not engaged (would be near-instant if unbounded)", writeDur)
	}
	t.Logf("backpressure paced %d datagrams (%d KB) over a %v write loop, 40ms queue limit", n, n*1316/1024, writeDur)
}
