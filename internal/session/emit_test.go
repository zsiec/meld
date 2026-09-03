package session

import (
	"sync"
	"testing"
	"time"
)

// TestEnqueueEmitOrderIsDrainOrder pins the delivery-order contract: batches queued
// by successive drainers reach deliverCh in exactly the order they were enqueued
// (drain order), even when a blocked emitter is mid-flush while later batches queue.
func TestEnqueueEmitOrderIsDrainOrder(t *testing.T) {
	r := &Receiver{deliverCh: make(chan []byte, 1), done: make(chan struct{})}

	// Drainer A: becomes the emitter and blocks on the full channel mid-batch.
	r.deliverCh <- []byte{0} // fill the buffer: the next send blocks
	var emitterDone sync.WaitGroup
	emitterDone.Add(1)
	go func() {
		defer emitterDone.Done()
		r.mu.Lock()
		r.enqueueEmit([][]byte{{1}, {2}})
	}()
	waitForBlockedEmitter(t, r)

	// Drainer B: with the emitter blocked, a second drainer must queue and return
	// immediately — its batch is flushed by A, after A's own.
	done := make(chan struct{})
	go func() {
		r.mu.Lock()
		r.enqueueEmit([][]byte{{3}})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second drainer blocked behind the busy emitter")
	}

	var got []byte
	for i := 0; i < 4; i++ {
		select {
		case c := <-r.deliverCh:
			got = append(got, c[0])
		case <-time.After(2 * time.Second):
			t.Fatalf("delivery stalled after %v", got)
		}
	}
	emitterDone.Wait()
	for i, b := range got {
		if int(b) != i {
			t.Fatalf("delivery order %v, want [0 1 2 3] (drain order)", got)
		}
	}
	if r.emitting || len(r.emitQ) != 0 {
		t.Fatalf("emitter state not reset: emitting=%v queue=%d", r.emitting, len(r.emitQ))
	}
}

// TestEnqueueEmitDoesNotHoldMuAcrossBlockingSend is the deadlock regression: with
// deliverCh full and the emitter blocked mid-send, mu must be free — Stats() (and
// every other mu path: intake, ticks, feedback) must proceed even when the app is
// slow, including the app that calls Stats() from the same goroutine that Reads.
// The pre-fix design (an emit mutex acquired before mu was released and held across
// the send) failed exactly here: mu → emitMu → deliverCh → mu.
func TestEnqueueEmitDoesNotHoldMuAcrossBlockingSend(t *testing.T) {
	r := &Receiver{deliverCh: make(chan []byte), done: make(chan struct{})} // unbuffered: emit blocks immediately

	var emitterDone sync.WaitGroup
	emitterDone.Add(1)
	go func() {
		defer emitterDone.Done()
		r.mu.Lock()
		r.enqueueEmit([][]byte{{1}})
	}()
	waitForBlockedEmitter(t, r)

	// The mu-taking path (what Stats/FrameStats/feedSymbol/tickLoop do) must not block.
	locked := make(chan struct{})
	go func() {
		r.mu.Lock()
		_ = r.peer // representative guarded read; acquiring the lock is the assertion
		r.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("mu held across the blocking deliverCh send: slow consumer freezes the receiver")
	}

	if got := <-r.deliverCh; got[0] != 1 { // unblock and finish the emitter
		t.Fatalf("delivered %v, want [1]", got)
	}
	emitterDone.Wait()
}

// waitForBlockedEmitter waits until the goroutine that called enqueueEmit has taken
// the emitter role and released mu (it is now blocked in the channel send).
func waitForBlockedEmitter(t *testing.T, r *Receiver) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		emitting := r.emitting
		r.mu.Unlock()
		if emitting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("emitter never started")
		}
		time.Sleep(time.Millisecond)
	}
}
