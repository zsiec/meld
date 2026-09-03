package flow

import "github.com/zsiec/meld/internal/clock"

// This file is the LTR-resync controller: the sender half of the reference-chain
// recovery loop exercised by ltr_resync_test.go. The burst-frontier failure
// mode is dependency-island death — a burst kills a reference anchor and every frame
// until the next scheduled IDR decodes into garbage — and the repair-ceiling arm
// showed more repair does not fix it: the island is structural. The proven media-side
// answer (WebRTC LTR/RPSI practice) is to resync at P-frame cost: the application
// marks occasional reference frames as long-term-reference candidates
// (FrameDesc.LTR); the receiver reports the newest one that RESOLVED decodable
// (Feedback.NewestDecodableLTR) plus a count of broken anchors
// (Feedback.BrokenAnchors); and when the chain breaks, this controller raises an
// advisory EncoderControl.Resync naming the safe frame the encoder should code its
// next frame against — no IDR, no waiting out the GOP.
//
// Like RecoveryCadenceFrames, the output is ADVISORY: a host/encoder may honor it
// (encode the next frame referencing the named LTR and write it with that
// FrameDesc), and if it cannot, the transport loop continues unchanged. The
// controller is deterministic and sans-I/O: time enters as explicit timestamps.
type resyncController struct {
	haveBroken bool
	lastBroken uint16 // last cumulative BrokenAnchors seen (wrapping uint16 counter)
	safe       uint32 // newest receiver-confirmed decodable LTR (FrameStart+1); 0 = none
	reqStart   uint32 // FrameStart+1 the active request names; 0 = no request
	raisedAt   clock.Timestamp
	holdUntil  clock.Timestamp
}

// observe folds one feedback report into the controller. holdMicros is the
// detection-lag hold-down: after raising a request, no re-raise until it elapses —
// the resync's own fate (it can die in the same burst that caused it) cannot be
// KNOWN sooner than deadline-expiry plus transit plus a feedback interval, so firing
// faster is blind re-spending. The hold-down is load-bearing:
// without it, large recovery frames retrigger-storm during a burst and drown the
// stream they are meant to save. An unhonored request also expires after one hold
// window (the application may not support resync at all).
func (c *resyncController) observe(now clock.Timestamp, brokenAnchors uint16, newestLTR uint32, holdMicros int64) {
	if newestLTR > c.safe {
		c.safe = newestLTR
	}
	if c.reqStart != 0 && now.Sub(c.raisedAt) > holdMicros {
		c.reqStart = 0 // unhonored past its useful life
	}
	// Serial-number arithmetic (RFC 1982 §3.2) on the cumulative wrapping counter: a
	// delta in [1, 2^15) is new damage (a genuine 65535→0 wrap included); a delta at
	// or past 2^15 is a STALE report — feedback rides UDP and can reorder (the AEAD
	// replay window admits in-window out-of-order datagrams), and a naive wrapping
	// difference read a stale lower value as ~65k new broken anchors, spuriously
	// raising a resync and skewing the next real delta. Stale reports are dropped
	// whole: lastBroken only advances forward.
	delta := brokenAnchors - c.lastBroken
	if c.haveBroken && delta >= 1<<15 {
		return // stale/reordered report: older than one already consumed
	}
	first := !c.haveBroken
	c.haveBroken, c.lastBroken = true, brokenAnchors
	if first || delta == 0 {
		return // no NEW chain damage (the first report is a baseline, not an event)
	}
	if c.safe == 0 || now.Before(c.holdUntil) {
		return // nothing safe to resync against, or a resync's fate is still unknown
	}
	c.reqStart = c.safe
	c.raisedAt = now
	c.holdUntil = now.Add(holdMicros)
}

// resyncHoldMicros is the LTR-resync hold-down, shared by both sender profiles: a
// raised resync's fate cannot be known sooner than its deadline expiry (the budget)
// plus transit plus a feedback interval.
func resyncHoldMicros(bufferMicros, rttMicros int64) int64 {
	return bufferMicros + rttMicros + feedbackIntervalMicros
}

// request returns the FrameStart+1 of the LTR an encoder should resync against, or
// 0 when no resync is being asked for.
func (c *resyncController) request() uint32 { return c.reqStart }

// honored clears the active request: the sender observed a written frame that
// references the requested LTR, so the recovery is in flight (the hold-down keeps
// gating re-raises until its fate is knowable).
func (c *resyncController) honored() { c.reqStart = 0 }
