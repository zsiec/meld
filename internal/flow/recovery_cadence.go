package flow

import "github.com/zsiec/meld/internal/wire"

// EncoderControl is Meld's advisory source-control output. It is intentionally
// side-effect free: a host/encoder may apply it, and if it cannot, the transport
// loop continues unchanged.
type EncoderControl struct {
	// TargetBitrateBps asks the encoder to keep its source payload at or below this
	// bitrate so the bounded recovery allowance fits inside the live total-rate
	// budget. Zero means no active reduction request.
	TargetBitrateBps int64
	// RecoveryCadenceFrames asks the encoder to bound the distance between recovery
	// points, in displayed frames. 0 means no active request. An encoder can satisfy
	// this with keyframes, recovery-point SEI, or intra-refresh, depending on codec.
	RecoveryCadenceFrames uint16
	// Resync asks the encoder to code its next frame referencing ResyncRefFrameID —
	// a long-term-reference frame (one the application marked FrameDesc.LTR) that
	// the receiver has CONFIRMED decodable — because the live reference chain is
	// broken. Honoring it resurrects the stream at P-frame cost instead of waiting
	// for the next scheduled IDR (the LTR-resync mechanism; see the resyncController
	// doc). An encoder that no longer retains that LTR simply ignores the request.
	Resync           bool
	ResyncRefFrameID uint32
}

const (
	recoveryCadenceSoftFrames uint16 = 3
	recoveryCadenceHardFrames uint16 = 2

	recoveryCadenceBurstTriggerQ8 = 24 * 256
	recoveryCadenceHardBurstQ8    = 40 * 256
	recoveryCadenceActivateScore  = 2
	recoveryCadenceRelaxReports   = 12
	recoveryCadenceMaxScore       = 8
)

type recoveryCadenceController struct {
	control EncoderControl

	haveMedia bool
	last      FrameStats

	damageScore int
	quiet       int
}

func (c *recoveryCadenceController) observeFeedback(fb wire.Feedback) {
	fs := FrameStats{
		Frames:             uint64(fb.Frames),
		DecodableFrames:    uint64(fb.DecodableFrames),
		Keyframes:          uint64(fb.Keyframes),
		DecodableKeyframes: uint64(fb.DecodableKeyframes),
	}
	if fs.Frames == 0 && fs.Keyframes == 0 && !c.haveMedia {
		return
	}
	if !c.haveMedia {
		c.haveMedia = true
		c.last = fs
		return
	}

	frames := counterDelta(fs.Frames, c.last.Frames)
	decFrames := counterDelta(fs.DecodableFrames, c.last.DecodableFrames)
	keys := counterDelta(fs.Keyframes, c.last.Keyframes)
	decKeys := counterDelta(fs.DecodableKeyframes, c.last.DecodableKeyframes)
	c.last = fs

	frameDamage := frames - minUint64(frames, decFrames)
	keyDamage := keys - minUint64(keys, decKeys)
	burst := int(fb.Burstiness)
	if burst < burstQ8One {
		burst = burstQ8One
	}
	longBurst := burst >= recoveryCadenceBurstTriggerQ8
	damagingLongBurst := longBurst && (frameDamage > 0 || keyDamage > 0)

	if damagingLongBurst {
		c.quiet = 0
		c.damageScore += int(frameDamage) + 1
		if keyDamage > 0 || burst >= recoveryCadenceHardBurstQ8 {
			c.damageScore += 2
		}
		if c.damageScore > recoveryCadenceMaxScore {
			c.damageScore = recoveryCadenceMaxScore
		}
		if c.damageScore >= recoveryCadenceActivateScore {
			if keyDamage > 0 || frameDamage >= 2 || burst >= recoveryCadenceHardBurstQ8 || c.damageScore >= recoveryCadenceActivateScore*2 {
				c.control.RecoveryCadenceFrames = recoveryCadenceHardFrames
			} else if c.control.RecoveryCadenceFrames == 0 || c.control.RecoveryCadenceFrames > recoveryCadenceSoftFrames {
				c.control.RecoveryCadenceFrames = recoveryCadenceSoftFrames
			}
		}
		return
	}

	if c.control.RecoveryCadenceFrames == 0 {
		if c.damageScore > 0 {
			c.damageScore--
		}
		return
	}
	if frames == 0 && fb.LossRate != 0 {
		return
	}
	c.quiet++
	if c.quiet < recoveryCadenceRelaxReports {
		return
	}
	c.quiet = 0
	c.damageScore = 0
	if c.control.RecoveryCadenceFrames <= recoveryCadenceHardFrames {
		c.control.RecoveryCadenceFrames = recoveryCadenceSoftFrames
		return
	}
	c.control.RecoveryCadenceFrames = 0
}

func (c *recoveryCadenceController) encoderControl() EncoderControl {
	return c.control
}

func counterDelta(cur, prev uint64) uint64 {
	if cur >= prev {
		return cur - prev
	}
	return cur
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
