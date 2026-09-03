package flow

import (
	"math"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/code"
	"github.com/zsiec/meld/internal/wire"
)

// Epoch repair groups a bounded run of sources under stable equation geometry.
// Policy selection is kept in recovery_policy.go; this file only integrates the
// selected share with sender credit, coding, and wire emission.

// outboundEpoch is one stable fixed-window epoch inside the sliding flow.
// due counts proactive equations already earned by the ordinary code-rate
// ledger; closing the block changes only their geometry and release time.
type outboundEpoch struct {
	base     uint32
	n        int
	used     int
	due      int
	nextRow  uint16
	share    float64
	deadline clock.Timestamp
}

const (
	outageDiversityMinHold    = 100_000
	outageDiversityHoldCycles = 2
	outageDiversityRTTShare   = 5
	// Delayed equations diversify a subset of the loss-derived repair budget.
	// Keeping most equations immediate preserves prompt rank accumulation while
	// avoiding the named-symbol gamble of substituting exact copies.
	outageDiversityShare      = 0.25
	outageDiversityShortShare = 0.125
	epochBlockSymbols         = 16
)

// noteOutageRun arms delayed-equation diversity after the receiver reports a
// run beyond its measured recovery horizon. The hold spans several feedback
// cycles so recurrent fades remain covered even though only the report containing
// a newly closed run is nonzero. A later report can only extend the hold.
func (s *SlidingSender) noteOutageRun(now clock.Timestamp, fb wire.Feedback) {
	if fb.OutageRun == 0 {
		return
	}
	if !s.disableOutageDiversity {
		wasActive := now.Before(s.outageDiversityUntil)
		if s.outageReportCount < ^uint32(0) {
			s.outageReportCount++
		}
		hold := outageDiversityHoldCycles * s.rttMicros
		if hold < 2*s.cfg.BufferMicros {
			hold = 2 * s.cfg.BufferMicros
		}
		if hold < outageDiversityMinHold {
			hold = outageDiversityMinHold
		}
		until := now.Add(hold)
		if until.After(s.outageDiversityUntil) {
			s.outageDiversityUntil = until
		}
		if !wasActive || uint32(fb.OutageRun) > s.outageRunSyms {
			s.outageRunSyms = uint32(fb.OutageRun)
		}
	}
}

// updateEpochDemand adapts wire feedback to the policy's protocol-independent
// observation model and publishes the resulting state as sender telemetry.
func (s *SlidingSender) updateEpochDemand(fb wire.Feedback, clean, offeredSince bool) {
	if s.disableEpochRepair {
		s.epochPolicy.reset()
		s.publishEpochPolicy()
		return
	}
	s.epochPolicy.observe(epochObservation{
		clean:    clean,
		offered:  offeredSince,
		lossy:    fb.CongestionLoss > 0 || fb.LossRate > 0 || fb.Deficit > 0 || fb.Missing != 0 || fb.SettledLost > 0,
		burstQ8:  int(fb.Burstiness),
		outage:   fb.OutageRun >= epochOutageMinSymbols,
		reactive: s.reactiveReachable(),
	})
	s.publishEpochPolicy()
}

func (s *SlidingSender) publishEpochPolicy() {
	s.stats.EpochDemandQ8 = uint16(s.epochPolicy.demandQ8)
	s.stats.EpochCorrelationQ8 = uint16(s.epochPolicy.correlationQ8)
	s.stats.EpochMemoryQ8 = uint16(s.epochPolicy.memoryQ8)
}

// epochBlockFor opens or advances a stable 16-source block whenever live
// demand assigns some proactive credit to the fixed geometry. The mix is frozen
// for this block, then recomputed at the next boundary. That preserves the wire
// matrix while making policy changes take effect at the earliest safe point.
func (s *SlidingSender) epochBlockFor(now clock.Timestamp, id uint32, deadline clock.Timestamp) *outboundEpoch {
	if s.epoch != nil {
		b := s.epoch
		if id != b.base+uint32(b.used) {
			// A source-id discontinuity cannot share a fixed algebraic block.
			s.epoch = nil
			return nil
		}
		b.used++
		b.deadline = deadline
		return b
	}
	if s.disableEpochRepair || s.epochPolicy.demandQ8 <= 0 || s.interMicros <= 0 ||
		s.effectiveBand() < epochBlockSymbols || !s.epochCostCompetitive() {
		return nil
	}
	if slack := s.cfg.BufferMicros - s.rttMicros/2; slack <= 0 {
		return nil
	}
	fill := int64(epochBlockSymbols-1)*s.interMicros + s.rttMicros/2 + targetedRepairGuardMicros
	if fill > s.cfg.BufferMicros {
		return nil
	}
	share := s.epochPolicy.share(s.reactiveReachable(), s.cfg.BufferMicros-s.rttMicros/2)
	if share <= 0 {
		return nil
	}
	b := &outboundEpoch{base: id, n: epochBlockSymbols, used: 1, share: share, deadline: deadline}
	s.epoch = b
	s.stats.EpochBlocks++
	s.stats.EpochShareQ8 = uint16(math.Round(share * epochDemandOne))
	return b
}

// epochCostCompetitive compares the full algebraic row charge with the exact
// systematic cost the source has recently put on the wire. The source mean is a
// bounded 64-sample observation, so one short tail cannot flip the lane. When a
// row costs more than two source datagrams, cold exploration is not cheap enough;
// after measured burst/outage memory the established three-unit crossover applies.
func (s *SlidingSender) epochCostCompetitive() bool {
	if s.sourceWireBytes <= 0 {
		return false
	}
	rowCharge := int64(repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize))
	multiple := s.epochPolicy.sourceCostMultiple()
	return rowCharge <= multiple*s.sourceWireBytes
}

// allocateEpochCredit spends exactly one already-earned proactive opportunity.
// The fractional accumulator makes the block's selected fixed/sliding mix exact
// over time without creating repair credit or consulting wall-clock timing.
func (s *SlidingSender) allocateEpochCredit(now clock.Timestamp, block *outboundEpoch) {
	s.epochSlidingCredit += 1 - block.share
	if s.epochSlidingCredit >= 1 {
		s.epochSlidingCredit--
		s.emitRepair(now)
		return
	}
	block.due++
}

// closeEpochBlock releases the proactive equations earned while the block was
// filling as distinct Cauchy rows over exactly that block. It never manufactures
// repair credit: due was debited from the same fractional ledger that otherwise
// emits moving-window RLNC.
func (s *SlidingSender) closeEpochBlock(now clock.Timestamp) {
	b := s.epoch
	s.epoch = nil
	if b == nil || b.used != b.n {
		return
	}
	// Unlike continuous sliding repair, a block releases its rows together. Keep
	// that close-time burst below the measured source-headroom ceiling with the
	// controller's standing safety discount, leaving serialization slack for the
	// next source symbols instead of turning algebraic protection into deadline
	// loss through queueing.
	if cap := int(headroomSafety * s.sourceHeadroomRate() * float64(b.n)); b.due > cap {
		b.due = cap
	}
	for ; b.due > 0 && int(b.nextRow) < maxBlockRepair(b.n); b.due-- {
		key := code.BlockRepairKey(b.nextRow)
		base, n, pay := s.enc.RepairAt(key, b.base, b.n)
		if base != b.base || n != b.n {
			return
		}
		if !s.emitEpochRow(now, wire.Symbol{
			Flow: s.cfg.Flow, Kind: wire.Repair, WindowBase: base, SrcIndex: uint32(key), N: uint16(n),
			RepairKey: key, Priority: uepCenterTier, Deadline: int64(b.deadline), SendTimestamp: int64(now), Payload: pay,
		}) {
			return
		}
		b.nextRow++
	}
}

func (s *SlidingSender) emitEpochRow(now clock.Timestamp, sym wire.Symbol) bool {
	sym.SendTimestamp = int64(now)
	if !s.repairAdmissible(now, sym.Priority, clock.Timestamp(sym.Deadline), 0, false) || !s.emit(sym) {
		return false
	}
	s.lastRepair = now
	s.stats.Repair++
	s.stats.RepairProactive++
	s.stats.RepairEpoch++
	return true
}
