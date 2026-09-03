package flow

import (
	"encoding/binary"
	"math/bits"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// Targeted repair handles receiver-identified source gaps. It is kept apart
// from continuous coding so exact-copy policy and wire accounting can evolve
// without coupling them to the moving-window encoder.

const (
	exactOffloadResidualMin   = 32
	exactOffloadHoldCycles    = 2
	exactCrossoverHeadroomMin = 0.25
	closureWordBits           = 64
	closureContinuationWords  = len(wire.Feedback{}.Deficits) / 8
	closureRangeMagic0        = 0xff
	closureRangeMagic1        = 0x43
	closureRangeMax           = 6
	closureRangeMarker        = 0x80
)

type closureRange struct {
	start uint16
	len   uint16
}

// setClosureExtensions stores sliding closure data in the fixed 32-byte field
// generation mode uses for per-generation deficits. Four raw bitmap words cover
// the common 320-value neighborhood (including Missing). When a deep residual is
// run-like, six compact offset/length pairs can instead cover the entire 2048-id
// receive window. Neither representation grows the feedback datagram.
func setClosureExtensions(fb *wire.Feedback, masks []uint64) {
	clear(fb.Deficits[:])
	bitmapCount := 0
	for i := 0; i < closureContinuationWords && i < len(masks); i++ {
		binary.BigEndian.PutUint64(fb.Deficits[i*8:(i+1)*8], masks[i])
		bitmapCount += bits.OnesCount64(masks[i])
	}

	var ranges []closureRange
	total := 0
	for i, mask := range masks {
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			start := uint32((i+1)*closureWordBits + bit)
			run := uint32(1)
			mask &^= uint64(1) << bit
			for start+run < uint32((i+2)*closureWordBits) &&
				mask&(uint64(1)<<(bit+int(run))) != 0 {
				mask &^= uint64(1) << (bit + int(run))
				run++
			}
			if n := len(ranges); n > 0 && uint32(ranges[n-1].start)+uint32(ranges[n-1].len) == start {
				ranges[n-1].len += uint16(run)
			} else {
				if start > uint32(^uint16(0)) || run > uint32(^uint16(0)) {
					continue
				}
				ranges = append(ranges, closureRange{uint16(start), uint16(run)})
			}
			total += int(run)
		}
	}
	if len(ranges) == 0 || len(ranges) > closureRangeMax || total <= bitmapCount {
		// Escape the range signature only in the vanishingly rare bitmap that
		// matches it byte-for-byte; ordinary bitmaps retain all 256 continuation
		// bits.
		if _, collision := closureRanges(*fb); collision {
			fb.Deficits[len(fb.Deficits)-1] &^= closureRangeMarker
		}
		return
	}
	clear(fb.Deficits[:])
	fb.Deficits[0], fb.Deficits[1], fb.Deficits[2] = closureRangeMagic0, closureRangeMagic1, byte(len(ranges))
	for i, r := range ranges {
		off := 4 + i*4
		binary.BigEndian.PutUint16(fb.Deficits[off:off+2], r.start)
		binary.BigEndian.PutUint16(fb.Deficits[off+2:off+4], r.len)
	}
	fb.Deficits[len(fb.Deficits)-1] = closureRangeMarker
}

func closureRanges(fb wire.Feedback) ([]closureRange, bool) {
	if fb.Deficits[len(fb.Deficits)-1]&closureRangeMarker == 0 ||
		fb.Deficits[0] != closureRangeMagic0 || fb.Deficits[1] != closureRangeMagic1 ||
		fb.Deficits[3] != 0 || fb.Deficits[2] == 0 || fb.Deficits[2] > closureRangeMax {
		return nil, false
	}
	ranges := make([]closureRange, 0, int(fb.Deficits[2]))
	var end uint32 = closureWordBits
	for i := 0; i < int(fb.Deficits[2]); i++ {
		off := 4 + i*4
		start := binary.BigEndian.Uint16(fb.Deficits[off : off+2])
		n := binary.BigEndian.Uint16(fb.Deficits[off+2 : off+4])
		if n == 0 || uint32(start) < end || uint32(start)+uint32(n) > slidingMaxWin {
			return nil, false
		}
		ranges = append(ranges, closureRange{start, n})
		end = uint32(start) + uint32(n)
	}
	return ranges, true
}

func closureIDs(fb wire.Feedback) []uint32 {
	ids := make([]uint32, 0, int(fb.Deficit))
	appendID := func(off uint64) {
		id := uint64(fb.DecodedLowEdge) + off
		if id <= uint64(^uint32(0)) {
			ids = append(ids, uint32(id))
		}
	}
	mask := fb.Missing
	for mask != 0 {
		bit := bits.TrailingZeros64(mask)
		appendID(uint64(bit))
		mask &= mask - 1
	}
	if ranges, ok := closureRanges(fb); ok {
		for _, r := range ranges {
			for off, end := uint32(r.start), uint32(r.start)+uint32(r.len); off < end; off++ {
				appendID(uint64(off))
			}
		}
		return ids
	}
	for word := 1; word <= closureContinuationWords; word++ {
		off := (word - 1) * 8
		mask := binary.BigEndian.Uint64(fb.Deficits[off : off+8])
		for mask != 0 {
			bit := bits.TrailingZeros64(mask)
			appendID(uint64(word*closureWordBits + bit))
			mask &= mask - 1
		}
	}
	return ids
}

// noteExactOffload records direct evidence that a wide unresolved neighborhood
// survived proactive coding. The closure map is the relevant signal here, rather
// than a path-wide average: it identifies precisely when overlapping equations
// failed together and targeted values have higher marginal utility. The short
// hold lets that evidence influence the remainder of the fade without permanently
// moving moderate or independent loss away from proactive protection.
func (s *SlidingSender) noteExactOffload(now clock.Timestamp, fb wire.Feedback) {
	if fb.Deficit == 0 || len(closureIDs(fb)) < exactOffloadResidualMin {
		return
	}
	hold := int64(exactOffloadHoldCycles) * reactiveCycleMicros(s.rttMicros)
	if hold <= 0 {
		return
	}
	if until := now.Add(hold); until.After(s.exactOffloadUntil) {
		s.exactOffloadUntil = until
	}
}

// closureEquationExpensive reports whether one compact/full equation over the
// stuck band costs more than three times the mean compact unit named by Missing.
// It derives the guaranteed equation extent from retained source content and unit
// sizes from exact lengths, so a zero-filled tail cannot bias the decision.
func (s *SlidingSender) closureEquationExpensive(fb wire.Feedback) bool {
	band := s.effectiveBand()
	base := fb.DecodedLowEdge
	if base < s.enc.Base() {
		base = s.enc.Base()
	}
	end := uint64(base) + uint64(band)
	encEnd := uint64(s.enc.Base()) + uint64(s.enc.Len())
	if end > encEnd {
		end = encEnd
	}
	maxExtent := 0
	for id := uint64(base); id < end; id++ {
		src, ok := s.enc.Source(uint32(id))
		if !ok {
			continue
		}
		payload, _, _, valid := parseCodedSource(src, s.cfg.SymbolSize)
		if !valid {
			continue
		}
		extent := len(payload)
		for extent > 0 && payload[extent-1] == 0 {
			extent--
		}
		if extent > maxExtent {
			maxExtent = extent
		}
	}
	equationBytes := repairWireBaseBytes + codedSymbolSize(s.cfg.SymbolSize)
	if s.cfg.SymbolSize-maxExtent > 4 {
		equationBytes = repairWireBaseBytes + 4 + maxExtent + codedSymbolMetadataBytes
	}
	unitBytes, units := 0, 0
	for _, id := range closureIDs(fb) {
		src, ok := s.enc.Source(id)
		if !ok {
			continue
		}
		_, n, _, valid := parseCodedSource(src, s.cfg.SymbolSize)
		if !valid {
			continue
		}
		unitBytes += repairWireBaseBytes + 4 + n
		units++
	}
	return units > 0 && equationBytes*units > 3*unitBytes
}

// answerMissing answers the receiver's stuck-neighborhood NACK bitmap
// (wire.Feedback.Missing) with UNIT repairs — base=id, n=1, the retained source
// itself — and reports how many rank deficits it has answered, either with a unit
// sent now or one still in flight. A unit vector closes at the decoder the
// moment it arrives, without waiting for full-rank closure of a coupled span.
// Gates: the source's own deadline, the shared wire-loss evidence budget, a
// per-id dedup within one honest cycle
// (a unit in flight is not re-sent on every report), retention clipping, and the
// provably-dead deadline arithmetic.
//
// Automatic mode normally waits for one persistent missing report before units
// fire, giving coded recovery already on the wire one opportunity to close an
// isolated residual. A clustered residual crosses at its last useful first-report
// opportunity, but only when measured source headroom can fund named recovery
// without starving the fungible lane. Deadline admission remains per source.
func (s *SlidingSender) answerMissing(now clock.Timestamp, fb wire.Feedback) int {
	// The closure map identifies free columns in the receiver's reduced system; each
	// exact value removes one independent degree of freedom. Deficit remains the hard
	// cap for malformed, stale, or compactly truncated feedback.
	need := int(fb.Deficit)
	ids := closureIDs(fb)
	if len(ids) == 0 || need <= 0 {
		return 0
	}
	goal := need
	if s.unitSentAt == nil {
		s.unitSentAt = make(map[uint32]clock.Timestamp)
	}
	for id := range s.unitSentAt {
		if id < fb.DecodedLowEdge {
			delete(s.unitSentAt, id) // delivered or dead history; keep the map bounded
		}
	}
	if s.missingSince == nil {
		s.missingSince = make(map[uint32]clock.Timestamp)
	}
	for id := range s.missingSince {
		if id < fb.DecodedLowEdge {
			delete(s.missingSince, id)
		}
	}
	cycle := reactiveCycleMicros(s.rttMicros)
	// Inventory the whole bitmap before sending. An in-flight unit at a later bit
	// still contributes one of the needed degrees of freedom; stopping the scan at
	// an earlier bit would otherwise send a redundant exact value.
	handled := 0
	for _, id := range ids {
		if at, ok := s.unitSentAt[id]; ok && now.Sub(at) < cycle {
			handled++
		}
	}
	if handled > goal {
		handled = goal
	}
	if s.wireLossBudget <= 0 {
		return handled
	}
	if s.disableExactRepair {
		return handled
	}
	eager := s.exactOffloadOn()
	bulk := len(ids) > 4
	exactClosure := false
	if !eager {
		exactClosure = s.exactClosureReachable(fb)
	}
	for _, id := range ids {
		if s.wireLossBudget <= 0 || handled >= goal {
			break
		}
		if at, ok := s.unitSentAt[id]; ok && now.Sub(at) < cycle {
			continue // already included by the inventory pass above
		}
		// Last-useful exact crossover is useful when loss is clustered. On a measured
		// memoryless channel, the ordinary coded response already closes isolated
		// holes and eager units only add packet headers in full-delivery cells. A
		// bulk closure is itself direct burst evidence, including during warmup.
		clustered := bulk || s.burstQ8 >= burstBandThresholdQ8
		crossover := !s.disableExactCrossover && clustered &&
			s.sourceHeadroomRate() >= exactCrossoverHeadroomMin &&
			s.exactLastUsefulDispatch(now, id)
		if !eager && !exactClosure && !crossover {
			if bulk {
				// Away from the deadline edge, a bulk hole remains cheaper as coded
				// deficit repair when closure slack/economy did not qualify it.
				continue
			}
			// Default automatic mode waits for persistence. A second report,
			// separated by the event-feedback floor, proves earlier equations did not
			// close this hole; only then spend an exact unit. The explicit benchmark
			// override retains its immediate behavior for controlled comparisons.
			first, seen := s.missingSince[id]
			if !seen {
				s.missingSince[id] = now
				continue
			}
			if now.Sub(first) < eventFeedbackMinMicros {
				continue
			}
		}
		if !s.emitUnitRepair(now, id) {
			continue // outside retention, or provably past its deadline
		}
		s.unitSentAt[id] = now
		s.wireLossBudget--
		handled++
	}
	return handled
}

// exactLastUsefulDispatch reports whether an exact value can still arrive for id
// now, but waiting for the receiver's next ordinary feedback opportunity would
// make it too late. The final emitter retains the hard now+OWD deadline check.
func (s *SlidingSender) exactLastUsefulDispatch(now clock.Timestamp, id uint32) bool {
	dl, ok := s.deadlines[id]
	if !ok || dl == 0 {
		return false
	}
	travel := s.rttMicros/2 + targetedRepairGuardMicros
	if travel < 0 || now.Add(travel).After(dl) {
		return false
	}
	return now.Add(feedbackIntervalMicros + travel).After(dl)
}

// exactClosureReachable reports whether a deadline has enough room for a unit
// closure round after the proactive FEC already observed at the receiver. Once
// feedback names the residual, a compact unit is exact and cheaper
// than another equation on an i.i.d. path. On a burst path it also qualifies when
// retained exact lengths prove an equation costs over three unit packets. The
// rank-deficit cap in answerMissing preserves the receiver's existing fungible
// rank, so short feedback horizons no longer need a blanket exclusion merely to
// prevent the named path from replacing the whole unresolved system. Expressing
// the gates in measured path/source terms avoids a user-facing mode switch.
const bulkExactHeadroomMax = 0.5

func (s *SlidingSender) exactClosureReachable(fb wire.Feedback) bool {
	cycle := reactiveCycleMicros(s.rttMicros)
	if cycle <= 0 || 2*s.cfg.BufferMicros < 3*cycle {
		return false
	}
	compact := s.fbCount >= coldStartFeedbacks &&
		(s.burstQ8 < burstBandThresholdQ8 ||
			s.closureEquationExpensive(fb))
	return compact || s.sourceHeadroomRate() <= bulkExactHeadroomMax
}

// emitUnitRepair retransmits one retained source id as an exact-length,
// repair-class UnitRepair.
func (s *SlidingSender) emitUnitRepair(now clock.Timestamp, id uint32) bool {
	return s.emitCompactUnit(now, id, uepCenterTier, true)
}

func (s *SlidingSender) emitCompactUnit(now clock.Timestamp, id uint32, priority uint8, reactive bool) bool {
	src, ok := s.enc.Source(id)
	if !ok {
		return false
	}
	payload, sourceLen, dl, valid := parseCodedSource(src, s.cfg.SymbolSize)
	if !valid {
		return false
	}
	if reactive && s.cfg.OutageAware && now.Add(s.rttMicros/2).After(dl) {
		s.stats.DeadReactiveSkips++
		return false
	}
	const sourceLengthExtensionBytes = 4
	wireBytes := repairWireBaseBytes + sourceLengthExtensionBytes + len(payload)
	// A feedback-proven exact value is the rank-closing lane, so it consumes
	// available recovery credit directly instead of reserving a speculative full
	// equation ahead of itself. Deadline-qualified burst duplicates use the same
	// byte budget; neither path purchases additional rate.
	if !s.repairAdmissibleWire(now, priority, dl, wireBytes, false) {
		return false
	}
	if !s.emit(wire.Symbol{
		Flow: s.cfg.Flow, Kind: wire.UnitRepair, WindowBase: id, SrcIndex: id, N: 1,
		Priority: priority, Deadline: int64(dl), SendTimestamp: int64(now), HasSourceLength: true,
		SourceLength: uint32(sourceLen), Payload: payload,
	}) {
		return false
	}
	s.lastRepair = now
	s.stats.Repair++
	if reactive {
		s.stats.ReactiveRepair++
		s.stats.RepairDeficit++
		s.stats.RepairExact++
	} else {
		s.stats.RepairProactive++
		s.stats.RepairBurstDuplicate++
	}
	return true
}
