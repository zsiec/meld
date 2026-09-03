package flow

import "github.com/zsiec/meld/internal/clock"

// This file is the loss-observation machinery SHARED by the generation and sliding
// receivers, embedded anonymously in both so the twinned state stays one
// implementation: the recovery-horizon slack estimate, the two-regime loss-run
// classification (Config.OutageAware), the per-symbol deadline fit, and the
// LTR-resync feedback state. Each was originally hand-mirrored across the two
// receivers; a divergence in one copy silently changes sizing or resync feedback on
// only one profile, so the copies were folded here.

// lossRunObserver carries the shared loss-run state: the max-hold recovery-horizon
// slack, the outage-censoring accumulator, the per-interval honest congestion count,
// and the burst-length EWMA.
type lossRunObserver struct {
	// slackUs is the EWMA of a directly-received symbol's remaining life at arrival
	// (deadline − now, clamped ≥ 0) — the span in which repair emitted for it could
	// still have mattered. Over intervalUs it gives the recovery horizon in SYMBOLS;
	// a loss run far past that horizon has a provably dead interior and is
	// classified an outage.
	slackUs int64
	// lossExcl is the outage span excluded from the current loss-estimate window
	// when censoring is enabled (channel time "pauses" across an outage).
	lossExcl int
	// clSinceFB is the pre-recovery loss since the last feedback (→ CongestionLoss).
	clSinceFB uint32
	// meanBurstQ8 is the smoothed mean loss-run length, Q8 (256 == i.i.d.; → Burstiness).
	meanBurstQ8 uint32
	// outageRunSinceFB is the largest run in the current report interval that
	// exceeded the measured recovery horizon. Outage composure excludes that dead
	// interior from rate sizing, but the automatic repair-time selector still needs
	// to know that recurrent fades exist.
	outageRunSinceFB uint16
}

// observeSlack folds one directly-received symbol's remaining life at arrival
// (deadline − now) into the recovery-horizon estimate. The horizon is a PATH property
// (≈ budget − propagation), so it is estimated as a max-hold with slow decay: queueing
// behind a burst shrinks individual samples exactly when classification matters most,
// and a mean-tracking estimate would shrink the outage threshold with them —
// classifying (and censoring) recoverable bursts mid-storm. The max-hold keeps the
// threshold anchored to the path's true slack; the slow decay still tracks a genuine
// budget/route change downward.
func (o *lossRunObserver) observeSlack(now, dl clock.Timestamp) {
	s := dl.Sub(now)
	if s < 0 {
		s = 0
	}
	if hold := o.slackUs - o.slackUs>>8; s > hold {
		o.slackUs = s
	} else {
		o.slackUs = hold
	}
}

// observeRun folds one fresh wire-loss run into the shared machinery: the honest
// counters (WireLost, clSinceFB — NEVER censored), the two-regime classification
// (a run beyond the recovery horizon is an OUTAGE — telemetry always; censored from
// the sizing estimators when aware, because no redundancy setting could have
// recovered its interior), and the burst-length EWMA for the GE sizer. Outage runs
// are censored as a whole; their recoverable edges are handled by lag-free
// per-window deficit estimates. Sub-threshold runs still feed the burst EWMA.
func (o *lossRunObserver) observeRun(run uint32, intervalUs int64, aware bool, stats *ReceiverStats) {
	o.countRun(run, stats)
	o.observeRunEstimates(run, intervalUs, aware, stats)
}

// countRun folds one loss run into the HONEST counters only (WireLost + the
// per-interval CongestionLoss accumulator). Split out so a receiver running the
// reorder window can count runs IMMEDIATELY on the raw arrival walk — detection
// latency: CongestionLoss arms the sliding retro-reactive tier and the loss-onset
// event feedback, and delaying them behind the holdoff measurably collapsed lossy
// delivery in the permutation sweep — while the sizing estimators settle behind
// the window (estimation fidelity: reorder must not read as loss).
func (o *lossRunObserver) countRun(run uint32, stats *ReceiverStats) {
	stats.WireLost += uint64(run)
	if cl := uint64(o.clSinceFB) + uint64(run); cl > 0xFFFF {
		o.clSinceFB = 0xFFFF // saturate; consumers integrate per-report deltas
	} else {
		o.clSinceFB = uint32(cl)
	}
}

// observeRunEstimates folds one loss run into the SIZING estimators (burst EWMA +
// outage classification/censoring) without touching the honest counters.
func (o *lossRunObserver) observeRunEstimates(run uint32, intervalUs int64, aware bool, stats *ReceiverStats) {
	outage := false
	if th := outageThresholdSyms(o.slackUs, intervalUs); th > 0 && run > th {
		outage = true
		stats.Outages++
		stats.OutageSymbols += uint64(run)
		if run >= 0xFFFF {
			o.outageRunSinceFB = 0xFFFF
		} else if uint16(run) > o.outageRunSinceFB {
			o.outageRunSinceFB = uint16(run)
		}
		if aware {
			o.lossExcl += int(run) // pause channel time across the outage in the loss window
		}
	}
	if !outage || !aware {
		// EWMA the run length toward the new sample in Q8, in SIGNED arithmetic (the
		// delta is negative when a short run follows a longer one — unsigned would
		// underflow and explode). Cap the per-run sample so one long run can't
		// dominate the burst estimate (WireLost above still counts the full run).
		s := int64(run)
		if s > burstSampleCap {
			s = burstSampleCap
		}
		mb := int64(o.meanBurstQ8) + ((s<<8)-int64(o.meanBurstQ8))>>burstEWMAShift
		if mb < burstQ8One {
			mb = burstQ8One
		}
		o.meanBurstQ8 = uint32(mb)
	}
}

// outageThresholdSyms returns the loss-run length (in symbols) beyond which a run is
// classified an OUTAGE rather than an erasure sample: κ × the recovery horizon
// (arrival slack over the inter-symbol interval, floored against estimate noise).
// Zero until the horizon inputs have primed (then nothing is ever classified — fail
// open to today's behavior).
func outageThresholdSyms(slackUs, intervalUs int64) uint32 {
	if slackUs <= 0 || intervalUs <= 0 {
		return 0
	}
	h := slackUs / intervalUs
	if h < outageCensorHorizonFloor {
		h = outageCensorHorizonFloor
	}
	h *= outageCensorKappa
	if h > 1<<30 {
		h = 1 << 30
	}
	return uint32(h)
}

// maxReorderDepth bounds the resequencer's in-flight set: if this many ids pile up above the
// still-missing low edge, the low edge is declared lost regardless of holdoff (a conservative
// fallback that bounds memory and forces progress on a pathological reorder/loss burst).
const maxReorderDepth = 1024

// reorderWindow is the loss-estimate resequencer shared by both receivers
// (ReorderHoldoffMicros / AutoReorderHoldoff): a small reorder window in front of
// the loss estimators that settles a source id received-or-lost only after lower
// ids have had a holdoff to arrive, so a reordered-late id is counted RECEIVED —
// not a fictitious loss that over-sizes repair (the measured pEst 0.01→0.51
// pathology under reorder). seen holds arrived ids in (next, high]; gapAt is when
// `next` first became a gap. Config is copied at construction (it is immutable);
// sink is the receiver's observeLoss, fed ids in strict increasing order with
// their own stamped pathID (single-path receivers ignore it).
type reorderWindow struct {
	cfgHoldoff int64 // Config.ReorderHoldoffMicros: fixed window (0 = adaptive)
	auto       bool  // Config.AutoReorderHoldoff
	budget     int64 // Config.BufferMicros: caps the adaptive window
	sink       func(id uint32, pathID uint8)
	// lateSink, when non-nil, receives ids that arrive AFTER their holdoff already
	// settled them lost. Such an id must still be CREDITED to the loss-rate window —
	// it arrived; only its position in the gap walk is unrecoverable. Without this,
	// queueing-induced delay spreads beyond the holdoff systematically remove real
	// receptions from the estimate, and the inflated rate feeds a runaway (more
	// repair → deeper pacer queues → more late arrivals → higher rate still): the
	// permutation sweep measured +50% overhead, +400 ms p99, −10 pp delivery on
	// paced lossy cells before this correction.
	lateSink func(id uint32, pathID uint8)

	started bool
	next    uint32
	high    uint32
	gapAt   clock.Timestamp
	seen    map[uint32]uint8 // arrived-out-of-order ids → their stamped pathID
	holdUs  int64            // AutoReorderHoldoff: tracked reorder spread (max-hold, decayed)
}

// enabled reports whether the reorder window is active; when it is not, arrivals
// feed the loss estimators directly.
func (w *reorderWindow) enabled() bool { return w.cfgHoldoff > 0 || w.auto }

// holdoff is the effective reorder window: the fixed config value, or — adaptive —
// the measured reorder spread plus a quarter margin, capped at half the deadline
// budget (holding longer than the budget would let the symbol miss its deadline
// regardless).
func (w *reorderWindow) holdoff() int64 {
	if w.cfgHoldoff > 0 {
		return w.cfgHoldoff
	}
	h := w.holdUs + w.holdUs/4
	if cap := w.budget / 2; cap > 0 && h > cap {
		h = cap
	}
	return h
}

// feed routes a first-arrival systematic id through the reorder window before the
// loss estimators; see the generation receiver's original doc (reseqFeed).
func (w *reorderWindow) feed(now clock.Timestamp, id uint32, pathID uint8) {
	if !w.started {
		w.started, w.next, w.high = true, id, id
		w.seen = make(map[uint32]uint8)
		w.sink(id, pathID) // the first id is in order by definition
		w.next = id + 1
		return
	}
	if id < w.next {
		// Arrived after it was already declared lost ⇒ the window was too short for this reorder. Grow
		// it (a kickstart from zero plus a quarter), capped, so the next such reorder is held long enough.
		if w.auto && w.cfgHoldoff == 0 {
			w.holdUs += w.holdUs/4 + 2_000
			if cap := w.budget / 2; cap > 0 && w.holdUs > cap {
				w.holdUs = cap
			}
		}
		if w.lateSink != nil {
			w.lateSink(id, pathID) // credit the arrival to the rate window (see the field doc)
		}
		return // settled for the gap walk — the run it belonged to was already counted
	}
	w.seen[id] = pathID // remember the stamped path so the in-order replay carries the right one
	if id > w.high {
		w.high = id
	}
	w.drain(now)
}

// drain settles ids from next upward: an arrived id is fed to the sink in order; a
// still-missing id with a higher id present is held until the holdoff expires (or
// the in-flight set would exceed the depth cap), then skipped — the next received
// id's gap counts it lost. A contiguous lost run shares one gap-open time, so it
// drains in one holdoff. Called on arrival and on Tick (the holdoff can expire
// with no new arrival).
func (w *reorderWindow) drain(now clock.Timestamp) {
	if !w.started {
		return
	}
	holdoff := w.holdoff()
	for {
		if pid, ok := w.seen[w.next]; ok {
			if w.auto && w.gapAt != 0 { // a held gap that just FILLED — a reorder sample
				if s := now.Sub(w.gapAt); s > w.holdUs {
					w.holdUs = s // max-hold up to cover the observed spread
				} else {
					w.holdUs -= w.holdUs / 8 // decay toward the recent spread
				}
			}
			delete(w.seen, w.next)
			w.sink(w.next, pid)
			w.next++
			w.gapAt = 0
			continue
		}
		if w.high > w.next { // next is a gap — a higher id has arrived
			if w.gapAt == 0 {
				w.gapAt = now
			}
			if now.Sub(w.gapAt) >= holdoff || w.high-w.next > maxReorderDepth {
				if w.auto {
					w.holdUs -= w.holdUs / 16 // a confirmed loss is not reorder — let the window relax
				}
				w.next++ // lost; the next received id's gap will count it (keep gapAt for the run)
				continue
			}
		}
		return
	}
}

// deadlineFit is the per-symbol deadline extrapolation shared by both receivers,
// used ONLY for never-directly-received (recovered/missing) ids: fit deadline(id) =
// refDL + (id-refID)*intervalUs from stamped (id, deadline) pairs. It anchors on the
// highest id seen and, once the id span since the fit anchor reaches
// intervalFitSpanIDs, computes the interval as the SLOPE across that window —
// deadline span over id span — so batched (whole-access-unit) writes with shared
// stamps average correctly instead of biasing a gap EWMA toward the per-batch
// interval (see intervalFitSpanIDs for the mass-eviction failure a gap EWMA causes).
type deadlineFit struct {
	haveRef    bool
	refID      uint32
	refDL      clock.Timestamp
	anchorID   uint32
	anchorDL   clock.Timestamp
	intervalUs int64
	refSamples int
}

// updateRef refines the fit from one stamped (id, deadline). Stamps at or below the
// ref are ignored (stale), as is a HIGHER id with an EARLIER deadline: writes are
// monotone, so that is not a fit sample — it is a retrospective repair stamped with
// its inferred window-end deadline, arriving while the frontier is dark. Anchoring
// on it would drag every extrapolated deadline into the past and mass-evict a
// recoverable window (the premature-drop oracle catches exactly this).
func (f *deadlineFit) updateRef(id uint32, dl clock.Timestamp) {
	if !f.haveRef {
		f.haveRef, f.refID, f.refDL = true, id, dl
		f.anchorID, f.anchorDL = id, dl
		return
	}
	if id <= f.refID || dl.Before(f.refDL) {
		return // stale or monotone-violating stamp: not a fit sample
	}
	f.refID, f.refDL = id, dl
	// Bootstrap on the first positive-slope span of any width (a fit now beats no fit:
	// with refSamples == 0 the cursor cannot evict never-received ids at all), then
	// re-fit only on full windows so batch-boundary noise averages out.
	span := int64(id - f.anchorID)
	if span >= intervalFitSpanIDs || (f.refSamples == 0 && span > 0 && dl.After(f.anchorDL)) {
		if fit := dl.Sub(f.anchorDL) / span; fit > 0 {
			f.intervalUs = fit
			if f.intervalUs > maxIntervalMicros {
				f.intervalUs = maxIntervalMicros // bound the extrapolation multiplier (forged stamps)
			}
			f.refSamples++
		}
		f.anchorID, f.anchorDL = id, dl
	}
}

// ltrResyncState is the LTR-resync feedback state shared by both receivers (the
// sender half is resyncController): the newest FrameDesc.LTR frame that resolved
// DECODABLE, as FrameStart+1 (0 = none) — the safe anchor an encoder can resync
// against — and a wrapping count of REFERENCED pictures that resolved undecodable
// at/after it (chain damage). Both ride the feedback tail.
type ltrResyncState struct {
	newestDecLTR  uint32
	brokenAnchors uint16
}

// noteResolvedLTR folds one resolved frame into the LTR-resync feedback state: a
// decodable LTR candidate advances the safe anchor; a broken REFERENCED picture at or
// after the safe anchor counts as chain damage (a broken disposable leaf harms only
// itself; a straggler behind the safe anchor is known-dead history — its dependents
// already resolved broken — so neither may re-trigger a resync).
func (l *ltrResyncState) noteResolvedLTR(start uint32, fi *frameInfo, dec bool) {
	switch {
	case dec && fi.ltr:
		if v := start + 1; v > l.newestDecLTR {
			l.newestDecLTR = v
		}
	case !dec && !fi.disc && !fi.nonPic && start+1 > l.newestDecLTR:
		l.brokenAnchors++ // wrapping uint16: the consumer differences reports
	}
}
