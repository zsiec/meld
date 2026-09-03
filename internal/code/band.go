package code

import "github.com/zsiec/meld/internal/gf"

// BandDecoder is the band-form sliding-window RLNC decoder (Caterpillar-style): the
// low-latency, CPU-bounded sliding-window decoder. When repairs are limited to a
// coding window of band width b (Encoder.RepairWindow), the linear system is BANDED
// — each equation touches at most b consecutive source columns — so online decode
// costs O(b²) per symbol regardless of the total in-flight (delivery) window. The
// two ideas that make it fast:
//
//   - Each row stores its coefficients relative to its OWN start id (start == the
//     pivot, the leading column), so advancing the delivery cursor never shifts any
//     row — a window advance is O(1), with no per-delivery O(rows·W) left-shift over
//     the whole window.
//   - Elimination only touches the band: a new pivot at column c is back-substituted
//     out of the at most b earlier rows whose span reaches c, and a solved column is
//     eliminated from the same band — never the whole window.
//
// So the decode cost is governed by the small coding window b, while the delivery
// window (how far the cursor may lag, maxWin) can be large to cover the bandwidth-
// delay product at only memory cost. Recovery span is b symbols: a loss not repaired
// within b newer symbols is unrecoverable and skipped at its deadline. It is pure
// and deterministic; the host drives time via Skip.
//
// Row-span invariant: every STORED row spans at most spanCap = 2b columns, and every
// neighbor scan (back-substitution, harvest, eliminateKnown, dropColumn) looks spanCap
// back — the pair is what makes the incremental elimination COMPLETE. Elimination
// chains can legitimately extend a row past b (repair windows need not share an
// anchor: the retrospective reactive tier emits windows anchored at a stuck delivery
// cursor while proactive repair keeps sliding with the frontier, and one back-
// substitution step over such mixed anchors reaches 2b). A row that would exceed
// spanCap is DISCARDED instead of stored — dropping an equation only forgoes rank,
// never corrupts — because a stored row wider than the scan radius is silently
// invisible to later eliminations: the window then reports full rank while its rows
// never collapse for delivery (the stranded-row premature-drop bug the no-premature-
// drop oracle caught when retrospective repair first mixed anchors).
type BandDecoder struct {
	symSize int
	b       int    // coding-window band width (decode cost ~ O((2b)²)/symbol)
	maxWin  int    // delivery-window cap: the cursor may lag the frontier by this much
	cursor  uint32 // next source id to deliver (window base)
	highest uint32 // one past the highest source id any symbol has covered

	rows   map[uint32]*brow  // pivot id -> RREF row (coeffs relative to row.start == pivot)
	known  map[uint32][]byte // solved source id -> data (above cursor, awaiting in-order delivery)
	recent map[uint32][]byte // last <=b DELIVERED source values, [cursor-b, cursor); lets a repair
	out    []Recovered       // that starts below the cursor but still covers it be reduced and used
	lost   uint64
	// droppedRows counts equations DISCARDED by the span cap (reduce/back-substitution
	// rows that would exceed spanCap): rank deliberately forgone to keep elimination
	// complete. Telemetry for the deep-burst autopsy — a high discarded count in a
	// stuck-window regime means covering repair is arriving but being binned, and
	// recovery waits for luckier equation geometry.
	droppedRows uint64
}

// Rank reports the decoder's current information rank above the cursor: solved
// symbols awaiting delivery plus independent stored equations. Diagnostic (the
// burst-autopsy replay tracks per-ingest rank growth with it).
func (d *BandDecoder) Rank() int { return len(d.known) + len(d.rows) }

// MissingIn returns a NACK bitmap over the source ids [base, base+64): bit k set
// means id base+k is neither delivered nor solved, masked to ids BELOW the decode
// frontier (an id no arrival has covered yet is unsent-or-in-flight, not missing).
// The receiver reports this for base = cursor so the sender can answer with unit
// repairs, which close instantly (no coupled-span closure wait).
func (d *BandDecoder) MissingIn(base uint32) uint64 {
	var m uint64
	for k := uint32(0); k < 64; k++ {
		id := base + k
		if id >= d.highest {
			break // beyond covered frontier: unknown, not missing
		}
		if id < d.cursor {
			continue // already delivered/skipped
		}
		if _, ok := d.known[id]; ok {
			continue // solved, awaiting in-order delivery
		}
		m |= 1 << k
	}
	return m
}

// ClosureIn returns the rank-closing subset of MissingIn over [base, base+64).
// Each set bit is a free column in the decoder's reduced system: supplying its
// exact value removes one independent degree of freedom. Pivot columns are
// unresolved too, but asking for an arbitrary set of them can be redundant (two
// requested pivots may depend on the same free variable) and leave the system
// rank-deficient even after every requested value arrives.
func (d *BandDecoder) ClosureIn(base uint32) uint64 {
	var m uint64
	for k := uint32(0); k < 64; k++ {
		id := base + k
		if id >= d.highest {
			break
		}
		if id < d.cursor {
			continue
		}
		if _, ok := d.known[id]; ok {
			continue
		}
		if _, pivot := d.rows[id]; pivot {
			continue
		}
		m |= 1 << k
	}
	return m
}

// DroppedRows reports how many equations the span cap has discarded (see the
// droppedRows field doc).
func (d *BandDecoder) DroppedRows() uint64 { return d.droppedRows }

// spanCap returns the maximum stored-row span (and the scan radius that keeps
// elimination complete against it): one back-substitution step of growth over the
// band width.
func (d *BandDecoder) spanCap() int { return 2 * d.b }

// brow is one equation in band-form RREF: coeffs[i] is the coefficient of source id
// start+i, with start == pivot (the leading column) and coeffs[0] == 1.
type brow struct {
	start  uint32
	pivot  uint32
	coeffs []byte
	pay    []byte
}

// beq is an equation being reduced: coeffs[i] is the coefficient of source id start+i.
type beq struct {
	start  uint32
	coeffs []byte
	pay    []byte
}

// NewBandDecoder returns a band-form decoder for symSize-byte symbols, a coding
// window (band) of b, and a delivery window of maxWin source symbols.
func NewBandDecoder(symSize, b, maxWin int) *BandDecoder {
	if b < 1 {
		b = 1
	}
	if maxWin < b {
		maxWin = b
	}
	return &BandDecoder{
		symSize: symSize,
		b:       b,
		maxWin:  maxWin,
		rows:    make(map[uint32]*brow),
		known:   make(map[uint32][]byte),
		recent:  make(map[uint32][]byte),
	}
}

// Cursor returns the next source id to be delivered.
func (d *BandDecoder) Cursor() uint32 { return d.cursor }

// Highest returns one past the highest source id any received symbol has covered.
func (d *BandDecoder) Highest() uint32 { return d.highest }

// Lost returns the number of source symbols skipped (declared lost / evicted).
func (d *BandDecoder) Lost() uint64 { return d.lost }

// Deficit returns the degree-of-freedom gap for the current window (unknown columns
// minus independent equations) — the feedback signal.
func (d *BandDecoder) Deficit() int {
	span := int(d.highest - d.cursor)
	info := 0
	for id := d.cursor; id < d.highest; id++ {
		if _, ok := d.known[id]; ok {
			info++
		} else if _, ok := d.rows[id]; ok {
			info++
		}
	}
	if def := span - info; def > 0 {
		return def
	}
	return 0
}

// AddSystematic feeds a source symbol delivered verbatim. Out-of-window or duplicate
// ids are ignored. Delivers any source symbols that become ready in order.
func (d *BandDecoder) AddSystematic(id uint32, data []byte) {
	if id < d.cursor {
		return
	}
	d.grow(id)
	if id < d.cursor {
		return
	}
	if _, ok := d.known[id]; ok {
		return
	}
	if r, ok := d.rows[id]; ok && len(r.coeffs) == 1 {
		return // already a solved-equivalent pivot
	}
	// Fast path: an in-order systematic at the cursor with no pivot row already at its column is
	// its own source value. Record it known and fold its column out of the <=b earlier rows that
	// span it (the harvest elimination + cascade), skipping the equation build, pivot-row creation,
	// lead scan, and normalize that reduce() would do — byte-identical result for this common case.
	if id == d.cursor && d.rows[id] == nil {
		d.known[id] = d.pad(data)
		d.eliminateKnown(id)
		d.deliverReady()
		return
	}
	d.reduce(&beq{start: id, coeffs: []byte{1}, pay: d.pad(data)})
	d.deliverReady()
}

// Cover advances the observed source frontier through id without adding an
// equation. A host uses it when an independently decoded recovery lane covers a
// source range but has not yet surfaced any source values. The normal delivery
// window bound and deadline-driven Skip semantics still apply.
func (d *BandDecoder) Cover(id uint32) {
	if id < d.cursor {
		return
	}
	d.grow(id)
}

// eliminateKnown folds the now-known source symbol at column id out of the earlier rows
// (within the spanCap scan radius) whose span reaches it, cascading any row that thereby
// collapses to a unit vector into a further solved symbol (the same harvest cascade
// reduce() runs). Used by the AddSystematic fast path.
func (d *BandDecoder) eliminateKnown(id uint32) {
	val := d.known[id]
	lo := uint32(0)
	if cap := uint32(d.spanCap()); id > cap {
		lo = id - cap
	}
	if lo < d.cursor {
		lo = d.cursor
	}
	var work []uint32
	for q := lo; q < id; q++ {
		r := d.rows[q]
		if r == nil {
			continue
		}
		if off := int(id - r.start); off < len(r.coeffs) && r.coeffs[off] != 0 {
			gf.MulAdd(r.pay, val, r.coeffs[off])
			r.coeffs[off] = 0
			work = append(work, q)
		}
	}
	d.harvest(work)
}

// AddRepair feeds a repair over the window [base, base+n) with coefficients
// GenCoeffs(repairKey, n). A repair wider than the band, or one whose whole window has slid
// below the cursor, is ignored. A repair that STARTS below the cursor but still covers it is
// usable: the sender's window lags the receiver's delivery cursor by the feedback delay, so it
// keeps emitting repairs whose base trails the cursor; dropping them outright loses the only
// coding that protects a stuck gap at the cursor. The already-delivered columns below the cursor
// are folded out using the retained recent values (a below-cursor column that was LOST, hence
// absent from recent, leaves an unrecoverable unknown, so the repair is unusable), and the
// residual over [cursor, base+n) is reduced into the live band as usual.
func (d *BandDecoder) AddRepair(base uint32, n int, repairKey uint16, payload []byte) {
	if n <= 0 || n > d.b {
		return
	}
	d.grow(base + uint32(n) - 1) // settle the cursor (a window overflow may advance it) before clipping
	if base+uint32(n) <= d.cursor {
		return // the whole window is below the cursor — nothing live to contribute
	}
	coeffs := GenCoeffs(repairKey, n)
	pay := d.pad(payload)
	start := base
	if base < d.cursor {
		for c := base; c < d.cursor; c++ {
			cc := coeffs[c-base]
			if cc == 0 {
				continue
			}
			v, ok := d.recent[c]
			if !ok {
				return // spans a below-cursor column that was lost — cannot use
			}
			gf.MulAdd(pay, v, cc) // fold the known delivered value out of the equation
		}
		coeffs = coeffs[d.cursor-base:]
		start = d.cursor
	}
	d.reduce(&beq{start: start, coeffs: coeffs, pay: pay})
	d.deliverReady()
}

// AddSparseRepair feeds a repair over an explicit, strictly increasing source-id
// set. The id span must fit inside the band so the existing bounded row operations
// remain O(b²). Columns not listed by ids have coefficient zero, which lets this
// protect reference-layer symbols without spending rank on disposable gaps in the
// same neighborhood.
func (d *BandDecoder) AddSparseRepair(ids []uint32, repairKey uint16, payload []byte) {
	if len(ids) == 0 || len(ids) > d.b {
		return
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			return
		}
	}
	first, last := ids[0], ids[len(ids)-1]
	if uint64(last)-uint64(first)+1 > uint64(d.b) {
		return
	}
	d.grow(last)
	if last < d.cursor {
		return
	}
	coeffs := GenCoeffs(repairKey, len(ids))
	pay := d.pad(payload)
	cursor := d.cursor
	startSet := false
	var start, end uint32
	for i, id := range ids {
		c := coeffs[i]
		if c == 0 {
			continue
		}
		if id < cursor {
			v, ok := d.recent[id]
			if !ok {
				return // spans a below-cursor column that was lost — cannot use
			}
			gf.MulAdd(pay, v, c)
			continue
		}
		if !startSet {
			start, startSet = id, true
		}
		end = id
	}
	if !startSet {
		return
	}
	dense := make([]byte, int(end-start)+1)
	for i, id := range ids {
		if id < start || id > end {
			continue
		}
		dense[id-start] = coeffs[i]
	}
	d.reduce(&beq{start: start, coeffs: dense, pay: pay})
	d.deliverReady()
}

// Skip declares the head-of-line symbol lost (deadline) and advances past it, so
// later symbols can be delivered. Reports whether it skipped (false when the cursor
// is already deliverable — drain Deliver first — or nothing is past it).
func (d *BandDecoder) Skip() bool {
	if d.cursor >= d.highest {
		return false
	}
	if _, ok := d.known[d.cursor]; ok {
		return false
	}
	d.dropColumn(d.cursor) // surgically remove the lost unknown, preserving neighbor recoverability
	d.lost++
	d.retire(d.cursor, nil, false)
	d.cursor++
	d.deliverReady()
	return true
}

// Evict drops the head-of-line symbol without charging decoder loss. It is used by
// media-aware receivers that deliberately abandon a source id whose frame is already
// known undecodable; the receiver accounts that separately from deadline loss.
func (d *BandDecoder) Evict() bool {
	if d.cursor >= d.highest {
		return false
	}
	if data, ok := d.known[d.cursor]; ok {
		delete(d.known, d.cursor)
		d.retire(d.cursor, data, true)
		d.cursor++
		d.deliverReady()
		return true
	}
	d.dropColumn(d.cursor)
	d.retire(d.cursor, nil, false)
	d.cursor++
	d.deliverReady()
	return true
}

// Deliver returns the next in-order recovered source symbol and true, or zero/false.
func (d *BandDecoder) Deliver() (Recovered, bool) {
	if len(d.out) == 0 {
		return Recovered{}, false
	}
	r := d.out[0]
	d.out = d.out[1:]
	return r, true
}

// grow extends the frontier to cover id, evicting the oldest (delivering it if known,
// else skipping it lost) when the delivery window would overflow.
func (d *BandDecoder) grow(id uint32) {
	if id < d.cursor {
		return
	}
	for id >= d.cursor+uint32(d.maxWin) {
		if data, ok := d.known[d.cursor]; ok {
			d.out = append(d.out, Recovered{ID: d.cursor, Data: data})
			delete(d.known, d.cursor)
			d.retire(d.cursor, data, true)
			d.cursor++
		} else {
			d.dropColumn(d.cursor)
			d.lost++
			d.retire(d.cursor, nil, false)
			d.cursor++
		}
	}
	if id+1 > d.highest {
		d.highest = id + 1
	}
}

// reduce inserts one equation into the band RREF and surfaces any newly solved
// symbols. eq is owned by reduce. A single left-to-right pass eliminates every
// KNOWN column (folded into the payload) and every existing PIVOT column from the
// equation — a pivot row at column c spans [c, c+len) so it only touches columns at
// or after c, which the increasing scan then processes — leaving the equation
// nonzero only at FREE columns; the first is the new pivot.
func (d *BandDecoder) reduce(eq *beq) {
	var lead uint32
	ok := false
	for i := 0; i < len(eq.coeffs); i++ {
		c := eq.coeffs[i]
		if c == 0 {
			continue
		}
		col := eq.start + uint32(i)
		if data, known := d.known[col]; known {
			gf.MulAdd(eq.pay, data, c)
			eq.coeffs[i] = 0
			continue
		}
		if row := d.rows[col]; row != nil {
			eqSubRow(eq, row, c) // zeros col, may extend eq into later (free) columns
			continue
		}
		// A free column with nonzero c — and since eqSubRow only ever extends eq forward, every
		// earlier column is already zero, so the first one reached is the pivot. Capturing it here
		// folds the eqLead rescan of the (now zero) prefix into this single pass.
		if !ok {
			lead, ok = col, true
		}
	}
	if !ok {
		return // linearly dependent — no new information
	}
	if f := eq.coeffs[lead-eq.start]; f != 1 {
		inv := gf.Inv(f)
		gf.MulSlice(eq.coeffs, eq.coeffs, inv)
		gf.MulSlice(eq.pay, eq.pay, inv)
	}
	coeffs := eq.coeffs[lead-eq.start:]
	for len(coeffs) > 1 && coeffs[len(coeffs)-1] == 0 {
		coeffs = coeffs[:len(coeffs)-1] // trim the zero tail before judging the span
	}
	if len(coeffs) > d.spanCap() {
		d.droppedRows++
		return // would exceed the stored-row span cap: discard (rank forgone, never corrupted)
	}
	nr := &brow{start: lead, pivot: lead, coeffs: coeffs, pay: eq.pay}
	d.rows[lead] = nr
	// Back-substitute lead out of the earlier rows (within the scan radius) whose span
	// reaches it. A row whose growth would exceed the span cap is discarded whole —
	// keeping it un-substituted would strand it wider than the scan radius instead.
	seeds := []uint32{lead}
	lo := uint32(0)
	if cap := uint32(d.spanCap()); lead > cap {
		lo = lead - cap
	}
	if lo < d.cursor {
		lo = d.cursor
	}
	for p := lo; p < lead; p++ {
		r := d.rows[p]
		if r == nil {
			continue
		}
		if off := int(lead - r.start); off < len(r.coeffs) && r.coeffs[off] != 0 {
			if need := int(nr.start-r.start) + len(nr.coeffs); need > d.spanCap() {
				d.droppedRows++
				delete(d.rows, p)
				continue
			}
			rowSubRow(r, nr, r.coeffs[off])
			seeds = append(seeds, p)
		}
	}
	d.harvest(seeds)
}

// harvest promotes rows that have collapsed to a unit vector to solved symbols,
// eliminating each solved column from the earlier rows (within the spanCap scan
// radius) that reference it, cascading within the band.
func (d *BandDecoder) harvest(work []uint32) {
	for len(work) > 0 {
		p := work[len(work)-1]
		work = work[:len(work)-1]
		r := d.rows[p]
		if r == nil || countNonzero(r.coeffs) != 1 {
			continue
		}
		d.known[p] = r.pay
		delete(d.rows, p)
		lo := uint32(0)
		if cap := uint32(d.spanCap()); p > cap {
			lo = p - cap
		}
		if lo < d.cursor {
			lo = d.cursor
		}
		for q := lo; q < p; q++ {
			r2 := d.rows[q]
			if r2 == nil {
				continue
			}
			if off := int(p - r2.start); off < len(r2.coeffs) && r2.coeffs[off] != 0 {
				gf.MulAdd(r2.pay, r.pay, r2.coeffs[off])
				r2.coeffs[off] = 0
				work = append(work, q)
			}
		}
	}
}

// dropColumn removes the lost (unrecoverable) source unknown at column id from the band
// while preserving the recoverability of its neighbors. The naive form — deleting every row
// that references id — discards equations that ALSO constrain still-recoverable columns, so
// one genuine loss cascades into dropping neighbors the surviving rank still supports (the
// sliding-path premature-drop gap). Instead, eliminate id as a free unknown by one step of
// Gaussian elimination: among the <=b rows spanning id, the one with the LARGEST start (its
// pivot is at or after every other's, satisfying rowSubRow's start ordering) absorbs id —
// subtract it from each other spanning row to zero that row's id coefficient, then discard
// only the absorber. Exactly one degree of freedom is spent (id itself); every other row
// survives, id-free, still pivoting its own column. The absorber's only nonzeros besides its
// own pivot are at FREE columns (any pivot column above it was already back-substituted out
// of it), so the subtraction never pollutes another pivot column — the RREF invariant holds
// without re-reduction. For a pivot column id this reduces to deleting its single pivot row,
// since RREF leaves id referenced only there.
func (d *BandDecoder) dropColumn(id uint32) {
	lo := uint32(0)
	if cap := uint32(d.spanCap()); id > cap {
		lo = id - cap
	}
	if lo < d.cursor {
		lo = d.cursor
	}
	for p := lo; p <= id; p++ {
		r := d.rows[p]
		if r == nil {
			continue
		}
		if off := int(id - r.start); off >= 0 && off < len(r.coeffs) && r.coeffs[off] != 0 {
			delete(d.rows, p)
		}
	}
}

// deliverReady delivers every solved symbol contiguous with the cursor.
func (d *BandDecoder) deliverReady() {
	for {
		data, ok := d.known[d.cursor]
		if !ok {
			return
		}
		d.out = append(d.out, Recovered{ID: d.cursor, Data: data})
		delete(d.known, d.cursor)
		d.retire(d.cursor, data, true)
		d.cursor++
	}
}

// retire records that source id (== the pre-increment cursor) is leaving the live window. A
// DELIVERED value is kept in recent so a later repair that starts below the cursor can fold it
// out and still contribute its residual; a LOST id is left absent, and that absence marks it
// unrecoverable to such a repair. The entry that falls a full band behind is pruned, bounding
// recent to the last <=b delivered values — exactly the reach of any band-width repair.
func (d *BandDecoder) retire(id uint32, data []byte, delivered bool) {
	if delivered {
		d.recent[id] = data
	}
	if id >= uint32(d.b) {
		delete(d.recent, id-uint32(d.b))
	}
}

func (d *BandDecoder) pad(data []byte) []byte {
	p := make([]byte, d.symSize)
	copy(p, data)
	return p
}

// eqSubRow does eq -= f*row (row.start >= eq.start), growing eq to cover the row.
func eqSubRow(eq *beq, row *brow, f byte) {
	off := int(row.start - eq.start)
	if need := off + len(row.coeffs); need > len(eq.coeffs) {
		grown := make([]byte, need)
		copy(grown, eq.coeffs)
		eq.coeffs = grown
	}
	gf.MulAdd(eq.coeffs[off:], row.coeffs, f)
	gf.MulAdd(eq.pay, row.pay, f)
}

// rowSubRow does dst -= f*src (src.start >= dst.start), growing dst to cover src.
func rowSubRow(dst, src *brow, f byte) {
	off := int(src.start - dst.start)
	if need := off + len(src.coeffs); need > len(dst.coeffs) {
		grown := make([]byte, need)
		copy(grown, dst.coeffs)
		dst.coeffs = grown
	}
	gf.MulAdd(dst.coeffs[off:], src.coeffs, f)
	gf.MulAdd(dst.pay, src.pay, f)
}
