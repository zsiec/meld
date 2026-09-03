// Package code implements Meld's recovery substrate: bounded systematic Cauchy
// MDS blocks and a sliding-window random linear network code (RLNC) over GF(2^8)
// (internal/gf). It is the pure,
// deterministic heart of the transport — no clock, no I/O, no goroutines.
//
// An Encoder accumulates fixed-size source symbols and emits two kinds of coded
// symbol: SYSTEMATIC (a source symbol verbatim, coefficient = unit vector) and
// REPAIR (a random linear combination of the current window). Recovery needs any
// k independent symbols covering a window, not the specific missing ones — so a
// single repair symbol heals any one loss in its window, r repairs heal any r,
// largely decoupled from the round-trip that bounds ARQ.
//
// A Decoder performs online Gauss-Jordan elimination (reduced row echelon form):
// each independent symbol raises the window's rank by one, and a source symbol is
// surfaced the instant its row reduces to a unit vector — so recovery is
// incremental, not all-at-once at full rank. The decoder never reports a symbol
// it cannot determine; its rank is the exact, sound measure of what it can
// recover (the property internal/code's rank oracle test asserts).
//
// Repair coefficients are not carried explicitly: a repair symbol carries a
// (window base, width, repair key) triple and both ends regenerate the identical
// GF(2^8) coefficient vector from the key via GenCoeffs. The bounded MDS key
// namespace forms a Cauchy parity matrix; sliding-window lanes use deterministic
// seeded RFC-8681-style coefficients.
package code

import "github.com/zsiec/meld/internal/gf"

const (
	mdsRepairKeyMask   uint16 = 0xc000
	mdsRepairIndexMask uint16 = 0x3fff
)

// BlockRepairKey maps a bounded parity-row index into the Cauchy-MDS key namespace.
func BlockRepairKey(index uint16) uint16 { return mdsRepairKeyMask | (index & mdsRepairIndexMask) }

// BlockRepairIndex returns the bounded row index and whether key names the Cauchy-MDS namespace.
func BlockRepairIndex(key uint16) (uint16, bool) {
	return key & mdsRepairIndexMask, key&mdsRepairKeyMask == mdsRepairKeyMask
}

// GenCoeffs deterministically derives n GF(2^8) coefficients for a repair symbol
// from repairKey. The encoder and decoder call it identically, so the coefficient
// vector never travels on the wire. Coefficient i scales the source symbol at
// window offset i (window base + i). Keys in the MDS namespace name rows of a
// Cauchy matrix disjoint from the n systematic points, so any n rows from that
// bounded systematic+repair set are independent. Other keys use deterministic
// seeded RLNC and are guaranteed not to be all-zero.
func GenCoeffs(repairKey uint16, n int) []byte {
	c := make([]byte, n)
	if n == 0 {
		return c
	}
	if row, mds := BlockRepairIndex(repairKey); mds && n <= 254 && int(row)+n < 255 {
		x := byte(n + int(row))
		for i := range c {
			c[i] = gf.Inv(x ^ byte(i))
		}
		return c
	}
	// SplitMix64 seeded by the key; 8 coefficient bytes per 64-bit draw.
	st := uint64(repairKey)*0x9E3779B97F4A7C15 + 0xD1B54A32D192ED03
	for i := 0; i < n; {
		var x uint64
		st, x = splitmix64(st)
		for b := 0; b < 8 && i < n; b++ {
			c[i] = byte(x)
			x >>= 8
			i++
		}
	}
	for _, v := range c {
		if v != 0 {
			return c
		}
	}
	c[0] = 1 // never emit an all-zero (information-free) coefficient vector
	return c
}

func splitmix64(s uint64) (next, out uint64) {
	s += 0x9E3779B97F4A7C15
	z := s
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	return s, z
}

// Encoder accumulates source symbols of a fixed size and emits systematic and
// repair coded symbols over its current window. It is not safe for concurrent
// use; the host serializes a single send path onto it.
type Encoder struct {
	symSize int
	base    uint32
	syms    [][]byte
	pool    *Pool // optional symbol-buffer recycler (nil ⇒ allocate)
}

// SetPool gives the encoder a buffer pool to draw its source/repair payload buffers from.
func (e *Encoder) SetPool(p *Pool) { e.pool = p }

// Recycle returns a buffer the encoder produced (a Repair/RepairWindow payload) once the caller
// has finished with it — after the host has serialized it onto the wire — for reuse. No-op
// without a pool or for a wrong-size buffer.
func (e *Encoder) Recycle(b []byte) {
	if e.pool != nil {
		e.pool.put(b)
	}
}

// Release returns every source-symbol buffer the encoder holds to its pool. Call it when the
// encoder is discarded (its generation retired). After Release the encoder must not be used.
func (e *Encoder) Release() {
	if e.pool == nil {
		return
	}
	for i, b := range e.syms {
		e.pool.put(b)
		e.syms[i] = nil
	}
	e.syms = e.syms[:0]
}

// symBuf returns a zeroed symbol-size buffer from the pool, or a fresh one.
func (e *Encoder) symBuf() []byte {
	if e.pool != nil {
		return e.pool.get()
	}
	return make([]byte, e.symSize)
}

// NewEncoder returns an Encoder for symbols of symSize bytes, with source ids
// starting at 0.
func NewEncoder(symSize int) *Encoder { return NewEncoderAt(symSize, 0) }

// NewEncoderAt returns an Encoder whose first source symbol has id base. The host
// keeps one encoder per generation so a retained generation's repair symbols
// carry the correct window base on the wire (reactive / deficit-driven repair).
func NewEncoderAt(symSize int, base uint32) *Encoder {
	return &Encoder{symSize: symSize, base: base}
}

// Add copies data as the next source symbol (zero-padded to the symbol size) and
// returns its source id. ids are assigned consecutively from the window base.
func (e *Encoder) Add(data []byte) uint32 {
	buf := e.symBuf() // zeroed, so the tail beyond len(data) is the zero pad
	copy(buf, data)
	id := e.base + uint32(len(e.syms))
	e.syms = append(e.syms, buf)
	return id
}

// AddWithSuffix adds a source while copying suffix at suffixOffset in the same
// pooled symbol buffer. It avoids an intermediate allocation for small coded
// metadata trailers. Out-of-range suffix bytes are safely truncated.
func (e *Encoder) AddWithSuffix(data []byte, suffixOffset int, suffix []byte) uint32 {
	buf := e.symBuf()
	copy(buf, data)
	if suffixOffset < 0 {
		skip := -suffixOffset
		if skip >= len(suffix) {
			suffix = nil
		} else {
			suffix = suffix[skip:]
		}
		suffixOffset = 0
	}
	if suffixOffset < len(buf) {
		copy(buf[suffixOffset:], suffix)
	}
	id := e.base + uint32(len(e.syms))
	e.syms = append(e.syms, buf)
	return id
}

// Base returns the source id of the first symbol in the window.
func (e *Encoder) Base() uint32 { return e.base }

// Len returns the number of source symbols in the window.
func (e *Encoder) Len() int { return len(e.syms) }

// Source returns the source symbol with the given id (zero-padded to the symbol
// size) and whether it is currently in the window.
func (e *Encoder) Source(id uint32) ([]byte, bool) {
	off := int64(id) - int64(e.base)
	if off < 0 || off >= int64(len(e.syms)) {
		return nil, false
	}
	return e.syms[off], true
}

// Repair builds a repair symbol: a random linear combination of every source
// symbol currently in the window, using the coefficient vector GenCoeffs derives
// from repairKey. It returns the window base and width the decoder needs to
// regenerate the coefficients, plus the combined payload. The caller must use a
// fresh repairKey for each repair symbol over the same window.
func (e *Encoder) Repair(repairKey uint16) (base uint32, n int, payload []byte) {
	n = len(e.syms)
	payload = e.symBuf() // zeroed, ready to accumulate the linear combination
	coeffs := GenCoeffs(repairKey, n)
	for j := 0; j < n; j++ {
		gf.MulAdd(payload, e.syms[j], coeffs[j])
	}
	return e.base, n, payload
}

// RepairWindow builds a repair symbol over only the trailing ew source symbols of
// the window (the bounded coding window / RFC 8681 ew_max_size), instead of the
// whole window. This bounds the band the receiver's BandDecoder must solve, so its
// decode cost is O(ew²) per symbol regardless of the total in-flight window. ew <= 0
// means the whole window (same as Repair). It returns the covered window
// [base, base+n) and the combined payload.
func (e *Encoder) RepairWindow(repairKey uint16, ew int) (base uint32, n int, payload []byte) {
	n = len(e.syms)
	start := 0
	if ew > 0 && n > ew {
		start, n = n-ew, ew
	}
	payload = e.symBuf() // zeroed, ready to accumulate the linear combination
	coeffs := GenCoeffs(repairKey, n)
	for j := 0; j < n; j++ {
		gf.MulAdd(payload, e.syms[start+j], coeffs[j])
	}
	return e.base + uint32(start), n, payload
}

// RepairAt builds a repair symbol over the EXPLICIT retained window
// [at, at+n) — retrospective repair for a stuck delivery cursor, whose holes the
// trailing-band repair (RepairWindow) can no longer reach once new source has slid
// the band past them. The requested range is clipped to the retained window; the
// clipped n' is returned (0 with a nil payload when nothing overlaps). The caller
// must use a fresh repairKey per emission, exactly as with RepairWindow.
func (e *Encoder) RepairAt(repairKey uint16, at uint32, n int) (base uint32, nOut int, payload []byte) {
	lo := int64(at) - int64(e.base)
	if lo < 0 {
		n += int(lo) // clip the below-window prefix
		lo = 0
	}
	if lo >= int64(len(e.syms)) || n <= 0 {
		return 0, 0, nil
	}
	if rem := int64(len(e.syms)) - lo; int64(n) > rem {
		n = int(rem)
	}
	payload = e.symBuf() // zeroed, ready to accumulate the linear combination
	coeffs := GenCoeffs(repairKey, n)
	for j := 0; j < n; j++ {
		gf.MulAdd(payload, e.syms[lo+int64(j)], coeffs[j])
	}
	return e.base + uint32(lo), n, payload
}

// RepairSparse builds a repair symbol over the explicitly listed source ids. ids
// must be strictly increasing and currently retained. It is the protected-layer
// companion to RepairWindow: the equation codes only the listed columns, so
// unrelated symbols inside the same span do not consume rank.
func (e *Encoder) RepairSparse(repairKey uint16, ids []uint32) (payload []byte, ok bool) {
	if len(ids) == 0 {
		return nil, false
	}
	payload = e.symBuf() // zeroed, ready to accumulate the linear combination
	coeffs := GenCoeffs(repairKey, len(ids))
	var prev uint32
	for i, id := range ids {
		if i > 0 && id <= prev {
			e.Recycle(payload)
			return nil, false
		}
		src, ok := e.Source(id)
		if !ok {
			e.Recycle(payload)
			return nil, false
		}
		gf.MulAdd(payload, src, coeffs[i])
		prev = id
	}
	return payload, true
}

// SlideTo drops every source symbol with id < newBase from the window (deadline
// eviction). It is a no-op if newBase is at or below the current base.
func (e *Encoder) SlideTo(newBase uint32) {
	if newBase <= e.base {
		return
	}
	drop := int(newBase - e.base)
	if drop > len(e.syms) {
		drop = len(e.syms)
	}
	if e.pool != nil {
		for i := 0; i < drop; i++ {
			e.pool.put(e.syms[i]) // the slid-out source symbols are no longer needed
		}
	}
	e.syms = e.syms[drop:]
	e.base = newBase
}

// Recovered is a source symbol the Decoder reconstructed. Data aliases the
// decoder's buffer and is valid for read; copy it to retain it past further
// decoder calls.
type Recovered struct {
	ID   uint32
	Data []byte
}

// row is one equation in the decoder's reduced row echelon form: coeffs[piv]==1,
// coeffs is zero at every other pivot column, and pay is the matching combined
// payload. nz is the count of nonzero coefficients; nz==1 means the row has
// collapsed to a single source symbol (pay) and is ready to surface.
type row struct {
	coeffs []byte
	pay    []byte
	piv    int
	nz     int
}

// Decoder reconstructs source symbols from systematic and repair coded symbols
// over a window of width win starting at base. It maintains reduced row echelon
// form so each newly independent symbol may immediately surface one or more
// recovered source symbols. It is not safe for concurrent use.
type Decoder struct {
	symSize  int
	base     uint32
	win      int
	rows     []*row // rows[off]: the pivot row whose pivot column is off (nil if none)
	pivots   []int  // active pivot offsets, for iteration
	done     []bool // done[off]: source symbol at off has been recovered
	data     [][]byte
	ndecoded int
	pool     *Pool   // optional symbol-buffer recycler (nil ⇒ allocate)
	ops      []payOp // reusable scratch: payload operations recorded while reducing the coeffs
}

// payOp is one deferred payload elimination: pay ^= f·src. addEqn records these while it reduces
// an incoming equation's coefficient vector, then replays them on the payload only if the equation
// turns out to be independent — so a linearly dependent symbol (no new rank) never pays for the
// payload copy or its GF mul-adds.
type payOp struct {
	src []byte
	f   byte
}

// SetPool gives the decoder a buffer pool to recycle its symbol-size payload buffers through
// (the dominant allocation). Call Release when the decoder is discarded to return its buffers.
func (d *Decoder) SetPool(p *Pool) { d.pool = p }

// Release returns every symbol-size buffer the decoder holds (recovered payloads and the
// payloads of still-undetermined pivot rows) to its pool, for the next decoder to reuse. After
// Release the decoder must not be used. Recovered payloads handed out earlier were copied by
// the host (receiver.absorb), so the buffers are no longer aliased. No-op without a pool.
func (d *Decoder) Release() {
	if d.pool == nil {
		return
	}
	for off, b := range d.data {
		if b != nil {
			d.pool.put(b)
			d.data[off] = nil
		}
	}
	for _, p := range d.pivots {
		if r := d.rows[p]; r != nil {
			d.pool.put(r.pay)
			d.rows[p] = nil
		}
	}
	d.pivots = d.pivots[:0]
}

// symBuf returns a zeroed symbol-size buffer from the pool, or a fresh one.
func (d *Decoder) symBuf() []byte {
	if d.pool != nil {
		return d.pool.get()
	}
	return make([]byte, d.symSize)
}

// NewDecoder returns a Decoder for symbols of symSize bytes over the window
// [base, base+win).
func NewDecoder(symSize int, base uint32, win int) *Decoder {
	return &Decoder{
		symSize: symSize,
		base:    base,
		win:     win,
		rows:    make([]*row, win),
		done:    make([]bool, win),
		data:    make([][]byte, win),
	}
}

// AddSystematic feeds a source symbol delivered verbatim (id, data). Out-of-window
// ids and already-recovered ids are ignored. It returns any source symbols
// recovered as a result (often just this one, but a repair symbol received
// earlier may now resolve too).
func (d *Decoder) AddSystematic(id uint32, data []byte) []Recovered {
	off := int64(id) - int64(d.base)
	if off < 0 || off >= int64(d.win) || d.done[off] {
		return nil
	}
	coeffs := make([]byte, d.win)
	coeffs[off] = 1
	return d.addEqn(coeffs, data)
}

// AddRepair feeds a repair symbol described by the window (base, n) and repairKey
// it was built from, plus its payload. A symbol whose window falls outside the
// decoder's window is ignored. It returns any source symbols recovered.
func (d *Decoder) AddRepair(base uint32, n int, repairKey uint16, payload []byte) []Recovered {
	start := int64(base) - int64(d.base)
	if start < 0 || start+int64(n) > int64(d.win) {
		return nil
	}
	coeffs := make([]byte, d.win)
	copy(coeffs[start:], GenCoeffs(repairKey, n))
	return d.addEqn(coeffs, payload)
}

// padded returns a symSize-byte copy of data (zero-padded / truncated), from the pool when set.
func (d *Decoder) padded(data []byte) []byte {
	p := d.symBuf() // zeroed, so the tail beyond len(data) is the zero pad
	copy(p, data)
	return p
}

// addEqn reduces an incoming equation into the RREF matrix and surfaces any source symbols that
// become determined. coeffs is owned by addEqn (it may store or mutate it); data is the equation's
// raw payload, which addEqn copies (d.padded) only if the equation is independent — a linearly
// dependent symbol adds no rank, so its payload copy and GF mul-adds would be pure waste.
func (d *Decoder) addEqn(coeffs, data []byte) []Recovered {
	// Phase 1 — reduce the COEFFICIENT vector against already-recovered symbols and existing pivot
	// rows, recording each payload elimination to replay later. Processing offsets in increasing
	// order is safe: a pivot row is zero at every column below its pivot, so eliminating it never
	// reintroduces a nonzero into a column we have already passed.
	d.ops = d.ops[:0]
	for off := 0; off < d.win; off++ {
		c := coeffs[off]
		if c == 0 {
			continue
		}
		if d.done[off] {
			d.ops = append(d.ops, payOp{d.data[off], c})
			coeffs[off] = 0
			continue
		}
		if r := d.rows[off]; r != nil {
			gf.MulAdd(coeffs, r.coeffs, c)
			d.ops = append(d.ops, payOp{r.pay, c})
		}
	}
	piv := -1
	for off := 0; off < d.win; off++ {
		if coeffs[off] != 0 {
			piv = off
			break
		}
	}
	if piv == -1 {
		return nil // linearly dependent — no new information; the payload was never copied or touched
	}
	// Phase 2 (independent only) — now materialize the payload and replay the recorded reduction on
	// it, reproducing exactly what the lockstep coeff+payload reduction would have computed.
	pay := d.padded(data)
	for i := range d.ops {
		gf.MulAdd(pay, d.ops[i].src, d.ops[i].f)
	}
	if coeffs[piv] != 1 {
		inv := gf.Inv(coeffs[piv])
		gf.MulSlice(coeffs, coeffs, inv)
		gf.MulSlice(pay, pay, inv)
	}
	nr := &row{coeffs: coeffs, pay: pay, piv: piv}
	// Back-substitute: clear the new pivot column from every existing row. The new
	// row is already zero at all existing pivot columns, so this preserves RREF.
	for _, p := range d.pivots {
		r := d.rows[p]
		if f := r.coeffs[piv]; f != 0 {
			gf.MulAdd(r.coeffs, nr.coeffs, f)
			gf.MulAdd(r.pay, nr.pay, f)
			r.nz = countNonzero(r.coeffs)
		}
	}
	nr.nz = countNonzero(nr.coeffs)
	d.rows[piv] = nr
	d.pivots = append(d.pivots, piv)
	return d.harvest()
}

// harvest surfaces every pivot row that has collapsed to a unit vector (nz==1),
// promoting it from an equation to a recovered source symbol.
func (d *Decoder) harvest() []Recovered {
	var rec []Recovered
	for changed := true; changed; {
		changed = false
		kept := d.pivots[:0]
		for _, p := range d.pivots {
			if r := d.rows[p]; r != nil && r.nz == 1 {
				d.data[p] = r.pay
				d.done[p] = true
				d.rows[p] = nil
				d.ndecoded++
				rec = append(rec, Recovered{ID: d.base + uint32(p), Data: r.pay})
				changed = true
			} else {
				kept = append(kept, p)
			}
		}
		d.pivots = kept
	}
	return rec
}

// Rank returns the number of linearly independent symbols absorbed so far
// (recovered source symbols plus undetermined pivot rows). It is the exact count
// of source symbols the decoder can recover, and never exceeds what the received
// symbols span.
func (d *Decoder) Rank() int { return d.ndecoded + len(d.pivots) }

// NumDecoded returns the number of source symbols fully recovered so far.
func (d *Decoder) NumDecoded() int { return d.ndecoded }

// Deficit returns how many more independent symbols are needed to recover all n
// source symbols of the window (n - Rank, clamped at zero) — the feedback signal
// the sender's redundancy controller acts on.
func (d *Decoder) Deficit(n int) int {
	if r := d.Rank(); r < n {
		return n - r
	}
	return 0
}

func countNonzero(c []byte) int {
	n := 0
	for _, v := range c {
		if v != 0 {
			n++
		}
	}
	return n
}
