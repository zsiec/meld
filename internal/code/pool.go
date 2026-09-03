package code

// Pool recycles symbol-size byte buffers across the coders of one flow so the warm
// encode/decode path stops allocating once the working set is sized. Symbol payload buffers
// (one per source/recovered symbol, ~1316 B each) dominate the transport's allocation; left
// un-pooled, every generation allocates a fresh set and frees the previous to the GC. A buffer
// handed back via put MUST no longer be referenced by the caller.
//
// It is NOT safe for concurrent use — one per flow, owned by the host half (internal/session
// via internal/flow), shared across the per-generation encoders/decoders of that flow.
type Pool struct {
	symSize int
	free    [][]byte
}

// poolMaxBufs bounds the retained free buffers so a burst of releases (e.g. a deep backlog of
// generations all reaped at once) cannot pin unbounded memory; beyond it, returned buffers are
// dropped to the GC. The cap is generous — well above any sane in-flight working set.
const poolMaxBufs = 8192

// NewPool returns a buffer pool for symSize-byte symbols.
func NewPool(symSize int) *Pool { return &Pool{symSize: symSize} }

// get returns a zeroed symSize buffer, recycling a free one when available. Zeroing matches the
// make([]byte, symSize) it replaces, so callers that copy-with-zero-pad or accumulate into the
// buffer (gf.MulAdd) behave identically.
func (p *Pool) get() []byte {
	if n := len(p.free); n > 0 {
		b := p.free[n-1]
		p.free[n-1] = nil
		p.free = p.free[:n-1]
		clear(b)
		return b
	}
	return make([]byte, p.symSize)
}

// put returns a buffer for reuse. Buffers of the wrong size, or beyond the retention cap, are
// dropped to the GC.
func (p *Pool) put(b []byte) {
	if cap(b) < p.symSize || len(p.free) >= poolMaxBufs {
		return
	}
	p.free = append(p.free, b[:p.symSize])
}
