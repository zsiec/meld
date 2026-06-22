package flow

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

// This file proves, against the REAL Sender + Receiver + delay-based congestionController,
// the failure the DPLPMTUD evaluation predicts: a size-based path black hole (a path that
// silently drops every datagram larger than its MTU) drives the coded transport to ZERO
// goodput while the congestion controller never reacts — because (a) repair symbols are the
// same size as the source symbols they protect, so the FEC that is supposed to mask loss is
// itself black-holed, and (b) the CC is loss-agnostic by design (RFC 9265), so size-loss
// produces no delay/ECN signal and it never backs off. Nothing in the stack can see it.
//
// Deterministic: explicit clock, seeded payloads. NOT a shipped test of intended behavior —
// it documents the blind spot DPLPMTUD must close.

// blackholeResult is what one run through a size-limited link observed.
type blackholeResult struct {
	sent          int   // datagrams the sender emitted
	droppedBySize int   // datagrams the link black-holed for exceeding the MTU
	delivered     int   // source chunks delivered in order to the application (goodput)
	repairWasted  int   // repair datagrams emitted that the link black-holed (FEC into the void)
	budgetStart   int64 // RateBudgetBitsPerSec at the first write
	budgetEnd     int64 // RateBudgetBitsPerSec at the end
	ccPrimed      bool  // did the congestion controller ever take a sample?
	senderPEst    float64
}

// sizedChunk builds a SymbolSize-byte chunk tagging its id in the first 4 bytes.
func sizedChunk(rng *rand.Rand, size int, id uint32) []byte {
	b := make([]byte, size)
	binary.BigEndian.PutUint32(b, id)
	rng.Read(b[4:])
	return b
}

// runOverMTU streams n chunks through Sender -> a link that DROPS any datagram whose
// on-wire length exceeds mtu -> Receiver, looping feedback back. It returns the observed
// outcome. Feedback datagrams are tiny and pass the MTU, so the loss path is purely the
// forward data direction — exactly a PMTU black hole.
func runOverMTU(cfg Config, n int, seed int64, mtu int) blackholeResult {
	rng := rand.New(rand.NewSource(seed))
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	var res blackholeResult
	now := clock.Timestamp(0)
	res.budgetStart = s.RateBudgetBitsPerSec()

	pump := func() {
		for {
			d, ok := s.PollSend()
			if !ok {
				break
			}
			res.sent++
			if len(d) > mtu { // the black hole: silently drop oversized datagrams
				res.droppedBySize++
				if sym, err := wire.DecodeSymbol(d); err == nil && sym.Kind == wire.Repair {
					res.repairWasted++
				}
				continue
			}
			r.FeedSymbol(now, d)
		}
		for {
			fb, ok := r.PollSend()
			if !ok {
				break
			}
			if len(fb) > mtu { // feedback is tiny; this never trips, but model the path symmetrically
				continue
			}
			if f, err := wire.DecodeFeedback(fb); err == nil {
				s.FeedFeedback(now, f)
			}
		}
		for {
			if _, _, ok := r.PollDeliver(); !ok {
				break
			}
			res.delivered++
		}
	}

	for i := 0; i < n; i++ {
		s.Write(now, sizedChunk(rng, cfg.SymbolSize, uint32(i)))
		pump()
		now = now.Add(1_000)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	s.Flush(now)
	// Settle well past every deadline so any recoverable symbol would have been delivered.
	for k := 0; k < 1000; k++ {
		now = now.Add(1_000)
		s.Tick(now)
		r.Tick(now)
		pump()
	}
	res.budgetEnd = s.RateBudgetBitsPerSec()
	res.ccPrimed = s.cc != nil && s.cc.primed
	res.senderPEst = s.pEst
	return res
}

// TestPMTU_BlackHoleKillsGoodput_CCBlind is the core proof. SAME sender config and SAME
// symbol size in both runs — only the path MTU differs — so the outcome isolates the size
// limit, nothing else.
func TestPMTU_BlackHoleKillsGoodput_CCBlind(t *testing.T) {
	const n = 160
	cfg := Config{
		Flow: 1, SymbolSize: 1316, GenSize: 16, Redundancy: 0.25, BufferMicros: 200_000,
		CongestionControl: true, MaxBitrate: 50_000_000,
	}
	// On-wire datagram = SymbolSize + ~30B header. With SymbolSize 1316 that is ~1346 B.
	fits := runOverMTU(cfg, n, 1, 2000) // MTU 2000: 1346 < 2000 → datagrams pass
	hole := runOverMTU(cfg, n, 1, 1000) // MTU 1000: 1346 > 1000 → every datagram black-holed

	t.Logf("FITS  (MTU 2000): delivered=%3d/%d  sent=%d droppedBySize=%d  budget %d→%d Mbps  ccPrimed=%v",
		fits.delivered, n, fits.sent, fits.droppedBySize, fits.budgetStart/1e6, fits.budgetEnd/1e6, fits.ccPrimed)
	t.Logf("HOLE  (MTU 1000): delivered=%3d/%d  sent=%d droppedBySize=%d (repair wasted=%d)  budget %d→%d Mbps  ccPrimed=%v  senderLossEst=%.3f",
		hole.delivered, n, hole.sent, hole.droppedBySize, hole.repairWasted, hole.budgetStart/1e6, hole.budgetEnd/1e6, hole.ccPrimed, hole.senderPEst)

	// (1) Baseline: at a fitting MTU the same stream delivers in full.
	if fits.delivered != n {
		t.Fatalf("fitting MTU should deliver all %d, got %d (harness/config problem, not the black hole)", n, fits.delivered)
	}
	// (2) FEC → 0 goodput: the size black hole defeats coding entirely.
	if hole.delivered != 0 {
		t.Fatalf("expected ZERO goodput through the black hole, delivered %d", hole.delivered)
	}
	// (3) FEC poured repair into the void — every emitted repair datagram was oversized too.
	if hole.repairWasted == 0 {
		t.Fatalf("expected the sender to emit repair that the black hole dropped; got 0 (FEC not engaging)")
	}
	// (4) The CC NEVER reacted: it never even took a sample (no delivery ⇒ no RTT sample ⇒
	//     never primed), so the budget sat at the ceiling the whole time and the loss
	//     estimate stayed 0 — the loss-agnostic controller is blind to size loss.
	if hole.ccPrimed {
		t.Errorf("CC took a sample under a total black hole — unexpected")
	}
	if hole.budgetEnd != hole.budgetStart || hole.budgetEnd != cfg.MaxBitrate {
		t.Errorf("CC budget moved off the ceiling under a size black hole: %d→%d (ceiling %d) — it should be blind",
			hole.budgetStart, hole.budgetEnd, cfg.MaxBitrate)
	}
	if hole.senderPEst != 0 {
		t.Errorf("sender saw a loss signal (pEst=%.3f) — a total forward black hole reports none", hole.senderPEst)
	}
	t.Logf("PROVEN: identical stream, only the path MTU differs. >MTU ⇒ 0 goodput, FEC wasted %d repair datagrams, CC never sampled (budget pinned at %d Mbps). FEC amplifies and the CC ignores it — only explicit PMTU probing escapes.",
		hole.repairWasted, cfg.MaxBitrate/1e6)
}

// TestPMTU_CCReactsToDelayButNotSize shows the controller is not simply inert: fed genuine
// queueing delay it DOES cut the budget. So its silence in the black-hole case above is
// specific blindness to size loss, not a broken controller.
func TestPMTU_CCReactsToDelayButNotSize(t *testing.T) {
	const mss = 1316 + symHeaderBytes
	cc := newCongestionController(0.1, mss, 1_000_000_000)             // 1 Gbps ceiling
	b := &bottleneck{capBytesPerSec: 2_500_000, baseRTTMicros: 40_000} // 20 Mbps, 40 ms
	rate := run(cc, b, 6000, 5_000)                                    // 30 s of delay-based control
	ceilBytes := int64(1_000_000_000 / 8)
	if rate >= ceilBytes {
		t.Fatalf("CC did not back off under genuine queueing delay: rate %d still at/above ceiling %d", rate, ceilBytes)
	}
	t.Logf("under genuine queue delay the CC cut the budget to %.1f Mbps (≈ the 20 Mbps bottleneck) — the controller works; it is specifically blind to SIZE loss, which produces no queue and no marks.", float64(rate)*8/1e6)
}
