// Command soak is the real-path soak harness: it drives a meld Sender and
// Receiver across two real hosts through the public API and scores the run with
// the same arbitered discipline as the loopback bench — so the mechanisms the
// lab cannot adjudicate (capacity estimation, real jitter, real reorder) meet
// their arbiter the afternoon a second endpoint exists.
//
// One static binary, three uses:
//
//	soak -rx  -listen :7601 -runs 8 -out reports/          # on the far box
//	soak -tx  -to host:7601 -arm headroom -mbps 8 -dur 30s # one run
//	soak -tx  -to host:7601 -arms default,headroom -reps 4 # the A/B protocol
//
// The receiver accepts successive runs (one fresh Receiver per run, keyed by the
// runID the sender stamps in every chunk), scores each against the deadline
// budget, and emits one JSON report per run plus a one-line summary. The sender
// prints a per-second timeline of the stats deltas — the confirmed-clean floor
// decay is visible as the proactive column collapsing to ~0 and re-arming on
// loss, and HeadroomAwareSizing engagement is the tightens column.
//
// Clocks: chunks carry the sender's wall clock, so raw one-way latency includes
// the inter-host clock offset. Reports use MIN-ANCHORED relative latency
// (lat − min(lat) over the run), the same convention the loopback bench applies
// to its ARQ anchors: the minimum sample stands in for propagation + offset, and
// a chunk is IN TIME when its relative latency fits the budget less a 20 ms
// release guard. Run NTP on both boxes anyway; the anchor only has to be stable
// within a run. SRT/RIST anchor comparisons ride txbench's adapters (separate
// module — those C stacks are outside this repo's dependency allowlist); see
// docs/soak.md for the pre-registered protocol and bars.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	meld "github.com/zsiec/meld"
)

// Chunk header layout (BigEndian), padded to the symbol size with zeros:
//
//	0:4   magic "MSK1"
//	4:8   runID
//	8:12  seq
//	12:20 tx wall clock, unix micros
//	20    flags (bit0 = FIN; seq carries the run's total chunk count)
//	21    arm-name length, then the arm name bytes
const (
	soakMagic   = "MSK1"
	headerFixed = 22
	flagFIN     = 1

	finRepeats    = 5                       // FIN copies (loss-tolerant end-of-run marker)
	finSpacing    = 20 * time.Millisecond   // spacing between FIN copies
	runIdleCutoff = 3 * time.Second         // receiver: silence that ends a run without a FIN
	interRunGap   = 2500 * time.Millisecond // sender: rebind window between A/B runs
	releaseGuard  = 20 * time.Millisecond   // arbitered deadline guard (release jitter allowance)
)

func main() {
	log.SetFlags(0)
	var (
		rx     = flag.Bool("rx", false, "receiver mode: accept runs and score them")
		tx     = flag.Bool("tx", false, "sender mode: drive one run or an A/B sequence")
		listen = flag.String("listen", ":7601", "rx: UDP bind address")
		out    = flag.String("out", ".", "rx: directory for per-run JSON reports")
		runs   = flag.Int("runs", 1, "rx: how many runs to accept before exiting (0 = forever)")
		to     = flag.String("to", "", "tx: receiver host:port")
		arm    = flag.String("arm", "default", "tx: config arm for a single run: "+armNames())
		arms   = flag.String("arms", "", "tx: comma list of arms for the interleaved A/B protocol (overrides -arm)")
		reps   = flag.Int("reps", 4, "tx: repetitions per arm in A/B mode")
		mbps   = flag.Float64("mbps", 8, "tx: offered source bitrate")
		dur    = flag.Duration("dur", 30*time.Second, "tx: source duration per run")
		budget = flag.Duration("budget", 150*time.Millisecond, "playout deadline budget (both ends must agree)")
		window = flag.Int("window", 256, "sliding CodingWindow (both ends must agree)")
		red    = flag.Float64("red", -1, "Redundancy floor override (<0 = default)")
	)
	flag.Parse()
	base := func() meld.Config {
		cfg := meld.DefaultConfig()
		cfg.Flow = 1
		cfg.BufferMicros = budget.Microseconds()
		cfg.CodingWindow = *window
		if *red >= 0 {
			cfg.Redundancy = *red
		}
		return cfg
	}
	switch {
	case *rx == *tx:
		log.Fatal("soak: exactly one of -rx / -tx is required")
	case *rx:
		runReceiver(*listen, *out, *runs, *budget, base)
	default:
		if *to == "" {
			log.Fatal("soak: -tx requires -to host:port")
		}
		list := []string{*arm}
		if *arms != "" {
			list = strings.Split(*arms, ",")
		}
		runSender(*to, list, *reps, *mbps, *dur, base)
	}
}

// arms maps arm names to config mutations. Every arm starts from the shared
// base (DefaultConfig + the both-ends flags) so an A/B isolates one lever.
var armMods = map[string]func(*meld.Config){
	"default":  func(*meld.Config) {},
	"headroom": func(c *meld.Config) { c.HeadroomAwareSizing = true },
	"shift":    func(c *meld.Config) { c.SlidingReactiveShift = true },
	"gen":      func(c *meld.Config) { c.Sliding = false },
}

func armNames() string {
	names := make([]string, 0, len(armMods))
	for n := range armMods {
		names = append(names, n)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// ---- sender ----

// senderReport is the tx-side JSON emitted after each run.
type senderReport struct {
	RunID       uint32  `json:"run_id"`
	Arm         string  `json:"arm"`
	Chunks      int     `json:"chunks"`
	DurationSec float64 `json:"duration_sec"`
	Mbps        float64 `json:"mbps"`
	Source      uint64  `json:"source"`
	Repair      uint64  `json:"repair"`
	Reactive    uint64  `json:"reactive"`
	Throttled   uint64  `json:"throttled"`
	Tightens    uint64  `json:"headroom_tightens"`
	OverheadPct float64 `json:"overhead_pct"`
}

func runSender(to string, arms []string, reps int, mbps float64, dur time.Duration, base func() meld.Config) {
	for _, a := range arms {
		if _, ok := armMods[a]; !ok {
			log.Fatalf("soak: unknown arm %q (have %s)", a, armNames())
		}
	}
	runID := uint32(time.Now().Unix()) // distinct across invocations; +1 per run
	single := len(arms) == 1 && reps == 1
	var reports []senderReport
	for rep := 0; rep < reps; rep++ {
		for _, a := range arms {
			if !single {
				log.Printf("=== run %d arm=%s (rep %d/%d) ===", runID, a, rep+1, reps)
			}
			rp, err := sendOneRun(to, a, runID, mbps, dur, base)
			if err != nil {
				log.Fatalf("soak: run %d (%s): %v", runID, a, err)
			}
			reports = append(reports, rp)
			runID++
			if !single {
				time.Sleep(interRunGap) // let the far end rebind a fresh Receiver
			}
		}
	}
	log.Printf("--- sender summary (arbitered scores live in the rx reports) ---")
	for _, rp := range reports {
		log.Printf("run %d %-9s chunks=%d overhead=%.1f%% reactive=%d tightens=%d throttled=%d",
			rp.RunID, rp.Arm, rp.Chunks, rp.OverheadPct, rp.Reactive, rp.Tightens, rp.Throttled)
	}
	enc := json.NewEncoder(os.Stdout)
	for _, rp := range reports {
		if err := enc.Encode(rp); err != nil {
			log.Fatalf("soak: encode report: %v", err)
		}
	}
}

func sendOneRun(to, arm string, runID uint32, mbps float64, dur time.Duration, base func() meld.Config) (senderReport, error) {
	cfg := base()
	armMods[arm](&cfg)
	s, err := meld.NewSender(to, cfg)
	if err != nil {
		return senderReport{}, err
	}
	defer s.Close()

	payload := make([]byte, cfg.SymbolSize)
	copy(payload, soakMagic)
	binary.BigEndian.PutUint32(payload[4:], runID)
	if n := copy(payload[headerFixed:], arm); n < len(arm) {
		return senderReport{}, fmt.Errorf("arm name %q does not fit the chunk header", arm)
	}
	payload[21] = byte(len(arm))

	interval := time.Duration(float64(len(payload)*8) / (mbps * 1e6) * float64(time.Second))
	total := int(dur / interval)
	log.Printf("run %d arm=%s: %d chunks of %dB every %v (%.2f Mbps) for %v -> %s",
		runID, arm, total, len(payload), interval.Round(time.Microsecond), mbps, dur.Round(time.Second), to)

	start := time.Now()
	next := start
	lastLine := start
	var last meld.SenderStats
	for seq := 0; seq < total; seq++ {
		binary.BigEndian.PutUint32(payload[8:], uint32(seq))
		binary.BigEndian.PutUint64(payload[12:], uint64(time.Now().UnixMicro()))
		payload[20] = 0
		if _, err := s.Write(payload); err != nil {
			return senderReport{}, fmt.Errorf("write seq %d: %w", seq, err)
		}
		next = next.Add(interval)
		if d := time.Until(next); d > 0 {
			time.Sleep(d)
		}
		// Per-second timeline: the floor decay reads as proact/s collapsing to ~0
		// (and re-arming on loss evidence); headroom engagement as tightens.
		if now := time.Now(); now.Sub(lastLine) >= time.Second {
			st := s.Stats()
			dRep := st.Repair - last.Repair
			dRx := st.ReactiveRepair - last.ReactiveRepair
			log.Printf("t=%4.1fs src=%d proact/s=%d react/s=%d tightens=%d throttled=%d",
				now.Sub(start).Seconds(), st.Source, dRep-dRx, dRx, st.HeadroomTightens, st.Throttled)
			last, lastLine = st, now
		}
	}
	s.Flush()
	time.Sleep(2 * time.Duration(cfg.BufferMicros) * time.Microsecond) // let the tail land
	payload[20] = flagFIN
	binary.BigEndian.PutUint32(payload[8:], uint32(total)) // FIN carries the run's chunk count
	for i := 0; i < finRepeats; i++ {
		binary.BigEndian.PutUint64(payload[12:], uint64(time.Now().UnixMicro()))
		// Best-effort: one landing suffices, and the receiver closes its socket the
		// moment it reads one, so later copies can bounce (connection refused).
		if _, err := s.Write(payload); err != nil {
			break
		}
		time.Sleep(finSpacing)
	}
	s.Flush()
	time.Sleep(500 * time.Millisecond)
	st := s.Stats()
	rp := senderReport{
		RunID: runID, Arm: arm, Chunks: total,
		DurationSec: time.Since(start).Seconds(), Mbps: mbps,
		Source: st.Source, Repair: st.Repair, Reactive: st.ReactiveRepair,
		Throttled: st.Throttled, Tightens: st.HeadroomTightens,
	}
	if st.Source > 0 {
		rp.OverheadPct = 100 * float64(st.Repair) / float64(st.Source)
	}
	return rp, nil
}

// ---- receiver ----

// rxReport is the per-run receiver-side JSON: the arbitered score.
type rxReport struct {
	RunID     uint32  `json:"run_id"`
	Arm       string  `json:"arm"`
	Total     int     `json:"total"`     // sender's chunk count (from FIN; -1 if the run ended on idle)
	Delivered int     `json:"delivered"` // distinct chunks read
	InTime    int     `json:"in_time"`   // relative latency within budget - guard (the arbitered count)
	Dups      int     `json:"dups"`      // duplicate seqs surfaced by Read (must be 0)
	Reorders  int     `json:"reorders"`  // out-of-order deliveries surfaced by Read (must be 0)
	LostPct   float64 `json:"lost_pct"`
	InTimePct float64 `json:"in_time_pct"`
	RelP50Ms  float64 `json:"rel_p50_ms"` // min-anchored relative latency percentiles
	RelP95Ms  float64 `json:"rel_p95_ms"`
	RelP99Ms  float64 `json:"rel_p99_ms"`
	WireLost  uint64  `json:"wire_lost"`
	Recovered uint64  `json:"recovered"`
	Outages   uint64  `json:"outages"`
}

func runReceiver(listen, outDir string, runs int, budget time.Duration, base func() meld.Config) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("soak: -out %s: %v", outDir, err)
	}
	log.Printf("soak rx: listening on %s, budget %v, reports -> %s", listen, budget, outDir)
	for done := 0; runs == 0 || done < runs; done++ {
		rp, err := receiveOneRun(listen, budget, base)
		if err != nil {
			log.Fatalf("soak rx: %v", err)
		}
		log.Printf("run %d %-9s delivered=%d/%d inTime=%.2f%% lost=%.2f%% relP50=%.1fms relP99=%.1fms dups=%d reorders=%d recovered=%d wireLost=%d",
			rp.RunID, rp.Arm, rp.Delivered, rp.Total, rp.InTimePct, rp.LostPct,
			rp.RelP50Ms, rp.RelP99Ms, rp.Dups, rp.Reorders, rp.Recovered, rp.WireLost)
		buf, err := json.MarshalIndent(rp, "", "  ")
		if err != nil {
			log.Fatalf("soak rx: marshal: %v", err)
		}
		path := filepath.Join(outDir, fmt.Sprintf("soak_run%d_%s.json", rp.RunID, rp.Arm))
		if err := os.WriteFile(path, buf, 0o644); err != nil {
			log.Fatalf("soak rx: write %s: %v", path, err)
		}
	}
}

func receiveOneRun(listen string, budget time.Duration, base func() meld.Config) (rxReport, error) {
	// A fresh Receiver per run: each run restarts the sequence space, so state
	// must not leak across runs. Arm-specific sender knobs are sender-side only;
	// the receiver config is the shared base for every arm.
	r, err := meld.NewReceiver(listen, base())
	if err != nil {
		return rxReport{}, err
	}
	defer r.Close()

	rp := rxReport{Total: -1}
	buf := make([]byte, 64*1024)
	seen := make(map[uint32]bool)
	var lats []float64 // raw one-way latencies, ms (offset-polluted; min-anchored below)
	lastSeq := int64(-1)
	started := false
	for {
		if err := r.SetReadDeadline(time.Now().Add(runIdleCutoff)); err != nil {
			return rxReport{}, err
		}
		n, err := r.Read(buf)
		if err != nil {
			if started {
				break // idle after traffic: the run is over (FIN lost or sender gone)
			}
			continue // idle before any traffic: keep waiting for the run to start
		}
		now := time.Now().UnixMicro()
		if n < headerFixed || string(buf[:4]) != soakMagic {
			continue // not a soak chunk (foreign traffic on the port)
		}
		runID := binary.BigEndian.Uint32(buf[4:])
		seq := binary.BigEndian.Uint32(buf[8:])
		txMicros := int64(binary.BigEndian.Uint64(buf[12:]))
		if !started {
			started = true
			rp.RunID = runID
			if l := int(buf[21]); headerFixed+l <= n {
				rp.Arm = string(buf[headerFixed : headerFixed+l])
			}
		}
		if runID != rp.RunID {
			continue // stray chunk from an adjacent run
		}
		if buf[20]&flagFIN != 0 {
			rp.Total = int(seq)
			break
		}
		if seen[seq] {
			rp.Dups++
			continue
		}
		seen[seq] = true
		rp.Delivered++
		if int64(seq) < lastSeq {
			rp.Reorders++
		}
		lastSeq = int64(seq)
		lats = append(lats, float64(now-txMicros)/1000)
	}
	st := r.Stats()
	rp.WireLost, rp.Recovered, rp.Outages = st.WireLost, st.Recovered, st.Outages

	if len(lats) > 0 {
		minLat := lats[0]
		for _, l := range lats {
			if l < minLat {
				minLat = l
			}
		}
		rel := make([]float64, len(lats))
		bar := budget.Seconds()*1000 - releaseGuard.Seconds()*1000
		for i, l := range lats {
			rel[i] = l - minLat
			if rel[i] <= bar {
				rp.InTime++
			}
		}
		sort.Float64s(rel)
		pct := func(p float64) float64 { return rel[int(p*float64(len(rel)-1))] }
		rp.RelP50Ms, rp.RelP95Ms, rp.RelP99Ms = pct(0.50), pct(0.95), pct(0.99)
	}
	if rp.Total > 0 {
		rp.LostPct = 100 * float64(rp.Total-rp.Delivered) / float64(rp.Total)
		rp.InTimePct = 100 * float64(rp.InTime) / float64(rp.Total)
	}
	return rp, nil
}
