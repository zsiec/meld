package flow

import (
	"bytes"
	"testing"

	"github.com/zsiec/meld/internal/clock"
	"github.com/zsiec/meld/internal/wire"
)

func generationMetadataConfig() Config {
	return Config{
		Flow: 1, SymbolSize: 32, GenSize: 3, Redundancy: 1,
		TargetFailure: 1e-3, BufferMicros: 100_000,
	}
}

func TestGenerationRecoveryPreservesExactLength(t *testing.T) {
	cfg := generationMetadataConfig()
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	sources := [][]byte{{1, 2, 3}, {4, 5, 6, 7, 8, 9, 10}, {11, 12, 13, 14, 15}}
	for i, src := range sources {
		s.Write(clock.Timestamp(1_000+i*1_000), src)
	}
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Systematic && sym.SrcIndex == 1 {
			continue
		}
		r.FeedSymbol(20_000, d)
	}
	for id, want := range sources {
		gotID, got, ok := r.PollDeliver()
		if !ok {
			t.Fatalf("missing delivery %d", id)
		}
		if gotID != uint32(id) || !bytes.Equal(got, want) {
			t.Fatalf("delivery %d = (id=%d data=%v), want (id=%d data=%v)", id, gotID, got, id, want)
		}
	}
}

func TestSendersEmitCompactSystematicPayloads(t *testing.T) {
	for _, sliding := range []bool{false, true} {
		cfg := generationMetadataConfig()
		cfg.Sliding = sliding
		var sender coreSenderT
		if sliding {
			sender = NewSlidingSender(cfg)
		} else {
			sender = NewSender(cfg)
		}
		want := []byte{1, 2, 3, 4, 5}
		sender.Write(1, want)
		d, ok := sender.PollSend()
		if !ok {
			t.Fatalf("sliding=%v: missing systematic", sliding)
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatal(err)
		}
		if sym.Kind != wire.Systematic || sym.SourceLength != uint32(len(want)) || !bytes.Equal(sym.Payload, want) {
			t.Fatalf("sliding=%v: systematic kind=%v sourceLen=%d payload=%v",
				sliding, sym.Kind, sym.SourceLength, sym.Payload)
		}
	}
}

func TestSystematicPayloadLengthForms(t *testing.T) {
	tests := []struct {
		name string
		sym  wire.Symbol
		want int
		ok   bool
	}{
		{"compact", wire.Symbol{HasSourceLength: true, SourceLength: 3, Payload: make([]byte, 3)}, 3, true},
		{"padded", wire.Symbol{HasSourceLength: true, SourceLength: 3, Payload: make([]byte, 8)}, 0, false},
		{"full-width", wire.Symbol{Payload: make([]byte, 8)}, 8, true},
		{"truncated-compact", wire.Symbol{HasSourceLength: true, SourceLength: 3, Payload: make([]byte, 2)}, 0, false},
		{"ambiguous-middle", wire.Symbol{HasSourceLength: true, SourceLength: 3, Payload: make([]byte, 5)}, 0, false},
		{"short-no-extension", wire.Symbol{Payload: make([]byte, 3)}, 0, false},
		{"declared-too-large", wire.Symbol{HasSourceLength: true, SourceLength: 9, Payload: make([]byte, 8)}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := systematicSourceLength(tt.sym, 8)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("systematicSourceLength = (%d,%v), want (%d,%v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestCompactRepairPayloadRoundTrip(t *testing.T) {
	const symbolSize = 16
	full := makeCodedSource([]byte{1, 2, 3, 4, 5}, symbolSize, 123_456)
	compact, prefix, ok := compactRepairPayload(full, symbolSize)
	if !ok || prefix != 5 || len(compact) != 5+codedSymbolMetadataBytes {
		t.Fatalf("compact repair = prefix %d len %d ok=%v", prefix, len(compact), ok)
	}
	sym := wire.Symbol{Kind: wire.Repair, HasSourceLength: true, SourceLength: uint32(prefix), Payload: compact}
	expanded, ok := expandRepairPayload(sym, symbolSize)
	if !ok || !bytes.Equal(expanded, full) {
		t.Fatalf("expanded repair = %v ok=%v, want %v", expanded, ok, full)
	}
	scratch := make([]byte, codedSymbolSize(symbolSize))
	reused, ok := expandRepairPayloadInto(sym, symbolSize, scratch)
	if !ok || !bytes.Equal(reused, full) || &reused[0] != &scratch[0] {
		t.Fatal("compact repair did not reuse the supplied expansion buffer")
	}

	malformed := sym
	malformed.Payload = malformed.Payload[:len(malformed.Payload)-1]
	if _, ok := expandRepairPayload(malformed, symbolSize); ok {
		t.Fatal("truncated compact repair was accepted")
	}
	tooWide := sym
	tooWide.SourceLength = symbolSize + 1
	if _, ok := expandRepairPayload(tooWide, symbolSize); ok {
		t.Fatal("overwide compact repair was accepted")
	}
}

func TestReceiversRejectMalformedCompactRepair(t *testing.T) {
	cfg := generationMetadataConfig()
	malformed := wire.EncodeSymbol(nil, wire.Symbol{
		Flow: cfg.Flow, Kind: wire.Repair, WindowBase: 0, N: 1,
		HasSourceLength: true, SourceLength: 3,
		Payload: make([]byte, 3+codedSymbolMetadataBytes-1),
	})
	tests := []struct {
		name string
		new  func() coreRejectingReceiver
	}{
		{"generation", func() coreRejectingReceiver { return NewReceiver(cfg) }},
		{"sliding", func() coreRejectingReceiver {
			slidingCfg := cfg
			slidingCfg.Sliding = true
			return NewSlidingReceiver(slidingCfg)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.new()
			r.FeedSymbol(1, malformed)
			if got := r.Stats().Rejected; got != 1 {
				t.Fatalf("Rejected = %d, want 1", got)
			}
		})
	}
}

type coreRejectingReceiver interface {
	FeedSymbol(clock.Timestamp, []byte)
	Stats() ReceiverStats
}

func TestGenerationCompactRepairRecoversExactSource(t *testing.T) {
	cfg := generationMetadataConfig()
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	sources := [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}}
	for i, src := range sources {
		s.Write(clock.Timestamp(1_000+i*1_000), src)
	}
	compactSeen := false
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatal(err)
		}
		if sym.Kind == wire.Systematic && sym.SrcIndex == 1 {
			continue
		}
		if sym.Kind == wire.Repair {
			if !sym.HasSourceLength || len(sym.Payload) >= codedSymbolSize(cfg.SymbolSize) {
				t.Fatalf("repair was not compact: sourceLength=%d payload=%d", sym.SourceLength, len(sym.Payload))
			}
			compactSeen = true
		}
		r.FeedSymbol(20_000, d)
	}
	if !compactSeen {
		t.Fatal("sender emitted no compact repair")
	}
	for id, want := range sources {
		gotID, got, ok := r.PollDeliver()
		if !ok || gotID != uint32(id) || !bytes.Equal(got, want) {
			t.Fatalf("delivery %d = id %d data %v ok=%v", id, gotID, got, ok)
		}
	}
}

func TestGenerationCompactRepairPreservesEquationAndControlCharge(t *testing.T) {
	cfg := generationMetadataConfig()
	s := NewSender(cfg)

	for i, src := range [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}} {
		now := clock.Timestamp(1_000 + i*1_000)
		s.Write(now, src)
	}

	firstRepair := func(t *testing.T, s *Sender) ([]byte, wire.Symbol) {
		t.Helper()
		for {
			d, ok := s.PollSend()
			if !ok {
				t.Fatal("sender emitted no repair")
			}
			sym, err := wire.DecodeSymbol(d)
			if err != nil {
				t.Fatal(err)
			}
			if sym.Kind == wire.Repair {
				return d, sym
			}
		}
	}

	compactDatagram, compactRepair := firstRepair(t, s)
	expanded, ok := expandRepairPayload(compactRepair, cfg.SymbolSize)
	if !ok {
		t.Fatal("compact repair did not expand")
	}
	fullRepair := compactRepair
	fullRepair.HasSourceLength = false
	fullRepair.SourceLength = 0
	fullRepair.Payload = expanded
	fullDatagram := wire.EncodeSymbol(nil, fullRepair)
	if !compactRepair.HasSourceLength || len(compactDatagram) >= len(fullDatagram) {
		t.Fatalf("compact repair did not save bytes: full=%d compact=%d sourceLength=%v",
			len(fullDatagram), len(compactDatagram), compactRepair.HasSourceLength)
	}
	reencoded, charge := encodeSymbol(fullRepair, cfg.SymbolSize)
	if !bytes.Equal(reencoded, compactDatagram) || charge != len(fullDatagram) {
		t.Fatalf("compact encoding changed equation or controller charge: charge=%d want=%d", charge, len(fullDatagram))
	}
	wantSaved := uint64(len(fullDatagram) - len(compactDatagram))
	if s.stats.RepairCompacted == 0 || s.stats.RepairBytesSaved < wantSaved {
		t.Fatalf("compact telemetry = count %d saved %d, want at least one packet and %d bytes",
			s.stats.RepairCompacted, s.stats.RepairBytesSaved, wantSaved)
	}
}

func TestGenerationRecoveredExactDeadlineRejectsLateInteriorSymbol(t *testing.T) {
	cfg := generationMetadataConfig()
	cfg.BufferMicros = 50_000
	s := NewSender(cfg)
	r := NewReceiver(cfg)
	sources := [][]byte{{1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11, 12}}
	// The first two chunks are a burst. Neighbor interpolation would assign id 1
	// a 60ms deadline between id 0 (50ms) and id 2 (70ms), but its exact deadline
	// is 50ms. Feed recovery at 55ms to distinguish the two behaviors.
	for i, at := range []clock.Timestamp{1, 1, 20_000} {
		s.Write(at, sources[i])
	}

	var repairs [][]byte
	for {
		d, ok := s.PollSend()
		if !ok {
			break
		}
		sym, err := wire.DecodeSymbol(d)
		if err != nil {
			t.Fatalf("DecodeSymbol: %v", err)
		}
		if sym.Kind == wire.Systematic {
			if sym.SrcIndex == 0 {
				r.FeedSymbol(30_000, d)
			}
			continue // ids 1 and 2 must be recovered from coded metadata
		}
		repairs = append(repairs, d)
	}
	for _, d := range repairs {
		r.FeedSymbol(55_000, d) // id 1 due at 50ms; id 2 remains live until 70ms
	}

	id, got, ok := r.PollDeliver()
	if !ok || id != 0 || !bytes.Equal(got, sources[0]) {
		t.Fatalf("first delivery = (id=%d data=%v ok=%v)", id, got, ok)
	}
	id, got, ok = r.PollDeliver()
	if !ok || id != 2 || !bytes.Equal(got, sources[2]) {
		t.Fatalf("post-deadline delivery = (id=%d data=%v ok=%v), want exact id 2", id, got, ok)
	}
	if _, _, ok := r.PollDeliver(); ok {
		t.Fatal("late interior symbol was delivered")
	}
	if st := r.Stats(); st.Lost != 1 || st.Recovered < 1 {
		t.Fatalf("stats = %+v, want one exact-deadline loss and coded recovery", st)
	}
}

func TestGenerationMDSBlockBound(t *testing.T) {
	if got := maxBlockRepair(254); got != 1 {
		t.Fatalf("maxBlockRepair(254) = %d, want 1", got)
	}
	if got := maxBlockRepair(255); got != 0 {
		t.Fatalf("maxBlockRepair(255) = %d, want 0", got)
	}
	cfg := generationMetadataConfig()
	cfg.GenSize = 512
	cfg.AutoGenSize = false
	if got := NewSender(cfg).genWidthNow(); got != 254 {
		t.Fatalf("bounded generation width = %d, want 254", got)
	}
}
