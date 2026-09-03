package flow

import "testing"

func TestAffordableSourceBitratePricesHeadersAndRecovery(t *testing.T) {
	// Ten source packets/sec. Each packet has 80 payload + 20 header bytes and
	// requires 0.2 of a 100-byte repair: the 1,000 B/s total budget therefore
	// leaves 60 payload bytes/packet = 4,800 bit/s.
	offered, affordable := affordableSourceBitrate(1_000, 100_000, 100, 80, 100, .2)
	if offered != 6_400 || affordable != 4_800 {
		t.Fatalf("offered/affordable = %d/%d, want 6400/4800", offered, affordable)
	}
}

func TestAffordableSourceBitratePreservesMediaShare(t *testing.T) {
	// An extreme recovery prediction is capped at 40 of the 100 bytes available
	// per source interval. After the 20-byte systematic header, 40 payload bytes
	// remain: the encoder is never advised below 3,200 bit/s here.
	_, affordable := affordableSourceBitrate(1_000, 100_000, 100, 80, 100, 10)
	if affordable != 3_200 {
		t.Fatalf("affordable = %d, want media-share floor 3200", affordable)
	}
}

func TestBitrateAdvisorIsStickyAndHysteretic(t *testing.T) {
	var a bitrateAdvisor
	a.observe(10_000, 7_000)
	if a.control() != 0 {
		t.Fatalf("activated on one overload report: %d", a.control())
	}
	a.observe(10_000, 7_000)
	if a.control() != 7_000 {
		t.Fatalf("target after confirmed overload = %d, want 7000", a.control())
	}
	// Encoder compliance must not clear the very request that caused it.
	for i := 0; i < 20; i++ {
		a.observe(7_000, 7_000)
	}
	if a.control() == 0 {
		t.Fatal("compliance cleared the sticky target")
	}
	// A worse envelope reduces immediately.
	a.observe(7_000, 5_000)
	if a.control() != 5_000 {
		t.Fatalf("worse envelope target = %d, want immediate 5000", a.control())
	}
	// Only sustained capacity for the original 10 kbps offer clears it.
	for i := 0; i < bitrateAdviceClearReports-1; i++ {
		a.observe(5_000, 11_000)
	}
	if a.control() == 0 {
		t.Fatal("target cleared before the hysteresis window")
	}
	a.observe(5_000, 11_000)
	if a.control() != 0 {
		t.Fatalf("target after sustained restored capacity = %d, want 0", a.control())
	}
}
