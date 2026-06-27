package session

import (
	"testing"

	"github.com/zsiec/meld/internal/flow"
)

func TestUsesL4SMarkingOnlyWhenControllerConsumesECN(t *testing.T) {
	if usesL4SMarking(flow.Config{}) {
		t.Fatal("default config should not mark ECT(1)")
	}
	if !usesL4SMarking(flow.Config{CongestionControl: true}) {
		t.Fatal("generation congestion-control profile should mark ECT(1)")
	}
	if usesL4SMarking(flow.Config{CongestionControl: true, Sliding: true}) {
		t.Fatal("sliding profile ignores ECN today, so it must not mark ECT(1)")
	}
}

func TestUsesECNReceiveForGenerationRegardlessOfLocalController(t *testing.T) {
	if !usesECNReceive(flow.Config{}) {
		t.Fatal("generation receiver should enable ECN ancillary reads even if local sender CC is off")
	}
	if !usesECNReceive(flow.Config{CongestionControl: true}) {
		t.Fatal("generation receiver with CC should enable ECN ancillary reads")
	}
	if usesECNReceive(flow.Config{Sliding: true, CongestionControl: true}) {
		t.Fatal("sliding receiver does not consume ECN today")
	}
}
