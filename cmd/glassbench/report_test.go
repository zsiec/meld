package main

import (
	"testing"

	"github.com/zsiec/meld/internal/shape"
)

func TestFailureAttributionReportsErasedRepairForDependencyIsland(t *testing.T) {
	c := &chunked{
		chunks: [][]byte{
			{0, 0, 0, 0, 1},
			{0, 0, 0, 1, 2},
			{0, 0, 0, 2, 3},
		},
		units: []shape.Unit{
			{ID: 0, Class: shape.ClassRAP, RAP: true, Picture: true, Size: 1},
			{ID: 1, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{0}, Size: 1},
			{ID: 2, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{1}, Size: 1},
		},
		shaped: []shape.Shaped{
			{Unit: shape.Unit{ID: 0, Class: shape.ClassRAP, RAP: true, Picture: true, Size: 1}},
			{Unit: shape.Unit{ID: 1, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{0}, Size: 1}},
			{Unit: shape.Unit{ID: 2, Class: shape.ClassBase, Picture: true, RefersTo: []uint32{1}, Size: 1}},
		},
		unitChunks: map[uint32][]uint32{
			0: {0},
			1: {1},
			2: {2},
		},
		chunkSize: 1,
	}
	seqs := map[uint32]bool{0: true, 2: true}
	tr := &seedTrace{Relay: []relayTraceEvent{
		{Kind: "systematic", SrcIndex: 0, Deadline: 1_000, RelayTimestamp: 100},
		{Kind: "systematic", SrcIndex: 1, Deadline: 1_000, RelayTimestamp: 110, Dropped: true},
		{Kind: "systematic", SrcIndex: 2, Deadline: 1_000, RelayTimestamp: 120},
		{Kind: "sparse_repair", SparseIDs: []uint32{1}, Deadline: 1_000, RelayTimestamp: 130, Dropped: true},
	}}

	got := failureAttributionFor(c, seqs, tr, missingSummaryFor(c, seqs))
	if got.Kind != "broken_dependency" {
		t.Fatalf("kind = %q, want broken_dependency", got.Kind)
	}
	if got.Island != traceIslandFirstBaseChain {
		t.Fatalf("island = %q, want %q", got.Island, traceIslandFirstBaseChain)
	}
	if !got.SourceDependency || got.DependencyRef != 1 {
		t.Fatalf("dependency fields = source_dep %t ref %d, want true/1", got.SourceDependency, got.DependencyRef)
	}
	if got.RepairInTime != 1 || got.RepairDropped != 1 || got.RepairSurvived != 0 || !got.RepairErased {
		t.Fatalf("repair attribution = in_time %d dropped %d survived %d erased %t, want 1/1/0/true",
			got.RepairInTime, got.RepairDropped, got.RepairSurvived, got.RepairErased)
	}
	if got.Cause != "source_dependency" {
		t.Fatalf("cause = %q, want source_dependency", got.Cause)
	}
}
