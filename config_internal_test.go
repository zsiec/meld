package meld

import "testing"

func TestConfigToFlowCopiesPMTUD(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ProbeMTU = true
	cfg.MaxProbeMTU = 1400

	fc := cfg.toFlow()
	if !fc.ProbeMTU {
		t.Fatal("toFlow did not copy ProbeMTU")
	}
	if fc.MaxProbeMTU != cfg.MaxProbeMTU {
		t.Fatalf("toFlow MaxProbeMTU = %d, want %d", fc.MaxProbeMTU, cfg.MaxProbeMTU)
	}
}

func TestDefaultConfigUsesSlidingMainProfile(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Sliding {
		t.Fatal("DefaultConfig should select the sliding main profile")
	}
	fc := cfg.toFlow()
	if !fc.Sliding {
		t.Fatal("toFlow did not preserve the default sliding main profile")
	}
	if !fc.ProtectedRepairPhasing {
		t.Fatal("default flow config should enable automatic protected repair phasing")
	}
	mp := cfg.toFlowPaths(2)
	if mp.Sliding {
		t.Fatal("multipath fallback should use the generation profile until sliding multipath is implemented")
	}
	if mp.Paths != 2 {
		t.Fatalf("toFlowPaths paths = %d, want 2", mp.Paths)
	}
}

func TestConfigCheckAcceptsSlidingCongestionControl(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Sliding = true
	cfg.CongestionControl = true
	if warns := cfg.Check(); len(warns) != 0 {
		t.Fatalf("supported Sliding+CongestionControl returned warnings: %v", warns)
	}
}
