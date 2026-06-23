package meld_test

import (
	"strings"
	"testing"

	"github.com/zsiec/meld"
)

// TestConfigCheck pins the AdaptiveGenSize misconfiguration warnings: a sound config (or one
// with the lever off) is silent; the lever on without NominalRTTMicros warns it is inert; the
// lever on with an RTT hint but no NominalBitrateBps warns the fill gate is off; the lever fully
// configured is silent. The sliding coder (no generations) never warns.
func TestConfigCheck(t *testing.T) {
	base := meld.DefaultConfig()
	base.SymbolSize, base.GenSize, base.BufferMicros = 1316, 16, 200_000

	cases := []struct {
		name    string
		mutate  func(c *meld.Config)
		wantSub string // "" ⇒ expect no warning
	}{
		// AutoGenSize is on by DefaultConfig and needs no hints, so the default config is silent;
		// the AdaptiveGenSize warnings are about the MANUAL form, so those cases turn AutoGenSize off
		// (it takes precedence and would otherwise suppress the warning, correctly).
		{"default (AutoGenSize on) — silent", func(c *meld.Config) {}, ""},
		{"AdaptiveGenSize, no RTT hint", func(c *meld.Config) {
			c.AutoGenSize, c.AdaptiveGenSize = false, true
		}, "NominalRTTMicros"},
		{"AdaptiveGenSize, RTT but no bitrate", func(c *meld.Config) {
			c.AutoGenSize, c.AdaptiveGenSize, c.NominalRTTMicros = false, true, 40_000
		}, "NominalBitrateBps"},
		{"AdaptiveGenSize fully configured", func(c *meld.Config) {
			c.AutoGenSize, c.AdaptiveGenSize, c.NominalRTTMicros, c.NominalBitrateBps = false, true, 40_000, 50_000_000
		}, ""},
		{"AutoGenSize on suppresses the AdaptiveGenSize warning", func(c *meld.Config) {
			c.AdaptiveGenSize = true // AutoGenSize already on (default) ⇒ no warning
		}, ""},
		{"sliding never warns", func(c *meld.Config) {
			c.AutoGenSize, c.Sliding, c.AdaptiveGenSize = false, true, true
		}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			got := c.Check()
			if tc.wantSub == "" {
				if len(got) != 0 {
					t.Fatalf("expected no warnings, got %v", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], tc.wantSub) {
				t.Fatalf("expected one warning mentioning %q, got %v", tc.wantSub, got)
			}
		})
	}
}
