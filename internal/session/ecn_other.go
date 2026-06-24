//go:build !darwin && !linux

package session

import "github.com/zsiec/meld/internal/flow"

// setECN is a no-op where the ECN socket options are not exposed here: datagrams go out NotECT
// and the receiver reads no codepoint, so the L4S marking path stays inert and the congestion
// controller runs delay-only (Copa). The marking is most meaningful on darwin/linux.
func setECN(Substrate) error { return nil }

// parseRecvECN reports no codepoint on these platforms (recv-TOS is never enabled).
func parseRecvECN([]byte) flow.ECN { return flow.NotECT }
