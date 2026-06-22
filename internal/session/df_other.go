//go:build !darwin && !linux

package session

// setDontFragment is a no-op on platforms without a Don't-Fragment socket option exposed
// here: the host still probes, but a probe may be fragmented, so a path smaller than the
// probe can look reachable. DPLPMTUD is most meaningful on darwin/linux.
func setDontFragment(Substrate) error { return nil }
