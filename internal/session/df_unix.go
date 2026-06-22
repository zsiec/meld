//go:build darwin || linux

package session

import "syscall"

// setDontFragment sets the IP Don't-Fragment bit on the substrate's UDP socket so DPLPMTUD
// probes (RFC 8899) test the path UNFRAGMENTED: a datagram larger than the path MTU is
// dropped, not silently fragmented (which would make a too-small path look reachable). It
// is best-effort: a substrate without a raw socket (the in-memory test pipe) is a no-op,
// and the per-OS setDF does the actual setsockopt.
func setDontFragment(s Substrate) error {
	sc, ok := s.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return nil // e.g. the pipe substrate — nothing to set
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) { setErr = setDF(int(fd)) }); err != nil {
		return err
	}
	return setErr
}
