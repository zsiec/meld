//go:build linux

package session

import "golang.org/x/sys/unix"

// setDF puts a Linux UDP socket into DPLPMTUD probe mode (IP_PMTUDISC_PROBE): the DF bit is
// set and the kernel's own path-MTU cache is bypassed, since Meld runs its own RFC 8899
// discovery. The socket is one address family, so the other family's option errors
// harmlessly; only a failure of BOTH is reported.
func setDF(fd int) error {
	e4 := unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_MTU_DISCOVER, unix.IP_PMTUDISC_PROBE)
	e6 := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_MTU_DISCOVER, unix.IPV6_PMTUDISC_PROBE)
	if e4 != nil && e6 != nil {
		return e4
	}
	return nil
}
