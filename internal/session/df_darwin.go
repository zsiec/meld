//go:build darwin

package session

import "golang.org/x/sys/unix"

// setDF sets the IPv4/IPv6 Don't-Fragment bit on a Darwin UDP socket (RFC 8899 probes must
// not be fragmented). The socket is one address family, so the other family's option errors
// harmlessly; only a failure of BOTH is reported.
func setDF(fd int) error {
	e4 := unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_DONTFRAG, 1)
	e6 := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_DONTFRAG, 1)
	if e4 != nil && e6 != nil {
		return e4
	}
	return nil
}
