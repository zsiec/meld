//go:build darwin || linux

package session

import (
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/zsiec/meld/internal/flow"
)

// setECN marks the substrate's outgoing datagrams ECT(1) — the L4S identifier (RFC 9331) an
// AQM sets CE on instead of dropping under load — and enables per-datagram TOS / traffic-class
// reception so the host can read the codepoint back (readECN). Best-effort and mirroring
// setDontFragment: a substrate without a raw socket (the in-memory test pipe) is a no-op.
func setECN(s Substrate) error {
	sc, ok := s.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return nil
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) { setErr = setupECN(int(fd)) }); err != nil {
		return err
	}
	return setErr
}

// setupECN sets ECT(1) on sent packets and turns on per-packet TOS reception, for both address
// families. The socket is one family, so the other family's option errors harmlessly; only a
// both-family failure of the send-marking is reported (the recv-enable is purely best-effort —
// a send-only socket still benefits from the outgoing mark).
func setupECN(fd int) error {
	ect1 := int(flow.ECT1) // 0b01, the low two bits of the DS field
	e4 := unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_TOS, ect1)
	e6 := unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_TCLASS, ect1)
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_IP, unix.IP_RECVTOS, 1)
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS, 1)
	if e4 != nil && e6 != nil {
		return e4
	}
	return nil
}

// parseRecvECN extracts the ECN codepoint (the low two bits of the IP differentiated-services
// field) from whatever TOS / traffic-class control messages recvmsg delivered. Linux delivers
// the IPv4 TOS as an IP_TOS cmsg, Darwin as IP_RECVTOS; IPv6 arrives as IPV6_TCLASS on both. A
// missing or short control message means NotECT. Walks the buffer one message at a time so the
// per-datagram recv path stays allocation-free (ParseSocketControlMessage would allocate a slice).
func parseRecvECN(oob []byte) flow.ECN {
	for len(oob) > 0 {
		hdr, data, remainder, err := unix.ParseOneSocketControlMessage(oob)
		if err != nil {
			break
		}
		switch {
		case hdr.Level == unix.IPPROTO_IP && (hdr.Type == unix.IP_TOS || hdr.Type == unix.IP_RECVTOS):
			if len(data) >= 1 {
				return flow.ECN(data[0] & 0x03)
			}
		case hdr.Level == unix.IPPROTO_IPV6 && hdr.Type == unix.IPV6_TCLASS:
			if len(data) >= 1 {
				return flow.ECN(data[0] & 0x03) // traffic-class int; the low byte holds the DS field
			}
		}
		oob = remainder
	}
	return flow.NotECT
}
