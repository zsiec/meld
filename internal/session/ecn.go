package session

import (
	"net"

	"github.com/zsiec/meld/internal/flow"
)

// readECN reads one datagram together with the IP ECN codepoint (RFC 3168 §5) the network
// delivered it with, so the receiver can echo the CE-marked fraction to the sender's L4S
// congestion controller (RFC 9331). It uses ReadMsgUDP to recover the per-datagram TOS /
// traffic-class control message that setECN turned on; a substrate without ReadMsgUDP (the
// in-memory test pipe) or a platform without the option falls back to a plain read reporting
// NotECT. ECN is advisory — any read/parse trouble degrades to NotECT and never drops the
// datagram (the flow still delivers; only the congestion signal is missing).
func readECN(s Substrate, buf, oob []byte) (int, net.Addr, flow.ECN, error) {
	rc, ok := s.(interface {
		ReadMsgUDP(b, oob []byte) (int, int, int, *net.UDPAddr, error)
	})
	if !ok {
		n, addr, err := s.ReadFrom(buf)
		return n, addr, flow.NotECT, err
	}
	n, oobn, _, uaddr, err := rc.ReadMsgUDP(buf, oob)
	if err != nil {
		return n, nil, flow.NotECT, err
	}
	var addr net.Addr // avoid a non-nil net.Addr wrapping a nil *net.UDPAddr
	if uaddr != nil {
		addr = uaddr
	}
	return n, addr, parseRecvECN(oob[:oobn]), nil
}
