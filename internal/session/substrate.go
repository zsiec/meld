package session

import (
	"errors"
	"io"
	"net"
	"reflect"
)

// ErrNilSubstrate is returned when a caller tries to build a session host over a nil
// datagram substrate.
var ErrNilSubstrate = errors.New("meld: session: nil substrate")

// Substrate is the datagram service a session host runs the coded flow over: an
// unreliable, unordered, point-to-point datagram pipe. The host is a dumb pump on top of
// it and the sans-I/O core never sees it, so the substrate is swappable — pure-stdlib UDP
// today, a QUIC DATAGRAM (RFC 9221) adapter behind the same interface tomorrow. It is the
// datagram subset of net.PacketConn (ReadFrom/WriteTo/LocalAddr/Close, no deadlines): a
// listening *net.UDPConn satisfies it as-is; a dialed (connected) socket needs the thin
// dialedSubstrate wrapper below, since a connected socket rejects WriteTo.
//
// Why this is the substrate fork's seam (PLAN §5): the host owns the substrate, the core
// owns the protocol, and the boundary between them is exactly these four methods. Either
// substrate — UDP or QUIC-datagram — plugs in without the core changing a line.
type Substrate interface {
	// ReadFrom blocks for the next datagram into p, returning its length and source
	// address. A connected substrate returns its fixed remote address.
	ReadFrom(p []byte) (n int, addr net.Addr, err error)
	// WriteTo sends one datagram to addr. A connected substrate ignores addr and sends to
	// its established peer.
	WriteTo(p []byte, addr net.Addr) (n int, err error)
	// LocalAddr is the local bound address.
	LocalAddr() net.Addr
	// Close releases the substrate; a ReadFrom blocked on it then returns an error.
	Close() error
}

func validateSubstrate(s Substrate) error {
	if s == nil {
		return ErrNilSubstrate
	}
	v := reflect.ValueOf(s)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return ErrNilSubstrate
		}
	}
	return nil
}

func writeDatagram(s Substrate, p []byte, addr net.Addr) error {
	n, err := s.WriteTo(p, addr)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return nil
}

// dialedSubstrate adapts a connected (dialed) UDP socket — which rejects WriteTo with a
// pre-connected error — to the Substrate interface by sending on the connected path and
// ignoring the target address. ReadFrom, LocalAddr, and Close are inherited from the
// embedded *net.UDPConn (ReadFrom on a connected socket returns the fixed remote address).
type dialedSubstrate struct{ *net.UDPConn }

func (d dialedSubstrate) WriteTo(p []byte, _ net.Addr) (int, error) { return d.UDPConn.Write(p) }

// dialUDP dials remote ("host:port") and returns it as a connected UDP Substrate.
func dialUDP(remote string) (Substrate, error) {
	raddr, err := net.ResolveUDPAddr("udp", remote)
	if err != nil {
		return nil, err
	}
	c, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}
	return dialedSubstrate{c}, nil
}

// listenUDP binds bind ("host:port"; ":0" for an ephemeral port) and returns it as a
// Substrate (a listening *net.UDPConn already implements the four methods).
func listenUDP(bind string) (Substrate, error) {
	baddr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp", baddr)
}

// closeConns closes every substrate, ignoring nil entries.
func closeConns(subs []Substrate) {
	for _, s := range subs {
		if s != nil {
			s.Close()
		}
	}
}
