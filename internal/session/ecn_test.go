//go:build darwin || linux

package session

import (
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/zsiec/meld/internal/flow"
)

// TestECNSetAndRead validates the host ECN plumbing over loopback: setECN marks a socket's
// outgoing datagrams ECT(1) (the L4S identifier) and enables TOS reception, and readECN recovers
// that codepoint; re-marking the sender to CE is read back as CE. If the platform/loopback does
// not deliver the TOS control message, the test skips rather than failing — the plumbing is still
// wired, the environment just does not exercise it.
func TestECNSetAndRead(t *testing.T) {
	rx, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer rx.Close()
	if err := setECN(rx); err != nil {
		t.Fatalf("setECN rx: %v", err)
	}

	tx, err := net.DialUDP("udp", nil, rx.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tx.Close()
	if err := setECN(dialedSubstrate{tx}); err != nil {
		t.Fatalf("setECN tx: %v", err)
	}

	read := func() flow.ECN {
		t.Helper()
		_ = rx.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf, oob := make([]byte, 64), make([]byte, 128)
		n, _, ecn, err := readECN(rx, buf, oob)
		if err != nil {
			t.Fatalf("readECN: %v", err)
		}
		if n != 4 {
			t.Fatalf("read %d bytes, want 4", n)
		}
		return ecn
	}

	// setECN marked the socket ECT(1); a sent datagram should arrive carrying it.
	if _, err := tx.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := read(); got == flow.NotECT {
		t.Skip("platform/loopback does not deliver the TOS codepoint; plumbing wired but not exercised here")
	} else if got != flow.ECT1 {
		t.Fatalf("default mark: ecn=%#b, want ECT1 (%#b)", got, flow.ECT1)
	}

	// Re-mark the sender socket to CE (what an AQM would set) and confirm it reads back as CE.
	setSockTOS(t, tx, int(flow.CE))
	if _, err := tx.Write([]byte("ping")); err != nil {
		t.Fatalf("write CE: %v", err)
	}
	if got := read(); got != flow.CE {
		t.Fatalf("CE mark: ecn=%#b, want CE (%#b)", got, flow.CE)
	}
}

// TestParseRecvECNNoAlloc gates the warm recv path: extracting the ECN codepoint from the TOS
// control message must not allocate (the recv loop runs it per datagram). It walks the buffer
// with ParseOneSocketControlMessage rather than ParseSocketControlMessage for exactly this.
func TestParseRecvECNNoAlloc(t *testing.T) {
	rx, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer rx.Close()
	if err := setECN(rx); err != nil {
		t.Fatalf("setECN: %v", err)
	}
	tx, err := net.DialUDP("udp", nil, rx.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer tx.Close()
	_, _ = tx.Write([]byte("x"))
	buf, oob := make([]byte, 64), make([]byte, 128)
	_ = rx.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, oobn, _, _, err := rx.ReadMsgUDP(buf, oob)
	if err != nil || oobn == 0 {
		t.Skip("platform delivered no TOS control message")
	}
	cm := append([]byte(nil), oob[:oobn]...)
	if got := testing.AllocsPerRun(1000, func() { _ = parseRecvECN(cm) }); got != 0 {
		t.Fatalf("parseRecvECN allocates %.0f/call on the warm recv path (want 0)", got)
	}
}

// setSockTOS forces the IPv4 TOS byte on a connected UDP socket (a stand-in for an on-path AQM
// re-marking the datagram), so the test can drive a specific received codepoint.
func setSockTOS(t *testing.T, c *net.UDPConn, tos int) {
	t.Helper()
	raw, err := c.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) {
		setErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TOS, tos)
	}); err != nil {
		t.Fatalf("control: %v", err)
	}
	if setErr != nil {
		t.Fatalf("setsockopt IP_TOS: %v", setErr)
	}
}
