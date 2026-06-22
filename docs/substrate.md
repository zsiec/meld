# Substrate — UDP vs QUIC-datagram (fork resolved)

Meld's deployability fork: does the goroutine host run the coded flow over
**(A)** a QUIC unreliable-DATAGRAM connection (RFC 9221) or **(B)** purist pure-Go UDP?
Resolved **B for the shipping core, with A reachable behind the substrate seam**, on the
evidence below. The decision is empirical — the lab is the arbiter.

## The seam

The host (`internal/session`) is a dumb pump on top of a datagram service; the sans-I/O
core never sees a socket. That boundary is now an explicit interface — the datagram subset
of `net.PacketConn`:

```go
type Substrate interface {
    ReadFrom(p []byte) (n int, addr net.Addr, err error)
    WriteTo(p []byte, addr net.Addr) (n int, err error)
    LocalAddr() net.Addr
    Close() error
}
```

A listening `*net.UDPConn` satisfies it as-is; a dialed (connected) socket gets a 3-line
wrapper. The host holds a `Substrate`, not a `*net.UDPConn`, so the same coded core + host
runs over any datagram service — proven over an in-memory pipe substrate
(`TestSeamOverPipe`, no sockets) and, for this decision, over a QUIC DATAGRAM adapter. The
whole QUIC integration is these four methods:

```go
func (q *quicSubstrate) ReadFrom(p []byte) (int, net.Addr, error) {
    msg, err := q.conn.ReceiveDatagram(context.Background())
    if err != nil { return 0, nil, err }
    return copy(p, msg), q.conn.RemoteAddr(), nil
}
func (q *quicSubstrate) WriteTo(p []byte, _ net.Addr) (int, error) {
    return len(p), q.conn.SendDatagram(append([]byte(nil), p...))
}
func (q *quicSubstrate) LocalAddr() net.Addr { return q.conn.LocalAddr() }
func (q *quicSubstrate) Close() error        { return q.conn.CloseWithError(0, "") }
```

## The A/B test

A throwaway harness ran the **identical** Meld core + host over both substrates, pacing the
sender like a real media source (to the encoder bitrate, not an unbounded blast). quic-go
lived in a separate nested module so Meld's core module stayed dependency-free; once the
comparison was captured the harness was removed (the four-method adapter above is all that
a future QUIC opt-in needs). Loopback, so the *only* loss/latency is what each substrate
itself adds.

## What the test says

The result is qualitative but decisive, and stable run to run.

- **QUIC double-controls the rate.** A QUIC DATAGRAM is congestion-controlled by QUIC's own
  CC (RFC 9221): it paces and queues datagrams beneath Meld's delay-based controller. Even on
  a *lossless* loopback link that pacing delays some datagrams past Meld's 200 ms deadline —
  they count as lost even though the coding would have recovered a genuine erasure. Two
  control loops fighting over one rate is the worst of both.
- **The latency tail is far worse** — because QUIC buffers datagrams behind its CC window and
  ACK timers, the tail blows out by orders of magnitude relative to bare UDP on the same
  loopback. For a transport whose entire thesis is delivering at ~one propagation time, that
  tail is disqualifying.
- **Higher per-datagram allocation** (crypto + framing), and a new dependency tree
  (`quic-go` + `golang.org/x/{crypto,net,sys}`) against a core that is otherwise pure
  standard library.
- **UDP lets Meld own loss (the coding) and rate (its CC), and delivers byte-exact with a
  tight, predictable tail** — exactly the determinism the project is built on.

QUIC's genuine wins — connection migration, 0-RTT, ECN plumbing — are real, but: Meld's
coding already provides QUIC's main service (loss/reorder resilience); migration is earnable
on UDP with a connection-id + socket rebind (the coded framing tolerates the reordering);
ECN is `golang.org/x/net`, already permitted. None of them outweighs the latency-tail and
double-CC cost for the low-latency core.

## Decision

- **Ship B (pure-stdlib UDP) as the core substrate.** Keep the core dependency-free and
  deterministic; earn migration/ECN/PMTUD incrementally on UDP as survivability work.
- **Keep A (QUIC-datagram) reachable behind the seam.** The host is already
  substrate-agnostic; a deployment that needs QUIC's migration/0-RTT and can absorb the
  latency tail adds the four-method adapter above and a small `NewSenderOver`-style
  constructor that injects it — the dependency stays in that deployment, never in the core.
