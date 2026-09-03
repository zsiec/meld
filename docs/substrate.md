# Datagram substrate

Meld's protocol core is independent of network I/O. The session host runs it over an
unreliable, unordered, point-to-point datagram service represented by `meld.Substrate`:

```go
type Substrate interface {
    ReadFrom(p []byte) (n int, addr net.Addr, err error)
    WriteTo(p []byte, addr net.Addr) (n int, err error)
    LocalAddr() net.Addr
    Close() error
}
```

`NewSender` and `NewReceiver` use UDP. `NewSenderOver` and `NewReceiverOver` accept a
caller-provided substrate, which is also how tests run the complete host over in-memory
datagram pipes. Multipath sessions use one substrate per path.

## Boundary

The split is deliberate:

- `internal/flow` owns coding, deadlines, feedback, congestion state, and packet-placement
  decisions. It has no sockets, goroutines, or implicit clock reads.
- `internal/session` owns reads, writes, clocks, pacing, path-MTU probes, security, and
  lifecycle.
- A substrate moves exactly one datagram at a time. It must not turn the packet stream
  into an ordered byte stream or silently retransmit expired data.

The caller owns any transport-specific connection setup. After construction the session
owns the supplied substrate and closes it with the session.

## UDP behavior

UDP is the built-in substrate because it exposes datagram loss and timing directly to
Meld's coding and rate-control loops. A listening `*net.UDPConn` already satisfies the
interface. Connected UDP sockets are wrapped internally because their write API does not
accept a destination address.

The host may additionally use platform socket features for ECN and Don't-Fragment path-MTU
probing. These are host concerns and do not change the coding core or wire format.

## Adapting another datagram service

An adapter is valid when it preserves message boundaries, reports its local address, and
provides close semantics. If the underlying service performs its own congestion control,
retransmission, or buffering, those mechanisms compose with Meld rather than disappear;
deployments must account for the resulting latency and rate-control interaction.

Stream transports are not suitable adapters without a separate datagram framing and
expiry layer.
