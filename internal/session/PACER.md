# Host transmit pacer + source budget contract

Implements the host arm of the budget contract the core already exposed via
`flow.Sender.RateBudgetBitsPerSec()` but nothing consumed. Lives in `internal/session`
(`pacer.go`), where it belongs: the sans-I/O core is non-blocking by construction (media
is non-droppable; the core's bucket goes negative rather than refuse it), so only the host
— at the `Write()` boundary — can bound the source and shape emission timing.

## What it is (and is NOT)

- A pure leaky bucket over a FIFO of the core's emitted datagrams, **slaved read-only** to
  the core budget. It is NOT a second congestion controller — an independently-controlled
  rate here is the "two loops fighting over one rate" anti-pattern (docs/substrate.md), and
  the design probe confirmed harm appears only when a second limiter clips *below* the
  budget. The pacer's ceiling is always the budget itself.
- Two SEPARATE knobs: a small smoothing burst-credit (`PaceBurstMicros`, default 5 ms) and
  a deadline-derived queue-time limit (`PaceQueueLimitMicros`, default 3/4 of
  `BufferMicros`). Conflating them lets the bucket hoard a quiet-gap's credit and dump it on
  the next burst — the prototype failed exactly this way (98 ms microburst) until they were
  split.
- Never drops media: the FIFO drains in order; overload is handled by **backpressure at
  `Write()`** (the writer blocks until the queue drains under the limit). An oversized
  datagram drives tokens briefly negative and pays back — never stuck, never dropped.

## Structure

- `pacerState` — pure, clockless decision core (explicit `now`); unit-tested
  deterministically.
- `hostPacer` — goroutine wrapper: a precise-sleep release loop (sleeps until the next
  datagram is due or it is signalled), `cond`-based `put()` backpressure, and a
  full-speed `flushClose()` for the end-of-stream tail.
- `Sender` routes `Write`/tick/flush through the pacer and re-slaves the rate on feedback
  and tick. `Config.Pace=false` restores the exact pre-pacer inline-transmit behaviour.

Scope: single-path `Sender`. `MultipathSender` is currently unpaced (a follow-up:
per-path pacers).

## Evidence

Unit + integration (`go test -race ./internal/session/`): rate adherence, oversized
pay-down, no-drop FIFO order, determinism, rate-cut credit clamp, burst floor, goroutine
backpressure, end-to-end paced delivery, and burst smoothing (peak **220 → 10
datagrams / 5 ms**, spread 1 ms → 236 ms).

txbench `-lowlat` (meld paced vs `meld-np` unpaced, 3-rep median, 20 Mbps, 60 ms budget):

| condition | metric | paced | unpaced |
|---|---|---|---|
| 20 ms RTT, 10% loss | p99 | **34.3 ms** | 41.8 ms |
| 20 ms RTT, 10% loss | deliv / goodput | 100% / 19.9 | 99.9% / 19.9 |
| 20 ms RTT, 20% loss | p99 | 51.2 ms | 49.3 ms (≈ noise) |
| 100 ms RTT | p50/p99 | identical | identical |

- **No regression**: delivery, goodput, CPU/alloc within noise across all conditions.
- **Tail win where bursts matter**: at moderate loss / LAN RTT the pacer smooths the
  coder's per-generation repair *batches* and cuts p99 by ~18% at an unchanged p50.
- Transparent where it can't help (budget-limited 100 ms RTT regime, and at 20% loss where
  reactive-repair rounds, not bursts, dominate the tail).
