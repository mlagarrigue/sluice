# Experimental

Three packages carry the **experimental** label, in the first sentence of
their documentation: `net/quic` and its helper `net/quic/udp`, and
`web/stream`. This page first says what the word means here, then what
each one does and what not to expect from it. The two packages that left
the label have [their own guide](httpstream.md); the deployments have
[theirs](deployments.md).

## What "experimental" means

**What is promised.** The code exists. It is tested with Go's data race
detector (`-race`). It is measured by the same benchmarks as the rest. And
what it claims holds end to end: `example/vertical` takes a real request
from the received byte all the way to PostgreSQL and back, counting its own
round trips.

**What is not promised.** That names and signatures stay the same from one
version to the next. Production use. For QUIC, **interoperability** beyond
the runner's peers: everything is checked against itself, against the
examples published in the standard (RFC 9001), against a test harness that
loses, shuffles and forges packets, and against the implementations written
by someone else that the QUIC Interop Runner crosses every Sunday
([the detail](limits.md#the-interop-runner-in-detail)) — which is serious,
but a test bench is not a real network, nor its peers every peer. A
review by security specialists. Fuzzing
(sending masses of random inputs to find flaws) against a public corpus.

> **In plain words: then why do these packages exist?** A **parser** (the
> code that reads bytes from the network and makes a request of them) is
> the most dangerous place in a server: it is where hostile input can do
> damage. Sluice.go's architecture refuses to write one, and `web` over
> `net/http` remains the supported path. But the library's thesis — "the
> batch is the unit of transport, from the byte to the answer" — can only
> be *verified* if reading the bytes is itself a stream stage. These
> packages exist to prove that it is feasible and to measure it, not to be
> deployed as they are. Put them behind something you trust, or do not
> expose them.

## `net/quic` — a recent transport, reimplemented

**QUIC** is the protocol HTTP/3 runs on. Unlike TCP, it runs over **UDP**
(**datagrams**: independent packets with no prior connection) and does
itself what TCP used to do: establish a connection, encrypt (with TLS 1.3,
built in), acknowledge receipt, retransmit what was lost, regulate the
rate, and carry **several streams** at once without one lost packet
blocking the others. Go does not provide QUIC in its standard library;
`net/quic` is an implementation of it.

Why it is here: a QUIC datagram carries pieces of several streams at once.
One read on the socket is therefore **a batch by construction**, without
asking anything of anyone — the library's thesis, verified at the transport
level.

```go
client, err := quic.Dial(ctx, pc, addr, tlsConfig, quic.DefaultParameters())               // client side
ln, err := quic.NewListener(pc, tlsConfig, quic.DefaultParameters(), quic.ListenerConfig{}) // server side
server, err := ln.Accept()
stats := server.Stats() // counters, RTT, congestion window, bytes in flight, losses
```

The **transport parameters** are the limits each end announces to the
other (how much data, how many streams, what idle timeout). Start from
`DefaultParameters()`; the zero value is refused, because it would announce
a connection with no credit at all, unable to send anything. What a server
must do beyond the handshake — route datagrams by connection ID, validate
addresses against amplification, follow a client whose NAT rebinds — is
[ADR 0015](../design/adr/0015-quic-server.md).

> **In plain words: the dependency QUIC needs.** QUIC's encryption needs an
> algorithm (ChaCha20-Poly1305) that `crypto/tls` uses but does not expose.
> It is for this that the project depends on `golang.org/x/crypto`, the only
> module outside the standard library that QUIC uses.

`net/quic/udp` is its helper, experimental with it: it opens the UDP socket
with the options a QUIC endpoint needs (no IP fragmentation, one IPv6 flow
label per path) and, on Linux, lets a server on a host with several
addresses answer from the address a datagram reached.

**Key update** (RFC 9001 §6, changing the encryption keys mid-connection)
is handled both ways: a peer's rotation is followed, and this end rotates
its own keys before the cipher's packet budget runs out: a long
connection does not die of its packet count. `Stats().KeyUpdates`
counts the rotations.

### `net/quic` settings

`DefaultParameters()` fills in the "Default" column. The fields marked
*local* never go on the wire: they set what this end does, not what it
announces.

| Field | Default | Unit, bound | What it bounds |
|---|---|---|---|
| `MaxIdleTimeout` | 30 s | duration; zero is no timeout, which this package never announces of its own | a connection nobody is using |
| `MaxUDPPayloadSize` | 1452 | bytes; 1200 to 65527, otherwise refused | the largest datagram read, and what the peer probes for |
| `InitialMaxData` | 1 MiB | bytes | everything the peer may send, all streams together |
| `InitialMaxStreamDataBidiLocal` | 256 KiB | bytes | one bidirectional stream this end opened |
| `InitialMaxStreamDataBidiRemote` | 256 KiB | bytes | one bidirectional stream the peer opened — an HTTP/3 request |
| `InitialMaxStreamDataUni` | 64 KiB | bytes | one unidirectional stream of the peer |
| `InitialMaxStreamsBidi` | 100 | streams | how many bidirectional streams the peer may open (RFC 9114 §6.1 asks for 100) |
| `InitialMaxStreamsUni` | 8 | streams | how many unidirectional streams |
| `AckDelayExponent` | — | exponent; *read from the peer*, never announced (this end uses 3) | the scale of the peer's ACK delays |
| `MaxAckDelay` | — | duration; *read from the peer*, never announced (this end uses 25 ms) | how long the peer may sit on an acknowledgement |
| `ActiveConnIDLimit` | 4 | identifiers; 0 or 1 become 4 | the peer's connection IDs kept active, and the ones this end issues |
| `DisableActiveMigration` | false | boolean | announces that this end does not follow a peer that changes address |
| `Congestion` | NewReno | *local* | the congestion controller this end sends with ([below](#choosing-a-congestion-controller)) |
| `HoldClosing` | false | *local* | whether a connection from `Dial` holds the closing states of RFC 9000 §10.2 |
| `MaxDatagramSize` | 1452 | bytes, *local*; 1200 to 65527, otherwise refused | the largest datagram sent, once the path is probed |

### Listener settings

`ListenerConfig`, the server side's own bounds and policies, none of them on
the wire:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `AlwaysRetry` | off | | a Retry round trip for every new attempt, on top of the amplification limit |
| `MaxPendingConns` | 1024 | ≥ 0 | handshakes in progress, and completed ones waiting for `Accept` |
| `HandshakeTimeout` | `DefaultHandshakeTimeout`, 10 s | ≥ 0 | how long one handshake may hold a pending slot |
| `MaxConns` | no bound | ≥ 0 | every connection the listener carries, closing ones included |
| `MaxConnsPerIP` | no bound | ≥ 0 | the same bound per source address |
| `IPv4PrefixBits` | 32 | 1 to 32 | how much of an IPv4 source `MaxConnsPerIP` counts by |
| `IPv6PrefixBits` | 64 | 1 to 128 | the same for IPv6 |
| `PreferredAddress` | nil: none announced | | a second socket clients move to once the handshake confirms |
| `PreferredIPv4` | the socket's own address | IPv4 | the address announced for that socket when it listens on the unspecified address |
| `PreferredIPv6` | the socket's own address | IPv6 | the same for IPv6 |
| `ConnQueueLen` | 64 | ≥ 0 | datagrams queued for one connection before they are dropped |
| `RetryTokenTTL` | 10 s | ≥ 0 | how long a Retry token stays acceptable |
| `MaxSpentRetryTokens` | 16 384 | ≥ 0 | the record of Retry tokens already used, against a replay |
| `StatelessResetsPerSecond` | 16 | ≥ 0 | stateless resets sent per second |
| `StatelessResetsPerAddress` | 4 | ≥ 0 | one source address's share of them |
| `StatelessResetKey` | random per listener | | derives the reset tokens; set it to survive a restart |
| `TokenKey` | random per listener | | authenticates Retry tokens; share it across instances |
| `SessionTickets` | off | | one session ticket per completed handshake, for resumption |
| `EarlyData` | off | | 0-RTT on resumption ([Deployments](deployments.md)) |
| `EarlyDataWindow` | 10 minutes | ≥ 0 | how long after its issue a ticket may carry 0-RTT |
| `MaxEarlyDataTickets` | 16 384 | ≥ 0 | the record of tickets whose 0-RTT was accepted once |

### Choosing a congestion controller

An advanced setting: it asks you to know your network path — round trip,
loss, how deep the routers' buffers are — and to measure with your own
traffic. Without that, leave it at zero.

> **In plain words.** **Congestion control** decides how fast this end
> sends: too slow and the line stays half empty; too fast and the routers'
> buffers fill, latency climbs and packets are lost. The choice is local:
> it is never announced to the peer, which sends with its own (RFC 9002
> §7).

Three controllers, chosen by `TransportParameters.Congestion`:

```go
p := quic.DefaultParameters()
p.Congestion = quic.CongestionConfig{Algorithm: quic.CongestionCubic} // or quic.CongestionBBR
```

- **NewReno** (RFC 9002 §7), the zero value and the default: the window
  grows by one datagram per round trip and halves on each loss episode.
  Outside the comparisons below, the package's measurements (throughput,
  interop) are made with it, and it stays the default until another has
  been measured against a real client.
- **CUBIC** (RFC 9438, `CongestionCubic`): after a loss the window drops
  to seven tenths rather than half, and grows back along a cubic curve. It
  wins on long paths that lose: 1.7 to 2.3 times faster than NewReno on the
  emulated 80 ms path with a shallow buffer, 1.24 times with 1% loss, 1.59
  times with burst loss; it is within 2.5% of NewReno on the clean and
  bottleneck profiles, and it fills a deep buffer exactly as NewReno does,
  because loss is all it reads. Two fine settings: `SlowStart` (HyStart++
  by default for CUBIC, RFC 9406 — the initial slow start ends on the
  round trip rising, with fewer losses and a somewhat fuller queue on a
  deep buffer) and `DisableFastConvergence`, for a flow alone on its path.
- **BBR** (draft-ietf-ccwg-bbr-06, what Linux ships as BBRv3,
  `CongestionBBR`): a model of the path — the highest delivery rate and the
  lowest round trip seen recently — in which loss is evidence of how much
  the path holds. 16 times NewReno with 1% loss, about 3 times with burst
  loss; on a deep buffer the queue stays at 1.8 times the path's round
  trip, against 5.9 under NewReno, at the same goodput. Against a NewReno
  flow it takes 0.56 of the line. Its startup has its own exit and ignores
  `SlowStart`.

The last two are **experimental**: measured on this repository's emulated
paths only ([QUIC, on an emulated path](../benchmarks.md#quic-on-an-emulated-path),
with the command that replays the comparison). To judge on yours,
`Conn.Stats()` gives the controller in use (`Algorithm`), its window
(`CongestionWindow`), the losses (`LostPackets`) and the round trip
(`SmoothedRTT` against `MinRTT`: the gap is the queue the flow keeps).

`CongestionConfig`, in `TransportParameters.Congestion`:

| Field | Zero | Bounds | What it sets |
|---|---|---|---|
| `Algorithm` | NewReno | unknown values run the default | the controller |
| `SlowStart` | the algorithm's default: HyStart++ for CUBIC, standard for NewReno | | how a loss-based controller ends its initial slow start |
| `DisableFastConvergence` | on | | turns off CUBIC's fast convergence, for a flow alone on its path |

## Deployments

Which deployments the network packages are written for — HTTP/1.1 exposed
directly, behind nginx or Envoy, TLS in front, QUIC behind a mobile NAT,
0-RTT — and what to set for each is on a page of its own:
[Deployments](deployments.md).

## `web/stream` — the same middle, for the other transport

`web` provides the middle of the supported path (route, authenticate,
authorize, decode, answer) for `net/http`. `web/stream` provides the **same
middle**, as batch stages, for `net/httpstream`'s requests, with the same
vocabulary (`web.Verifier`, `web.Authorizer`, `web.Report`) — so that the
two paths do not drift apart. What the stages learn (the route, the
principal, a refusal) travels in the element itself, an `Exchange` that
wraps the request. Why these stages ship beside the core rather than in
it is [ADR 0011](../design/adr/0011-web-stream-beside-core.md). It keeps
the label alone: its transport has left, but
no real consumer has yet built its request path on these stages. That
confrontation is what it lacks, not a measurement.

```go
rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
handle := func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
    ex := stream.Exchanges(nil, b)              // one envelope per request
    rt.Route(ex)                                // which route for each
    stream.Authenticate(ex, verifier, "orders") // who is calling; a 401 names the realm "orders"
    stream.Authorize(ex, authorizer)            // what they may do
    for i := range ex {
        order, _ := stream.PathInt64(&ex[i], 0, "order") // the route's first wildcard
        // the business stage, one batch at a time
    }
    return stream.Render(ex, func(i int) (int, any) { return 200, results[i] })
}
```

The router is a table compiled once: first match wins in table order,
`{name}` matches one segment, and nothing is percent-decoded
([ADR 0010](../design/adr/0010-routing-table.md)).

## The end-to-end example

[`example/vertical`](../../example/vertical) runs the whole architecture on
this path: an HTTP request authenticated, authorized, batched by principal,
decided by one business rule, read from PostgreSQL under row-level
security, and answered with its problems and their remedies. Every arrow
of the journey is a stream stage, and the example counts its round trips.
`go test ./example/vertical -run TestVerticalEndToEnd -v` runs it, against the
PostgreSQL the `SLUICE_PG`, `SLUICE_PG_USER`, `SLUICE_PG_DB` and
`SLUICE_PG_PASSWORD` variables name; without them it skips. Its twin on the
supported path is `example/orders`.
