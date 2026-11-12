# Exit conditions

What each experimental package must prove to leave the label, and where it
stands. [Limits](../guide/limits.md) says what is missing; this page says
what would make the label go.

## Experimental: the exit conditions

These packages leave the label only by meeting these conditions; they stay
in the module ([ADR 0021](../design/adr/0021-net-quic-stays.md)). One
bullet per package, each condition in it checkable:

- **`net/quic/udp`**: it leaves with `net/quic`, not before. Its options
  are the socket half of the transport and are judged by the same passes;
  the `test-udp` job already runs it on Windows, macOS and Linux ARM.
  Checked by: `net/quic`'s conditions met, and `test-udp` green on the
  commit that lifts the label.
- **`net/quic`**: three conditions, each checked on its own.
  - *Interop*: the *QUIC Interop Runner*'s weekly workflow green, not a
    pass started by hand, with `interop/expected-results.json` unchanged:
    every case against quic-go and ngtcp2, each direction, passing or
    failing for its written cause. Not yet: the weekly pass runs every
    Sunday, and none has finished green so far
    ([in detail](protocol-positions.md#the-interop-runner-in-detail)).
  - *Review*: an external review of the handshake and key handling,
    linked from `SECURITY.md`. Not done.
  - *Measurement*: throughput and latency against a real third-party
    client, over a network between two machines, in
    [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client)
    with date and machines. Missing: see "Measurements: what is missing".

What `net/quic` does today, and within which bounds — limits it keeps
whichever way the conditions go:

- **`net/quic` throughput**: a 64 KiB response over HTTP/3 stays below
  the same response over HTTP/2 with every core available, and catches up
  on two; the figures, their date and the machine that measured them are
  in [Benchmarks](../benchmarks.md#http2-and-http3-from-a-real-client),
  the one place that keeps them current. On Linux and Windows, the
  datagrams one `Stream.Write` produces leave in one system call (UDP
  segmentation offload); on macOS, which offers no public interface for
  it, or on a system that refuses the option, each datagram is still its
  own write. Sizing the QUIC windows does not buy the gap back (scaled 1
  to 16 times, on either side, they move nothing): the gap is the Go
  scheduler, as a request crosses goroutines and each hand-off wakes one.
- **`net/quic` datagram size**: datagrams start at 1200 bytes, the
  smallest maximum RFC 9000 §14.1 guarantees, and rise by a search for the
  largest datagram the path lets through (DPLPMTUD, §14.3, RFC 8899), up to
  `quic.Config.MaxDatagramSize` (zero: 1452; 8952 for a 9000-byte
  jumbo path); the size reached reads in `Stats.DatagramSize`. A path that
  shrinks under a connection is detected and the size falls back to 1200
  (`Stats.MTUBlackHoles`). ICMP "packet too big" messages are ignored, so a
  path that cannot carry 1200 bytes is never detected: the connection ends
  at its idle timeout. How the search probes, and the RFC positions:
  [Datagram size, in detail](protocol-positions.md#datagram-size-in-detail).
- **`net/quic`, a Dial to a closed port on Windows**: on Linux and macOS,
  the ICMP refusal that answers each flight fails the Dial after a few of
  them. On Windows, `net` and `udp.Configure` turn `SIO_UDP_CONNRESET` off
  on every UDP socket, so the refusal never surfaces: the Dial waits out
  the whole handshake deadline (`DefaultHandshakeTimeout`, 10 s, or the
  context's). A `PacketConn` that keeps the option on gets the early
  failure.
- **`net/quic` reassembly**: the bytes of a stream that arrive out of
  order are kept until the gap before them fills, up to the flow-control
  limit this end granted — and in at most `max(held / 1024, 256)` disjoint
  fragments, 16,384 at the ceiling, `held` being the bytes parked. Past
  that the connection closes with INTERNAL_ERROR, the reason naming the
  bound. A conformant sender stays well under it: on a deep-buffered path
  a 1.4 MB flight left 257 holes, each between whole packets, where the
  bound by then allows 1,367. The bound is there for the peer that sends
  one byte per frame, who would otherwise choose this end's bookkeeping
  cost. In plain words: a stream may be delivered in any order, but not
  as confetti.
- **`net/quic` closing period**: a closed connection stays closing or
  draining for three probe timeouts (PTO, the delay after which a sender
  resends what was not acknowledged), as RFC 9000 §10.2 recommends, and
  ten seconds at most — the peer chooses most of a PTO (its
  `max_ack_delay`, legal up to 2^14 ms), and uncapped, a closed
  connection would hold a listener's `MaxConns` slot 49 s. A late packet
  is then answered with a Stateless Reset. The §10.1 floor of three PTOs
  on the idle timeout, a MUST, has no cap: a peer that wants a slot keeps
  it anyway with a PING now and then, and `MaxConns` and `MaxConnsPerIP`
  bound that. In plain words: a closed connection keeps its place ten
  seconds at most; an idle one, its idle timeout or three resend delays.
- **`net/quic` versions**: version 1 only. QUIC version 2 (RFC 9369) and
  compatible version negotiation (RFC 9368) are not implemented: a server
  that does not speak version 1 ends the dial, and a peer's
  `version_information` parameter is skipped like any unknown parameter.
  Version 2 differs from 1 in constants only and exists against
  middleboxes that ossify on version 1; none of this package's peers has
  shown one, and when one does, that is when to add it. In plain words:
  there is one dialect, and it is the one everybody speaks today.
- **`net/quic` spin bit**: the latency spin bit (RFC 9000 §17.4), an
  optional signal from which an observer on the path reads the round
  trip, is not implemented: the bit is random per connection and per
  connection identifier, as the section asks of an endpoint that does not
  spin, and the peer's is ignored. An operator who asks to measure paths
  this way is the signal to add it. In plain words: the packets carry no
  signal put there for a watcher to time the round trip.
- **`net/quic` TLS**: the handshake is `crypto/tls`'s, from the Go release
  `go.mod` names as the floor, run through `tls.QUICClient` and
  `tls.QUICServer`; what it refuses leaves as CRYPTO_ERROR. Eleven of its
  positions depart from RFC 9846, none visible to a conformant peer, and
  the compatibility mode is neither asked for nor policed; they are
  inherited as they are, listed in [TLS, in detail](protocol-positions.md#tls-in-detail).
  Rewriting the TLS stack is out of the question (ADR 0002); a peer refused
  over one of them is the signal to revisit. In plain words: the lock on
  the door is the one Go ships, quirks included, and it is kept rather
  than copied.
- **`net/quic` secrets**: a secret or key is forgotten by becoming
  unreachable, never by being overwritten. RFC 9846 §6 has endpoints
  "forget" the keys of a failed connection and §7 would have each secret
  erased once used; here the sealers and openers of a closed connection
  are released to the garbage collector, a rotated key is replaced, and
  no copy is cleared — Go gives no guarantee that clearing a slice erases
  its bytes, and `crypto/tls` and `crypto/cipher` hold their own copies
  that this package cannot reach. What that costs is a window the length of the
  process, open only to whoever can read the process's memory, who has
  everything else as well. To revisit once Go or `crypto/tls` offers
  guaranteed erasure. In plain words: keys are dropped, not shredded,
  because the shredder does not exist.
- **`net/quic` IPv6 flow label**: on Linux the kernel labels each flow
  (`IPV6_AUTOFLOWLABEL`, set by `net/quic/udp`), by a hash of the
  addresses and ports: the label changes when the peer's address does and
  stays when only the connection identifier rotates, where RFC 9000 §9.7
  would have the identifier counted in. Setting a label per datagram would
  cost a control message on every send; and on a constant four-tuple the
  packets are already linked for any observer, label or not. macOS and
  Windows offer no control over the label at all. To revisit on a
  measurement where the label alone tells two connections apart. In
  plain words: the tag on the envelope is the postal service's, and the
  address links the envelopes anyway.
- **`net/quic` slowloris**: RFC 9000 §21.6 asks for mitigations against a
  peer that keeps many connections open on the least activity. Here a
  connection has no maximum lifetime and no minimum transfer rate, and
  `ListenerConfig.MaxConns` and `MaxConnsPerIP` are zero — no bound — by
  default, because they are the deployer's memory budget (ADR 0018): a
  server facing the internet sets both. A lifetime or a rate floor is
  added when a deployment shows slots held this way. In plain words: a
  slow client is not thrown out; the number of clients is what you cap.
- **`net/quic` 0-RTT before its Initial**: a 0-RTT packet that reaches a
  `Listener` before the Initial of its connection is dropped; RFC 9000
  §5.2.2 lets a server buffer a few. The client sends that data again, in
  1-RTT once the handshake completes, so the cost is a round trip on a
  reordered first flight. Buffering is added when a measured path
  reorders first flights often enough to show that round trip.
- **`net/quic` loopback**: `Dial` does not refuse a loopback destination,
  which RFC 9000 §21.5.6 allows a client to refuse against request
  forgery. A program that dials addresses someone else chose — a proxy, a
  crawler — filters them before `Dial`, the way it would before
  `net.Dial`. In plain words: the package dials what you tell it to.
- **`net/quic` `grease_quic_bit`** (RFC 9287): neither announced nor read;
  every packet carries the QUIC Bit set, as RFC 9000 has it. Clearing it
  guards a protocol against middleboxes that come to rely on the bit, and
  no peer of this package has shown one; it is added when one does.
- **`net/quic` NEW_TOKEN**: a server issues no address validation token
  (RFC 9000 §8.1.3 allows it, §9.3 recommends it after a migration), and a
  client neither keeps nor presents one (§8.1.3 recommends it): that takes
  a key, an expiry and replay protection as Retry tokens have (ADR 0015),
  and a client-side store per server. The cost is a Retry round trip on
  each reconnection to a server that requires validation; a client
  returning here goes through the 3× limit, or a Retry under
  `AlwaysRetry`. A migrating endpoint sends what it has before validation,
  not probing frames only (§21.5.6 leaves the choice; the 3× limit bounds
  it). Tokens come when a real client shows that round trip. In plain
  words: a returning client is validated again, like a new one.
- **`net/quic` stream priority**: no API says how streams rank against
  each other (RFC 9000 §2.3 recommends one; HTTP/3's priority scheme, RFC
  9218, is not read); writes are synchronous and leave in the order of the
  calls, as far as the congestion window allows. The application orders
  what it sends by the order it writes it. An API comes with its first
  consumer. In plain words: what is written first leaves first.
