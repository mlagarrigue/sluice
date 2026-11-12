# QUIC reference

The settings of `net/quic`: `quic.Config`, what this end announces and what
it keeps to itself, and `ListenerConfig`, the server side's own bounds; then
what the package implements of QUIC and what it leaves out, and the two
things that stay the caller's — the socket and the TLS callbacks. The
[guide](../guide/experimental.md#netquic--a-recent-transport-reimplemented) says when to reach
for them.

## Contents

- [`net/quic` settings](#netquic-settings)
- [Listener settings](#listener-settings)
- [What is implemented](#what-is-implemented)
- [What is not implemented](#what-is-not-implemented)
- [The socket is the caller's](#the-socket-is-the-callers)
- [The TLS callbacks are the caller's](#the-tls-callbacks-are-the-callers)

## `net/quic` settings

`DefaultConfig()` fills in the "Default" column. The fields marked
*local* are `quic.Config`'s own, beside the `TransportParameters` it
embeds: they never go on the wire, and set what this end does, not what it
announces.

| Field | Default | Unit, bound | What it bounds |
|---|---|---|---|
| `MaxIdleTimeout` | 30 s | duration; zero and negative refused (zero would announce no timeout) | a connection nobody is using |
| `MaxUDPPayloadSize` | 1452 | bytes; 1200 to 65527, otherwise refused | the largest datagram read, and what the peer probes for |
| `InitialMaxData` | 1 MiB | bytes | everything the peer may send, all streams together |
| `InitialMaxStreamDataBidiLocal` | 256 KiB | bytes | one bidirectional stream this end opened |
| `InitialMaxStreamDataBidiRemote` | 256 KiB | bytes | one bidirectional stream the peer opened — an HTTP/3 request |
| `InitialMaxStreamDataUni` | 64 KiB | bytes | one unidirectional stream of the peer |
| `InitialMaxStreamsBidi` | 100 | streams | how many bidirectional streams the peer may open (RFC 9114 §6.1 asks for 100); each is memory this end holds, `MaxConns` times |
| `InitialMaxStreamsUni` | 8 | streams; 3 to 32 under `ServeH3Conn` | how many unidirectional streams; each stream the peer may open is memory this end holds, `MaxConns` times |
| `AckDelayExponent` | — | exponent; *read from the peer*, never announced (this end uses 3) | the scale of the peer's ACK delays |
| `MaxAckDelay` | — | duration; *read from the peer*, never announced (this end uses 25 ms) | how long the peer may sit on an acknowledgement |
| `ActiveConnIDLimit` | 4 | identifiers; 0 or 1 become 4 | the peer's connection IDs kept active, and the ones this end issues |
| `DisableActiveMigration` | false | boolean | announces that this end does not follow a peer that changes address |
| `Congestion` | NewReno | *local* | the congestion controller this end sends with ([the guide](../guide/experimental.md#choosing-a-congestion-controller)) |
| `HoldClosing` | false | *local* | whether a connection from `Dial` holds the closing states of RFC 9000 §10.2 |
| `MaxDatagramSize` | 1452 | bytes, *local*; 1200 to 65527, otherwise refused | the largest datagram sent, once the path is probed |

## Listener settings

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
| `EarlyData` | off | | 0-RTT on resumption ([Deployments](../guide/deployments.md)) |
| `EarlyDataWindow` | 10 minutes | ≥ 0 | how long after its issue a ticket may carry 0-RTT |
| `MaxEarlyDataTickets` | 16 384 | ≥ 0 | the record of tickets whose 0-RTT was accepted once |

## What is implemented

A section number with no RFC named is RFC 9000's.

- **A `Conn`**: TLS 1.3 handshake via `crypto/tls.QUICConn`, packet
  protection per RFC 9001, streams with interval reassembly, per-stream
  failure via `Stream.Reset` and `Stream.StopSending`, and read and write
  deadlines (`Stream.SetReadDeadline`, `Stream.SetWriteDeadline`) with the
  `net.Conn` contract: `os.ErrDeadlineExceeded`. What the handshake settled
  is readable — `Conn.ConnectionState` for the negotiated protocol, server
  name and peer certificates — and so are the two addresses,
  `Conn.LocalAddr` and `Conn.RemoteAddr`, the latter following the peer
  across a migration.
- **Recovery per RFC 9002**: ACK generation with delayed acknowledgements,
  packet- and time-threshold loss detection, probe timeouts,
  retransmission; NewReno with slow start and recovery periods by default,
  CUBIC or BBR through `Config.Congestion`.
- **Flow control per RFC 9000 §4**, both levels and both directions:
  senders wait for credit, receivers grant it as data is consumed, and a
  peer that overruns its grant loses the connection.
- **Lifecycle**: CONNECTION_CLOSE both ways — at once with `Conn.Close`, or
  once everything sent is acknowledged with `Conn.Shutdown` —
  HANDSHAKE_DONE, the negotiated idle timeout and the `Conn.SetKeepAlive`
  PING that holds a connection under it; why a connection ended is one
  typed value from `Conn.Err` (`ErrIdleTimeout`, `ErrStatelessReset`,
  `ErrVersionNegotiation`, `ConnectionError`), transport-parameter
  validation with the §7.3 identifier-binding checks, version-negotiation
  and Retry handling on the client (the Retry integrity tag is verified per
  RFC 9001 §5.8).
- **A server `Listener`**: one shared `net.PacketConn` demultiplexed to
  many connections by connection identifier, the §8.1 anti-amplification
  limit on every address until it validates, and server-generated Retry
  (`ListenerConfig.AlwaysRetry`) for the deployments that want the address
  proved before a connection exists at all. On Linux, macOS and Windows a
  Listener on a UDP socket bound to 0.0.0.0 or [::] answers each datagram
  from the address it reached (IP_PKTINFO / IPV6_PKTINFO), so a multi-homed
  server does not look like a path change to its clients. `Listener.Addr`
  reports where it listens. A server, even one that serves a single
  connection, is a Listener.
- **Connection-identifier rotation and server-side passive migration**
  (§5.1, §9): every connection issues additional identifiers once its
  handshake confirms — a client's are what let a server follow it across a
  NAT rebind (§9.5) — every connection pools what its peer issues and
  honours RETIRE_CONNECTION_ID and Retire Prior To — switching what it
  sends under when a peer (a load balancer, typically) forces rotation —
  and a server whose client's packets start arriving from a new address
  follows it there: path probed with PATH_CHALLENGE, amplification-limited
  until the answer arrives, congestion and round-trip state reset (§9.4), a
  fresh destination identifier adopted when one is pooled (§9.5). The probe
  is expanded to 1200 bytes as far as the amplification budget allows, and
  a path validated by a smaller one is validated again at full size
  (§8.2.1). The path left behind is challenged too (§9.3.3), and the peer
  returning to it ends the move; a probe unanswered after three
  retransmissions sends the connection back to the last validated path
  (§9.3.2) rather than leaving it throttled — a peer gone from both dies by
  the ordinary idle timeout, not by a second kill path.
- **Client-initiated migration and the server's preferred address** (§9.2,
  §9.6): `Conn.Rebind` moves a client onto a new socket behind a
  PATH_CHALLENGE, with the connection ending if the new path never answers;
  a client offered a preferred address by its server probes it once the
  handshake confirms and moves there on its own when the path validates.
  Both reset congestion and round-trip state as §9.4 asks. The server side
  is `ListenerConfig.PreferredAddress`: a second socket the listener reads
  alongside its main one and announces in every handshake.
- **Datagram coalescing** (§12.2): the ACK a datagram earns and the CRYPTO
  flight it provokes leave together, Initial and Handshake packets in one
  datagram, with §14.1's padding carried by the packet that closes it
  rather than by the Initial alone.
- **Datagram size** (§14): datagrams start at the 1200 bytes §14.1
  guarantees on any path and rise by DPLPMTUD (§14.3, RFC 8899):
  PING+PADDING probes, 1452 bytes first — an Ethernet MTU less IPv6 and
  UDP headers — then a search between the size confirmed and the smallest
  lost, to 20 bytes, up to `Config.MaxDatagramSize` and the peer's
  `TransportParameters.MaxUDPPayloadSize`; after the handshake and after
  each migration. A path that shrinks under the connection (a black hole:
  three probe timeouts, or full-size packets lost while smaller ones get
  through) brings the size back to 1200 and the search runs again. No ICMP
  (§14.2.1 does not require it); a lost probe is not a congestion event
  (§14.4). `Stats.DatagramSize` is the size reached; the
  [protocol positions](protocol-positions.md#datagram-size-in-detail) say
  how the search was measured.
- **Stateless resets** (§10.3): a connection recognises a reset by the
  tokens its peer announced — NEW_CONNECTION_ID's, and the handshake's
  stateless_reset_token — and dies immediately rather than by idle
  timeout; a `Listener` answers unroutable short-header datagrams with a
  reset whose token derives from a per-listener key, which is what lets it
  be honoured with no connection state left.
- **The failure model of §5.2 and §12.2**: nothing an unauthenticated
  sender can put in a datagram ends a connection, except forgeries past
  the AEAD's integrity limit (AEAD_LIMIT_REACHED, RFC 9001 §6.6). Garbage,
  forgeries and strays are counted (`Conn.Stats`, and the listener's own
  `Listener.DiscardedPackets` in front) and dropped.
- **A peer that does not acknowledge**: every ack-eliciting packet stays in
  memory until its ACK, and that record has a written bound, 32,768
  packets. The sends the congestion window governs stop at half of it: an
  honest transfer levels off there (about 23 MB in flight in full-size
  packets) and waits for its acknowledgements as it would for the window.
  Only the sends the window does not govern (flow-control credit, the
  answer to a STOP_SENDING, path challenges, probes) go past it, one per
  event; a peer that acknowledges none of them fills the record, and its
  next packet closes the connection with PROTOCOL_VIOLATION (§13.2.1:
  acknowledge within `max_ack_delay`).
- **Key update** (RFC 9001 §6): a peer's rotation is followed, old read
  keys are kept three probe timeouts for stragglers, and this end rotates
  its own keys before the AEAD's confidentiality limit (RFC 9001 §6.6)
  rather than closing — a long-lived connection does not die of its packet
  count. `Stats.KeyUpdates` counts the rotations.
- **ECN** (§13.4), on Linux, macOS and Windows: the field is read off every
  datagram and reported in ACK frames, this end marks its own packets
  ECT(0) once the path validates, and a rise in the CE count a peer
  reports is a congestion event for the controller. Elsewhere the field is
  neither read nor set, and `Stats` shows zero.
- **Session resumption and 0-RTT** (RFC 9001 §4.5, §4.6;
  [ADR 0019](../design/adr/0019-quic-early-data.md)), on request. A `Listener`
  issues tickets (`ListenerConfig.SessionTickets`) and accepts 0-RTT
  (`ListenerConfig.EarlyData`) once per ticket and only for its own;
  `DialEarly` sends the first requests in 0-RTT under the parameters
  remembered with the ticket, and a refusal fails the streams that carried
  them (`ErrEarlyDataRejected`).

## What is not implemented

- **Server migration.** A server never changes its own address (§9): it
  may announce a preferred one (above), nothing else; a client's
  preferred_address is refused (§18.2).
- **Versions other than 1.** QUIC version 2 (RFC 9369) and compatible
  version negotiation (RFC 9368, the version_information parameter) are
  not implemented: the only version spoken is 1. A Version Negotiation
  packet from a server that does not speak it ends the dial with
  `ErrVersionNegotiation`, and a version_information parameter from a peer
  is skipped as any unknown parameter is (§7.4.2). Version 2 differs from
  1 in constants only and exists against middlebox ossification, which no
  peer of this package has shown; it is to be added the day one does.
- **The latency spin bit** (§17.4), an optional signal that lets an
  observer on the path measure the round trip. The bit is drawn at random
  per connection and per connection identifier, as the section asks of an
  endpoint that does not spin, and the peer's bit is ignored.

## The socket is the caller's

Every entry point takes a `net.PacketConn` and never opens one. Two things
RFC 9000 asks of the socket itself — the IPv4 Don't Fragment bit (§14) and
RFC 6437 flow labels on IPv6 (§9.7) — are socket options with a different
name on each operating system, so they live in the `net/quic/udp` helper:
its `Listen` opens a UDP socket with them applied, its `Configure` applies
them to one the caller already holds. When the socket given is a
`net.UDPConn`, `Dial` and `NewListener` apply them themselves, best
effort, as they ask it to report ECN; a plain `net.ListenPacket` socket
therefore works as one from `udp.Listen`. A socket of any other type is
used as it comes: the package cannot set DF on it, takes it as the caller
configured it, and searches the path as on a configured socket — a socket
that fragments makes that search's probes prove nothing.

## The TLS callbacks are the caller's

The callbacks of the `tls.Config` given to `Dial` and `NewListener` —
`VerifyPeerCertificate`, `VerifyConnection`, `GetCertificate`,
`GetConfigForClient` and the rest — run on the connection's own receive
goroutine, inside the handshake: `crypto/tls` drives them from the CRYPTO
bytes that goroutine feeds it, and returns to it only when the handshake
waits for more. A callback that blocks — an OCSP query, a directory lookup
— therefore stops the reading of every datagram of that one connection
for as long as it runs: acknowledgements and Handshake packets arriving
meanwhile wait in its queue, and only the timer loop keeps retransmitting.
Other connections behind a Listener are untouched, since the Listener's
loop never waits on a connection; what overflows the stalled connection's
queue (`ListenerConfig.ConnQueueLen`) is dropped and resent by the peer.

RFC 9001 §4.1.4 asks an endpoint to keep answering what it can while TLS
works asynchronously; this package buffers the packets whose keys are
missing but does not answer during a callback, so the slow work belongs
before the handshake — a staple fetched in advance, a cache in front of
the lookup — and the callback under the path's round trip. Moving the
callbacks off the receive goroutine would take a TLS goroutine per
handshake and its events replayed under the connection's lock; it is
worth doing the day a measured callback outlasts the round trip.
