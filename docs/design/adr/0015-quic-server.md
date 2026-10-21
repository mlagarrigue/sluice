# ADR 0015 — The QUIC server: transport machinery, a real listener, a moving client

- **Created:** 2026-08-22
- **Revised:** 2026-10-03 (listener, migration), 2026-10-04 (stateless reset, `Rebind`, preferred address, path abandonment, surface pass), 2026-10-07 (three records merged into one current state), 2026-10-09 (`PreferredAddress` exported), 2026-10-10 (key update), 2026-10-14 (`HandshakeTimeout`, `MaxConns`, `MaxConnsPerIP`), 2026-10-16 (`MaxConnsPerIP` counts by prefix), 2026-10-18 (tuning and keys public, one preferred address per family), 2026-10-21 (shared reset key: the §21.11 routing condition)
- **Theme:** quic

## Problem

A QUIC package that runs a real TLS 1.3 handshake and real packet
protection, and nothing else, works on a loopback and nowhere else: no
loss recovery, no congestion control, no flow control, one connection per
socket, every stray datagram fatal. A server also has obligations the RFC
makes non-optional — demultiplexing by connection identifier, address
validation against amplification, and surviving a client whose NAT rebinds
its port, which from the server's side is indistinguishable from
deliberate migration.

## Decision

**One connection is a read loop, a timer loop, and synchronous writers.**
The read loop owns decryption, frame dispatch, ACK bookkeeping and the idle
clock; the timer loop owns every deadline; writers block on a condition
variable when credit runs out. The read loop never waits for credit,
because it is what delivers it. Unauthenticated input never ends a
connection: it is counted (`Stats`) and dropped; fatality is reserved for
what an authenticated packet proves the peer did. Recovery is RFC 9002's —
packet- and time-threshold loss detection, probe timeouts, NewReno — with
persistent congestion approximated as three unanswered probes. Flow control
waits instead of refusing, grants follow consumption, retired streams give
their slots back with `MAX_STREAMS`. The batch seam, `OnStreamFrames`,
delivers each packet's newly contiguous bytes per stream, so the layer
above never sees an offset.

**A `Listener` owns the socket; connections are fed, not readers.** One
goroutine runs the only `ReadFrom` loop and routes each datagram by
destination connection identifier; a managed `Conn` reads from a channel.
Each handshake runs on its own goroutine, bounded by `MaxPendingConns`
(counting connections parked for `Accept`, capped at twice that figure
with a stateless `CONNECTION_REFUSED` beyond) and by `HandshakeTimeout`
(how long one may hold its slot; the client's default when unset); what the
listener carries in total, accepted connections included, is bounded by
`MaxConns` and per source address by `MaxConnsPerIP`, both unset by default
and refused the same way. `MaxConnsPerIP` counts an address by its prefix —
/32 for IPv4 and /64 for IPv6 by default, set by `IPv4PrefixBits` and
`IPv6PrefixBits` — because an IPv6 host is handed a whole /64 (RFC 6177)
and a bound on the full 128 bits is one it steps around by changing
address; `httpstream.Config` counts the same way, through the same
function. The stateless reset budget stays per whole address: it bounds
what a forged source reflects to one victim, and a victim is an address.
In plain words: two machines on one home IPv6 network share a count — the
price of a bound one host cannot multiply; an operator serving many
clients behind one /64 lengthens `IPv6PrefixBits`. The amplification limit is
always on — sends past three times receipts are dropped until a
Handshake-level packet decrypts; `AlwaysRetry` adds stateless Retry with an
HMAC token for deployments under attack. `Close` ends pending and parked
connections and follows `net.Listener`'s line around accepted ones. On
Linux a listener bound to the unspecified address answers from the address
each datagram reached. Unroutable short-header datagrams get a stateless
reset whose token derives from a listener key, rate-limited per source IP.
That key, `StatelessResetKey`, and the one that authenticates Retry tokens,
`TokenKey`, are drawn at random per listener when the caller gives none;
given, they survive a restart and are shared by the instances behind one
address (RFC 9000 §10.3.2): the client of a dead process receives a reset
it recognises and stops at once instead of waiting out its idle timeout.
Sharing is conditioned by §21.11: the instances MUST be arranged so that
every packet of a connection identifier reaches the instance holding the
connection while it is active — routing by connection identifier, not by
address and port. The listener resets any unknown short-header identifier
and cannot tell a connection it never had from one alive next door; behind
a four-tuple load balancer, a client whose address changes is reset with
its real token by the instance it lands on, and an attacker who steers the
routing resets connections at will. In plain words: one key for all
instances only when the balancer routes by connection identifier;
otherwise one key per instance, whose resets then cover the connections it
held itself — enough for a restart.
What the listener used to fix in constants is tuned the way its bounds
are: `ConnQueueLen` (64), `RetryTokenTTL` (10 s), `MaxSpentRetryTokens`
(16,384), zero for the default, negative refused (ADR 0018). The number
of identifiers a connection issues for itself is the minimum of the
peer's limit and its own `ActiveConnIDLimit`: each identifier is an entry
in the listener's table, and the figure a deployment chooses for the
peer's is the one it pays for its own.

**Identifiers rotate and the server survives a moving client.** A
listener-managed connection issues additional identifiers via
`NEW_CONNECTION_ID` and honours the peer's, enforcing
`active_connection_id_limit`. An authenticated, highest-numbered,
non-probing 1-RTT packet from a new address moves the send path, and pays
migration's full bill at once: the new path is amplification-limited, a
`PATH_CHALLENGE` sized to the budget probes it, congestion and RTT state
reset, the destination identifier rotates. The old path is challenged too;
a legitimate peer answering there moves the connection straight back,
which defeats a migration forged from copied packets. A path that never
validates is abandoned and the last validated one restored (§9.3.2); with
none known, the connection closes silently. The client has `Conn.Rebind`
(a new socket, a fatal probe), and moves to a server's preferred address on
its own after the handshake. The server half is `ListenerConfig.
PreferredAddress`, a second socket the listener reads and announces: it
was kept private by the surface pass of 2026-10-04, which exported nothing
only the package's own tests used, and is public since the feature is
otherwise unreachable and the parameter's whole point (§9.6) is a server
behind a shared address — a deployment, not a test. The preferred address
is announced by `PreferredIPv4` and `PreferredIPv6`, one `netip.Addr`
each: the transport parameter carries one address per family (§18.2), and
the type enforces it instead of a list being checked.

## Rationale

Each tier is the RFC's own obligation, taken when the deployment model
stopped tolerating its absence: a server behind one UDP port, a client
behind a NAT. Two cipher suites come from the standard library and the
third, ChaCha20-Poly1305, from the project's one dependency (ADR 0002).
Costs: a timer goroutine per connection, a dispatch goroutine per listener,
one copy per inbound datagram at the demux point, and one address
comparison on the packet hot path. Sharing `TokenKey` between instances
does not share the record of tokens already spent: a token replayed at a
second instance within `RetryTokenTTL` opens a second connection there,
and the TTL is then the whole bound (§8.1.4 says "prevented or limited").
In plain words: a given key is a deployment secret; whoever holds it can
end any connection (`StatelessResetKey`) or forge a token that validates
an address (`TokenKey`).

Known residuals, stated rather than hidden: interoperation against a
foreign implementation remains the missing proof — the harness drops,
reorders and forges datagrams, but only a peer sharing no code catches a
shared wrong assumption; key update (RFC 9001 §6) rotates in both
directions since 2026-10-10, but the only rotation this end initiates is
the one the AEAD limit forces, and the §6.2 refusal of a double rotation
is applied strictly where the RFC says MAY; RTT samples from an abandoned
path can inflate the probe timeout after a migration, in the conservative
direction only; 0-RTT is absent.
