# ADR 0015 — The QUIC server: transport machinery, a real listener, a moving client

- **Created:** 2026-08-22
- **Revised:** 2026-10-03 (listener, migration), 2026-10-04 (stateless reset, `Rebind`, preferred address, path abandonment, surface pass), 2026-10-07 (three records merged into one current state)
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
with a stateless `CONNECTION_REFUSED` beyond). The amplification limit is
always on — sends past three times receipts are dropped until a
Handshake-level packet decrypts; `AlwaysRetry` adds stateless Retry with an
HMAC token for deployments under attack. `Close` ends pending and parked
connections and follows `net.Listener`'s line around accepted ones. On
Linux a listener bound to the unspecified address answers from the address
each datagram reached. Unroutable short-header datagrams get a stateless
reset whose token derives from a per-listener key, rate-limited per source
IP.

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
its own after the handshake; the server half of `preferred_address` is
implemented and tested but not public.

## Rationale

Each tier is the RFC's own obligation, taken when the deployment model
stopped tolerating its absence: a server behind one UDP port, a client
behind a NAT. Two cipher suites come from the standard library and the
third, ChaCha20-Poly1305, from the project's one dependency (ADR 0002).
Costs: a timer goroutine per connection, a dispatch goroutine per listener,
one copy per inbound datagram at the demux point, and one address
comparison on the packet hot path.

Known residuals, stated rather than hidden: interoperation against a
foreign implementation remains the missing proof — the harness drops,
reorders and forges datagrams, but only a peer sharing no code catches a
shared wrong assumption; key update (RFC 9001 §6) is not implemented, and a
peer that rotates keys shows in `Stats().KeyPhaseSuspects` then dies of
idle timeout; RTT samples from an abandoned path can inflate the probe
timeout after a migration, in the conservative direction only; 0-RTT is
absent.
