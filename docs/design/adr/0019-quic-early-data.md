# ADR 0019 — 0-RTT: accepted once, delivered early, acted on when allowed

- **Created:** 2026-10-23
- **Revised:** 2026-10-27 (the ticket is entered when the resumption is checked, and given back if unused), 2026-10-28 (a full register is the one load signal, and why; the intermediary role is not this package's)
- **Theme:** quic

## Problem

A client resuming a session can send its first requests in 0-RTT packets
(early data, encrypted under a key derived from the previous connection's
ticket) before the handshake ends. The gain is one round trip per resumed
connection; the price is replay: anyone who copies the client's first flight
can send it again, and the server cannot tell the copy apart, since the
client only authenticates with its Finished. `crypto/tls` decrypts 0-RTT but
offers no replay protection, while RFC 9846 §8 requires a server instance to
accept the 0-RTT of a given handshake at most once.

Three more constraints. In 0-RTT the client applies the transport parameters
and HTTP/3 settings of the previous connection: the server may only accept
if it still honours them (RFC 9000 §7.4.1, RFC 9114 §7.2.4.2). The gain only
exists if the server acts on a request before the handshake ends: waiting
for the client's Finished delivers the answer at the same instant as without
0-RTT. And RFC 8470 §2 says an origin server with no explicit information
from the resource must either refuse early data or wait for the handshake to
complete before processing it.

## Decision

**Server, transport.** `ListenerConfig.EarlyData` (zero: off) issues tickets
that allow 0-RTT, as `SessionTickets` issues tickets that do not. The ticket
carries, in `SessionState.Extra` (encrypted and authenticated with the
ticket), an instance identifier drawn at random when the `Listener` is
created, a random ticket identifier and the time of issue.

On resumption the
`Listener` accepts 0-RTT only if the instance identifier is its own, the
ticket is younger than `EarlyDataWindow` (zero: 10 minutes), the ticket
identifier has never been used, and the register bounded by
`MaxEarlyDataTickets` (zero: 16,384) has room: the ticket is then entered in
the register at once, when `crypto/tls` asks about the resumption, and the
entry lives until the window ends. With the register full, or the ticket
entered by another connection first (a replay racing the original), 0-RTT is
refused and the connection resumes in 1-RTT. The register is sized as the
rate of 0-RTT connections times the window. Every refusal is a TLS refusal
(`SessionState.EarlyData` set to false): no 0-RTT packet is then processed
(RFC 9001 §4.6.2).

A full register is the one global refusal of 0-RTT: neither processor load
nor the number of requests in flight makes it refuse, where RFC 8470 §6.3
advises refusing early data under load. The reason: a request the
predicate does not admit in 0-RTT is held until the handshake ends, and
what is held is bounded by flow control and `MaxConcurrentStreams`; a
replay never gets past the handshake; and load is not measured here in a
way a threshold would make right. An operator who wants less 0-RTT under
load turns `EarlyData` off or lowers `MaxEarlyDataTickets`.

`crypto/tls` asks before it verifies the PSK binder and before it knows
whether the client offered 0-RTT at all, so an entry may go unused — a
resumption without 0-RTT, a handshake that fails — and is then given back,
when the handshake completes without the early keys or the connection ends:
the ticket's 0-RTT, and the slot, are intact, and "at most once" still
holds, since no 0-RTT handshake with that ticket was accepted.

Until 2026-10-27 the ticket was entered later, once the 0-RTT was accepted;
a full register or a lost race was then found with `early_data` already
announced, and the connection kept no early key: its 0-RTT packets were
dropped, which the client saw as loss and resent in 1-RTT after a probe
timeout.

A `Listener` with `EarlyData` hands the connection to `Accept` as soon as
its 0-RTT is accepted, before the handshake ends; `Conn.HandshakeComplete`
returns a channel closed when it does. What arrives before may be a replay.
`HandshakeTimeout` applies as before: a replay, which cannot produce the
client's Finished, has its connection closed at the deadline.

The remembered transport parameters are not copied into the ticket: the
instance identifier is enough, since a `Listener`'s parameters do not change
during its life. Of the HTTP/3 settings, two are constant (QPACK dynamic
table and blocked streams at zero, ADR 0013); the third,
`SETTINGS_MAX_FIELD_SECTION_SIZE`, comes from the `Config`, a single one for
a `Listener` served by `Server.ServeQUIC`. With `ServeH3Conn` and a `Config`
smaller than the previous connection's, a 0-RTT request larger than the new
bound is refused like any oversized request: the connection holds.

**Server, HTTP.** `httpstream.Config.EarlyData` is a predicate on the request
(method, path) saying whether it may be processed before the handshake
ends; nil: none may. An HTTP/3 request that arrives while the handshake is
not complete is held until it is when the predicate does not allow it, and
delivered at once when it does. The same predicate judges an HTTP/1.1 or
HTTP/2 request carrying `Early-Data: 1` (an intermediary received it in
early data): not allowed, it is answered 425 Too Early, because waiting does
not make it safe (RFC 8470 §5.2). The intermediary's own duties in RFC 8470
— adding `Early-Data: 1` to what it forwards, retrying on a 425 — are not
this package's: it is a server, never a proxy. A handler's own
`Early-Data` field in a response is refused, as §5.1 asks.

**Client.** `quic.DialEarly` returns the connection as soon as the 0-RTT key
is installed, when the ticket allows it, and otherwise when the handshake
ends, as `Dial` does. The client keeps the server's transport parameters
with the ticket (RFC 9000 §7.4.1) and applies them to what it sends in
0-RTT. If the server refuses 0-RTT, every stream opened in 0-RTT fails with
`ErrEarlyDataRejected` and the connection continues in 1-RTT: the caller
replays, because RFC 9001 §4.6.2 requires resetting the application state
bound to those streams, which the transport does not know.

## Rationale

The instance identifier makes replay across instances impossible by
construction rather than asking the operator for a shared register: another
instance, or the same one after a restart, refuses 0-RTT and resumes in
1-RTT. The local register gives RFC 9846 §8's "at most once" on the instance
that issued the ticket, and its bound fails closed: under load 0-RTT is
lost, never the guarantee. What this costs: behind a balancer that does not
bring a client back to the same instance, little 0-RTT; and a ticket older
than the window resumes in 1-RTT. The register costs about 2 MB at the
default bound, like the record of Retry tokens.

The predicate applies RFC 8470 as written: with no word from the resource,
wait. Holding the request rather than answering 425 spares the client a
round trip and the resending of its body, and a replay stays harmless, since
it never reaches the end of the handshake. The method alone does not decide
(RFC 8470 §2: a resource may have side effects on GET), hence a predicate
and not a list of methods.

On the client, a refusal goes back to the caller instead of a silent resend
in 1-RTT: a transparent resend would ignore RFC 9001 §4.6.2 as soon as the
application state depends on what was sent.

Revisit if a measurement shows 0-RTT lost in production behind a balancer
(a shared register to design), or if an HTTP/3 server needs several
`Config`s on one `Listener` with 0-RTT requests near the field-section bound
(the settings would then go into the ticket).
