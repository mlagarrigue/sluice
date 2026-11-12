# Protocol positions

How the longest limits were measured or argued, and where the library
departs from a specification: the platforms, authentication, the interop
runner, the datagram size, the QUIC transport and TLS. [Limits](../guide/limits.md) says each
limit in a line and links here.

## Platforms, in detail

What CI runs on every push, for Go 1.26 (the floor `go.mod` names) and the
latest stable release:

| System | Architecture | What runs | Supported |
|---|---|---|---|
| Linux | amd64 | the whole suite under `-race`, PostgreSQL 15 to 19 (19 from its release candidate), QUIC and HTTP interop | yes |
| Linux | arm64 | `net/quic/udp`, `net/quic`, `net/httpstream` under `-race` | yes for these packages; the rest is pure Go, not run |
| Windows | amd64 | the same packages; all the code is vetted for Windows from the Linux runner (`GOOS=windows go vet`) | likewise |
| macOS | arm64 | the same packages; all the code is vetted for macOS from the Linux runner (`GOOS=darwin go vet`) | likewise |
| BSD, others | — | nothing | no: builds, without "Don't Fragment" (QUIC stays at 1200 bytes) |

To contribute on Windows, use WSL: the scripts are `sh`
([CONTRIBUTING](../../CONTRIBUTING.md)). In plain words: in production,
Linux is the safe platform; Windows and macOS run the tested network code,
and the rest of the code is Go that compiles there.

## Authentication, in detail

SCRAM-SHA-256 (with channel binding over TLS, required
or disabled by `Config.ChannelBinding`),
cleartext password on explicit request, and `trust`. SCRAM-SHA-1, which
RFC 5802 §4 requires of every SCRAM client, is not implemented: no
PostgreSQL server offers it. The SCRAM username field is sent empty, as
libpq sends it: PostgreSQL takes the role from the startup packet and
ignores it, where RFC 5802 §5.1 wants a name. A password that SASLprep
(RFC 4013) would refuse — not UTF-8, a prohibited character, mixed
bidirectional text — is sent as its raw bytes, the way `pg_saslprep`
derived the role's key on the server: refusing it here would lock out a
password the server accepts.

**`md5` is refused**
— on purpose: the hash stored server-side is as good as the password, and
PostgreSQL has deprecated it. The error message says what to change on
the server.

The PBKDF2 iteration count the server asks for is accepted
between 4096 (RFC 7677's floor, PostgreSQL's default) and 1,048,576, and
the salt at 16 bytes or more; a server outside that range is refused with
the field to set in the message — `Config.MinSCRAMIterations`,
`MaxSCRAMIterations`, `MinSCRAMSaltLen` — for a role stored at
`scram_iterations = 1000` (a fast test fixture) or at two million (a
hardened server). Zero means the default; a negative value is refused
before anything is sent.

## The interop runner, in detail

The `Interop runner` workflow (the runner at revision `ab039a4`, the one
`scripts/interop-runner.sh` pins) crosses
every implementation in its list every Sunday, on each side its role
allows — 29 peer-directions — except those whose image the registry no
longer serves: mvfst (the registry answers 403). Every
case against such a peer would fail before reaching sluice;
`scripts/interop-runner.sh` keeps them in `UNPULLABLE`, leaves them out of
the matrix, and a run started against one says so. The Sunday pass
builds the endpoint from `main`, so a regression shows up before a
release carries it; the table names the commit it crossed. A release is
crossed on request: the workflow's `image` input pulls the image the
`Interop image` workflow builds from each release tag
(`ghcr.io/mlagarrigue/sluice-interop`, `latest` for the newest). The
table of its latest complete pass is on the
[`interop-results` branch](https://github.com/mlagarrigue/sluice/tree/interop-results);
a failed case, or a direction that wrote no results, is run once more.

A Sunday pass is what the [exit condition](exit-conditions.md) counts, and
none has finished green yet (three, from 2026-10-25 to 2026-11-08), while
passes started by hand have; every pass, scheduled or by hand, is listed on
the workflow's page.

`interop/expected-results.json` records
every case of every implementation the workflow crosses, with the cause
of every failure, and a pass that departs from it fails the job. Green
means every case of every peer went as the record says; a case the
record marks `random`, whose outcome follows the losses the simulator
draws or a capture the runner cuts short, is accepted succeeded or
failed, its cause written beside it.
One pass does not judge it; its history does: the branch keeps the last
eight passes, and a `random` case that succeeded in none of the last six
that ran it fails the weekly job — a client that has stopped succeeding,
where one pass cannot tell that from a bad draw.

What the record holds against quic-go and ngtcp2. As a server, sluice
passes handshake, transfer, longrtt, chacha20, multiplexing, retry,
resumption, zerortt, http3, blackhole, keyupdate, amplificationlimit,
transferloss, transfercorruption, ipv6, rebind-port and rebind-addr
against both, plus ecn and connectionmigration against ngtcp2;
handshakeloss and handshakecorruption pass against quic-go and, against
ngtcp2, in most passes: under 30 % loss a handshake sometimes outlives
`ListenerConfig.HandshakeTimeout` (10 s), and under corruption a file
sometimes never arrives; both are recorded as alternating, with their
cause.

As a client it passes handshake, transfer,
longrtt, multiplexing, retry, resumption, zerortt, blackhole,
amplificationlimit, handshakeloss, transferloss, handshakecorruption,
transfercorruption, ipv6, rebind-port and rebind-addr against both, plus
ecn against ngtcp2. It refuses, with the exit status 127 the runner asks
for, what is documented as not there: QUIC v2, and on the client side
chacha20 (Go's TLS 1.3 offers no way to restrict the cipher suites it
offers), a forced key update and HTTP/3.

Failures whose cause lies outside sluice: quic-go's image refuses ecn
itself. quic-go's client never moves to a server's preferred address, so
connectionmigration sees one path. msquic's server sometimes answers
first under the client's own connection ID, then changes it, and RFC
9000 §7.2 has the client discard what follows (four client-side cases).
And against ngtcp2's server, connectionmigration fails on the client
side: the runner wants a PATH_CHALLENGE in the server's first packet on
the new path, and ngtcp2 probes only after a non-probing packet, which a
client that validates first, as §9.6.2 requires, never sends there.

## Datagram size, in detail

Datagrams start at 1200 bytes, the
smallest maximum RFC 9000 §14.1 guarantees on any path QUIC runs on, and
rise by a search (DPLPMTUD, §14.3, RFC 8899 — probing for the largest
datagram the path lets through): a probe — one PING+PADDING
datagram — of 1452 bytes first (an Ethernet MTU less the IPv6 and UDP
headers), then, while nothing is lost, the ceiling itself; after the
first size lost, each probe halves the range between the last size
confirmed and the smallest lost, down to 20 bytes. A size is lost after
three unanswered probes. A tunnel with an MTU of 1420 settles within
20 bytes of its 1372 bytes of payload, the IPv6 minimum of 1280 at 1212
or more.

The ceiling is `quic.Config.MaxDatagramSize` (zero:
1452; 8952 for a 9000-byte jumbo path), bounded by the
`MaxUDPPayloadSize` the peer announces; the size reached reads in
`Stats.DatagramSize`. The search starts over at 1200 after each
migration. It needs a socket that refuses to fragment (the "Don't
Fragment" option, which the package sets on a `*net.UDPConn` on Linux,
macOS and Windows): on a platform without the option, datagrams stay at
1200 bytes, since a probe the network may fragment proves nothing.

A path that shrinks under a connection (a "black hole",
RFC 8899 §4.3) shows as three probe timeouts in a row, or three
full-size packets lost while only smaller ones are acknowledged: the size
falls back to 1200, what was lost is resent cut to that size, the search
runs again, and `Stats.MTUBlackHoles` counts the fall back. A total
outage of three probe timeouts counts too, and costs one probe once the
path is back. A completed search is not run again on a path that might
have grown. ICMP "packet too big" messages are ignored (§14.2.1 does not
require them, and `net.PacketConn` does not deliver them). The kernel
still reads them: on Linux, `udp.Configure` keeps the path MTU it caches
from one from refusing the 1200-byte datagram (`IP_PMTUDISC_PROBE`,
tested); on macOS a cached path MTU below 1200 does not refuse it either,
which CI tests through a host route of MTU 600; on Windows the socket is
set to `IP_PMTUDISC_PROBE`, which Winsock documents as ignoring the cached
path MTU — the mode is read back, its effect not measured.

A path that cannot carry 1200 bytes is therefore never detected: sending on it goes
on at probe-timeout pace, doubling each time, until the idle timeout
ends the connection; §14.2's "cease sending" has no trigger here, and
the backoff is the bound. A lost probe is not a congestion event
(§14.4).

A Retry token (the token a server sends back for the client to prove its
address, RFC 9000 §8.1.2) is repeated in every Initial packet the client
sends afterwards, and those packets stay at 1200 bytes until the handshake
ends. The client takes any token that leaves, with its length field, 100
bytes of an Initial for handshake data: a token of at most 1034 bytes at a
1200-byte datagram. The ClientHello then goes again in as many Initial
packets as the room left asks — fifteen for a 1500-byte ClientHello beside
the longest token — as RFC 9000 §17.2.5.3 expects. A Retry with a longer
token, one no Initial could carry beside a useful piece of the handshake,
is ignored as a malformed Retry would be — a later, well-formed Retry is
still honoured — and counted in `Stats.DiscardedPackets`. RFC 9000 sets no
bound; a server's token is a few dozen to a few hundred bytes, this
package's own 130 at most.

## The QUIC transport, in detail

Ten requirements of RFC 9000 and RFC 9001 are met otherwise than the text
writes them. Each was chosen, and a conformant peer sees no difference:
the departure only touches a peer that already breaks the protocol. For
each: what the text says, what the package does, why, and where to read
the code in `net/quic`.

**The flow-control limit enforced is the grant computed** (RFC 9000 §4.1,
§19.9, §19.10; §4.6 and §19.11 for streams). The text closes the
connection (FLOW_CONTROL_ERROR, STREAM_LIMIT_ERROR) when the peer goes past
the value "it has sent" — the last MAX_DATA, MAX_STREAM_DATA or
MAX_STREAMS that left. Here the limit enforced rises when the new grant is
computed, a few microseconds before the frame carrying it leaves. A peer
past what it was told, but not past what it is about to be told, is
therefore not refused. Why: keeping two bounds (sent and computed) would
double the hot path's accounting to refuse a peer the next grant covers
anyway. Code: `recvQuota.nextMax` (`flow.go`),
`Conn.returnStreamSlotLocked` (`stream.go`).

**The ACK is owed from the parse** (RFC 9000 §13.1). The text has a packet
acknowledged only once all its frames have been processed. Here the
acknowledgement is owed as soon as the packet is decrypted and parsed,
before its frames act: a reader woken by a frame that answers at once — the
response to a request — carries the ACK out with it. The cost: if a later
frame of the same packet closes the connection, the peer sees an ACK for
the packet that closed it, of no consequence since the close follows. The
gain: one ACK-only datagram fewer per request. Code: `Conn.framesIn`
(`receive.go`).

**The reserved bits of a first Initial close nothing** (RFC 9000 §17.2).
The text treats non-zero reserved bits as a PROTOCOL_VIOLATION. On the
first Initial of a connection not yet created, the `Listener` has no
connection to close: the packet is dropped silently and counted. Why:
Initial keys are public, so the packet can come from anyone; a
CONNECTION_CLOSE to an unvalidated address would be an unsolicited send
toward a possibly forged source (§8.1), and a connection created only to
be closed is the state the drop saves. A real client that sets the bits is
not conformant, and its handshake timeout tells it so. Code:
`initialPacketDecrypts` (`listener.go`).

**A stateless reset with a long header is not recognised** (RFC 9000
§10.3). The text asks that "any packet" ending in a valid reset token be
recognised. Only short headers are compared: a reset has the short-header
shape by construction, no conformant peer sends a long-header one, and
comparing the long headers that route nowhere would make every Initial of
a new client pay a lookup. Code: `Listener.resetFor` (`listener.go`),
`Conn.readShortHeader` and `Conn.openPacket` (`receive.go`).

**A NEW_CONNECTION_ID retired more than 32 retirements ago is new**
(RFC 9000 §19.15). The text forbids treating a frame received several
times as an error. The identifiers this end retired on its own are kept in
a ring of 32: a retransmission arriving after 32 other retirements is no
longer in it, enters the pool as a new identifier, and if the pool is full
the connection closes (CONNECTION_ID_LIMIT_ERROR). A retransmission follows
the original by a probe timeout or two (§13.3), and a peer retires an
identifier per path change: 33 retirements while a duplicate is in flight
do not happen with a conformant peer, and the ring does not grow with the
connection's age. Code: `maxRetiredSelf` and `Conn.handleNewConnectionID`
(`migration.go`).

**At a key's limit, the connection closes with AEAD_LIMIT_REACHED**
(RFC 9001 §6.6). The text has an endpoint that reaches a key-usage limit
"stop using the connection and only send stateless resets", and in the
next sentence recommends closing before that with AEAD_LIMIT_REACHED. The
second path is taken: one CONNECTION_CLOSE under the key in use, then the
closing state, which answers the peer with that same CONNECTION_CLOSE,
bounded. The peer gains a readable error code; the cost is a few bytes
still sent under the spent key. Code: `Conn.atConfidentialityLimitLocked`
(`send.go`), and the integrity limit in `Conn.openPacket` (`receive.go`).

In plain words: these ten departures are choices, not oversights. Each
avoids a cost — a send toward a doubtful address, one more datagram, a
double count — and changes nothing for a peer that follows the protocol. A
peer found to fail on one of them is the reason to revisit it.

## TLS, in detail

The handshake is `crypto/tls`'s, from the Go release
`go.mod` names as the floor, run through `tls.QUICClient` and
`tls.QUICServer`. None of the cryptography is invented here. The key
schedule is HKDF from `crypto/hkdf` with the labels RFC 9001 §5.1
publishes, the packet protection is the AEAD of the negotiated suite —
AES-GCM from `crypto/cipher`, or ChaCha20-Poly1305 from
`golang.org/x/crypto`, the only module outside the standard library
`net/quic` imports ([ADR 0002](../design/adr/0002-dependency-graph.md)) — and
the header protection is one block of the same suite's cipher over a
sample, as RFC 9001 §5.4 specifies. What `net/quic` contributes is the
plumbing, and the plumbing is pinned two ways: RFC 9001 Appendix A.1's
published derivation vectors, and seal/open round trips for what the
vectors do not reach.

What `crypto/tls` refuses, `net/quic` reports as `crypto/tls` says it: a
TLS alert leaves in a CONNECTION_CLOSE with code 0x0100 plus the alert
(RFC 9001 §4.8), which is also how the codes that RFC mandates — 0x0178
for no application protocol (§8.1), 0x016d for missing transport
parameters (§8.2) — reach the wire.

Two refusals RFC 9001 names
PROTOCOL_VIOLATION (0x0a) go out under their alert's code instead:
CRYPTO data of a level already finished, past the end received (§4.1.3,
internal_error); data of a level left unconsumed when the next level's
keys arrive (§4.1.3, unexpected_message). After the handshake,
`crypto/tls` gives no alert at all: its post-handshake path sends the
alert on a record layer QUIC does not have and returns an error without
it, so a KeyUpdate message (§6, which wants 0x010a), a CertificateRequest
on a client (§4.4) and a NewSessionTicket whose early_data is not
0xffffffff (§4.6.1) — the last two PROTOCOL_VIOLATION — close as
internal_error, 0x0150. The connection closes in each case; only the code
differs. The error is all `net/quic` sees, and the same alert serves other
causes — an internal_error is also a real one — so no translation is made.
To revisit when `crypto/tls` states the cause of an alert, or surfaces the
one it records after the handshake.

The ClientHello's X25519 key share repeats the key inside the hybrid
X25519MLKEM768 share, the group RFC 10024 defines. RFC 9846 §4.3.8 asks
for key shares generated independently, and RFC 9954 §3.2, which RFC 10024
builds on, relaxes that for the same algorithm reused within one
ClientHello. RFC 9954 is Informational and updates nothing: the repetition
departs from the letter of RFC 9846 in the way RFC 9954 describes.

Eleven of its positions depart from RFC 9846 and are inherited as they
are; none is visible to a conformant peer, which never sends what they
judge.

Refused, but under another alert than the text's — the handshake stops,
and only the CRYPTO_ERROR code the peer sees differs:

- a ServerHello whose `legacy_version` is not 0x0303: illegal_parameter
  rather than protocol_version (§4.2.3, Appendix E.5);
- a ServerHello whose `legacy_compression_method` is not 0: decode_error
  rather than illegal_parameter (§4.2.3);
- a `pre_shared_key` extension out of last place, or a version never
  offered: decode_error or protocol_version rather than illegal_parameter
  (§4.3.11, §4.3.1);
- a ClientHello with neither `supported_groups` nor a PSK:
  handshake_failure rather than missing_extension (§9.2).

Accepted where the text asks for a refusal:

- a ClientHello whose `legacy_version` is not 0x0303, when
  `supported_versions` is present: the field is not read (§4.2.2,
  Appendix E.5);
- a `legacy_session_id` of 33 to 255 bytes, which the server echoes at
  the same length, past the bound of 32 (§3.4);
- an extension known but forbidden in the message carrying it: ignored
  (§4.3, §4.4.1);
- an empty SNI acknowledgement in EncryptedExtensions when no SNI was
  sent: not refused (§4.3);
- `early_data` in EncryptedExtensions with no PSK selected: not refused,
  and the client takes its 0-RTT as accepted — only the faulty server
  suffers for it (§4.3.11);
- `supported_groups` without `key_share`, or the reverse: the ClientHello
  is accepted (§9.2);
- a `pre_shared_key` without `psk_key_exchange_modes`: the handshake
  continues without the PSK (§4.3.9).

This end's own certificate chain is checked before a packet leaves, in
`Dial`, `DialEarly` and `NewListener`: a leaf with a Key Usage that lacks
digitalSignature, or a member signed with SHA-1, is refused (§4.5.1.2,
§4.3.3). Two cases stay with the caller, as the godoc of `Dial` and
`NewListener` says: a chain that `GetCertificate`, `GetConfigForClient` or
`GetClientCertificate` returns during the handshake, which the package
never sees; and, with several chains configured, the choice of one signed
with an algorithm the peer's `signature_algorithms_cert` lists, which
`crypto/tls` does not compare (§4.5.1.2).

The TLS 1.3 compatibility mode is neither asked for by this
client nor policed by this server: `crypto/tls` sends no
`legacy_session_id` in QUIC and echoes one it receives without the
change_cipher_spec RFC 9846 Appendix E.4 would want — RFC 9001 §8.4
removes that record from QUIC — and the ClientHello field is not
exposed to the server, so the PROTOCOL_VIOLATION §8.4 asks of a server
waits on `crypto/tls`.

A server issues one ticket per connection:
`crypto/tls` refuses a second `SendSessionTicket` on a QUIC connection. A
client presents a ticket once — RFC 9846 Appendix C.4 asks it not to reuse
one, and the cache `Dial` wraps drops it at its first resumption. Of six
connections opened at once from one ticket, the first resumes and the other
five run a full handshake — one round trip more, the certificate checked
again, and no 0-RTT — until each resumed connection has handed back a ticket
of its own; the same appendix recommends that a server offer as many
tickets as a client might open connections.

A `DialEarly` client that
offers 0-RTT does not survive a HelloRetryRequest — the answer of a server
that wants a key share the ClientHello did not carry: `crypto/tls` computes
the PSK binders of its second ClientHello before it drops `early_data` from
it, and the server closes with decrypt_error. To avoid it, the ticket
keeps the key exchange group the server chose, and `DialEarly` offers that
group alone when it resumes with 0-RTT: a server that makes the same
choice from one connection to the next — a server set to P-256 alone,
facing a client with the default groups — has no reason left to ask for
another. What remains is a server that changed its group between the two
connections, or a ticket stored before this version: the connection is
then lost, not only its 0-RTT; its error wraps `ErrEarlyDataRejected` and
names the HelloRetryRequest, and `Dial`, which offers no 0-RTT, resumes
through a HelloRetryRequest unharmed.

Rewriting the TLS stack is out of the question (ADR 0002:
the standard library owns the sensitive ground); these follow the Go
release, and a peer refused over one of them is the signal to revisit.
In plain words: the lock on the door is the one Go ships, quirks
included, and it is kept rather than copied.
