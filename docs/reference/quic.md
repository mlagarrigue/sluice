# QUIC reference

The settings of `net/quic`: `quic.Config`, what this end announces and what
it keeps to itself, and `ListenerConfig`, the server side's own bounds. The
[guide](../guide/experimental.md#netquic--a-recent-transport-reimplemented) says when to reach
for them.

## Contents

- [`net/quic` settings](#netquic-settings)
- [Listener settings](#listener-settings)

## `net/quic` settings

`DefaultConfig()` fills in the "Default" column. The fields marked
*local* are `quic.Config`'s own, beside the `TransportParameters` it
embeds: they never go on the wire, and set what this end does, not what it
announces.

| Field | Default | Unit, bound | What it bounds |
|---|---|---|---|
| `MaxIdleTimeout` | 30 s | duration; zero is no timeout, which this package never announces of its own | a connection nobody is using |
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
