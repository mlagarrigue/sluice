# Deployments

The network packages make deployment choices, and each one is carried at the
place in the code that decides it. This page gathers them: what is supported
as it is, what is only supported behind something else, what is not
supported yet.

> **In plain terms.** "Supported" means here: the code makes this choice
> explicitly, a test covers it, and the section says where to read the
> reason. It takes nothing away from the *experimental* label of `net/quic`
> ([What "experimental" means](experimental.md#what-experimental-means)):
> this page says what the code decides, not what an external review has
> confirmed.

## At a glance

| Situation | Supported? |
|---|---|
| [HTTP/1.1 exposed directly](#http11-exposed-directly) | Yes, by construction |
| [Clear text on a bare TCP socket](#clear-text-on-a-bare-tcp-socket) | For interoperability, not across a network |
| [Behind nginx or Envoy](#behind-nginx-or-envoy) | Yes, with the proxy's idle timeout shorter |
| [HTTP/2 clients with a reduced HPACK table](#http2-clients-with-a-reduced-hpack-table) | Yes |
| [HTTP/2 uploads on a link with latency](#http2-uploads-on-a-link-with-latency) | Yes, with a window to set |
| [QUIC clients behind a mobile NAT](#quic-clients-behind-a-mobile-nat) | Yes |
| [A QUIC server on a host with several addresses](#a-quic-server-on-a-host-with-several-addresses) | Linux only |
| [HTTP/3 clients resuming in 0-RTT](#http3-clients-resuming-in-0-rtt) | On request |
| [TLS in front](#tls-in-front) | Yes, by wrapping the listener |

## HTTP/1.1 exposed directly

Supported by construction, with no proxy in front. Parsing refuses anything
two hops could read differently: obsolete line folding, a space before the
colon, control characters, `Transfer-Encoding` next to `Content-Length`. An
absolute-form target (`GET http://host/path`) is served as the origin-form
it holds (`GET /path`), its authority taking the Host's place. A proxy that
re-splits chunks is not required.

Where it is decided: `net/httpstream/request.go`, `splitRequestLine` and
`parseHeaders`.

## Clear text on a bare TCP socket

HTTP/1.1 or HTTP/2 in clear text on a bare TCP socket is kept for
interoperability — a terminating proxy, a health probe, local tooling —
**not for a network boundary**: whatever crosses a network goes behind TLS
([TLS in front](#tls-in-front)) or over HTTP/3, whose transport encrypts by
construction.

Where it is decided: `net/httpstream/serve.go`, the documentation of
`Serve`; `h2conn.go`, the documentation of `ServeH2Conn`.

## Behind nginx or Envoy

HTTP/1.1 behind nginx or Envoy, with the upstream connections reused, is
supported. The rule: the proxy's upstream idle timeout must be **shorter**
than `IdleTimeout`, so that the proxy closes first instead of reusing a
connection this server is closing and reporting a 502.

- **nginx** keeps its upstream connections 60 s by default, below the 2 min
  default of `IdleTimeout`: nothing to set as long as neither is raised or
  lowered.
- **Envoy** keeps them an hour: set `idle_timeout: 60s` under
  `common_http_protocol_options` in the cluster's `HttpProtocolOptions`.

Every response that keeps the connection does announce
`Connection: keep-alive` and `Keep-Alive: timeout=<IdleTimeout in seconds>`,
but neither proxy reads that value: nginx ignores the header, Envoy strips
it. HTTP/2 announces the end with `GOAWAY` — the frame that says no new
stream will be taken — instead.

The nginx configuration in full, to copy as it is, where only the server's
address changes. An *upstream* is the group of servers nginx relays to;
`keepalive` there keeps connections open between two requests instead of
opening one per request.

```nginx
upstream app {
    server 10.0.0.1:8080;
    keepalive 16;           # connections kept per worker; on by default since nginx 1.29.7
    keepalive_timeout 60s;  # nginx's default, below IdleTimeout's 2 min
}

server {
    listen 80;

    location / {
        proxy_pass http://app;
        proxy_http_version 1.1;          # needed before nginx 1.29.7
        proxy_set_header Connection "";  # needed before nginx 1.29.7
    }
}
```

Before 1.29.7, nginx speaks HTTP/1.0 to the upstream and sends
`Connection: close` with every request: without the last two lines, every
request opens a connection and `keepalive` does nothing. Since 1.29.7 that
is the default, and the two lines no longer change anything. If you raise
`keepalive_timeout` above 2 min, raise `IdleTimeout` above it.

Where it is decided: `net/httpstream/response.go`, `keepAliveField`;
`httpstream.go`, `defaultIdleTimeout`.

## HTTP/2 clients with a reduced HPACK table

HPACK is HTTP/2's header compression: both ends keep a table of the fields
already sent, and a client may ask for a smaller one than the default, as
nghttp2 and Envoy do. Supported: the table size requested by
`SETTINGS_HEADER_TABLE_SIZE` is honored from the next header block, in the
same critical section as the acknowledgement.

Where it is decided: `net/httpstream/h2write.go`, `ackSettings`.

## HTTP/2 uploads on a link with latency

Supported, with a window to set. HTTP/2 lets a sender have only a *window*
of unacknowledged bytes in flight; the default stream window of 65535 bytes
is one round trip's worth, at most 0.65 MB/s at 100 ms of RTT (round-trip time,
the delay for a packet to go and its answer to come back).
`Config.InitialWindowSize` raises it, for `Serve`, `Server` and
`ServeH2Conn` alike, and 1 MiB measured eleven times the default on a 4 MiB
upload through a proxy adding 100 ms. The connection window follows it, so
it never queues what the streams alone admit.

Where it is decided: `net/httpstream/httpstream.go`,
`Config.InitialWindowSize`; `h2rtt_test.go`, `TestH2UploadThroughputAtRTT`.

## QUIC clients behind a mobile NAT

A NAT (network address translation, what a home router or a mobile network
does) may change a client's address in the middle of a connection.
Supported: four active connection IDs per endpoint by default
(`TransportParameters.ActiveConnIDLimit`), enough for a NAT rebind and a
deliberate rotation to overlap; the new path is validated by
`PATH_CHALLENGE` — a probe the other end must echo — before it is believed.

Where it is decided: `net/quic/migration.go`, `issueLocalCIDsLocked` and
`migrateLocked`.

## A QUIC server on a host with several addresses

**Linux only**: `net/quic/udp` learns which address a datagram arrived on
and answers from it. Elsewhere, `EnablePacketInfo` returns
`errors.ErrUnsupported` and the kernel picks the source by route: the
client sees a reply come from an address it never wrote to, which for QUIC
is a different path. Bind the socket to one specific address, or stay on
Linux.

Where it is decided: `net/quic/udp/udp.go`, `EnablePacketInfo`.

## HTTP/3 clients resuming in 0-RTT

0-RTT lets a client that has connected before send its first requests
before the handshake ends — and lets whoever recorded them replay them.
Supported on request. `ListenerConfig.EarlyData` accepts the 0-RTT of a
ticket this `Listener` issued, once, within ten minutes; anywhere else the
connection resumes without 0-RTT. `Config.EarlyData` says which requests to
act on before the handshake ends; nil: none, they wait for its end, which a
replay never reaches. A request carrying `Early-Data` (an intermediary
received it in early data) is answered 425 unless the predicate allows it.
Allow only what a duplicate does not hurt.

Where it is decided: [ADR 0019](../design/adr/0019-quic-early-data.md);
`net/quic/earlydata.go`; `net/httpstream/h3serve.go`, `allowedEarly`.

## TLS in front

On the `net/http` path, `web.ServeTLS` serves TLS and HTTP/2
([web](web.md#over-tls-and-http2)). The `httpstream` package does not
terminate TLS ([ADR 0014](../design/adr/0014-tls-stays-in-front.md)): `crypto/tls` wraps
the listener, and `Serve` reads the decrypted stream as it would read a
bare socket.

```go
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    MinVersion:   tls.VersionTLS12,
    CipherSuites: []uint16{ // TLS 1.2 only: TLS 1.3 picks its own, all AEAD
        tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
        tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
    },
    NextProtos: []string{"h2", "http/1.1"}, // ALPN: a browser opens HTTP/2 this way only
}
err := httpstream.Serve(ctx, tls.NewListener(ln, tlsConfig), httpstream.Config{}, handle)
```

TLS 1.3 alone would do (`MinVersion: tls.VersionTLS13`, no `CipherSuites`):
all of its suites are **AEAD** (encryption and authentication in one pass)
and Go picks them itself. The list only serves clients still on TLS 1.2, to
keep them off the suites RFC 9113 (Appendix A) prohibits for HTTP/2; the
compression and renegotiation it prohibits too, `crypto/tls` never does on
the server side. ALPN is the TLS extension by which client and server agree
on the protocol inside. The package itself checks nothing of what was
negotiated: it reads neither the version, nor the suite, nor the ALPN, and
the preface picks the protocol, as in clear text. `scripts/interop-http.sh`
replays curl and h2spec over `https://` with exactly this configuration.

## Settings at deployment

The library reads no environment variable and no command-line flag: your
program reads its settings, with `os.LookupEnv`, `flag` or `encoding/json`,
and puts them in the `Config` types. A field left at zero takes the default
the tables give. Before listening, call `Validate` (`httpstream.Config`,
`gateway.Config`): a value the server would refuse stops the program at
start, naming the field, before it takes its port.
[`example/settings`](../../example/settings/settings.go) does it for the
gateway and the HTTP timeouts. The tables:
[PostgreSQL](postgres.md#connection-settings),
[gateway](web.md#gateway-settings),
[token verifier](web.md#verifier-settings),
[`net/quic`](experimental.md#netquic-settings) and
[`net/httpstream`](experimental.md#nethttpstream-settings).

> **In plain words:** your settings come from wherever you like; check
> them with `Validate`, and only then open the port.

## What applies to every section

Two behaviors of `ServeH3Conn` complete the sections above without fitting
in one: manual stream credit only turns on if the announced windows cover
the admission bounds, and otherwise falls back to the transport's automatic
credit, bounded by the ceilings of `Config` (`net/httpstream/h3serve.go`,
at the top of `ServeH3Conn`); `Config.TransportParameters` produces
accepted windows.
