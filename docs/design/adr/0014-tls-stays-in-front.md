# ADR 0014 — TLS stays in front of the experimental transport

- **Created:** 2026-08-22
- **Revised:** 2026-10-15 (recommended configuration documented), 2026-10-21 (the TLS requirements of RFC 9113 §9.2 are the host's), 2026-10-21 (the same delegation holds inside `net/quic`)
- **Theme:** quic

## Problem

Without TLS termination there is no ALPN (the TLS extension by which a
client and server agree on the application protocol), and without ALPN a
browser never negotiates HTTP/2 — so `httpstream`'s HTTP/2 was
prior-knowledge only, and nothing said whether that was a choice.

## Decision

**The package does not terminate TLS; the deployment puts
`tls.NewListener` in front.** `Serve` takes a `net.Listener`, and
`crypto/tls` wraps one: the stdlib, no new parser, no configuration
surface here. ALPN (`NextProtos: ["h2", "http/1.1"]`) settles what the
client speaks, and the existing preface sniff reads the decrypted stream
exactly as it reads a plaintext one. HTTP/3 is unaffected: QUIC carries TLS
1.3 inside the transport, and `net/quic` runs that handshake itself — with
the same stack, `crypto/tls` through `tls.QUICClient` and `tls.QUICServer`,
and the same consequence: the handshake's positions are the Go release's,
inherited as they are and listed in the limits guide (`net/quic` TLS),
never reimplemented here.

## Rationale

Terminating TLS inside the package buys nothing the listener seam does not
already provide, and it would put certificate and handshake configuration
— material that rotates on operational schedules — into a package whose
whole posture is "put something you trust in front of this". What this does
not provide is protocol selection *by* ALPN — the sniff decides, ALPN
merely makes the client cooperate — which is fine while the two agree and
would need revisiting only if a protocol without a distinguishing preface
ever joined. The configuration to put in front — TLS 1.2 floor, AEAD
suites, ALPN — lives in
[Deployments](../../guide/deployments.md#tls-in-front), and
`scripts/interop-http.sh` replays curl and h2spec through it.

The TLS requirements RFC 9113 §9.2 puts on an HTTP/2 endpoint — TLS 1.2 at
least, no compression, no renegotiation, the key-size floors and the
prohibited cipher suites of Appendix A — are therefore met by the host and
its `tls.Config`, never checked by the package, which reads neither the
version, the suite nor the ALPN. `crypto/tls` never compresses and never
renegotiates on the server side (`Renegotiation` defaults to
`RenegotiateNever`), so where §9.2.1 asks for a connection error of type
PROTOCOL_ERROR on a renegotiation the peer gets a TLS alert instead: the
connection ends the same, one layer down and without a GOAWAY. The package
cannot do otherwise without seeing the session, which this record says it
does not.
