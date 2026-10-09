# ADR 0014 — TLS stays in front of the experimental transport

- **Created:** 2026-08-22
- **Revised:** 2026-10-07 (renumbered)
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
1.3 inside the transport, and `net/quic` runs that handshake itself.

## Rationale

Terminating TLS inside the package buys nothing the listener seam does not
already provide, and it would put certificate and handshake configuration
— material that rotates on operational schedules — into a package whose
whole posture is "put something you trust in front of this". What this does
not provide is protocol selection *by* ALPN — the sniff decides, ALPN
merely makes the client cooperate — which is fine while the two agree and
would need revisiting only if a protocol without a distinguishing preface
ever joined.
