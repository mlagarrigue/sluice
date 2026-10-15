# ADR 0018 — Zero is the safe default, the bound is mandatory

- **Created:** 2026-10-15
- **Revised:** 2026-10-15
- **Theme:** httpstream

## Problem

`Config` bounded "everything a client controls" and its comment said no zero
value was safe — while the code filled every zero, and every negative value
with it: a `-1` typed for `1` became the default without a word, and an
`H2Config.MaxFrameSize` outside RFC 9113's range was moved to the nearest
bound, again without a word. The godoc of `MaxConcurrentStreams` announced
64, "the transport's own MAX_STREAMS grant", while `quic.DefaultParameters`
grants 100 (RFC 9114 §6.1: an HTTP/3 server SHOULD grant at least 100) and
`H2Config` advertised 100 on its own side; `Serve` carried a special case so
that a zero would not become 64 over HTTP/2. Six bounds existed twice, in
`Config` and in `H2Config`, with different defaults: an HTTP/2 header block
of 64 KiB under `ServeH2`, of 32 KiB under `Serve`. Three `Config` fields
were never read by HTTP/3 — `MaxRequestsPerBatch` and `MaxRequestsPerConn`,
which have no meaning there, and `IdleTimeout`, which has one: the transport
kept its own 30 s whatever the configuration said. A deployer could not tell
what a field did over HTTP/1.1, HTTP/2 and HTTP/3 without reading the code.
Lastly, `H2Config.InitialWindowSize` — the window that carried an upload
eleven times faster at 100 ms of RTT — was reachable through `ServeH2`
alone: `Serve` and `Server`, the entry points of a deployment, derived
their `H2Config` through `h2ConfigOf` and left everything HTTP/2 alone has
on its defaults.

## Decision

**Zero is the default, named in the field's comment; the bound itself is
never removed.**

1. **Zero means the default.** Every field of `Config` says
   what its zero is worth ("Zero means 32 KiB"). The default is safe on a
   public socket: modest rather than generous. Zero is the one value that
   stands for another: a negative value, or one outside the range the
   protocol allows (`MaxFrameSize` in 16384..2^24−1,
   `InitialWindowSize` at most 2^31−1, RFC 9113 §6.5.2;
   `MaxConcurrentStreams` at most 4096 for `Config.TransportParameters`,
   the streams `ServeH3` serves on one connection), is refused before the
   first byte: `Serve`, `Server`, `ServeH2`, `ServeH3` and `TransportParameters`
   return an error naming the field, the value and what zero would have
   meant, all errors joined (`errors.Join`). A default may depend on
   another field: `LingerTimeout` at zero is one second, or `ReadTimeout`
   if shorter; a value set is kept as is. There is no "unbounded" setting:
   `math.MaxInt` is one. `Config{}` is a bounded server, not a server
   waiting to be bounded.

2. **The bound is mandatory.** No field removes a bound by its absence.
   The exception is named: `MaxConns`, whose zero means "no bound",
   because it is the deployer's memory budget and a default would be a
   guess; the per-connection bounds hold without it, and its godoc says a
   public socket sets it.

3. **One configuration, one default, one place.** The three protocols read
   the same `Config`, and each default is chosen once, in `Config.resolve`.
   Nothing derives one configuration from another: `Serve`, `Server` and
   `ServeH2` hand an HTTP/2 connection the resolved `Config` itself, so a
   bound that is set is set for every protocol that reads it.
   `TestServeBoundsHTTP2ByConfig` and `TestServerHonoursInitialWindowSize`
   lock it.

4. **A field says which protocols read it.** A field read by one protocol
   stays in `Config` and says so ("HTTP/1.1 only": `MaxRequestLineBytes`,
   `MaxRequestsPerBatch`, `MaxRequestsPerConn`; "HTTP/2 only":
   `MaxFrameSize`, `MaxFramesPerBatch`, `MaxContinuationFrames`,
   `InitialWindowSize`, `MaxPendingControlBytes`, `MaxWriteBufferBytes`;
   "HTTP/3 only": `QPACKStatic`). A field that reaches a protocol through another object
   says so and is derived there: `IdleTimeout` becomes the
   `max_idle_timeout` that `Config.TransportParameters` produces, and a
   connection served on other parameters has those parameters' timeout.

5. **The boundary is the package, not the name on the wire.** `Config`
   carries everything an `httpstream` server bounds, whether or not the
   protocol names it on the wire: an HTTP/2 `SETTINGS_*` such as
   `InitialWindowSize` or `MaxFrameSize` is a field like any other there.
   What belongs to the transport stays in its package: the QUIC windows are
   `quic.TransportParameters`, which `Config.TransportParameters` derives
   and `ServeH3` takes as they are, because `net/quic` also serves
   connections that do not speak HTTP/3.

## Rationale

**Why zero is the default, and a negative value an error.** Zero is what Go
gives a field nobody wrote: requiring every bound would make `Config{}` an
error, and every deployer would copy the same numbers. A negative value, on
the other hand, was written: turning it into the default hides a typo behind
a server that starts, and the failure shows up later, far from its cause: a
timeout that is not the one typed, a frame size that is not the one chosen.
Refused at start-up, with the field, the value and the range, the typo is
fixed by reading the message; `errors.Join` gives them all at once rather
than one per restart. The `net/http` convention — zero means "no timeout" —
is exactly what the rule refuses: there, a `Server{}` waits for a slow
client without end; here, `Config{}` cuts it at 10 s. The architecture's S3
invariant (restrictive defaults) holds by construction, not by review.

**Why the bound is mandatory.** A bound a configuration can remove is a
bound that will be removed in a hurry, one evening of incident, and never
put back. The S1 invariant — no unbounded allocation — only holds if the
bound does not depend on a field being filled in.

**Why one place.** The 64 KiB / 32 KiB gap and the 64 / 100 gap were only
found by reading; two default functions that each choose diverge at the
first edit. The shared constant makes a divergence a compile error or a red
test.

**Why one `Config`.** The previous version of rule 5 kept the HTTP/2
settings in `H2Config`, by the name on the wire. The result: `Serve` and
`Server`, what a deployment calls, could set none of them — neither the ×11
window nor the frame size — and `h2ConfigOf` was a mirror to maintain:
every shared bound added had to be copied into it, every HTTP/2 field added
raised the question of whether `Serve` would reach it, and `ShutdownGrace`
and `Logger`, which `ServeH2` needs to stop cleanly, would have grown it
further. The name on the wire says how a bound travels, not who sets it.
The API break (`!`) is accepted in 0.x, grouped with the others of the
breaking release.

**What is lost.** `H2Config` is gone: a `ServeH2` caller writes `Config{…}`
instead of `H2Config{…}` and `MaxHeaderBytes` instead of
`MaxHeaderBlockBytes`. `Config` has six fields an HTTP/1.1-only deployment
never sets; each says "HTTP/2 only". `ServeH2` on an empty configuration
bounds the header block at 32 KiB, instead of the 64 KiB before this
decision — a client that sent blocks between the two is
refused; under `Serve` it already was. `MaxConcurrentStreams` goes from 64
to 100 over HTTP/3: more assembly state per connection, but the aggregate
stays bounded by `4 × MaxBodyBytes`. A configuration that started with a
negative or out-of-range value no longer starts: an API break, announced
as one, and the error says what to fix.

**Revision.** A real client sensitive to a 32 KiB header block; a bound
that would mean two things under one name depending on the protocol, which
would call for splitting the field rather than sharing it.
