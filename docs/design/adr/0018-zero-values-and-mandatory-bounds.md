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
And the previous change, adding `H2Config.InitialWindowSize`, raised the
question of copying it into `Config`.

## Decision

**Zero is the default, named in the field's comment; the bound itself is
never removed.**

1. **Zero means the default.** Every field of `Config` and `H2Config` says
   what its zero is worth ("Zero means 32 KiB"). The default is safe on a
   public socket: modest rather than generous. Zero is the one value that
   stands for another: a negative value, or one outside the range the
   protocol allows (`H2Config.MaxFrameSize` in 16384..2^24−1,
   `H2Config.InitialWindowSize` at most 2^31−1, RFC 9113 §6.5.2;
   `MaxConcurrentStreams` at most 4096 for `Config.TransportParameters`,
   the streams `ServeH3` serves on one connection), is refused before the
   first byte: `Serve`, `ServeH2`, `ServeH3` and `TransportParameters`
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

3. **One default, one place.** A bound two configurations share takes its
   default from one constant (`httpstream.go`: `defaultMaxHeaderBytes`,
   `defaultMaxBodyBytes`, `defaultIdleTimeout`…). A configuration derived
   from another copies the values once the defaults are applied; it does
   not choose again: `Serve` builds the `H2Config` of an HTTP/2 connection
   through `h2ConfigOf(cfg)`, the one place one is derived from the other,
   and `H2Config.resolve` chooses only what HTTP/2 alone has.
   `TestH2ConfigDefaultsFollowConfig` locks it.

4. **A field says which protocols read it.** A field read by one protocol
   stays in `Config` and says so ("HTTP/1.1 only": `MaxRequestLineBytes`,
   `MaxRequestsPerBatch`, `MaxRequestsPerConn`; "HTTP/3 only":
   `QPACKStatic`). A field that reaches a protocol through another object
   says so and is derived there: `IdleTimeout` becomes the
   `max_idle_timeout` that `Config.TransportParameters` produces, and a
   connection served on other parameters has those parameters' timeout.

5. **What goes into `Config` and what does not.** `Config` carries what
   the deployer bounds — bytes, counts, durations — and what has a meaning
   for at least two protocols. What the protocol names on the wire — an
   HTTP/2 `SETTINGS_*`, a QUIC transport parameter — stays in that
   protocol's configuration: `H2Config.InitialWindowSize`,
   `H2Config.MaxFrameSize`, the QUIC windows of `TransportParameters`.
   `InitialWindowSize` is not copied into `Config`: it is the textbook case
   of the rule.

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

**Why not merge `H2Config` into `Config`.** Removing `ServeH2(…, H2Config)`
is an API break (`!`). The derivation already gives a `Serve` user one
configuration; `ServeH2` keeps the settings the protocol names. The merge
stays among the v1 prerequisites, to be decided with the other breaks.

**What is lost.** `ServeH2` on an `H2Config{}` now bounds the header block
at 32 KiB instead of 64 KiB — a client that sent blocks between the two is
refused; under `Serve` it already was. `MaxConcurrentStreams` goes from 64
to 100 over HTTP/3: more assembly state per connection, but the aggregate
stays bounded by `4 × MaxBodyBytes`. A configuration that started with a
negative or out-of-range value no longer starts: an API break, announced
as one, and the error says what to fix.

**Revision.** A real client sensitive to a 32 KiB header block; a third
protocol configuration, which would tell whether rule 5 holds or the merge
is due.
