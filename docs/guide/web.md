# Web

## What this is about

A web server receives HTTP **requests** ("give me order 42", "change line
7") and returns **responses** with a **status code** (200: fine; 400: your
request is malformed; 422: I understood it but refuse it; 500: I made a
mistake). In Go, the standard library's `net/http` does all the network and
protocol work; you write **handlers**, functions called for each request.

Sluice.go adds neither a server, nor a router (what decides which handler
serves which address), nor a layer on top of your handlers. It brings two
things:

- `web`: functions a handler **calls** — read a JSON body safely, check a
  token, choose a status code, write the response;
- `gateway`: a way to **group** the requests that arrive at the same time,
  so that a database asked sixty-four times is asked once.

## A handler you can read on one screen

```go
mux := http.NewServeMux() // the standard library's router
mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
    var body struct{ SaleQty int64 `json:"saleQty"` }
    if d, ok := web.DecodeJSON(r, 1<<16, &body); !ok {            // read at most 64 KB of JSON
        web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d)) // malformed: 400
        return
    }
    order, d, ok := web.PathInt64(r, "order")                     // the {order} from the address
    if !ok {
        web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
        return
    }
    res, err := svc.Apply(r.Context(), Amendment{OrderID: order, SaleQty: body.SaleQty})
    // …
    web.WriteJSON(w, web.Status(res.Diagnostics), web.Report{  // 200 or 422, depending on what happened
        Problems: web.Problems(res.Diagnostics),
        Remedies: web.Remedies(res.Affordances),
    })
})
```

> **In plain terms: why no wrapper around the handler?** Many frameworks
> have you write a function that returns an object, and choose the status
> code and the format for you. That is comfortable, and it is precisely
> what disappears from the code: the two decisions that matter in a handler
> are the status and the encoding, and you must be able to read them on one
> line. One screen, no adapter.

`DecodeJSON` requires a maximum size: a body without a limit is an open
door to a client sending gigabytes. `PathInt64` reads a **wildcard** from
the address (`{order}`), which `http.ServeMux` can extract since Go 1.22.
`Status` picks the code from the diagnostics (see below); `WriteJSON`
writes the response as JSON.

`DecodeJSON` also requires the request to say it carries JSON: one
`Content-Type` header, `application/json` or a `+json` subtype (such as
`application/merge-patch+json`). Any other type, or none, is refused with a
400 before a byte is read: a browser sends a form or `text/plain` to another
site without asking permission, and a JSON document hidden in one would
otherwise be decoded as if the client had meant to send it. With `curl`,
remember `-H 'Content-Type: application/json'`.

To stay short, this example authenticates no one. A handler that serves
protected data authenticates first and decodes after: see
[Authenticating, authorizing](#authenticating-authorizing).

## Diagnostics and remedies: refusing cleanly

When a business rule refuses a value ("quantity must be positive"),
Sluice.go does not return an error that stops everything. It attaches a
**diagnostic** to the value — what is wrong, where, how serious — and,
beside it, a **remedy** (`Affordance`) — what the client can do to fix it,
through which action, with which fields:

```go
at := diagnostics.Field("lines").Index(i).Field("saleQty") // where, in the business object
refused := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Line.InvalidQty", at).
    WithMessage("QtyMustBeOverZero", map[string]any{"min": 1}). // a message key, and its named arguments
    WithOrigin(diagnostics.OriginBusinessRule)
correct := diagnostics.Affordance{
    Path: at, Action: "Sales.Line.CorrectQty", Method: "PATCH",
    Target: fmt.Sprintf("/orders/%d/lines/%d", order, i),
    Inputs: []diagnostics.Input{{Name: "saleQty", Kind: "integer", Required: true}},
}
```

The client receives a `422` whose body lists the problems and the
remedies, and pairs them by comparing **paths** (`/lines/7/saleQty`): the
remedy is at the same place as the problem. What travels is the message
**key** (`QtyMustBeOverZero`) and its arguments, never a sentence already
written: the client picks the language.

> **In plain terms: three nuances that matter.**
> A diagnostic is not a Go `error` — it does not implement the interface,
> on purpose: a mere warning must not trip any `if err != nil`, and the
> value keeps moving. The internal **cause** (the SQL error, the trace) is
> not exported: the code that serialises the response is *unable* to leak
> it to the client. And `Status` applies a simple rule: no diagnostic, or
> warnings only → 200; at least one error → 422.

`diagnostics.Collector` bounds the number of diagnostics a batch may
produce (`NewCollector(limit)`), keeps the first ones in order and counts
the others: a pathological batch does not drown the signal.

## Grouping calls with `gateway`

Picture sixty-four requests arriving within the same millisecond, each
wanting one row from the database. Usually, sixty-four handlers run in
parallel and make sixty-four SQL queries. `gateway` does something else: it
builds the pipeline **once**, when the server starts, and each handler call
becomes an **element** of a batch. The sixty-four calls leave together,
after **one** `= ANY($1)` query, and each gets its answer.

```go
pipeline := func(calls sluice.Stream[*gateway.Call[Amendment, Result]]) {
    var keys []LineKey                  // buffers reused for the life of the service
    calls(func(b sluice.Batch[*gateway.Call[Amendment, Result]]) bool {
        keys = keys[:0]
        for _, c := range b.Items {     // 1. validate each call; nothing is dropped
            keys = append(keys, LineKey{c.In.OrderID, c.In.Index})
        }
        found, err := store.LoadLines(ctx, keys) // 2. one read for the whole batch
        if err != nil {
            for _, c := range b.Items { c.Fail(err) } // everyone is told, nobody waits
            return true
        }
        for _, c := range b.Items {     // 3. answer each one, applied or refused
            c.Reply(apply(c.In, found))
        }
        return true
    })
}
gw := gateway.New(gateway.Config{Size: 64, Within: 2 * time.Millisecond}, pipeline)

// In a handler, from any goroutine:
res, err := gw.Do(r.Context(), amendment)
```

`Size` is the maximum number of calls per batch; `Within` is how long a
lone call agrees to wait for company. Both are required: no value suits
every service, and without a ceiling an isolated call would wait for a
batch that never fills. Sixty-four and two milliseconds are a reasonable
start in front of a database. `Logger`, optional, receives the pipeline's
panic — its value and its stack — before the callers get
`ErrPipelinePanicked`; nil means `slog.Default()`. A gateway whose pipeline
panicked stays dead: build a new one. What the gateway and the server log,
and what never leaves the log, is
[ADR 0025](../design/adr/0025-observability.md).

> **In plain terms: the gateway's rule.** Every call gets **exactly one**
> answer, through `Reply` or `Fail`. Never zero: the pipeline's element *is*
> the call, with the channel its answer returns through, and a call removed
> from the stream is a client waiting until its context expires. A business
> refusal is answered with its diagnostics. Cancelling a `Do`'s context ends
> the client's wait, not the batch's work.

> **In plain terms: when it pays.** The gateway only saves round trips.
> Measured in front of a resource that serves 16 queries at once: 11.9 times
> the throughput of one query per call, 15.4 with parallelism inside the
> pipeline. With nothing behind it, it only costs: one call every 3.7 µs at
> best, where a direct call is free. Without a database or a remote service,
> no gateway. And under WSL2, a `Within` below one millisecond is rounded up
> by the system clock: 500 µs cost a lone call 1.11 ms. The benchmarks are in
> [the measurements](../benchmarks.md#the-gateway-against-a-bounded-backend).

## Authenticating, authorizing

**Authenticating** is knowing who is calling; **authorizing** is deciding
what they may do. The client sends a **token** (a `Bearer` in the
`Authorization` header), signed by your authentication service; `web`
verifies it and derives a **principal** (the identity), then a **grant**.

```go
verifier, err := web.NewHMACVerifier(web.HMACConfig{
    Secret: secret, Audience: "orders", Issuer: "auth.internal",
    TenantClaim: "tenant", Leeway: 30 * time.Second,
})

principal, d, err := web.Authenticate(r, verifier) // reads the Bearer, checks signature and claims
if err != nil {
    log.Println(err)                                                     // "expired", "bad signature": for you
    w.Header().Set("WWW-Authenticate", web.BearerChallenge("orders", d)) // a 401 names its scheme and realm
    web.WriteJSON(w, http.StatusUnauthorized, web.ReportOf(d))          // for the client: "refused", no detail
    return
}
grant, d, err := web.Authorize(principal, policy) // policy: your Authorizer, or ClaimAuthorizer
```

> **In plain terms: two recipients, two messages.** The error says *why*
> (bad signature, expired token) and goes to your log. The diagnostic
> rendered to the client says only "refused": explaining to an attacker
> which half of their forgery was right is telling them how to fix the
> other. Everything in `HMACConfig` is a ground for refusal: audience,
> issuer, clock, maximum token size.

The challenge header tells the client how to authenticate: the `Bearer`
scheme, and the *realm*, the name of what it is authenticating for. Pass
the audience your verifier checks — `"orders"` here — so a client holding
tokens for several services knows which one to send.

**The order: `Authenticate`, then `Authorize`, then `DecodeJSON`.**
Verifying a token reads only the `Authorization` header, bounded to 8 KB by
default; decoding reads the body up to its limit — 64 KB in the first
example — and parses it. Decoding first offers that work to any anonymous
caller; authenticating first keeps it for those you serve. And a refused
authentication says nothing about the body: an anonymous client does not
learn which fields your API expects.

`ClaimAuthorizer` builds the grant from the token's own claims; that is
correct only if the token's issuer is your own service. Otherwise,
implement `Authorizer` against whatever knows the policy. The grant is then
passed to the database through `tx.SetLocal`, so that PostgreSQL's
row-level security filters the right tenant's data — without ever writing
the tenant into the SQL text.

## Starting and stopping cleanly

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt) // Ctrl-C cancels ctx
defer stop()
srv := &http.Server{Addr: ":8080", Handler: web.Recover(observe, mux)}
if err := web.Serve(ctx, srv, 10*time.Second, gw); err != nil {
    log.Fatal(err)
}
```

`Serve` blocks while the server runs and returns when it is properly
stopped: on context cancellation, it gives in-flight requests ten seconds
to finish, then closes what it was handed — the gateway, the database
connection. A grace of zero means five seconds, as an `httpstream`
`ShutdownGrace` left at zero does, and a negative grace means no bound. A server without `ReadHeaderTimeout` gets ten seconds by
default: a client sending its headers one byte at a time cannot hold the
process hostage. Likewise, a server with neither `IdleTimeout` nor
`ReadTimeout` gets two minutes: a *keep-alive* connection (kept open for
the next request) that goes quiet after its response is closed instead of
holding a file descriptor forever. And within a request: a server without
`ReadTimeout` gets thirty seconds to read the whole request, body
included, and a server without `WriteTimeout` a minute to answer — without
them, a client that sends one byte of body and then goes quiet holds the
handler inside `DecodeJSON` until the forced stop. A handler that streams
for long, or reads a slow body on purpose, moves its own deadlines with
`http.NewResponseController(w)` (`SetReadDeadline`, `SetWriteDeadline`); a
negative value lifts the bound for the whole server. `Recover` turns a
panic in a handler into a 500 response you observe, instead of killing the
process and every request in flight.

Behind a CDN or a TLS-terminating proxy, a request may arrive with the
header `Early-Data: 1`: the proxy received it in *early data*, sent by the
client before its TLS handshake ended, which a third party can replay
as is. `web.EarlyData(allow, h)` answers such a request 425 Too Early,
unless `allow` accepts it; the client sends it again once its handshake is
through. Accept only what a duplicate does not hurt — a catalogue read, not
an order.

> **In plain terms.** With no such proxy in front of you, the header never
> arrives and the wrapper costs one table lookup per request.

## The complete example

[`example/orders`](../../example/orders) is this whole vertical: a PATCH
served by a pipeline built once, one read and one write per batch into a
real PostgreSQL, refusals answered with their remedy, and an integration
test that counts the round trips to prove there is only one per batch:
`go test ./example/orders -run TestIntegration -v`, against the PostgreSQL the
`SLUICE_PG`, `SLUICE_PG_USER`, `SLUICE_PG_DB` and `SLUICE_PG_PASSWORD`
variables name (it skips without them).

## You can now

- write a handler that reads on one screen and refuses with a remedy;
- group concurrent calls into one round trip with `gateway`;
- authenticate and authorize before the pipeline;
- start and stop the server without cutting a response.

The vertical's tests run without a database, its store faked:

```sh
go test ./example/orders -run 'TestOverHTTP|TestConcurrentRequestsShareOneRoundTrip' -v
```

A variant to try: in `TestConcurrentRequestsShareOneRoundTrip`, change the
batch size given to `orders.New` from `n` to `n/2`. The test fails on
"32 requests caused 2 loads, want 1": the batch size bounds how many
requests share a round trip.
