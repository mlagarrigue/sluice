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

## A handler, eight lines you can see

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
> line. Eight lines, no adapter.

`DecodeJSON` requires a maximum size: a body without a limit is an open
door to a client sending gigabytes. `PathInt64` reads a **wildcard** from
the address (`{order}`), which `http.ServeMux` can extract since Go 1.22.
`Status` picks the code from the diagnostics (see below); `WriteJSON`
writes the response as JSON.

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
start in front of a database.

> **In plain terms: the gateway's rule.** Every call gets **exactly one**
> answer, through `Reply` or `Fail`. Never zero: the pipeline's element *is*
> the call, with the channel its answer returns through, and a call removed
> from the stream is a client waiting until its context expires. A business
> refusal is answered with its diagnostics. Cancelling a `Do`'s context ends
> the client's wait, not the batch's work.

> **In plain terms: when it pays.** The gateway only saves round trips.
> Measured: ×6.9 against one query per call in front of a database, ×8.2
> with parallelism inside the pipeline — and ×65 *slower* when there is
> nothing behind to amortize. Without a database or a remote service, no
> gateway. And under WSL2, a `Within` below one millisecond is rounded up to
> 1.12 ms by the system clock; a test measures it.

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
    log.Println(err)                                              // "expired", "bad signature": for you
    web.WriteJSON(w, http.StatusUnauthorized, web.ReportOf(d))   // for the client: "refused", no detail
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
connection. A server without `ReadHeaderTimeout` gets ten seconds by
default: a client sending its headers one byte at a time cannot hold the
process hostage. `Recover` turns a panic in a handler into a 500 response
you observe, instead of killing the process and every request in flight.

## The complete example

[`example/orders`](../../example/orders) is this whole vertical: a PATCH
served by a pipeline built once, one read and one write per batch into a
real PostgreSQL, refusals answered with their remedy, and an integration
test that counts the round trips to prove there is only one per batch.
