# ADR 0011 — The middle arrows ship beside the core, as `web/stream`

- **Created:** 2026-08-22
- **Revised:** 2026-10-07 (renumbered; `webstream` became `web/stream`)
- **Theme:** web

## Problem

The architecture draws the web vertical as a chain of stream operators,
and the transport arrows existed on the experimental path. The middle
arrows — routing, authentication, authorisation, body decoding, response
rendering — were either `net/http`-typed functions in `web` or nothing at
all, so a handler over `httpstream` switched on raw target bytes by hand.
Which stages become shipped operators, and where do they live?

## Decision

**Beside the core, not in it.** Nothing in `Stream`/`Batch` needs routing
or principals, so the stages live in `web/stream`, under `web` because
they are the same middle for another transport, and they inherit the
experimental caveat. **The shipped set is the minimum honest one**: route,
authenticate and authorise (batch-shaped over `web`'s `Verifier` and
`Authorizer` — one credential vocabulary, not a fork), decode, and render.
**Context travels in the element**: an `Exchange` wraps the request plus
the route, principal, grant, refusal status and diagnostics — the wrapper
ADR 0005 says belongs outside the core. **Refusals travel; nothing is
filtered**: a dropped element answers the wrong caller on a pipelined
connection, so a refusing stage records a status and later stages step
over the element, and `Render` answers refused and served alike,
positionally.

## Rationale

One `httpstream.Handler` built from these stages serves HTTP/1.1, /2 and
/3 without learning which carried the batch, and `example/vertical` runs
the whole table on it. The cost is a second implementation of bearer
parsing and JSON decoding beside `web`'s `net/http`-typed ones, kept
behaviourally identical and sharing the body helpers through
`web/internal/reqbody`. Content negotiation, sessions and caching wait for
a caller who needs them.
