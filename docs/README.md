# Documentation

Sluice.go is a Go library for batch-oriented stream processing. This page says
where to go for what you want to do. It is the one page the issue and pull
request templates link to: if a template sent you here, you are in the right
place.

## I am discovering the library

- [README](../README.md) — what it is, why batches, three scenarios, a first
  pipeline.
- [Guide: getting started](guide/getting-started.md) — `Stream`, `Batch`,
  `Source`, `Try`, and what a batch promises.
- [Glossary](glossary.md) — the terms that recur, one sentence each: source,
  pane, demand, principal, grant, QUIC, OID and the rest.

## I am building something

- [Choosing an operator](guide/operators.md) — what each package retains
  between two batches, and what that costs.
- [PostgreSQL](guide/postgres.md) — queries, binary parameters, `= ANY($1)`,
  COPY, transactions, generated hydrators.
- [Redis and Valkey](guide/redis.md) — one connection, a batch of
  commands in one round trip.
- [NATS JetStream](guide/nats.md) — a pull consumer read in batches,
  acknowledged by your code.
- [Files](guide/files.md) — CSV and JSON Lines read and written in
  batches, each record bounded.
- [Web](guide/web.md) — the supported path over `net/http`, the gateway,
  diagnostics and remedies.
- [Native HTTP](guide/httpstream.md) — `net/httpstream`, HTTP/1.1, 2 and 3
  read as batches of requests, the request stages of `web/stream`, and
  `pushdown`, a reader telling its source what it no longer needs. All
  supported.
- [Experimental](guide/experimental.md) — QUIC: what is promised, and what
  is not.

## I want to…

| I want to… | Where |
|---|---|
| install the library | [Getting started: Installing](guide/getting-started.md#installing) |
| write a first pipeline | [Getting started: A first program](guide/getting-started.md#a-first-program) |
| know what an operator keeps in memory | [Operators: One question](guide/operators.md#one-question-what-does-it-keep-in-memory) |
| join two streams | [Operators: `join`](guide/operators.md#join-retains-one-side) |
| spread work over several goroutines | [Operators: `parallel`](guide/operators.md#parallel-retains-goroutines) |
| connect to PostgreSQL | [PostgreSQL: Connecting](guide/postgres.md#connecting) |
| write many rows at once | [PostgreSQL: In bulk, COPY](guide/postgres.md#in-bulk-copy) |
| look up many keys in Redis at once | [Redis: A batch of commands](guide/redis.md#a-batch-of-commands) |
| read messages from NATS JetStream | [NATS: Reading a consumer](guide/nats.md#reading-a-consumer) |
| read a CSV or JSON Lines file in batches | [Files: Reading](guide/files.md#reading) |
| follow the changes of a database | [PostgreSQL: Following changes](guide/postgres.md#following-changes-logical-replication) |
| cancel a running query | [PostgreSQL: Cancelling](guide/postgres.md#cancelling-a-running-query) |
| authenticate a request by its token | [Web: Authenticating](guide/web.md#authenticating-authorizing) |
| group calls into batches | [Web: `gateway`](guide/web.md#grouping-calls-with-gateway) |
| stop a server cleanly | [Web: Starting and stopping](guide/web.md#starting-and-stopping-cleanly) |
| put TLS in front of the server | [Deployments: TLS in front](guide/deployments.md#tls-in-front) |
| choose a QUIC congestion controller | [Experimental: congestion](guide/experimental.md#choosing-a-congestion-controller) |
| serve HTTP/1.1, 2 or 3 without `net/http` | [Native HTTP: `net/httpstream`](guide/httpstream.md#nethttpstream--supported) |
| find a setting, its default and its bounds | [Settings, one page per package](#i-am-looking-up-a-setting-or-an-api) |
| understand an error | [Troubleshooting: By what you see](reference/troubleshooting.md#by-what-you-see) |
| move to a newer version | [Migrating between versions](migration.md) |
| watch a running pipeline | [Deployments: What to watch](guide/deployments.md#what-to-watch-in-production) |

## I am running it in production

- [Deployments](guide/deployments.md) — behind nginx or Envoy, TLS in
  front, QUIC behind a NAT (a router that rewrites addresses), 0-RTT (a
  client resuming a session sends its request before the handshake ends):
  what is supported and what to set.
- [Limits](guide/limits.md) — what is not done, module by module, and the
  [conditions experimental packages leave under](reference/exit-conditions.md).
- [Protocol positions](reference/protocol-positions.md) — where the library
  departs from a specification, and how the longest limits were measured
  or argued: platforms, authentication, the interop runner, the datagram
  size, the QUIC transport and TLS.
- [Measurements](benchmarks.md) — what each operator costs, and how it is
  measured.
- [Troubleshooting](reference/troubleshooting.md) — every error the library
  exports, by package: when it happens, why, and what to do; when the thing
  you need is simply not there, Limits above.
- [SECURITY](../SECURITY.md) — reporting a vulnerability, privately.
- [CHANGELOG](../CHANGELOG.md) — each release: what it adds, fixes or
  breaks.
- [Migrating between versions](migration.md) — what to change in code
  written against v0.4.0, v0.3.0 or v0.2.2.

## I am looking up a setting or an API

Every setting of a package, its zero value, its bounds and what it sets, on
one page per package; the guides say when to reach for them.

- [PostgreSQL settings](reference/postgres.md) — connection, query and
  replication settings, and the types the codec reads and writes.
- [HTTP settings](reference/httpstream.md) — `net/httpstream.Config`.
- [QUIC settings](reference/quic.md) — `quic.Config` and `ListenerConfig`.
- [Web settings](reference/web.md) — the token verifier.

The API documentation is the code's own: `go doc github.com/mlagarrigue/sluice`
and its packages, or pkg.go.dev.

## I want to understand the choices

- [Architecture](design/architecture.md) — the reasoning: two types, the
  batch as the unit of transport, stopping, diagnostics, joins.
- [Decisions (ADR)](design/adr/README.md) — every structural decision, by
  theme: the problem, the decision, the rationale.
- [Security — verifiable properties](design/architecture.md#security--verifiable-properties)
  — the fourteen guarantees, each with the check that holds it.
- [QUIC interop results](https://github.com/mlagarrigue/sluice/tree/interop-results)
  — the latest weekly pass of the QUIC Interop Runner, sluice against every
  implementation, peer by case.

## I want to connect another system

A **connector** links Sluice.go to an external system (a database, a
queue, a file format, a protocol). It is a **provider** when it produces
batches (a `Source`), a **consumer** when it receives them (a function that
takes batches, like the PostgreSQL connector's `COPY`), or both. Two exist
today: PostgreSQL (`database/postgres`, [guide](guide/postgres.md)), both a
provider and a consumer, and HTTP (`net/httpstream`, over `net/quic` for
HTTP/3), whose requests arrive as batches. A new one goes under
`database/<system>` or `net/<protocol>`. What is missing is in
[Limits](guide/limits.md).

What the batch model asks of a connector:

- whole batches, never one element at a time: one round trip per batch
  where the system allows it (`= ANY($1)`, `COPY`);
- a bound the caller states, never memory that grows with the data
  (guarantees S1 and S11 in
  [Security — verifiable properties](design/architecture.md#security--verifiable-properties));
- the stop honoured: a consumer that stops stops the provider, and
  resources are released on every path (S9, S10).

An integration is requested with the "Integration request" issue template.

## I am contributing

- [CONTRIBUTING](../CONTRIBUTING.md) — rules, checks, commit format, releases;
  a vulnerability is reported privately, as [SECURITY](#i-am-running-it-in-production) says.
- [Code of conduct](../CODE_OF_CONDUCT.md) — what everyone here keeps to.
- A question about using the library, or feedback after trying it: the
  "Question or feedback" issue template.
