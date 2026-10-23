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

- [Migrating from v0.2.2](migration.md) — what to change in code written
  against the last release.
- [Choosing an operator](guide/operators.md) — what each package retains
  between two batches, and what that costs.
- [PostgreSQL](guide/postgres.md) — queries, binary parameters, `= ANY($1)`,
  COPY, transactions, generated hydrators.
- [Web](guide/web.md) — the supported path over `net/http`, the gateway,
  diagnostics and remedies.
- [Deployments](guide/deployments.md) — behind nginx or Envoy, TLS in
  front, QUIC behind a NAT, 0-RTT: what is supported and what to set.
- [Experimental](guide/experimental.md) — QUIC and the native request path
  as stream stages; `net/httpstream` and `pushdown`, now supported.
  What is promised, and what is not.
- [Limits](guide/limits.md) — what is not done, module by module.

## Something is wrong

- [Troubleshooting](guide/troubleshooting.md) — every error the library
  exports, by package: when it happens, why, and what to do.
- [Limits](guide/limits.md) — when the thing you need is simply not there.

## I want to understand the choices

- [Architecture](design/architecture.md) — the reasoning: two types, the
  batch as the unit of transport, stopping, diagnostics, joins.
- [Decisions (ADR)](design/adr/README.md) — every structural decision, by
  theme: the problem, the decision, the rationale.
- [Security — verifiable properties](design/architecture.md#security--verifiable-properties)
  — the fourteen guarantees, each with the check that holds it.
- [Measurements](benchmarks.md) — the method, the ceiling, the current
  figures.
- [QUIC interop results](https://github.com/mlagarrigue/sluice/tree/interop-results)
  — the latest weekly pass of the QUIC Interop Runner, sluice against every
  implementation, peer by case.

## I want to connect another system

A **connector** links Sluice.go to an external system (a database, a
queue, a file format, a protocol). It is a **provider** when it produces
batches (a `Source`), a **consumer** when it receives them (a function that
takes batches, like the PostgreSQL connector's `COPY`), or both. Only
PostgreSQL has one today ([guide](guide/postgres.md)); what is missing is in
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

- [CONTRIBUTING](../CONTRIBUTING.md) — rules, checks, commit format, releases.
- [SECURITY](../SECURITY.md) — reporting a vulnerability, privately.

## Reference

The API documentation is the code's own: `go doc github.com/mlagarrigue/sluice`
and its packages, or pkg.go.dev.
