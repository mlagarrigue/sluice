# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> **Pre-1.0.** Anything may change in a minor release. Breaking changes are
> listed under **Changed** or **Removed** and will not be silent, but they will
> happen: the point of v0 is to get the API right before freezing it at v1.

## [Unreleased]

### Added

- `Stream[T]` and `Batch[T]`, the two core types.
- Operators: `Of`, `Empty`, `Map`, `Convert`, `Filter`, `Concat`, `Coalesce`,
  `Split`.
- Architecture specification (`docs/ARCHITECTURE.md`) and measured performance
  ceiling (`docs/BENCHMARK-STEP-0.md`).
- Testable examples for every exported operator.
- `Merge`, which interleaves several streams a batch at a time in O(1) memory,
  with the completion condition as an explicit `Completion` argument — `WhenAll`
  or `WhenAny`, never an implicit default. Measured at 0.40 ns/element for two
  sources and 0.43 for eight, against a 0.33 ns baseline.
- `ErrSplitStalled`, reported when `Split` branches are drained one after the
  other instead of in alternation.
- A test asserting the documented per-stage budget, so a performance regression
  fails the build instead of living on in a Markdown file.
- [ADR 0001](docs/adr/0001-split-bounded-slot.md), recording the `Split`
  trade-off and what would reverse it.

### Fixed

- `Split` traversed its source once per consumed branch. On a single-pass source
  — a cursor, a network read — every branch after the first silently received
  nothing. It now drives a single traversal, with one batch of slack per branch;
  consuming concurrent branches one after the other panics with
  `ErrSplitStalled` rather than yielding part of the data.
- `Split` dereferenced a nil `route` instead of rejecting it.
- `Split` called `route` several times for the same batch, which double-advanced
  a stateful routing function: round-robin — a documented mode — misrouted its
  data. `route` is now called exactly once per batch.
- Documented the contracts that could previously only be found by reading the
  implementation: `Coalesce` discards what it has buffered on an early stop and
  reuses its buffer between batches, and `Map` composed with `Of` writes through
  to the caller's slice.

[Unreleased]: https://github.com/mlagarrigue/sluice/compare/main...HEAD
