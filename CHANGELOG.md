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

[Unreleased]: https://github.com/mlagarrigue/sluice/compare/main...HEAD
