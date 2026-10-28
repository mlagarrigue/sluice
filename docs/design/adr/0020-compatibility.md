# ADR 0020 — What v1 freezes

- **Created:** 2026-10-23
- **Revised:** 2026-10-28 (`gateway.Partitions` listed experimental; written in the present tense)
- **Theme:** docs

## Problem

In v0 a minor version may rename or remove anything, and the limits guide
promised a compatibility policy "still to be published". Nothing said what
v1.0.0 commits to: the experimental packages, the sentinel error values, the
diagnostic codes a client reads in a response, the nested modules, the Go
floor. And `scripts/check-api.sh` let through any break announced with a
`!`, a rule that after v1 would produce a major version under the same
module path, which Go modules forbid.

## Decision

**What is promised.** From v1.0.0 until v2, every exported identifier of a
*stable* package of the root module keeps its name, its shape and the
behavior its documentation describes. A package is stable if it is neither
experimental (the first sentence of its documentation says so), nor
`internal`, nor under `example/`. A minor version adds; a patch version
fixes; neither removes.

Part of the contract, like the signatures:

- the identity of sentinel errors (`ErrIdleTimeout`, `ErrEarlyDataRejected`…)
  under `errors.Is` — not the text of their message;
- the strings a client sees and compares: diagnostic codes (`Code*`), message
  identifiers, the JSON field names of `Report`, `Problem`, `Remedy`; new
  ones are added, existing ones do not change;
- documented behavior — a bound, the role of a zero value, an HTTP status.
  The figure of a default (a window, a queue, a timeout) may change in a
  minor version, announced in the CHANGELOG, when a measurement justifies
  it; that zero means the safe default (ADR 0018) does not.

Not part of the contract: the experimental packages (today `net/quic`,
`net/quic/udp`, `web/stream`), whose breaks are still announced with `!` and
listed in the CHANGELOG; the few symbols of a stable package whose
documentation opens a paragraph with "Experimental:", listed in
`scripts/api-experimental.txt` — a listed type with its fields and methods
— today the HTTP/3 entry points of `httpstream` (ADR 0021) and
`gateway.Partitions`, whose cost is measured but not yet when it pays;
`internal/`; `example/`; the nested modules (`interop`, `benchmarks`,
`comparison/…`), which are not published; the text of errors and logs;
performance, measured and published but not promised.

**What still moves.** The Go floor rises in a minor version, never in a
patch, through a `build:` commit (the rule in `CONTRIBUTING.md` and its test
stay). A dependency is added by decision record (ADR 0002). A deprecated
identifier (`// Deprecated:`) stays until v2. An experimental package that
leaves the label enters the promise the day the first sentence of its
documentation changes, a change decided against the exit conditions in
`docs/guide/limits.md`.

**The mechanical reader.** `scripts/check-api.sh` compares every commit with
its parent. Before any `v1.*` tag, a break needs a `!` in the commit
subject, as today. After, a break of a stable package is refused, `!` or
not: it would need a `/v2` module path, which is a decision, not a commit. A
break of an experimental package keeps v0's rule. The list of experimental
packages is read from their documentation, the source `repo_test.go` holds
the README and the guides to; `CHECK_API_MAJOR=1` forces the v1 era so the
rule can be exercised before the tag.

## Rationale

The promise follows the contract Go already puts on modules: a break is a
new path. Writing it down and having the script that judges every commit
read it makes an accidental break impossible rather than documented. The
strings a client sees enter the promise because a client compares them; the
text of errors does not, because it improves and a caller that compares it
holds the wrong handle.

What this costs: after v1, fixing a badly shaped stable API costs a v2,
hence one grouped breaking version before v1. The
promise does not cover the experimental packages, which are precisely the
ones whose shape has not met real use.

Revisit when a first production use needs a guarantee on an experimental
package, or when a stable break is wanted without a v2 — which would mean
revising this record, not working around it.
