# ADR 0027 — The phase runner: resuming a long job where it died

- **Created:** 2026-11-16
- **Revised:** 2026-11-16 (the lock's connection is the job's own: the connector has no pool; the lock's key), 2026-11-16 (the lock's connection is never one a pool lends), 2026-11-19 (a lock lost while a phase runs ends that phase's context: `Lease.Lost`)
- **Theme:** core

## Problem

An ETL job that runs for hours and whose process dies starts over from the
beginning. The architecture names what is missing — the **phase**, a unit
of restart that outlives the process, beside the stage, a unit of dataflow
that does not — and had left it out on purpose: the state of a resumption
must live in the application's schema, not in the library. It also records
the three properties that make a resumption work: a skip is re-validated, a
resume is valid only against the same input (a fingerprint of the source,
the domain parameters and a `logicVersion`, the operator's settings left
out), and a second process is refused loudly.

Without code, every application writes these three again, and the second
is the one it forgets: the silently hybrid result — half the rows written
by last week's code, half by today's, and no error. The reason for leaving
the machinery out does not require leaving it out; it requires that the
library own none of the state.

## Decision

### The package and its common case

A package `phase`, at the root, a cross-cutting extension like
`diagnostics` and `gateway`. The core does not need it (the dependency
test), and `phase` does not import the core: a phase is any piece of work,
most often a sluice pipeline.

```go
job := phase.Job{
	Name:   "orders-nightly",
	Input:  phase.Input{"source": src, "day": day},
	Logic:  3,
	Store:  phase.Dir("/var/lib/etl"),
	Phases: []phase.Phase{
		{Name: "extract", Run: extract, Verify: stagingCounts},
		{Name: "transform", Run: transform, Verify: transformedCounts},
		{Name: "load", Run: load},
	},
}
report, err := job.Run(ctx)
```

`Run` does the work and returns what it produced as named counts
(`phase.Counts`, integers); `Verify` reads reality back and checks it
against the counts recorded. `Input` maps strings to strings: the
application formats its own values (a date in ISO 8601), so the canonical
form has no ambiguity. The `Report` says, phase by phase, what was skipped,
run again or run, and why the job started over if it did.

### Where the state lives

The state is in a `Store` the application chooses, and the library keeps
none. A `Store` does three things — take a job's lock, read its record,
write it whole — and there is one record per job name: the fingerprint,
the canonical input, and each phase's state (`done` with its counts,
`running`, `failed` with its message).

Two implementations. `phase.Dir`: one file per job in a directory, written
through a temporary file, `fsync` and a rename, for a job on one machine.
`postgres.PhaseStore`, in `database/postgres`: a table the application
names in its own schema. The objection that kept the machinery out falls
by construction: the state lives with the application, the mechanism comes
from the library.

### A skip is re-validated

A phase recorded as done is skipped only when its `Verify` passes; a phase
without a `Verify` is never skipped. A failed `Verify` invalidates that
phase and every phase after it: each reads what the earlier ones wrote.

### A resume is valid only against the same input

The fingerprint is a SHA-256 of the canonical form of the job's name,
`Input` (keys sorted), `Logic` and the ordered list of phase names. `Logic`
is bumped if and only if the same input can now produce different output.
The operator's settings — worker counts, verbosity — have no place in
`Input`: they live in the phases' functions, which the fingerprint does not
see, and so apply to the phases that remain.

A run whose fingerprint differs from the record starts from zero, and the
`Report` says so, naming what changed — the record keeps the canonical
input, not only its hash: "`day` was 2026-11-14, is 2026-11-15", "`Logic`
3 → 4". That is a periodic job's ordinary case, not an error; what is
excluded is resuming phases done under another fingerprint. `Job.Fresh`
forces a restart with the same input.

### A second process is refused

`Store.Lock` comes first and fails at once (`ErrBusy`), never waits.
PostgreSQL: a session-level advisory lock (`pg_try_advisory_lock`), which
"is held until explicitly released or the session ends", so the server
releases it when the process dies. It is taken on a connection of the job's
own, opened for its whole run — never one a pool lends, which would hand
the session to other calls, and one session may take the lock twice — and the record is written
on that same connection: a lost connection — and the lock with it — fails
the next write and stops the job. The lock's key is a 64-bit hash of the
table and the job's name, computed by the client.

The file store: a lease the runner renews while the job runs; an expired
lease may be taken over, a live one refuses. The lease carries a token, and
a write under a token that is no longer the lease's is refused: a process
suspended for longer than its lease, then resumed, stops at its next write
instead of writing beside its successor.

A refused record is not enough: the record is written between phases, and
the phase running when the lock goes keeps filling its working tables
while the successor, which took the lock, empties and fills the same ones
from the start. So a lease says when it is lost (`Lease.Lost`, a channel
closed once): the file store when its renewal finds the lease expired or
another's, PostgreSQL when a `SELECT 1` on the lock's connection, every
third of `Config.CancelTimeout`, finds the session gone — nothing else is
read on that connection while a phase runs. `Job.Run` cancels the running
phase's context then, with `ErrLeaseLost` as its cause. A phase that
honours its context stops within a renewal period of the loss, not at its
end.

### The grain, failure, and the order of work

The phase is the grain of resumption. Nothing resumes inside a phase: it
runs again from its start and must allow it (empty its working table
before filling it). Resuming from the highest committed key stays
excluded, for the reason the architecture gives under "Delivery, at least
once". A job run again after it finished runs again the phases without a
`Verify` and skips the others; that is safe because a phase reruns from
its start.

A phase's error is recorded (`failed`, its message) and returned; the next
run resumes at that phase. A panic propagates and is never turned into an
error; the record stays `running`, which counts as not done.

First `phase`, `phase.Dir` and their tests — a process killed between two
phases and in the middle of one, a failing `Verify`, a changed input, a
second process, an expired lease; then `postgres.PhaseStore`. Outside this
decision: several machines (the distribution decision) and parallelism
between phases — phases are sequential by definition, and parallelism is
inside each one.

## Rationale

A package rather than a recipe: the fingerprint is four lines, and it is
exactly the part a recipe lets one forget; an API where the input and the
settings do not sit in the same place makes it impossible to forget. The
lease and the lock are code each application would get wrong once.

Outside the core and without importing it: the core knows nothing of its
extensions, and a job is not always a pipeline.

A `Store` with two implementations: the reason the machinery was left out
is kept without giving up the mechanism. The file serves a job with no
database; PostgreSQL serves one whose working tables already live there,
the lock and the state beside the data they protect.

No skip without a `Verify`: an `UNLOGGED` table is emptied by a crash
while the checkpoint row survives saying `done`, and a default that trusts
is the default that produces that case.

Starting over and saying so, rather than failing: a new input is a
periodic job's ordinary case, and a refusal would have the operator write
`Fresh` every night — that is, disarm the guard. Keeping the canonical
input in the clear makes the report something one reads: "the fingerprint
changed" says nothing, "`Logic` 3 → 4" says why the phases done were run
again.

Refusing rather than waiting: two processes on the same working tables
corrupt each other, and waiting would hide the orchestration error that
started two.

What this costs: one more package to maintain, and a public interface
(`Store`) whose shape commits — in v0, revisable. What would change it: a
phase of several hours whose resumption inside the phase is measured
necessary, an external orchestrator that already holds the lock, or the
distribution decision.

The lost lease is a method of `Lease` rather than an interface a store may
implement: a store that cannot tell its lock lost cannot keep its phases
from running beside a successor, and saying so is part of being a store.
Polling the PostgreSQL session costs one round trip every third of
`CancelTimeout` per running job, against a phase that writes for minutes;
the session's own end is what the protocol tells only to a statement.
