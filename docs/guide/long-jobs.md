# Long jobs: resuming where the process died

A word you do not know is likely in the [glossary](../glossary.md).

## What this is about

An ETL job (extract, transform, load) that runs for hours and whose
process dies — a machine restarted, a deployment, memory run out — starts
over from the beginning. The package `phase` cuts the job into **phases**,
records each one when it finishes, and on the next run skips the phases
done and resumes at the first one that is not.

> **In plain words: a phase is a unit of restart.** Nothing resumes in the
> middle of a phase: it starts again from its beginning. So a phase must be
> able to run again — empty its working table before filling it.

## A job

Two phases: `extract` fills a working table, `load` copies it to its
destination.

```go
job := phase.Job{
	Name:         "orders-nightly",
	Input:        phase.Input{"day": "2026-11-15"},
	LogicVersion: 1,
	Store:        phase.Dir("/var/lib/etl"),
	Phases: []phase.Phase{
		{Name: "extract", Run: extract, Verify: checkStaging},
		{Name: "load", Run: load},
	},
}
report, err := job.Run(ctx)
```

`Run` does the work and returns named counts (`phase.Counts{"rows":
1000}`); `Verify` reads back what the phase left and compares it with the
counts recorded when it finished. The `report` says, phase by phase,
whether it was skipped or ran, and why.

## What is skipped, and why

A phase recorded as done is skipped only when its `Verify` passes. Without
a `Verify`, it runs again every time. A `Verify` that fails runs that
phase again, and every phase after it, since each reads what the earlier
ones wrote.

> **In plain words: why the record is not trusted.** An `UNLOGGED` table
> (one PostgreSQL does not journal, faster to fill) is emptied by a crash,
> while the record still says "done". `Verify` is what notices.

## The same input, or from zero

`Input` is what the output depends on — the day processed, the source;
`LogicVersion` is a number you raise when the code changes what it produces. If
either changed since the record was written, the job starts from zero and
`report.RestartReason` says what changed: `day was "2026-11-14", is
"2026-11-15"`, `LogicVersion 1 → 2`. The number of workers or the log level do
not go in `Input`: they do not change the output, and a run that resumes
applies its own to the phases that remain. `Job.Fresh` starts from zero
with the same input.

## One process at a time

A second run of the same job while a first one is running fails at once
with `phase.ErrBusy`, naming the process that holds it. The store
`phase.Dir` keeps a **lease** — a file the running process renews every
ten seconds; if the process dies, the lease expires after thirty seconds
(`DirStore.LeaseTTL`) and the next run takes it over. A process that lost its
lease — frozen for longer than the lease — has the context of its running
phase cancelled as soon as its renewal sees the loss, and `Run` returns
`phase.ErrLeaseLost`: a phase that honours its context stops instead of
writing beside its successor. With `postgres.PhaseStore`, the same happens
when the lock's connection is found gone.

## Where the state lives

In the directory given to `phase.Dir`: `<job>.json`, the record, and
`<job>.lease`, the lease. Or in a table of your own PostgreSQL schema, with
`postgres.PhaseStore`:

```go
store := &postgres.PhaseStore{Dial: postgres.DialTCP(addr), Config: cfg, Table: "etl.phases"}
```

The table is created on first use. Its lock is the server's: an
**advisory lock** — a lock the application names itself — taken on a
connection of the job's own; a process that dies releases it at once, with
its connection, without waiting for a lease to run out. The library keeps
nothing of its own either way; the decision is
[ADR 0027](../design/adr/0027-phase-runner.md).
