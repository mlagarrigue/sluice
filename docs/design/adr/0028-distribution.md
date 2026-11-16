# ADR 0028 — Several machines: the orchestrator and the broker, not the library

- **Created:** 2026-11-16
- **Revised:** —
- **Theme:** core

## Problem

Everything fits in one process. The list of limits left two questions
open: spreading a pipeline over several machines, and resuming a job on
another machine after a crash — the job of a library, or of the
orchestrator around it (Kubernetes, systemd, a message queue)?

## Decision

**sluice does not distribute.** No cluster, no scheduler, no exchange of
batches between machines (a shuffle), no state shared between processes.
What runs N processes on M machines, restarts them and places them is the
orchestrator; what shares the work among them and keeps what is not yet
processed is the message broker.

**What the library provides for several machines** — all of it exists:

- *The network boundary is one more operator.* A pipeline on one machine
  feeds another through a durable broker (NATS JetStream, a Redis stream)
  or over HTTP in a stream; the logical pipeline does not change.

- *Horizontal scale by competing consumers.* N identical processes read
  the same Redis consumer group — each entry "goes to one of them"
  ([Redis guide](../../guide/redis.md#reading-a-stream-as-a-consumer-group))
  — or the same durable JetStream consumer, whose server splits the stream
  among its workers. Each batch is acknowledged by the code that handled
  it: at least once, with idempotent writes, as the PostgreSQL connector
  already states
  ([Delivery, at least once](../architecture.md#delivery-at-least-once)).

- *Resuming on another machine.* A job whose store is
  `postgres.PhaseStore` resumes on any machine that reaches the database:
  its record and its lock live in the server
  ([ADR 0027](0027-phase-runner.md)), so two machines started on the same
  job by an overlapping schedule exclude each other. `phase.Dir` stays on
  one machine: a shared filesystem is not supported, its leases by file
  have no guarantee there.

**What stays outside the library**: placement, restarts, service
discovery, partitioning by key across machines (the broker partitions),
distributed state.

## Rationale

A library, not a framework: a distributed scheduler is a product to run,
watch and debug — an operating cost the orchestrator and the broker
already pay, better, and that each user has already chosen. Duplicating
them would add one more system to an architecture that already has two.

The hard part of a pipeline over several machines is delivery and
resumption, not transport: the durable broker gives the first
(acknowledgement per batch), the PostgreSQL store of ADR 0027 the second,
and both exist. There is nothing new to write.

What this costs: a pipeline that must redistribute by key between machines
halfway through — a shuffle, as for a join of two streams partitioned
differently — goes through the broker, with one more network hop and one
more durable write. What would change it: such a shuffle measured as a
bottleneck a broker cannot serve, and then a separate module, outside the
core.
