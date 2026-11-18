# The comparison study — what is measured, and how

Specification document: **what** is measured and **under which conditions**,
before choosing the tools and writing a single bench. The choice of the
contenders is in §2.5. The questions that have been settled are gathered
under **Decisions**.

---

## 0. Topology: three separate roles

| Role | What runs | Why separate |
|---|---|---|
| **db** | PostgreSQL, frozen in version and configuration | a contender's SQL cost must not steal CPU from the web server |
| **web** | one contender at a time | this is what is measured; alone on its machine |
| **tester** | the load generator, or Playwright | a generator on the same machine as the server measures their quarrel |

**Where `tc netem` runs** (the Linux tool that delays packets): it only
delays outgoing traffic and it uses CPU wherever it runs. So **never on
web**, the measured machine:

- client↔web link: the whole round-trip delay on the **tester**'s
  interface;
- web↔db link: the whole round-trip delay on **db**'s interface.

**The netem queue is enlarged.** By default, netem keeps 1,000 packets in
its queue (`limit 1000`); at a 20 ms delay, the queue holds packets per
second × 0.02, so it drops packets from ~50,000 packets/s (ACKs included)
on, which shows up as TCP retransmissions that would be read as a server
p99. Typical command:
`tc qdisc add dev eth0 root netem delay 20ms limit 1000000`. After each
step, `tc -s qdisc show`: **a single loss (`dropped` > 0) invalidates the
step**, like the tester's CPU above 70% (§4).

**Locally (preparation of levels 1, 2 and 3)**: the three roles are containers on
separate Docker networks (tester↔web, web↔db), each with its own memory
limit and its own **physical cores**:

- `cpuset` pins threads, not cores. Two roles on two sibling threads (SMT)
  share the same core and get in each other's way. Pinning is done by
  physical core, siblings included, from `lscpu -e` (column `CORE`).
- The dev machine is a WSL2 VM with 12 vCPUs = 6 physical cores. With a
  tester that must dominate web (§4), the budget is tight: for example db 1
  core, web 2 cores, tester 3 cores.
- `sch_netem` is a module (`CONFIG_NET_SCH_NETEM=m`): `modprobe sch_netem`
  on the WSL kernel side before any run.
- Locally, a single kernel serves the three containers: the softirq of
  netem and of the Docker bridge does not obey `cpuset` and can land on
  web's cores. The rule "netem never on web" is only half kept there, one
  more reason to publish nothing local.
- **Local figures validate the chain** (scripts, answer checks,
  statistics, report), not the ratios between contenders: on a 6-core WSL2
  VM, even the ratios move with the order of the runs. Nothing local is
  published.

**On Azure (publication)**: three VMs of the same size, same region,
accelerated networking enabled, in a *proximity placement group* (an option
that places the VMs physically close together, for a low and stable latency
between them). Sizes with dedicated vCPUs only (no B-series "burstable",
whose CPU melts away when its credits run out). On most sizes, an Azure
vCPU is a thread, not a core: same pinning rule as locally. Some sizes have
no SMT (one vCPU = one core): the topology is **read** at deployment
(`lscpu -e`), never assumed, and published.

**Azure host interference**: two causes outside the contender hit web and
db without any tester-side check seeing them: the time stolen by the
hypervisor (`%steal` in `mpstat`) and the maintenance events that freeze
the VM for a few seconds, announced by the *Scheduled Events* of the
metadata service (IMDS, `169.254.169.254`). Both are read on the three VMs
and are part of the end-of-step checks (§4). Foreign to the contender, they
invalidate without bias.

The same images as locally, but **`network_mode: host`** on the three VMs:
in bridge networking, every packet goes through NAT and through `conntrack`
(the kernel table that tracks each connection for NAT). That cost would run
in softirq on web, the measured machine, and with the 10,000-connection
profile or with new connections the table saturates (`nf_conntrack: table
full` in `dmesg`): SYNs are dropped silently and read as server errors. If
`conntrack` stays loaded, `conntrack -S` (`drop`, `insert_failed`) is part
of the end-of-step checks (§4). To check at the first deployment: that
`tc netem` on the main interface does apply to the traffic going through
the accelerated network (measure the real RTT with `ping` before/after).

**VM network ceiling.** Each Azure VM size has a maximum network throughput
and a packets-per-second ceiling. The 1,000-row list weighs ~100 KB: at
10,000 req/s, ~8 Gbit/s leave web, and the level 3 files go up to 100 MB.
Without precautions, the published capacity would be that of the VM size.
So: the size is chosen with a ceiling of at least **2× the expected
throughput** of the heaviest scenario (target rate × response size, TCP and
TLS headers included); the documented ceiling is published with it; web's
network throughput is read at every step (§1.2) and a step beyond ~70% of
the ceiling, in bytes or in packets, is invalidated (§4).

**Network interrupts on web.** The kernel's network processing runs in
softirq, outside any process, on the cores that receive the card's
interrupts. If it lands on free cores, the server gets free CPU, and the
contender that sends the most packets (h3 and its datagrams, uncompressed
answers) benefits the most. So: `irqbalance` stopped, interrupts and RPS
(the software spreading of received packets) **pinned to the cores
allocated to the server**, identically for all and published. A
contender's network cost falls back on it, as in production.

## 1. The metrics

### 1.1 Cost of writing

| Metric | Definition | Why |
|---|---|---|
| Lines of code | lines written by hand for the scenario: handler + wiring + database access. Excluding tests, generated code, build config. Counted with a single tool (`scc` or `cloc`) across all languages. | this is what the developer pays, once |
| Dependencies | number of third-party modules, direct **and** transitive, counted on what is **linked into the deliverable** (`go version -m` on the binary, `npm ls --omit=dev --all`, `cargo tree -e normal`), not `go mod graph`, which counts the test dependencies of the dependencies | this is what maintenance pays, all the time |
| Concepts to know | qualitative list (e.g. "pool, context, DataLoader") | two pieces of code of the same length can demand very different knowledge |

### 1.2 Behaviour under load

All these measures are taken **as a function of load** (see §3), never at a
single point: an isolated figure does not say where the system breaks.

| Metric | Definition |
|---|---|
| Capacity | the highest throughput held **under the scenario's SLO** (§3.2). This is the headline figure, and it is measured with repetitions (§7.6). |
| Throughput | **successful** requests per second. An answer that is an error or timed out does not count. Below capacity, with a fixed-rate generator, it equals the rate sent: it only becomes a measurement beyond. |
| Requests handled | total succeeded / errored / timed out / **wrong**, over the duration of the step |
| Wrong answers | answers whose content fails validation (§4). **A separate counter**, never merged into errors: published for every configuration, over the whole campaign. Beyond zero, the configuration is marked "incorrect answers under load" and its capacity is not published (§4). |
| Timed out | request with no complete answer after **10 × X + 4 × injected RTT** (X = the p99 threshold of the scenario's SLO, §3.2; injected RTT = the client↔web netem delay), fixed together with the SLO and published with it. The network term covers TCP handshake + TLS 1.3 + request + margin: without it, at 80 ms of RTT, a new connection (3 RTT = 240 ms) would exceed the 100 ms of the static case and would always come out as an error. A 2xx answer whose body fails validation (§4) is not an error: it is a **wrong answer**, counted apart (row "Wrong answers"). |
| Simultaneous client connections | HTTP connections opened by the generator. An input parameter **derived from the target rate** (§3.5), never a constant, and published. |
| Database connections | PostgreSQL connections opened on the server side (measured through `pg_stat_activity`), the scarce resource |
| Connections held by the server | TCP connections established on web's listening port (`ss -Htn state established sport :<port>`), read every second, maximum and mean over the step, **compared to C**. With them, the open file descriptors of the server process (`/proc/<pid>/fd`) set against its effective limit (`/proc/<pid>/limits`). A gap from the requested C says that the server does not accept everything it is sent. |
| Accept queue | the queue where the kernel keeps the established connections that the server has not yet taken (`accept`). On web, per step: `nstat` `TcpExtListenOverflows`, `TcpExtListenDrops`, `TcpExtSyncookiesSent`; the maximum of `Recv-Q` in `ss -ltn` on the listening port (the current queue) and its `Send-Q` (the effective size of the queue, published). An **explanatory** measure, with no verdict: with the default setting (`net.ipv4.tcp_abort_on_overflow=0`), an overflow produces **no error**: the kernel ignores the last packet of the handshake, the client believes it is connected, and the connection only moves on at the retransmission, ~1 s then 3 s later. It shows up in the p99, not in the errors; without this counter, a p99 that jumps by a second stays unexplained. |
| Closures by the server | connections closed at the server's initiative during the step (end of stream on an idle connection, `Connection: close` in h1, `GOAWAY` in h2/h3), and the generator reconnections that follow, counted for **every** contender, not only Sluice.go (§8.1). Published per step; beyond zero, the condition "0% of new connections" (§3.6) is no longer held and the publication says so. |
| Errors by nature | the errors broken down: HTTP status 4xx, 5xx; connection refused (`ECONNREFUSED`); reset (`ECONNRESET`, typically a server closure that crosses a request already sent on a reused connection); timeout at connection opening; timed out. A single total does not say whether the server refuses connections or fails on requests. All of them are errors **of the contender**; failures of the tester itself invalidate the step separately (§4). |
| Batch size | for each "with" variant (`gateway` included): the mean number of keys per SQL request sent, and its distribution. Instrumented in the contender's code (a counter in the batch function), published for every step. A descriptive metric, with no verdict: it is what says whether the "with" column measures batching or only its overhead (§2.1). |
| TTFB | *time to first byte*: delay between sending the request and receiving the first byte of the answer, what the user feels as "it responds" |
| TTLB | *time to last byte*: delay until the last byte, what the user feels as "it is loaded" |
| CPU | CPU % of the server process, of PostgreSQL, **and of the whole tester machine**, not only of the generator process: netem's work runs in *softirq* (the kernel's network processing, counted outside any process), it only appears in `mpstat` (column `%soft`). Used to validate the step (§4). On web too, the CPU **of the whole machine** per core (`mpstat -P ALL`), softirq included: the kernel's network processing is a cost of the contender (§0, "Network interrupts on web") and the process CPU alone would hide it. |
| Memory | RSS (memory actually occupied by the process): peak and mean over the step |
| Network throughput | bytes and packets per second, sent and received, on web's interface (`/proc/net/dev`, or `sar -n DEV`), per step, set against the VM size's ceiling (§0). Descriptive for the contender (compression makes it vary, §2.3 bis), a check for the apparatus (§4). |
| Crashes | stops of the server process during a step: changed PID, `RestartCount` or `OOMKilled` in `docker inspect`, an OOM line in `dmesg`. Behaviour **of the contender**: the step is broken, the cause is published (§4). |
| Disk writes | bytes written by the server process (`/proc/<pid>/io`, `write_bytes`) and by Docker's log driver, per step. Makes the cost of logs visible (§4). |

For delays (TTFB, TTLB): the **distribution** is published, p50, p90, p99,
p99.9 (the delay under which 50%, 90%... of the requests fall), never the
mean alone, which hides slow requests.

### 1.3 The cost of memory management

An **explanatory** measure, not a ranking: it says why a p99 degrades, it
receives no verdict. It is taken **on all the runs** of a runtime, standard
and optimised, with the same setting, never only when an explanation is
sought, otherwise its own cost (small, but not zero for GC logs) would
become a gap.

**What is measured depends on the GC: "pauses" is not enough.** A
*stop-the-world* GC (one that stops the whole program while it works) is
read in its pauses. A concurrent GC (one that works while the program runs)
has pauses of a few tens of µs and costs elsewhere: the CPU it takes from
requests. Reading only its pauses would publish "0" for a real cost. Hence
two quantities per runtime: **pauses** (distribution, max) and **share of
CPU** consumed by the GC.

| Runtime | Contenders | Default GC | Pauses | Share of CPU | Remark |
|---|---|---|---|---|---|
| Go | **Sluice.go**, Go `net/http` (+ DataLoader), Gin | concurrent, parallel marking | `runtime/metrics` `/sched/pauses/total/gc:seconds` (histogram, read at the end of the step, zero cost) | `/cpu/classes/gc/total:cpu-seconds` ÷ total CPU | the Go cost is mostly CPU: 25% of `GOMAXPROCS` during marking, plus the *assist* (a goroutine that allocates too fast is put to work, inside the latency of its request). `GOGC` / `GOMEMLIMIT` tuning = a listed optimisation |
| Node | Express 5 (+ DataLoader), Fastify | generational, Orinoco (V8) | `PerformanceObserver` on the `gc` entries | sum of the `gc` durations ÷ step duration | a single event loop: one pause blocks **all** the in-flight requests of the process |
| Java | Spring Boot | G1 | `-Xlog:gc*:file=…` (the JVM's unified log) | same, cumulated GC time; JFR (*Java Flight Recorder*, the built-in profiler) as a cross-check | generational ZGC = a listed optimisation, it is the documented p99 lever |
| .NET | ASP.NET Core | Server GC (ASP.NET Core's default) | EventPipe GC events (`dotnet-counters`, `dotnet-trace`) | `% Time in GC` counter | |
| Python | FastAPI | reference counting + generational cyclic GC | `gc.callbacks` (start/end per generation), timestamped | sum ÷ step duration | freeing by reference counting is **not** a GC and does not show there: it lands in the request that drops the last reference (a list of 100,000 objects freed in one go) |
| Rust | axum | none | - | - | "no GC" ≠ "no cost": freeing is deterministic and lands in the request, the allocator (glibc, or mimalloc/jemalloc in the optimised version) weighs. Read instead: RSS and, when profiling, the share of `malloc`/`free` |

Pauses and the share of CPU are published next to the p99, per step.

---

### 1.4 Containerisation

| Metric | Definition | Why |
|---|---|---|
| Image size | compressed (what travels at deployment) and uncompressed; number of layers | deployment time, registry cost, attack surface |
| Build time | from scratch (no cache) and incremental (one line of code changed) | the developer's loop, CI |
| Cold start | from `docker run` to the **first successful answer** (not the first log line) | what matters in serverless and in autoscaling |
| Idle memory | RSS once started, with no load | how many instances fit on a machine |
| Time to steady state | time from the first answer until the p99 is **stable**: three successive 5 s windows whose p99s stay within **±15%** of one another, at the rate of the measured point (JIT, pool filled). ±5% was too tight: the p99 of a 5 s window moves more than that even in steady state, and the warm-up would never have been declared over. **5 min cap**: beyond it, the contender is published "never stable" at that rate, and the common warm-up does not wait for it. | the first minute of a new instance; also used to set the warm-up (§4) |

Two images per contender, as for the code (§2.4): the one from the official
documentation, and an optimised one (multi-stage, minimal base image,
`distroless` or `scratch` when the language allows it).

## 2. The requests

### 2.1 Every request is different

**Rule:** every request asks for a different key, drawn at random from a
set large enough that none repeats within a step. This prevents a cache, a
deduplication or a batching of *identical* requests from making a
contender look better than it is.

**Size of the key space.** The step that asks for the most keys is the
fixed point: up to 0.8 × the capacity, over 90 s + 5 s of lead-in. The
check is made as soon as scouting bounds the capacity, with the cautious
bound 0.8 × 1.25 C: 95 C keys are needed. The level 1 matrix has 20
million, enough for C = 8 × 26,036 req/s: eight web cores at the rate that
the study's server held on a single core at the rehearsal (`unit/h1`,
2026-11-14). A space that is too small stops the campaign right after
scouting, with the number of rows that are needed. The static case draws
no key and is never refused.

**Same draw for everyone.** The draw's seed is fixed by (scenario, step,
round) and is **identical for all the contenders of a round**: all read the
same rows, in the same order, hence the same pages. This removes a source
of variance from the paired test and an objection about fairness. The seeds
are published.

**Answer contract:** every dynamic answer contains the key it serves
(single: the key; list: the requested bound). This is what makes it
possible to check 100% of the answers under load (§4), and the most likely
class of bug of a batching is precisely one key's row returned to another
key's request.

**Batching of *different* requests.** Sluice.go's `gateway` batches
different requests (64 distinct keys → a single `= ANY`). This is not a
cache: it is the project's mechanism. Every contender is therefore
measured **without and with** batching, when its ecosystem has a
documented mechanism. Both columns are published side by side: the project
shows what it claims to do better, and also shows that it exists
elsewhere.

| Ecosystem | "With" mechanism | Remark |
|---|---|---|
| Sluice.go | `gateway` | merge into `= ANY`. A **time** window: `Within` starts at the first key of a batch, the batch leaves at `Size` keys or at the deadline. `Within` is mandatory and **has no default**: the standard version takes the values of the package doc's example (`Size: 64, Within: 2 ms`), declared as such. |
| Go | DataLoader (`vikstrous/dataloadgen` or `graph-gophers/dataloader`) | **the most direct comparison**: same language, same driver, only the mechanism changes. Without it, Sluice.go would be the only Go "with", and the gap would mix the effect of batching and that of the framework. Default window 16 ms (`graph-gophers`, to check for `dataloadgen`). |
| Node | DataLoader | merge by keys. Default window: the batch leaves on `process.nextTick`, **no delay**. Now Node drains the `nextTick` queue after **every** I/O callback, and every HTTP/1.1 request arrives through its own socket callback: the batch leaves before the next request is read. **Expected batch size ≈ 1** in standard (a few HTTP/2 streams arrived in the same packet, at best). Node's standard "with" column then measures the overhead without the gain: it is the result of the default, published as such, with the batch size (§1.2) that shows it. The optimised version passes a time-window `batchScheduleFn` (`setTimeout`), listed as an optimisation. **To check at the local scouting** before building the rest of the column. |
| .NET | GreenDonut (the .NET port of DataLoader) | **not `NpgsqlBatch`**: it sends several requests from the **same** caller in one round trip, it does not batch concurrent HTTP requests. **Settled (`c1b7b93`, `comparison/README.md` "`dotnet-minimal/` — the .NET contender")**: GreenDonut merges concurrent keys into `= ANY` like the other columns, it is the "with" column (`dataloader-doc`); Npgsql multiplexing (`Multiplexing=true`) *pipelines* the commands of concurrent requests over fewer connections without merging them, it is a variant of the optimised image (`multiplexing-opt`, same code, the pool option), measured rather than decided on paper. |
| Rust | `async-graphql`'s `DataLoader`, used **alone** (without GraphQL: it takes a batch function and a Tokio `spawn`) | the only documented and maintained Rust DataLoader; **time** window by default (1 ms, batches ≤ 1,000, to check on the frozen version). Adds `async-graphql` to the contender's graph: counted in its dependencies (§1.1), it is the real cost. Level: see §2.6. |
| Java | `java-dataloader` (graphql-java's DataLoader) with `ScheduledDataLoaderRegistry` | without a scheduled registry, the batch only leaves on an explicit `dispatch()`, which the GraphQL engine does and an HTTP controller does not: the scheduled registry, which triggers at a fixed interval, is the documented mechanism to do without. Default interval to read and publish. |
| Python | `aiodataloader` (the DataLoader port used by Graphene/Strawberry), `cache=False` | batch triggered by `loop.call_soon`: it batches the requests whose data arrived in the same asyncio loop iteration, not beyond: batch size probably small, but larger than Node (asyncio handles all the ready callbacks before draining those they schedule). **Async only**: Python's standard version is synchronous (§2.6), it has no "with" column. |

**The DataLoader "with" variant is not the pattern of the docs, and this is
declared.** The DataLoader docs (Node, GreenDonut via HotChocolate)
prescribe a loader **per request**: with one key per HTTP request, that
pattern batches nothing. Batching *across* requests needs a loader **shared
by the process**, which the docs advise against (because of the cache). The
"with" variant is therefore defined as follows, for all the DataLoader
ecosystems:

- a loader shared by the process, **memoisation cache disabled**
  (`cache: false` in Node, no-op cache in Go). Enabled by default, it
  retains every requested key without bound: since all the keys are
  different, it never serves, but the RSS climbs and the plateau profile
  would read a leak that does not belong to the framework;
- it is published as **a deviation from the docs**, with its justification;
- "standard" means here **the loader's default settings** (window, batch
  size), not "the docs' pattern".

- **No "with batching" variant on the static cases**: with no database,
  there is nothing to batch.
- **The batching settings are published** for every contender: accumulation
  window and maximum batch size (64 keys / 2 ms for `gateway`, 16 ms for
  the Go DataLoaders, `nextTick` for Node), with the **observed batch size**
  (§1.2). The standard version keeps the default values, or, with no
  default, those of the docs' example; only the optimised version (§2.4)
  may tune them, and the tuning is listed as an optimisation.
  A 16 ms window consumes two thirds of a 25 ms SLO: it is the real cost of
  the default, and the "why" sentence of §5 says so.
- **The cost of batching is published too**: the window that accumulates
  keys adds latency when traffic is low. The lowest load point (§3.2) shows
  it; it is a legitimate "where Sluice.go loses" (§5).

### 2.2 Static and dynamic

| Type | Case | What it isolates |
|---|---|---|
| Static, small | fixed in-memory answer, ~1 KB JSON | the pure cost of the HTTP server |
| Static, file | file served from disk: 10 KB, 1 MB, 100 MB | sending large volumes, streaming, the TTLB |
| Dynamic, single read | one row per key | database access, pool latency |
| Dynamic, list | N rows per request (N = 10, 1,000, 100,000) | serialisation, memory, end-to-end streaming |
| Dynamic, write | insertion of a row, transaction | the cost of a commit, contention |
| Mixed | 80% single read, 15% list 1,000, 5% write | realistic traffic, in addition to the pure cases |

**State of the database: it must not drift during the campaign.**

- **Reads**: the table that is read and its index fit entirely in
  `shared_buffers` (91 bytes per row measured, a quarter more, never less
  than 256 MB), loaded by `pg_prewarm` after loading. Checked **at every
  step** by the cache hit rate of its reads (`pg_statio_user_tables`, table
  + index of `bench_orders`, difference over the step): under 99.9%, the
  step is invalidated (§4). The OS cache does not count: `pg_statio` sees a
  read there. Measured on 2026-11-15 at 16 M rows: 100.000% over 400,000
  random reads, 89.3% cold after a restart, 100.000% after `pg_prewarm`.
  PostgreSQL is not restarted between repetitions, so that all the
  contenders see the same cache.
- **Writes (write-only and mixed)**: they go into a table that the reads
  never touch, otherwise the expected answer of a read would depend on past
  writes and could no longer be computed by the generator (§4). Before
  **every repetition**, the write table is reset (`TRUNCATE`, or restoration
  from a template database), then `VACUUM ANALYZE` and `CHECKPOINT`.
  Without that, the table grows from repetition to repetition, and
  autovacuum or a checkpoint fires on whichever contender happens to be
  running.
- **Nothing fires during a repetition.** `TRUNCATE` + `CHECKPOINT`
  beforehand is not enough, two internal triggers would hit in the middle
  of a step, and first of all the contender that writes the fastest:
  - the **insert-only autovacuum** (PG ≥ 13: a table that only receives
    insertions is cleaned after ~1,000 rows + 20% of the table). The write
    table is under `ALTER TABLE ... SET (autovacuum_enabled = off)`: it is
    emptied at every repetition, there is nothing to clean;
  - the **checkpoint on WAL volume** (`max_wal_size`, 1 GB by default,
    triggers a checkpoint whatever `checkpoint_timeout` says).
    `checkpoint_timeout` is set longer than a repetition, and
    `max_wal_size` above the WAL volume of the most write-heavy repetition,
    measured at the local scouting to prepare, then **recalibrated at the
    first Azure scouting**, where the rates (hence the WAL) will be higher;
  - the **anti-wraparound autovacuum** (the forced freeze that PostgreSQL
    launches when the age of a table's transaction identifiers exceeds
    `autovacuum_freeze_max_age`, 200 M by default). Every write consumes an
    XID: ~28 mixed configurations at level 1 × ~3.9 h at ~1,000 writes/s
    make ~400 M, the threshold will be crossed. It touches **all** the
    tables, reads included, and `autovacuum_enabled = off` does not prevent
    it. On a large table never frozen, throttled by
    `autovacuum_vacuum_cost_delay`, it can span several repetitions.
    So: `VACUUM FREEZE` on the whole dataset at loading (the forced freeze
    then skips the already frozen pages and becomes almost free), and
    `age(datfrozenxid)` read before every round, published with the
    PostgreSQL configuration.
  - End-of-step check (§4): the difference of
    `pg_stat_checkpointer.num_requested` (`pg_stat_bgwriter.checkpoints_req`
    before PG 17) is 0, and no autovacuum ran
    (`pg_stat_user_tables.autovacuum_count` unchanged, **on all the
    tables**: the anti-wraparound one also hits the read tables).
- The complete PostgreSQL configuration is published.

### 2.3 Protocols

**The three protocols, for all those that speak them.** HTTP/1.1 and
HTTP/2 for all; HTTP/3 for those that support it. With and without TLS: TLS
costs CPU and it is what a real deployment sees; without TLS, the framework
is isolated. HTTP/3 only exists with TLS (QUIC, HTTP/3's transport over
UDP, always encrypts).

| Contender | HTTP/1.1 | HTTP/2 | HTTP/3 |
|---|---|---|---|
| Sluice.go | `net/http` (standard), `httpstream` `Serve` (optimised) | `net/http` (standard), `httpstream` `ServeH2Conn` (optimised) | `httpstream` `ServeH3Conn` on Sluice.go's `quic`, optimised only, the standard path (`net/http`) does not speak it |
| Go | `net/http` | `net/http` | **`quic-go`** (`http3`), the de facto HTTP/3 in Go (Caddy's), optimised only: the stdlib does not speak it. Same language as Sluice.go, so **the direct reference** for Sluice.go's `quic` |
| .NET | Kestrel | Kestrel | Kestrel (msquic), supported by Microsoft |
| Python | uvicorn | Granian (optimised) | Hypercorn + `aioquic` (optimised, to evaluate) |
| Node, Java, Rust | yes | yes | not supported by the chosen stack (§2.6) |

The comparison can only pit those that speak it against each other in
HTTP/3: **Sluice.go against Go/`quic-go`** at level 1, and .NET at level 2.
The comparison of the three protocols with one another is the object of the
internal study (§8).

**System settings specific to HTTP/3**, fixed and published (§4): size of
the UDP buffers (`net.core.rmem_max`, `wmem_max`; `quic-go` asks for ~7.5 MB
and says so at startup), and state of the GSO/GRO offloads (the kernel or
the card splits and groups UDP datagrams; without them, every packet costs
a system call and HTTP/3 looks more expensive than it is).

**Generator in HTTP/3**: its QUIC client is `quic-go`, which shares no code
with Sluice.go. Against a `quic-go` server, however, client and server
share the same code and a symmetric bug would hide itself: the HTTP/3
cross-check is made with an independent client, `h2load` (nghttp2 compiled
with ngtcp2, which speaks h1, h2 and h3).

### 2.3 bis Compression

**What it changes.** Fewer bytes on the network, more CPU on both sides. On
the bench's network (several Gb/s between VMs), only the CPU cost would be
seen: measuring it only there would be biased against it. It is therefore
read in quantities that transpose (**bytes per request, µs of server CPU
per request, compression ratio**), and the TTLB is measured in addition on
a throttled link (`netem rate`, declared throughput, e.g. 100 Mb/s), where
it must win.

**Where it makes sense.** The 1,000-row list (~100 KB of JSON, very
compressible) and beyond. Not the 1 KB static: most middlewares do not
compress under a threshold (2 KB by default for Spring Boot, 500 bytes for
`GZipMiddleware`), and these default thresholds are read and published.

**What each one offers, according to its docs** (to check on the frozen
versions):

| Contender | Documented mechanism | Algorithms |
|---|---|---|
| Sluice.go | **none today.** The Go stdlib gives `compress/gzip` and `compress/flate` (deflate); no brotli or zstd | gzip, deflate, to write, stdlib only (§8) |
| Go `net/http` | none in the stdlib; a hand-written `compress/gzip` wrapper (optimised) | gzip, deflate |
| Express / Fastify | `compression` / `@fastify/compress` | gzip, deflate, br (zstd depending on the version) |
| Rust | `tower-http` `CompressionLayer` | gzip, deflate, br, zstd |
| .NET | `ResponseCompression` | br, gzip |
| Java | `server.compression.enabled` (Tomcat) | gzip |
| Python | `GZipMiddleware` | gzip |

**Pitfalls to check before measuring:**

- **The default level** of each algorithm is published. Brotli at level
  11, which is the default of Node's `zlib`, targets pre-compressed static
  content and ruins a dynamic answer. Each middleware's default is read,
  not assumed.
- **Compression and streaming.** An answer served as a stream (list by
  batches) must flush the compressor at every batch (*sync flush*) so that
  the client receives something: better TTFB, worse ratio. Both modes are
  measured where the contender allows the choice.
- **The generator decompresses** to validate 100% of the answers (§4): this
  is CPU on the tester side, so the "generator is not the bottleneck" test
  (§4) is redone **per encoding**.
- **An uncompressed answer** when the client asked for compression
  (`Accept-Encoding` ignored, threshold not reached) is not an error. It is
  counted and published: "compressed: x% of the answers".

**In the comparison**: **gzip**, the only algorithm that all can produce,
at level 1 on the list 1,000, standard version, variants without batching
(compression is independent of batching), h1 and h2, at the fixed points
only, **with no verdict**: 4 contenders × 2 protocols = 8 configurations.
Brotli, zstd and deflate at level 2, among those that offer them. What
Sluice.go loses for lack of brotli and zstd is read there, against
`tower-http`.

---

### 2.4 Two code versions per contender

| Version | Rule |
|---|---|
| **Standard** | the code as written by the official documentation or the framework's starter example. No optimisation that does not appear there. This is what a developer who is starting out does. |
| **Optimised** | the same feature, after an optimisation effort **at an identical time budget for all** (e.g. one day per contender). Every optimisation applied is listed, with its source, preferably the performance documentation of the framework itself. |

Safeguards for the optimised version:
- stays acceptable in production: no disabling of validation, TLS or error
  logs; no response cache (the requests are all different, a cache would be
  cheating); no violation of the protocol;
- Sluice.go also has its standard version and its optimised version, at the
  same budget;
- **bias to declare**: the same author writes all the versions, and knows
  Sluice.go better than the others. Mitigation: the optimisations of the
  others come from their official sources, everything is published, and
  review by practitioners of each ecosystem is sought before publication.

Lines of code (§1.1) are measured for both versions: the gap between
standard and optimised is a result in itself, what performance costs in each
ecosystem.

**Enabling a protocol is not an optimisation.** TLS, HTTP/2, HTTP/3 are
test conditions (§2.3): the configuration that enables them is the same in
both versions, taken from the framework's docs, and published. If the
documented server of the standard version cannot speak a protocol, the cell
is published "**not supported**", never filled by a proxy in front of it,
which would measure the proxy.

### 2.5 Scope of the compared solutions

**Selection rule.** In each ecosystem, we measure **what teams deploy**,
not what wins benchmarks:

- **HTTP framework**: the most used in the ecosystem, according to the 2025
  surveys and the 2026 downloads;
- **database access**: the most common PostgreSQL driver, **hand-written
  SQL**. ORMs (layers that generate SQL from objects) are more widespread
  than raw SQL in almost every ecosystem (JPA, EF Core, SQLAlchemy ORM,
  Prisma/Drizzle, GORM). They are not in the core, because the ORM would be
  measured and not the framework: a query generated differently from one
  ORM to another distorts the comparison. They are candidates at level 3,
  as a **declared variant**;
- **"with" batching**: the ecosystem's documented mechanism (§2.1);
- **runtime**: the latest LTS version (long-term support) at the date of
  the campaign, frozen and published.

When the most used is not the fastest of the ecosystem (Express against
Fastify, Gin against `net/http`), **the most used goes in first**, the
fastest at the next level. The reverse would publish an ecosystem at its
best, hence in a light that few teams live in.

**The evidence, and its limits.** Two sources, neither sufficient alone:

- **Surveys** (Stack Overflow 2025, JetBrains *Developer Ecosystem* 2025,
  State of JS 2025). Bias: they measure who answers. Multiple choices, the
  totals exceed 100%.
- **Downloads** from the registries (read on 2026-10-06: npm and PyPI over
  30 days, crates.io over 90 days, NuGet cumulative). Bias: CI, transitive
  dependencies and mirrors inflate the figures. They are used to **compare
  within one registry**, never from one registry to another.

No numeric source was found for database access in Java nor for the Go
drivers: there, the choice follows the official documentation, and this is
said.

| Ecosystem | Runtime | HTTP framework chosen | What justifies it | Database access | "With" batching | Level |
|---|---|---|---|---|---|---|
| **Sluice.go** | Go 1.26 | standard: **`net/http`** + the glue of `example/orders` (the documented pattern); optimised: **`httpstream`**, Sluice.go's h1/h2/h3 server, listed as an optimisation (like Granian for Python) | the subject. With `net/http` in standard, Sluice.go without batching against Go `net/http` without batching isolates the framework at an equal HTTP server | **`sluice/postgres`**, Sluice.go's driver (protocol v3 on the stdlib alone, binary parameters, `= ANY($1)` in one round trip); `pgx` under the same `gateway` is an axis of the internal study (§8) | `gateway` | 1 |
| **Go** | Go 1.26 | **`net/http`** (stdlib) | 32% of Go developers use it directly, and it is the base under Gin, Echo, Chi: it is the reference with no framework, on the same runtime as Sluice.go. Gin is **the most used** (48%) | `pgx` (`pgxpool`), the reference PostgreSQL driver in Go | DataLoader (`dataloadgen`) | 1 |
| | | **Gin** | the most used (48%, JetBrains 2025). On top of `net/http`, the expected gap is small, but a Go reader will ask for it | `pgx` | none (the mechanism is already measured on `net/http`) | 2 |
| **Node** | Node 24 LTS | **Express 5** | still leading in usage (State of JS 2025); 572 M downloads/month, against 62 M for NestJS and 58 M for Fastify | `pg` (node-postgres): 235 M/month, it is also the base of Drizzle | DataLoader (57 M/month) | 1 |
| | | **Fastify** | the performance-oriented Node framework, the choice of the initial scoping | `pg` | none (already measured on Express) | 2 |
| **Rust** | today's stable, frozen | **axum** | 127 M downloads over 90 days, against 11 M for actix-web and 2 M for Rocket: the de facto standard | `sqlx`: 43 M, against 19 M for `tokio-postgres` and 8 M for Diesel (ORM) | `async-graphql`'s DataLoader | 1 (without), "with" to decide (§2.6) |
| **Python** | 3.14 (standard build, with GIL) | **FastAPI** | the most used since 2025: 38%, against 35% for Django and 34% for Flask (JetBrains); 382 M/month | standard: SQLModel (on SQLAlchemy, 330 M/month) + psycopg; optimised: `asyncpg` (107 M/month) | `aiodataloader`, in optimised only | 2 |
| **Java** | Java 25 LTS | **Spring Boot 4.1.2** (Spring MVC, Tomcat 11, Jackson 3; delivered in `e7aa2df`, validated on 2026-11-13) | Spring/Spring Boot leading the 2025 cloud-native surveys (44%, against 35% for Jakarta EE and 16% for Quarkus) | `JdbcTemplate` + HikariCP (§2.6) | `java-dataloader` + scheduled registry | 2 |
| **.NET** | .NET 10 LTS | **ASP.NET Core, Minimal APIs** | the form that Microsoft recommends for new projects | **Dapper** on Npgsql: .NET's "hand-written SQL" route (Dapper 779 M cumulative; EF Core 2.5 billion, but it is an ORM) | GreenDonut; Npgsql multiplexing as a variant of the optimised one (§2.1) | 2 |

**What is excluded, and why**: every exclusion is published, so that a
reader does not take an absence for an oversight:

| Excluded | Why | Where it could come in |
|---|---|---|
| ORM variants (JPA/Hibernate 7, EF Core, SQLAlchemy ORM, Drizzle, Prisma, GORM) | they measure the ORM, not the framework; yet it is the most common stack in most ecosystems | level 3, a variant declared "the ecosystem's ORM", with no verdict against the core |
| Django + DRF | the second Python framework (35%), but another shape: synchronous, built-in ORM, no hand-written SQL in the docs | level 3, with the ORMs |
| NestJS | rising, but on Express by default: its HTTP layer is already measured, it adds dependency injection | level 3 |
| Quarkus | third in Java (16%), mostly interesting in native | level 3 |
| Hono, Elysia, Bun, Deno | Hono: 279 M/month, but driven by the edge and Cloudflare Workers; another runtime, another deployment. Top satisfaction in State of JS, server usage still a minority | level 3, if server usage is confirmed |
| actix-web | second in Rust (11 M against 127 M), a little faster in public benchmarks; the ceiling is already represented by axum | level 3 |
| Python without GIL (*free-threaded*) | supported in 3.14, but not the default build | no |
| GraalVM *native image* | another compiler, another GC: it is not the same stack | containerisation only (§2.6) |

**What this changes compared with the initial plan**: Node enters at level
1 through **Express**, and not through Fastify; Fastify and Gin enter at
level 2. At level 1, Express replaces Fastify at an equal number of
variants: the counts of §6 and §7 are unchanged. At level 2, Gin and
Fastify add 4 comparisons against Sluice.go and 2 standard/optimised (§7.4).

Sources (consulted on 2026-10-06):
[Stack Overflow 2025](https://survey.stackoverflow.co/2025/technology/) ·
[JetBrains, Go 2025](https://blog.jetbrains.com/go/2025/11/10/go-language-trends-ecosystem-2025/) ·
[JetBrains, Python 2025](https://blog.jetbrains.com/pycharm/2025/10/the-most-popular-python-frameworks-and-libraries-in-2025/) ·
[State of JS 2025, back-end](https://2025.stateofjs.com/en-US/libraries/back-end-frameworks/) ·
[Jakarta EE, cloud-native survey](https://jakarta.ee/blogs/key-takeaways-cloud-native-java-survey/) ·
[Microsoft, Minimal APIs or controllers](https://learn.microsoft.com/aspnet/core/fundamentals/apis) ·
[Spring Boot, release notes](https://spring.io/projects/release-highlights/) ·
the crates.io, npm, pypistats.org and NuGet APIs.

### 2.6 Rust, Java, Python: what is compared, stack by stack

Each stack answers **one** question, and it is that question which decides
what goes into the stack. Exact versions are frozen at the local scouting
and published; those cited here are the targeted state, not a decision.

| | Rust | Java | Python |
|---|---|---|---|
| **The question** | **the ceiling**: the compiled language, with no GC, on the fastest asynchronous runtime. Separates what comes from the language from what comes from the mechanism: if Rust without batching beats Sluice.go without, and Sluice.go with beats Rust without, the gap is the batching, not the language | **the enterprise default**: one thread per request, JIT, the HikariCP pool. What the majority of teams actually deploy | **the most widespread API ecosystem**: interpreter, GIL (the lock that prevents two Python threads from executing code at the same time: a process uses only one core), parallelism by process. A much lower capacity is expected; what is informative is **elsewhere**: lines of code, concepts, and the standard → optimised gap, probably the largest of all |
| **Level** | 1 (without); "with" at 2, see below | 2 | 2 |
| **HTTP** | `axum` on `hyper`, multi-thread Tokio runtime | Spring Boot, Spring MVC (`spring-boot-starter-web`), embedded Tomcat | FastAPI |
| **Server** | `axum::serve` in clear; `axum-server` + `rustls` for TLS (the `tls-rustls` example of the axum repository) | Tomcat, 200 threads (default) | standard: `fastapi run` (uvicorn); optimised: Granian |
| **Database access** | `sqlx` (`PgPool`), hand-written SQL | `JdbcTemplate` + HikariCP, the Spring guide "Accessing Relational Data using JDBC" | standard: **SQLModel + psycopg, synchronous endpoints** (`def`, run in a thread pool), the pattern of FastAPI's official *full-stack template*; optimised: `asyncpg` with hand-written SQL |
| **Serialisation** | `serde_json` | Jackson | Pydantic v2 (validation of the `response_model` included: it is the documented path) |
| **Default pool** (level 2, §4) | `PgPool`: 10 | HikariCP: 10 | SQLAlchemy: 5 + 10 overflow **per process**; asyncpg: 10 per process |
| **Cores** | all (Tokio: one thread per visible core, respects the `cpuset`) | all (Tomcat threads, GC and JIT threads) | standard: the template's workers (`--workers 4`, to check on the frozen version); one worker = one process = one core |
| **HTTP/2** | yes (`hyper`, ALPN via `rustls`) | yes, `server.http2.enabled=true` (protocol configuration) | standard: **not supported** (uvicorn does not speak h2); optimised: yes (Granian) |
| **HTTP/3** (level 3) | not supported by axum; `quinn` + `h3` is experimental → **not supported** | not supported by Tomcat → **not supported** | Hypercorn (`aioquic`) in optimised only, to evaluate at level 3 |
| **Doc image** | multi-stage from the Docker docs for Rust | the Dockerfile of the "Dockerfiles" page of the Spring Boot reference (the jar extracted in layers on the JRE that it names, Liberica with CDS); `spring-boot:build-image` (Buildpacks) does not carry the certificate of a local run without a *binding*: declared gap (`e7aa2df`, `comparison/README.md` "`java-spring/` — the Java contender", validated on 2026-11-13) | Dockerfile of the "FastAPI in Containers" page |
| **Optimised image** | `distroless/cc` (glibc). **Not** `scratch` + musl without a replacement allocator: musl's allocator is slow under concurrency, the "optimised" image would be slower | minimal JRE through `jlink`, the JVM's AOT cache (startup) | `python:*-slim` or distroless, multi-stage |
| **Candidate optimisations** (official sources, budget §2.4) | `release` profile with LTO and `codegen-units = 1`; mimalloc or jemalloc; pool size | virtual threads (`spring.threads.virtual.enabled=true`); generational ZGC; `-XX:MaxRAMPercentage`; pool size | async + `asyncpg`; Granian; workers = cores; direct serialisation without `response_model` if the FastAPI docs document it for performance |

**Pitfalls specific to each stack, to check at the local scouting:**

- **Java, the default heap.** In a container, the JVM takes by default 25%
  of the visible memory for the heap (`MaxRAMPercentage=25`). The standard
  version keeps it (it is the default); if it spends its time in GC, §1.3
  shows it and the "why" of §5 says it. Tuning it is the first listed
  optimisation.
- **Java, the warm-up.** Tiered JIT compilation (interpreted → C1 → C2)
  can exceed the 5 min cap of the time to steady state (§1.4) at low
  throughput. Published "never stable" at that rate if need be, as the
  rule provides, with no exemption.
- **Java, GraalVM *native image*.** It is not an optimisation of the same
  stack: another compiler, no JIT, another GC. Measured **only** in
  containerisation (§1.4: size, cold start, idle memory), as a third
  declared image, never under load.
- **Python, pool × workers.** The pool is **per process**: 4 workers ×
  (5 + 10) = up to 60 connections in standard. The identical size of §4 is
  a **total** over all the contender's processes (pool per worker = total ÷
  workers), otherwise Python gets four times the others' pool. Same rule for
  Node in multi-process mode and for any contender that launches several
  processes.
- **Python, sync against async.** The standard version is synchronous
  because the official template is; this is where the standard → optimised
  gap will be the largest, and it is a publishable result: "what following
  the template costs". The "with" variant only exists in optimised (§2.1):
  Python's without/with comparison is made on the optimised version alone.
- **Rust, the test binary.** `cargo build --release` is the standard (it is
  what all the docs say); LTO and `codegen-units` are listed as
  optimisations. A bench in `debug` profile is the classic mistake, the
  pipeline refuses a binary built without `--release`.

**Rust "with": at level 2, not 1.** It is the comparison that the reader
will ask for ("Rust with batching would crush you"), and it must exist.
Placing it at level 1 costs: 8 variants instead of 7, 112 configurations
instead of 100 (~+12% of time), a Holm family of 56 instead of 46, which
still concludes at 20 rounds (18/20 gives p = 0.0004 < 0.05/56 = 0.0009).
Placing it at level 2 keeps the first publication as costed.
**Recommendation: level 1** if the budget allows it: without it, the first
publication leaves open the most obvious question. Open decision (below).

## 3. The load

### 3.1 Fixed-rate load, not closed loop

A "closed-loop" generator (each client waits for the answer before sending
the next one) **slows down when the server slows down**, and the requests
that should have left during a stall are never measured. The published
delays are then better than reality; the flaw is called *coordinated
omission*. We therefore use a **fixed-rate** generator: it sends N requests
per second whatever happens, and measures each delay from the instant the
request *should have* left.

### 3.2 The SLO, the capacity, the load points

**SLO per scenario** (the service objective: the threshold we commit to
holding): `p99 ≤ X ms` **and** (errors + timed out) < 0.1%. X is set once
per scenario, before any measurement, and published. Starting orders of
magnitude, to confirm at the local scouting: small static 10 ms, single
read 25 ms, list 1,000 100 ms, mixed 50 ms (excluding injected network
latency).

**Capacity** = the highest throughput where the SLO holds. This is the
headline figure of §5, and it goes through the same statistic as the rest
(§7.6).

**Latency points in absolute req/s**, fixed once per scenario on a
geometric grid (e.g. 500 / 2,000 / 8,000 / 32,000 req/s), **identical for
all the contenders and for all the levels**. A contender is measured at the
points it holds under its SLO; a point beyond is published as "outside
SLO".

**Who decides that a point is held**: the capacity measurements, at 0 ms
client, not the measurements at 20 ms. A point P is measured for a
contender if P ≤ **0.8 × the lower bound** of the confidence interval of
its capacity (§7.4). The 20% margin covers the difference in conditions: at
20 ms, C is several times larger than at 0 ms (§3.5), and a server can hold
less with more open connections.
At 20 ms, the p99 contains the injected RTT, and in addition several
handshake round trips for new connections (level 2): applying the SLO there
would require an arbitrary subtraction. The measurements at 20 ms
**describe** the perceived latency; they decide nothing.

**Every publication is frozen.** The arrival of Python at level 2 adds
columns; it touches neither the figures nor the verdicts of level 1. This
is not self-evident, it has to be imposed, because two things would move
otherwise:

- the **designated point** (§7.4) is the highest point eligible for all: a
  slow contender would make it go down, and the primary metric of level 1
  would change point. It is therefore **fixed by the publication that
  chose it** and no longer moves. A contender added later that does not hold
  it is published "outside SLO" at that point, which is the information;
- the **Holm family** (§7.4) would grow, the threshold would tighten and a
  published verdict could flip. Each publication has **its own family**,
  closed when it comes out, with its own α; the comparisons added at level
  2 form a new family.

### 3.3 Profiles

| Profile | Shape | What is read |
|---|---|---|
| Scouting | rate increasing by steps of **×1.25** (e.g. 100, 125, 160, 200, 250...), 30 s each, once only, after warm-up; same drain between steps as for the capacity (§7.6). Produces two bounds: **last step held** and **first step broken**. | bracket the capacity to place the fine ramp (§7.6). **Never published as a figure.** |
| Capacity | fine ramp on the common ×1.05 grid, bounded by scouting, repeated (§7.6) | the capacity under SLO, with its confidence interval |
| Fixed points | absolute grid of §3.2 | the throughput/latency curve compared at equal load |
| Plateau | fixed rate at ~70% of **the contender's** capacity, 10 min | stability: memory leak, p99 drift |
| Spike | plateau, then ×5 for 30 s, then back. **C sized on the spike's rate** (§3.5), otherwise it is the generator's connections that limit during the spike, not the server. | absorption and recovery |
| Connections | fixed rate, increasing number of client connections (10, 100, 1,000, 10,000) | the cost of a connection, the limit (below) |

**The Connections profile, in detail.** Per step, we read:

- the **connections held by the server** compared to C (§1.2);
- the **memory per connection**: rise in RSS ÷ rise in held connections,
  between two steps;
- the server's CPU at equal throughput, and the p99;
- the accept queue overflows, the refusals and the closures by the server
  (§1.2).

**The limit** = the first step where the held connections stay under C, or
where an overflow, a refusal or a closure by the server appears. It is
published with its cause. The connections open during the drain, outside
the measurement (§3.5), and the kernel counters are read as a
**difference** between the start and the end of the measured window: a
burst opening of 10,000 connections can overflow the queue without the
measured step being responsible.

**The idle timeout.** At a fixed rate R, every connection stays idle C ÷ R
seconds between two requests. Beyond the server's idle timeout, it closes
the connection: the profile would measure that setting, not the cost of a
connection. Default timeouts to read on the frozen versions: of the order
of 5 s for Node (Express) and uvicorn, unlimited for Go `net/http`. R is
chosen so that C ÷ R stays under half of the shortest of them; when that
is not possible without exceeding a contender's capacity, its closures are
counted and published as **a result of its default** (it is what 10,000
barely active clients would live), and lengthening the timeout is a listed
optimisation.

**The default ceilings.** Each stack has its own connection ceilings, which
decide "the limit" before the cost does. Example: Tomcat (Spring Boot)
accepts 8,192 connections (`server.tomcat.max-connections`) plus a queue of
100 (`accept-count`): at the 10,000 step, Spring Boot is capped by
configuration. Tomcat also closes every connection after 100 requests
(`max-keep-alive-requests`), which touches **all** the profiles, not only
this one (§1.2, "Closures by the server"). For each contender, we read and
publish: connection ceiling, listen queue requested from `listen` (which
the kernel bounds to `net.core.somaxconn`), idle timeout, maximum number of
requests per connection. Lifting them is a listed optimisation, never a
setting of the standard version.

### 3.4 The network

On loopback, the network costs almost nothing and the TTFB resembles
nothing real; with 20 ms, an HTTP/1.1 connection caps at ~50 req/s and it
is the number of connections that sets the throughput (§3.5). The two
conditions therefore serve two different measurements:

| Measurement | Client↔web latency | Web↔db latency |
|---|---|---|
| **Capacity** | 0 ms injected (direct network between VMs, ~0.1 ms) | 1 ms |
| **Perceived latency** (TTFB, TTLB at the fixed points) | 20 ms, then 80 ms at level 3 (≈ same city / another continent) | 1 ms |
| Full loopback (level 2) | 0 ms | 0 ms, to isolate the server alone |

### 3.5 How many client connections

Little's law: requests in flight = throughput × duration of a request. In
HTTP/1.1 without *pipelining*, a connection carries one request at a time,
so:

```
C ≥ 2 × R × (RTT + X)
```

R = target rate, RTT = injected round trip, X = p99 threshold of the
scenario's SLO (§3.2). The factor 2 leaves room for latency spikes.
Example: single read, 8,000 req/s at 20 ms of RTT, X = 25 ms → C ≥ 720.

**Why X and not the measured p99.** The p99 measured at scouting is taken
at low load; near capacity, the p99 climbs up to X. With C computed on 1 ms
at 200,000 req/s (C = 400), as soon as the p99 reaches 8 ms the 400
connections only carry 50,000 req/s: the generator queues the requests at
home, the coordinated omission correction counts them as server latency,
and the published capacity is that of the generator's pool. The measured
p99 would also depend on the contender (a slow server would receive more
connections); with X, C depends only on the scenario and the rate,
**identical for all**.

**C is recomputed at every step.** In a capacity ramp, the steps traversed
differ from one contender to another (§7.6): a C frozen at the top of the
ramp would give two contenders a different C at the same rate. Each step
uses C(R) for its own R; the missing connections open during the drain that
precedes the step, at the latest in its first 5 uncounted seconds, so that
TLS handshakes never fall in the measurement.

**Arrival process: constant interval**, frozen and published. Randomly
drawn intervals (a Poisson process) produce bursts that form queues and
inflate the p99; at equal rate, the two do not give the same figure. The
constant interval (wrk2's) is more reproducible and it is what readers
compare.

**Safeguard**: the generator publishes its internal queue (requests due but
with no free connection). A step where this queue is not empty while the
p99 is under X is invalidated (§4): it is the generator that limits.

In HTTP/2 (and HTTP/3), **the same C as in HTTP/1.1**, with **at most 8
simultaneous streams per connection** (published ceiling). Dividing C by
`SETTINGS_MAX_CONCURRENT_STREAMS` would amount to loading a few connections
of 100 streams: we would measure the parallelism *per connection* (one
reader, one writer per connection depending on the servers), and the
ranking would follow each one's internal architecture rather than its
cost. Real traffic is many clients, each with its connection and few
streams in flight. One virtual client = one connection, whatever the
protocol.

C is **published** with every point. The Connections profile (§3.3) is the
only one where C varies independently of the throughput.

---

### 3.6 End to end: what it covers exactly

A dynamic measurement goes **from the client to the database and back**:
the generator sends the request, the server handles it, queries PostgreSQL,
serialises, and the delay is taken on the client side at the first and at
the last byte received. The static cases deliberately stop at the server:
they isolate the HTTP cost.

Three additions so that "end to end" is true:

- **Latency between the server and the database as well.** In production
  the database is on another machine, at ~0.5-2 ms round trip. Without this
  latency, every SQL request looks free, which favours the contenders that
  make many. 1 ms injected between server and PostgreSQL (`tc netem` on
  db's interface, §0); 0 ms in full loopback at level 2.
- **Connection establishment counts.** Part of the requests open a new
  connection (TCP + TLS handshake) instead of reusing an existing one, like
  a real visitor arriving. Parameter: share of new connections, 0% and 10%.
  **It is the client that closes**, so the `TIME_WAIT` (the state where the
  kernel keeps a port reserved for 60 s after closing) falls on the tester:
  with the default port range (~28,000), it would hold ~470 new connections
  per second, whereas 10% of 20,000 req/s asks for 2,000. Hence
  `net.ipv4.tcp_tw_reuse=1` and a widened port range on the tester (§4),
  and any `connect` failure on the generator side (`EADDRNOTAVAIL`)
  invalidates the step.
- **PostgreSQL's CPU and memory are read too**, not only the server's: a
  contender that sends 64× more SQL requests moves its cost onto the
  database, and this cost must appear.

**What is not covered**: a real browser (rendering, cache, its own
connection rules). The load generator measures what an HTTP client
receives, not what a page displays. Optional level 3: a control sample
with a driven browser (Playwright) at low load, to verify that the
generator's TTFB and TTLB look like a browser's.

## 4. Validity

- **Same answer**: before each series, every contender answers a set of
  control requests. Comparison **after parsing and normalising the JSON**
  (key order, number format, whitespace), plus the status and the
  `Content-Type`, not byte by byte, which would fail everywhere for
  legitimate reasons. For writes, the effect in the database is checked too
  (row present, values). No figure without it.
- **Answers also checked under load**, at two levels:
  - **100% of the answers**, a cheap check: complete body (length =
    `Content-Length` when the header exists; in chunked or in HTTP/2
    without `Content-Length`, that is for lists served as a stream, clean
    end of stream and closed JSON), JSON that parses to the end,
    **returned key = requested key** (§2.1);
  - **1% of the answers**, drawn at random, full validation against the
    expected answer, which the generator computes since the dataset is
    deterministic.

  **A single wrong answer, at either level, fails the step** for that
  contender (SLO not held), with no extrapolation. Counting only the sample
  as an error would dilute by a factor of 100: a contender with 5% of
  truncated bodies would show 0.05%, under the SLO's 0.1% threshold. A
  wrong answer is worse than a slow one.

  **But it does not boil down to a lower capacity.** In a ramp, the failure
  of the step would only give "capacity 18,000 req/s" where the truth is
  "returns wrong data beyond 19,000", exactly the class of bug that §2.1
  names as the most likely of a batching. So: wrong answers have their
  **separate counter** (§1.2), published per configuration; a single one,
  on a single repetition, marks the configuration "**incorrect answers
  under load**", with the rate where it appeared, and **no capacity is
  published** for it. If it is Sluice.go, it is a bug to fix before any
  publication, and the campaign for that scenario is redone.
- **The generator is not the bottleneck**: at the opening of the campaign,
  **for each scenario**, the generator targets a trivial server that
  returns a **pre-serialised answer of the scenario's size and shape** (1 KB
  for the small static, ~100 KB and 1,000 rows for the list 1,000, the mix
  for the mixed), with the 100% validity check active. A fixed 1 KB answer
  says nothing about the list 1,000, where the generator parses hundreds of
  MB/s of JSON. It must hold **at least 2× the capacity of the fastest
  contender** of that scenario, from scouting, the only capacity known at
  the opening of the campaign; otherwise the tester gets more cores before
  going any further.
- **End-of-step checks.** During the campaign, if any of them fails, the
  step is **invalidated**:
  - CPU of the tester machine (softirq included, §1.2) above ~70%;
  - netem lost a packet (`dropped` > 0, §0);
  - `conntrack -S` shows a `drop` or an `insert_failed` (§0), if the module
    is loaded;
  - the generator could not open a connection for lack of a local port
    (`EADDRNOTAVAIL`, §3.6) or of a descriptor (`EMFILE` on the tester).
    A refusal or a silence **from the server** (`ECONNREFUSED`, timeout at
    opening, `ECONNRESET`) does not invalidate: it is the contender's
    behaviour, counted in its errors (§1.2, "Errors by nature");
  - the generator's internal queue was not empty while the p99 was under X
    (§3.5);
  - `%steal` above ~2% on any of the three VMs, or an Azure *Scheduled
    Event* overlapping the step (§0);
  - PostgreSQL did a requested checkpoint or an autovacuum during the step
    (§2.2);
  - PostgreSQL refused a connection (`too many clients already` in its
    log): `max_connections` was badly sized (below, "PostgreSQL accepts all
    the connections");
  - web's network throughput exceeded ~70% of the VM size's ceiling, in
    bytes or in packets (§0, §1.2).

  A **crash** of the server (§1.2) does not invalidate: it is the
  contender. The containers run **with no automatic restart**
  (`--restart=no`): a restart would hide the crash behind a few seconds of
  errors. The step is broken, the following ones too, the ramp stops there;
  the capacity stays the last step held and the crash is published with its
  cause (OOM, signal, exit code).

  These checks all bear on the measuring apparatus, never on the
  contender's behaviour: invalidating on a signal that the contender
  provokes (its own CPU, its memory) would bias the result.

  **Invalidating ≠ replaying a step.** An invalidation makes the **whole
  repetition** be redone, with a new process and the warm-up included,
  never the step alone in the middle of the ramp, which would inherit a
  process in another state and would contradict the "no replay" rule of
  §7.6.

  **One invalidation counter per cause**, because the cause dictates the
  remedy: a single counter would have the tester accused of an autovacuum:

  | Cause | Checks | Three in a row on a configuration mean |
  |---|---|---|
  | **tester** | CPU, netem, `conntrack`, the generator's `EADDRNOTAVAIL` / `EMFILE`, the generator's queue | the tester is undersized: we stop and resize it |
  | **host** | `%steal`, *Scheduled Events*, network ceiling | the Azure host is unstable or the VM size too small: we stop and redeploy the set of VMs |
  | **database** | requested checkpoint, autovacuum, refused connection | the PostgreSQL configuration (§2.2, §4) is wrong: we stop and fix it |

  The **censoring** of a ramp (§7.6) is not an invalidation of the
  apparatus: it is due to the contender, redoes the scouting, and enters
  none of these counters.

  In every case, we stop, we do not loop.
- **Warm-up**: a duration common to all = the maximum, over all the
  contenders, of the time to steady state measured in §1.4, × 1.5. Done
  after every restart, not measured, **at the rate of the first point of
  the round** (fixed points) or **of the first step of the ramp**
  (capacity), see §7.1. 30 s serve as a floor while waiting for the first
  §1.4 measurement; .NET's tiered JIT may demand more.
- **Repetitions and statistics**: see §7 (21 rounds minimum, one of them
  discarded, for what receives a verdict, 7 for the descriptive points, a
  new process every time, interleaved and random order).
- **Same environment**: same set of hardware (three VMs of the same size,
  §0), same PostgreSQL, same dataset, versions frozen and published.
- **State of the database**: reset before every repetition for the
  scenarios that write (§2.2).
- **Identical resource limits**: same number of physical cores allocated to
  the server (e.g. 4), database pool measured in both settings, the
  documented default of each contender, and an identical size for all.
  **The identical size is fixed before the campaign**, by a rule
  independent of Sluice.go: the PostgreSQL wiki formula,
  `(core_count × 2) + effective_spindle_count`, computed on the physical
  cores of the db machine, with `effective_spindle_count = 0` since the
  dataset fits in memory (§2.2). It weighs heavily: at 1 ms web↔db, a
  contender without batching caps in single read at ~pool / 1.1 ms (pool
  8 ≈ 7,000 req/s), a ceiling that `= ANY` exceeds by construction. This
  ceiling is announced in the "why" of §5.
  The identical size is a **total per contender**, all processes together:
  a contender with N processes (Python workers, Node in multi-process mode)
  receives total ÷ N per process (§2.6). Checked under load through
  `pg_stat_activity` (§1.2).
- **PostgreSQL accepts all the connections.** The default of
  `max_connections` is 100, a few of them reserved for superusers. Now the
  default pool can exceed it: Npgsql (.NET) opens up to 100 connections per
  connection string (`Max Pool Size`), SQLAlchemy 5 + 10 **per process**
  (§2.6): four Python workers ask for 60. A refusal from PostgreSQL would be
  read as a contender error whereas it comes from the database.
  `max_connections` is therefore set **above the largest possible total**
  among all the contenders and settings (default pool × processes, + a
  margin for the control connections), identical for all, published; a
  refusal during a step invalidates (cause **database**, above). The pool
  defaults are read on the frozen versions.
- **Container memory limited and published**: same limit for all on web,
  chosen so that no contender reaches it at the plateau (§3.3); reaching it
  is a crash (§1.2), published as such.
- **Logs declared**: log level and access log of each contender read and
  published. The standard version keeps the default (uvicorn, for example,
  writes one line per request by default; it is what the docs deploy), the
  optimised version may turn it off (listed optimisation). Docker's log
  driver identical for all and published; web's disk writes are read
  (§1.2), so that this cost shows instead of passing for framework cost.
- **Use of cores declared**: standard Node (one process) uses only one of
  the allocated cores; FastAPI depends on its number of workers (§2.6).
  Defensible for the standard version, but declared per contender
  (processes, workers, threads), with the CPU actually consumed, so that
  the gap is not read as a framework gap. Switching to multi-process mode
  is a listed optimisation of the optimised version.
- **System identical and published**: `net.core.somaxconn`, `ulimit -n`
  (`nofile`), ephemeral port range (widened on the tester),
  `net.ipv4.tcp_tw_reuse=1` on the tester (§3.6), `host` network (§0),
  `nf_conntrack_max` or absence of `conntrack`, kernel version, UDP buffers
  (`net.core.rmem_max`, `wmem_max`) and GSO/GRO state for HTTP/3 (§2.3),
  `net.ipv4.tcp_max_syn_backlog`, `net.core.netdev_max_backlog`,
  `net.ipv4.tcp_abort_on_overflow` (left at 0, the default, §1.2): the
  10,000-connection profile, the new connections and HTTP/3 depend on them.
  Also: transparent *huge pages* (`/sys/kernel/mm/transparent_hugepage/enabled`
  and `defrag`), which change the memory occupied and the latency of Go and
  of the JVM; `irqbalance` stopped and the pinning of interrupts and RPS
  (§0); the VM size and its network ceiling (§0).
- **File descriptor limit fixed, not only published**: identical `nofile`
  on web and on the tester (`--ulimit nofile=…`, above 10,000 + margin), and
  the **effective** value of the server process checked in
  `/proc/<pid>/limits` before every repetition. Depending on the Docker
  version, a container's soft limit can be 1,024; Go raises it by itself up
  to the hard limit (since Go 1.19), most other runtimes do not. Without
  this setting, the 10,000-connection profile would measure the container's
  configuration, not the framework.
- **Connection ceilings of each stack published** (ceiling, listen queue,
  idle timeout, requests per connection, §3.3).
- **TLS frozen and published**: certificate type (ECDSA P-256), allowed
  suites, session resumption enabled or not. With 10% of new connections,
  this choice weighs more than the framework.
- **Raw data published** with the code, so that anyone can replay.

---

## 5. What is published, per scenario

1. A diagram of what is measured.
2. Two curves: latency (p50, p99) as a function of throughput on the
   absolute grid (§3.2); memory as a function of throughput.
3. A short table: lines of code, dependencies (with the caveat that npm,
   cargo and go do not slice their modules at the same granularity: the
   figure compares within an ecosystem, barely between them), **capacity
   under SLO** (median and confidence interval), database connections used,
   SLO and number of client connections used, connections held by the
   server.
4. Three sentences: **why** the gap (the mechanism), and where Sluice.go
   loses. For the dynamic cases, the sentence says what is arithmetic: the
   pool / web↔db RTT ceiling of a contender without batching (§4), so that
   "faster framework" is not read where it is "fewer SQL round trips".
5. The verdicts ("faster", "slower", "equivalent to ±5%" for the p99,
   "equivalent to within one step" for the capacity, "inconclusive", §7.4)
   bear only on the primary metrics, with the floor of each; everything
   else is published as a median and interval, with no verdict. The scope
   of Holm (per family, not per publication) is written in the method
   (§7.4).
6. For each "with" variant, the observed batch size (§1.2).
7. The wrong-answer counter (§1.2) of each configuration, even at zero:
   zero is a result.
8. Per step, the closures by the server and the accept queue overflows
   (§1.2), even at zero, and the errors by nature.
9. The crashes (§1.2) of each configuration, with their cause, even at
   zero.

---

## Decisions (2026-10-05)

- **Q1, batching**: every contender is measured **without and with**
  batching, when its ecosystem has a documented mechanism for it. No "with"
  variant on the static cases.
- **Q2, mixed scenario**: yes, in addition to the pure cases (starting
  proportion: 80% single read, 15% list, 5% write).
- **Q3, TLS**: both, with and without.
- **Q4, machine**: three separate roles (db, web, tester), see §0.
  Everything is prepared and validated locally (containers pinned on
  distinct physical cores) for levels 1, 2 and 3, then replayed on three Azure
  VMs for the published figures. Local figures validate the chain; they
  serve neither as ratios nor as publication (revised after review: on a
  6-core WSL2 VM, even the ratios are not stable).
- **Q5, database pool**: both, the documented default of each contender,
  and an identical size for all.
- **Capacity and load points** (revised after review): the capacity is
  defined by an SLO per scenario and measured with repetitions; the latency
  points are in absolute req/s, common to all and to all the levels.
- **Network** (revised after review): capacity measured with no injected
  client latency, perceived latency measured at 20 ms; client connections
  derived from the throughput (§3.5).

Revised after the second review (2026-10-05):

- **Statistics**: like-for-like comparisons only, Holm family = scenario ×
  protocol × metric × load point (~23 comparisons), **21 rounds minimum**
  with the first discarded (§7.2, §7.4).
- **Capacity**: fine ramp anchored between the two bounds of a scouting at
  ×1.25 steps, drain between steps, capacity = the step that precedes the
  first failure, censoring rule (§7.6).
- **Eligibility for the fixed points**: decided by the lower bound of the
  capacity's interval, not by the SLO at 20 ms (§3.2).
- **netem**: enlarged `limit`, one loss invalidates the step (§0).
- **Batching**: + Go with DataLoader at level 1; .NET = GreenDonut, Npgsql
  multiplexing as a variant of the optimised one, not `NpgsqlBatch` (§2.1).
- **HTTP/2**: same C as in HTTP/1.1, ≤ 8 streams per connection (§3.5).
- **Validity of the answers**: normalised JSON, sampled validation under
  load (§4).

Revised after the third review (2026-10-05), replaces the corresponding
points above:

- **Common capacity grid**: steps `100 × 1.05^k` req/s identical for all;
  scouting only chooses the k traversed. A grid specific to each contender
  rounded each one differently, at every round, and the sign test turned
  that rounding into a verdict (§7.6).
- **No replay** of a broken step: the ramp stops at the first failure
  (§7.6).
- **Holm family settled**: verdicts on two primary metrics only (capacity,
  p99 TTLB at the designated point), family = scenario × protocol, n ≈ 46;
  everything else with no verdict (§7.4).
- **Validation**: cheap check on 100% of the answers (returned key =
  requested key), full on 1%; a wrong answer fails the step (§4).
- **C = 2 × R × (RTT + X)**, identical for all, with a safeguard on the
  generator's queue (§3.5).
- **Timed out = 10 × X + 4 × injected RTT** (§1.2).
- **Azure network in `host`**, `tcp_tw_reuse` and widened ports on the
  tester, end-of-step checks grouped (§0, §3.6, §4).
- **Identical pool = 2 × physical cores of db**, fixed before the campaign
  (§4).
- **One process per round for the fixed points**, a single warm-up;
  eligibility at 0.8 × the lower bound of the capacity (§3.2, §7.1).

Revised after the fourth review (2026-10-05), replaces the corresponding
points above:

- **Verdicts with three outcomes + inconclusive**: "faster / slower" (sign
  test + Holm), "**equivalent to ±5%**" (interval of the median of the
  per-round ratios entirely within [0.95, 1.05]), otherwise
  "**inconclusive**". "Indistinguishable" disappears: it was read as
  "equal" without proving it (§7.4).
- **Published gap = median of the per-round ratios**, with its interval,
  like `paired.awk`, not the ratio of the medians (§7.4).
- **Frozen publication**: designated point and Holm family specific to each
  publication, closed when it comes out; level 2 opens a new family (§3.2).
- **Wrong answers**: separate counter, published; a single one marks the
  configuration "incorrect answers under load", with no capacity published
  (§1.2, §4).
- **DataLoader "with"**: loader shared by the process, cache disabled,
  declared as a deviation from the docs; default windows published (§2.1).
- **PostgreSQL**: autovacuum off on the write table, `max_wal_size` sized,
  requested checkpoint or autovacuum during a step = step invalidated
  (§2.2).
- **"Generator is not the bottleneck" test per scenario**, with an answer
  of the scenario's size and validation active (§4).
- **End-of-step checks**: + `%steal` and *Scheduled Events* on the three
  VMs, + PostgreSQL; an invalidation redoes the whole repetition, three in
  a row stop the campaign (§0, §4).
- **C recomputed at every step**, connections opened outside the
  measurement; **constant-interval arrivals** (§3.5).
- **Fixed points**: 21 rounds at the designated point, 7 at the descriptive
  points (§7.2, §7.6). Level 1: ~27 → **~21 days** on one set of VMs.
- **Generator: in-house**, `oha` as a cross-check (below).

**Generator, settled: in-house.** The deciding criterion: no tool on the
market checks the key in the body on 100% of the answers (`oha` does not
validate bodies; wrk2 has no h2; k6 is too CPU-hungry on the tester side).
Whether `oha` separates TTFB and TTLB therefore no longer changes anything.
`oha` (`-q` for the fixed rate, `--latency-correction`) serves as an
**independent cross-check tool**. Requirements of the in-house generator:

- fixed load at constant interval with coordinated omission correction,
  TTFB and TTLB separated, h1/h2/h3, keys with no repetition, share of new
  connections, key check on 100% and sampled full validation, ≤ 8 streams
  per connection, C recomputed per step, internal queue and `connect`
  failures published;
- its h1/h2/h3 client shares **no code** with Sluice.go: a symmetric
  client/server bug would hide itself;
- **calibration over the whole distribution** against a server that sleeps
  exactly 5 ms: the p50 must equal 5 ms + injected RTT, to the resolution,
  **and p99 and p99.9 are read too**. The GC pauses of a generator written
  in Go, or its scheduling, appear in the tail, not in the median; now the
  p99 TTLB is a primary metric. The observed tail inflation (p99.9 − p50)
  is the **generator's noise floor**, published; it enters, brought back to
  a ratio, into the p99 TTLB floor (§7.4);
- **cross-check** of a subset of points with `oha`. The A/A tests are not
  enough: a generator bias is identical on both sides and cancels out;
- it also passes the A/A tests and the "trivial server" test per scenario
  (§4, §7.5).

Still open:

- **Cost**: level 1 between **~27 days (nominal) and ~46 days (worst
  case)** of continuous measurement on one set of VMs, ~11 to ~17 days on
  three sets (§7.6); **plus the internal study** (§8), ~19 days nominal,
  which precedes it. A trade-off to make between sets of VMs in parallel and
  the scope of level 1, on the worst case.
- **Rust with batching: level 1 or 2** (§2.6). Recommended: level 1, +12%
  of time, Holm still concludes.
- **Python standard = official template (synchronous SQLModel)** rather
  than `asyncpg` as the initial scoping proposed (§2.6). It is the rule of
  §2.4 applied; `asyncpg` becomes the optimised version.
- **Node at level 1: Express 5 rather than Fastify** (§2.5). Express is
  what teams deploy (about 10× the downloads of Fastify); Fastify goes to
  level 2.
- **Sluice.go standard on `net/http`, `httpstream` in optimised** (§2.5).
  The other choice, `httpstream` everywhere, would make the comparison of
  Sluice.go against Go `net/http` a mix of two effects (server and
  framework).
- **Internal study before the comparison** (§8): it fixes the optimised
  version of Sluice.go, with a declared bias (a dedicated study against one
  day for the others). The other option: limit the optimised Sluice.go to
  the same day and publish the study separately. Recommended: the first,
  declared; it is the true best configuration that a user will adopt.
- **Compression in Sluice.go**: gzip/deflate through the stdlib, to
  prototype in `benchmarks/` before any API; brotli/zstd impossible without
  a dependency; the gap against `tower-http` will say whether the question
  deserves to be asked.
- **Java standard = `JdbcTemplate`** rather than Spring Data JPA: the other
  stacks write their SQL, an ORM would measure the ORM. WebFlux/R2DBC
  (mentioned in the initial scoping) does not come in: a level 3 candidate
  for the streaming of large results, only.

Revised after the fifth review (2026-10-05), replaces the corresponding
points above:

- **One floor per primary metric**, built into the verdict: "faster"
  requires significant after Holm **and** gap ≥ floor; under the floor,
  "equivalent" or "inconclusive". The phrase "real and negligible"
  disappears, it contradicted §7.5 and `paired.awk` (§7.4).
- **Capacity in integer Δk**, never as a floating-point ratio; equivalence
  = interval of the Δk within [−1, +1], published "equivalent to within one
  step" (§7.4, §7.6).
- **Batch size** published for each "with" variant; standard Node expected
  at ~1 key per batch (`nextTick` fires after every socket callback), time
  `batchScheduleFn` in optimised; standard `gateway` = `Size: 64, Within: 2
  ms` from the docs' example, for lack of a default (§1.2, §2.1).
- **PostgreSQL**: `VACUUM FREEZE` at loading, `age(datfrozenxid)` read per
  round, autovacuum checked on all the tables; `max_wal_size` recalibrated
  on Azure (§2.2).
- **Invalidations counted per cause** (tester / host / database), censoring
  outside the counter (§4).
- **Cost in two figures**, A/A counted (one series per protocol and per set
  of VMs) (§7.5, §7.6).
- **Fixed points**: the 7 full rounds placed at random among the 20 useful
  rounds (§7.6).
- **Same draw seed** for all the contenders of a round (§2.1).
- **Multiplicity declared**: Holm holds 5% per family, not per publication;
  equivalences not corrected; rule for leaving a family fixed in advance
  (§7.4).
- **Step duration**: ≥ 1,000 counted requests for capacity, ≥ 10,000 at the
  fixed points (§7.3).

Completed on 2026-10-06:

- **§1.3 redone**: pauses **and** share of CPU, per runtime, on all the
  runs; Go (Sluice.go included) measured mostly in CPU, its GC being
  concurrent; Python added; Rust read through the allocator.
- **§2.6 Rust, Java, Python**: the question of each stack, its standard and
  optimised stack, protocols, default pool, images, pitfalls.
- **"With" mechanisms**: `async-graphql` DataLoader (Rust),
  `java-dataloader` + `ScheduledDataLoaderRegistry` (Java), `aiodataloader`
  in optimised only (Python) (§2.1).
- **Protocol not supported** by the standard server = "not supported" cell,
  never a proxy (§2.4).
- **Identical pool = total per contender**, all processes together (§4).
- **§2.5 Scope of the solutions**: "most used first" rule, hand-written
  SQL, LTS; evidence (2025 surveys + downloads read on 2026-10-06) and
  their biases; exclusions published. Node at level 1 = **Express 5** (and
  not Fastify); Gin and Fastify at level 2; .NET = Minimal APIs + Dapper;
  Java = Spring Boot 4.0 on Java 25 (4.1.2 delivered, §2.5); Python 3.14;
  Sluice.go standard on `net/http`, `httpstream` in optimised.
- **The three protocols** (§2.3): HTTP/3 at level 1 for Sluice.go
  (`httpstream` + `quic`) and Go (`quic-go`), in optimised; .NET at level
  2; own HTTP/3 Holm family (n = 8). HTTP/3 cross-check with `h2load`
  (independent client). Level 1: 114 configurations.
- **Compression** (§2.3 bis): gzip at level 1 on the list 1,000, at the
  fixed points, with no verdict; brotli, zstd, deflate at level 2; bytes
  and CPU per request, TTLB on a throttled link; generator test per
  encoding.
- **§8 Sluice.go internal study**: one-factor-at-a-time axes around the
  supported path (transport, driver, `gateway`, transport batch, stream,
  compression, combination), grid protocols × losses × new connections ×
  traffic shape, "ready for production" column; it precedes the comparison
  and fixes Sluice.go's optimised version.
- **Correction**: Sluice.go reaches the database through `sluice/postgres`,
  not through `pgx` (§2.5).
- **Level 2 Holm family**: 32 comparisons per metric, n = 64, Gin and
  Fastify included (§7.4).
- **Server-side connections** (revision 2026-10-15): connections held by
  the server compared to C, accept queue (`nstat`, `ss -ltn`), closures by
  the server and errors by nature read for all; a refusal from the server
  is a contender error, only the tester's exhaustion (`EADDRNOTAVAIL`,
  `EMFILE`) invalidates; `nofile` fixed and checked; Connections profile
  specified (limit, idle timeout, default ceilings published) (§1.2, §3.3,
  §4).
- **Machine and database** (revision 2026-10-15): `max_connections` fixed
  above the largest total of pools, a refusal invalidates (database cause);
  VM network ceiling ≥ 2× the expected throughput, exceeding 70%
  invalidates (host cause); web's network interrupts pinned to the server's
  cores, CPU of the whole web machine read; crash = contender behaviour,
  with no automatic restart; logs declared, disk writes read; transparent
  *huge pages* published (§0, §1.2, §4).

## 6. The matrix, and how to make it feasible

All the decisions above multiplied give several thousand configurations (6
contenders × with/without batching × 7 request types × 3 protocols × TLS
yes/no × 3 networks × 2 pools × 4 profiles), each costing several hours
with the protocol of §7. This is out of reach. We therefore measure by
**levels**: each level is complete and publishable on its own.

| Level | Content | Role |
|---|---|---|
| **1: core** | Sluice.go, Go (`net/http` + `pgx`, without and with DataLoader), Node (Express 5 + `pg`, without and with DataLoader), Rust (axum + `sqlx`), scope in §2.5 · standard **and** optimised versions · with/without batching (dynamic cases only) · small static, single read, list 1,000, mixed · HTTP/1.1 and HTTP/2 · **HTTP/3** for Sluice.go and Go/`quic-go`, in optimised (§2.3) · **gzip compression** on the list 1,000, at the fixed points, with no verdict (§2.3 bis) · TLS yes · capacity at 0 ms client, latency at 20 ms client · web↔db 1 ms · identical pool · containerisation (doc + optimised images) | the first publication |
| **2: extended** | + Python, Java, .NET (stacks in §2.5, §2.6) · + HTTP/3 for .NET (Kestrel) · + brotli, zstd, deflate among those that offer them · + Gin, Fastify (without batching) · + Rust with DataLoader (unless it moves to level 1, §2.6) · + TLS no · + default pool · + full loopback · + 10% of new connections · + plateau and spike profiles | complete the camps and the conditions |
| **3: complete** | + HTTP/3 for Python (Hypercorn) · + 80 ms · + files 10 KB-100 MB · + list 100,000 · + write only · + connections profile · + Playwright sample | the edge cases |

**Level 1 count**: 7 variants (Sluice.go, Go and Node without/with, Rust) ×
2 code versions on the 3 dynamic cases = 42, plus 4 variants × 2 on the
small static = 8, × 2 protocols = 100 configurations, **+ 14 in HTTP/3**
(Sluice.go and Go/`quic-go`, optimised version only, without and with on
the 3 dynamic cases + the static: 2 × (6 + 1)) = **114 configurations**,
plus 8 compression configurations at the fixed points only (§2.3 bis).
Everything (levels 1, 2 and 3) is built and validated locally before the first
Azure hour. The move to Azure VMs replays level 1 first.

## 7. Repetitions and statistics

Goal: that a published difference be a real difference, that a published
equivalence be proven within a declared margin, and that everything else
be published for what it is: inconclusive. Failing to show a difference
never proves an equality.

### 7.1 What a repetition is

A repetition = **a new server process** (restarted), a warm-up (§4), then
the measurement. Repeating without restarting only measures the noise of a
single state (memory placement, JIT already hot, pool already filled): it
gives tight and wrong statistics.

What a repetition covers:

- **Capacity**: one ramp (§7.6). Warm-up at the rate of the first step.
- **Fixed points**: **the points of the round, in the same process**, in
  increasing order of rate, a single warm-up at the first point. Depending
  on the round (§7.2), it is either the designated point alone, or all the
  eligible points. The points are all under the capacity (§3.2), so no
  overload leaves state for the next point; the increasing order is the
  same for all, so the age of the process at each point is too. One
  process per point would multiply the warm-ups by the number of points for
  information that does not change the ranking.

The fixed points need the capacity (eligibility, §3.2): a scenario's
capacity campaign ends before that of its points.

A **round** = every contender of the scenario goes through one repetition,
in a randomly drawn order (A C B, then B A C...). A drift of the machine
(warm-up, CPU frequency, another process) then touches everyone equally
instead of penalising whoever goes last. The round is the paired unit of
the sign test (§7.4). **The first round is discarded**: it absorbs the
machine's own ramp-up (CPU frequency, kernel caches); this is already what
`paired.awk` does.

### 7.2 How many

- **21 rounds minimum for what receives a verdict**, the capacity and the
  designated point (§7.4), with the first discarded (§7.1): **20 useful
  rounds**. The **descriptive points** (the other fixed points, published
  with no verdict) only need **7 rounds**: their median is published with
  the interval by the ranks from the 1st to the 7th value (98%), wider, and
  announced as such. Concretely, 14 rounds at the designated point alone +
  7 rounds at all the eligible points (the designated point included): the
  designated point has its 21 values. The figure is not arbitrary, it comes
  from the Holm correction (§7.4). With m rounds, the two-sided sign test
  never goes under p = 2^(1−m); Holm demands 0.05 / n of the smallest p, n
  being the size of the family. So **m ≥ log₂(40 · n)**, plus a margin for
  ties and lost rounds. m counts the **non-tied** rounds: the sign test
  discards ties, and with the common capacity grid (§7.6) two contenders
  within one step produce many of them; this is intended: they tighten the
  interval of the gap around 0 steps and lead to "equivalent to within one
  step" (§7.4). With n ≈ 46 (§7.4), log₂(1,840) ≈ 10.8: even 10/10
  (p = 0.002) would not pass 0.05/46 = 0.0011, no difference could be
  asserted. At 20 rounds, 18/20 gives p = 0.0004 and passes; 17/20
  (p = 0.0026) does not pass the first threshold. If the family grows, the
  minimum is recomputed with the formula, it is not kept.
- **Adaptive stop, by whole round**, on the primary metrics only: the unit
  that stops is the *scenario × primary metric*, never a configuration
  alone. If the 95% confidence interval of the median of **one** contender
  exceeds ±5% (for the capacity, in integers: it covers more than one step
  on either side of the median, §7.4), **all** the contenders redo 5
  rounds, up to 40. Otherwise one contender ends at 40 repetitions and
  another at 20, and the extra rounds have no partner left for the paired
  test. Beyond 40, the unstable contender is published as **unstable** with
  its gap, not as a figure.
- The stopping rule bears on the **width** of the interval, never on the
  result of the test: stopping as soon as the test says "faster" would
  manufacture false positives.

### 7.3 Enough requests for the percentiles

A percentile only makes sense with enough samples beyond it. For p99.9, at
least ~10,000 counted requests are needed (10 requests beyond the
threshold); at 100 req/s, that demands 100 s of step. The need depends on
what the step decides:

- **capacity steps**: the SLO bears on the p99, so **≥ 1,000 counted
  requests** (10 beyond the p99). At 30 s per step, that only lengthens the
  steps under ~40 req/s. Aiming for 10,000 here would make 200 s steps for
  a contender at 50 req/s, for a p99.9 that the ramp does not publish;
- **fixed points**: ≥ 10,000 counted requests, since the p99.9 is
  published there.

A percentile that does not reach its number **is not published** for that
step.

### 7.4 What is computed

| Computation | For what |
|---|---|
| Median of the repetitions | the published value (less sensitive than a mean to an outlying repetition) |
| 95% confidence interval of the median **by ranks** (an exact interval drawn from the binomial law: with 20 values, from the 6th to the 15th smallest) | the margin of uncertainty, published next to every figure. Preferred to the *bootstrap*, which gives coarse intervals with no guarantee on so few repetitions. |
| Paired sign test over the rounds ("over 20 rounds, how many times does A beat B?"), the method already in place in `internal/bench/scripts/versus.sh` / `paired.awk`, **to be evolved**: today it applies a fixed threshold (2.5% per side) and does not display the p value; it must display it, and Holm applies downstream on the family | say whether a difference is real: "faster" or "slower" |
| Size of the gap, **p99 TTLB**: **median of the per-round ratios** (A/B in each round, then the median of these ratios, what `paired.awk` already computes), with its 95% interval by ranks. Not the ratio of the medians: it is another quantity, which ignores the pairing. | say whether a difference matters, and found the equivalence |
| Size of the gap, **capacity**: **in integers**, Δk = kA − kB per round (the gap in grid steps, §7.6), median of the Δk and its interval by ranks. No floating-point ratio enters a decision: `100·1.05^(k+1) / 100·1.05^k` is 1.0499999 or 1.0500001 depending on k, and a bound at exactly one step would flip on a bit. The gap is **displayed** as a percentage (1.05^Δk − 1), never computed that way. `paired.awk` gains this integer mode. | same, with no rounding |
| The metric's **floor** (below) | separate a real difference from a difference that matters |
| Equivalence test: interval of the gap entirely within the **margin** (below) | published "**equivalent**", with the margin |

**The floor: a single one per primary metric**, the largest of what the
apparatus cannot distinguish:

| Metric | Floor = the largest of |
|---|---|
| Capacity | **1 step** (Δk = 1, i.e. 5%); the largest median \|Δk\| of the A/As (§7.5) |
| p99 TTLB at the designated point | `MINEFFECT` (3%); the largest A/A gap (§7.5); the **generator's tail inflation** (p99.9 − p50 at calibration, in ms), **divided by the smaller of the two p99s compared**, so that a floor in ms becomes a ratio, and the less favourable of the two |

**The equivalence margin** equals the floor, at minimum ±5% for the p99.
For the capacity, it is written in steps: **interval of the Δk entirely
within [−1, +1]**, published "**equivalent to within one step**" (±5%).
This is what the grid can say, nothing finer: two contenders of which one
wins by one step in 10 rounds out of 20 and ties in the other 10 come out
equivalent to within one step, which is true.

**The three verdicts**, in this order:

| Condition | Verdict |
|---|---|
| sign test significant after Holm **and** \|median gap\| ≥ floor | "faster" / "slower", with the gap and its interval |
| otherwise, interval of the gap entirely within the margin | "**equivalent to ±5%**" (p99), "**equivalent to within one step**" (capacity), with the margin if the floor widened it |
| otherwise | "**inconclusive**", with the gap and its interval |

A difference that is significant but under the floor **is not** published
as a difference: it falls in the second or third row. This is already what
`paired.awk` does (`noise`, under `MINEFFECT`).

Equivalence follows the logic of TOST (two one-sided tests: is the gap
surely above −margin *and* surely below +margin?): equality is only
declared if the uncertainty itself fits within the margin. The margin is
not chosen: it is the floor, that is, what the measurement cannot
distinguish.
Why it is necessary: with 18/20 required, a contender really 3% faster
under a noise of 5% per round only wins about 66% of the rounds; it used
to come out "indistinguishable", which the reader reads as "same
performance". It will now come out "inconclusive", which is true.

**Comparisons declared in advance, like-for-like only.** Seven variants × 2
code versions give 14 configurations and 91 pairs per scenario; across all
the scenarios and load points, hundreds of tests at 5% would produce dozens
of chance "faster". Only the following comparisons receive a verdict,
fixed before the campaign, and always at equal code version:

- Sluice.go without batching against each other variant without (Go, Node,
  Rust); Sluice.go with against each other variant with (Go, Node), for the
  standard version, then for the optimised one;
- without against with batching, for each contender that has one, in each
  version;
- standard against optimised, for each variant.

That is, for a dynamic scenario, (3 + 2 + 3) × 2 + 7 = **23 comparisons**
per metric.

**Level 2: a new family** (§3.2), same rules, on the variants it adds alone
(Python, Java, .NET, Gin, Fastify, and Rust with if it has not moved to
level 1):

- Sluice.go without against Python, Java, .NET, Gin, Fastify without: 5 × 2
  versions = 10;
- Sluice.go with against Java, .NET, Rust with: 3 × 2 = 6, plus Python with
  in optimised only (§2.6): 7;
- without against with: Java, .NET, Rust × 2 versions, Python in optimised:
  7;
- standard against optimised: Python without, Java without and with, .NET
  without and with, Rust with, Gin, Fastify: 8 (Python with has no
  standard).

That is **32 comparisons** per metric, n = 64 over the two primary metrics:
m ≥ log₂(40 × 64) ≈ 11.3 non-tied rounds, 20 rounds are still enough (18/20:
p = 0.0004 < 0.05/64 ≈ 0.00078).

**HTTP/3 at level 1: its own family per scenario**, optimised version only
(the only one to speak it): Sluice.go without against Go/`quic-go` without,
Sluice.go with against Go with, without against with for each: **4
comparisons** per metric, n = 8 (n = 2 on the static). The verdict pits two
complete stacks (framework + QUIC); what falls to Sluice.go's `quic` alone
is read in the internal study (§8.4).

**Primary metrics, the only ones to receive a verdict**, declared before
the campaign:

- the **capacity under SLO**;
- the **p99 TTLB at a designated fixed point**: the highest point of the
  grid (§3.2) eligible for all the contenders of the scenario, fixed after
  the capacity campaign and before that of the points, then **frozen for
  the following publications** (§3.2).

Everything else (p50, p90, p99.9, TTFB, memory, CPU, other points) is
published as a median and interval, **with no verdict**. What we lose: no
"faster" on the p50 nor on memory. If the project's public argument becomes
memory, it replaces the p99 as the primary metric.

The **family** = scenario × protocol **within a publication** (§3.2), over
the two primary metrics:
n = 2 × 23 = **46** for a dynamic scenario, 2 × 10 = 20 for the static (3
Sluice.go comparisons × 2 versions + 4 standard/optimised).
A family per load point let false positives through *between* families;
reducing what receives a verdict settles the problem without pushing up
the number of rounds.

On this family, **Holm correction** (each test's threshold is tightened
according to the number of tests in the family: the smallest p must pass
0.05 / n, the next 0.05 / (n−1), etc.). The other pairs are in the raw
data, with no verdict.

**What Holm guarantees, and what it does not guarantee: published as is.**
The risk of a wrong "faster" is held at 5% **per family**, not per
publication: level 1 has 8 families (4 scenarios × 2 protocols), that is up
to ~0.4 family containing a false positive per publication. It is the price
of a family that can conclude in 20 rounds: a single family per publication
(n ≈ 330) would require 19/20. **Equivalences** are not corrected by Holm:
each is held at 2.5% on each side (95% interval). Both sentences appear in
the published method.

**Comparisons that leave the family.** A configuration marked "incorrect
answers under load" (§4) or "unstable" (§7.2) has no figure to compare: its
comparisons leave the family and n decreases. The rule is fixed here,
before the campaign, and it does not depend on the result of the tests: it
does not bias Holm.

### 7.5 Measuring the noise itself

At the opening (rhythm below), **three A/A tests**: the same contender
measured twice as if it were two contenders, on three different scenarios.
None must come out "faster" or "slower" at a single test's threshold, 5%,
without Holm: this is the harshest case for the machine. A single failure
out of three is expected from time to time (a test at 5% is wrong one time
in twenty); **two failures or more**: the machine is too noisy, we stop
before publishing anything. The largest observed A/A gap enters the
**floor** of each primary metric (§7.4), which is also its equivalence
margin. Each A/A must also come out "equivalent" (§7.4): if it comes out
"inconclusive", the noise exceeds the margin, and the margin is widened to
the largest observed A/A gap: we do not claim an equivalence finer than
what the machine can distinguish.

**What the A/As cost**, counted in the estimate (§7.6). A **campaign** = one
scenario × one protocol on a set of VMs; the three A/As open each of them,
on the capacity alone (the longest metric to measure, and the one whose
grid sets the margin), at 21 rounds. So as not to pay for them eight
times: the three A/As are done **once per protocol and per set of VMs**, on
three different scenarios, and cover the four campaigns of that protocol;
redone if the set of VMs changes or if a "host" invalidation (§4) stops
one.

### 7.6 What it costs, and how to hold it

Ten complete ramps per configuration do not fit. So, in three stages:

1. **Scouting**: one ramp by steps of ×1.25 per configuration (§3.3), which
   brackets the capacity between **L** (last step held) and **H** (first
   step broken). Once only, never published.
2. **Capacity**: at each repetition (new process, warm-up), a fine ramp on
   an **absolute geometric grid, common to all**: steps `100 × 1.05^k`
   req/s, identical for all the contenders and all the levels, k possibly
   negative (a slow contender on the list 1,000 will hold less than 100
   req/s). L and H only serve to choose the k traversed: from the largest
   step ≤ 0.9 L to the smallest step ≥ 1.1 H. ~30 s per step (or more per
   §7.3).
   - **Why a common grid.** A repetition's capacity is a step, hence a
     rounding down. With a grid anchored on each contender's L, the
     rounding differs from one contender to another and repeats at every
     round: true capacity of A = 100, A's grid …99.9 / 104.9 → A publishes
     99.9; true capacity of B = 102, B's grid …97.5 / 102.4 → B publishes
     97.5. If the noise of a round is under 5%, A beats B 20 times out of
     20, Holm validates, and we publish the opposite of reality. With a
     common grid, two contenders within one step land on the same step:
     tie (Δk = 0), discarded by the sign test, which tightens the interval
     of the Δk around 0, verdict "equivalent to within one step" (§7.4).
   - The old fixed window (75%-115% of the scouting) only covered part of
     the uncertainty of a scouting at ×2 steps: a contender at 1.8× the
     last step held would have been published at 1.15×, capped by the ramp
     and not by itself, hence the bounds 0.9 L / 1.1 H.
   - **Drain between steps**: beyond the capacity, a queue forms; the next
     step would inherit it and break mechanically. Between two steps, the
     load stops until all the in-flight requests have come back (or timed
     out, §1.2), and the **first 5 seconds** of each step are not counted.
   - **Repetition capacity** = the step that **precedes the first
     failure**; the ramp stops there. **No replay** of a broken step:
     replaying the failures without replaying the successes biases the
     capacity upward, and especially for contenders with a noisy p99 (JIT,
     GC). Accidents are absorbed by the 20 rounds, that is their role. No
     "last step under SLO": with holds / breaks / holds / breaks, it gave
     two answers.
   - **Censoring**: if the first step breaks, or if the last one holds, the
     capacity is outside the window; the repetition is **invalid**, the
     window is widened (new scouting) and the repetition redone.

   Statistic of §7 on the 20+ values, **in integers Δk** (§7.4). The
   resolution is one step: two contenders within one step of each other
   tie and come out equivalent to within one step (§7.2, §7.4), and no
   capacity gap under one step is published as a difference (floor, §7.4).
3. **Fixed points**: at 20 ms client side, all the points of a round in a
   single process (§7.1): **14 rounds at the designated point alone** and
   **7 rounds at all the absolute points** of §3.2 eligible for each
   contender (§3.2), designated point included: 21 values for the primary
   metric, 7 for the descriptive points (§7.2). The **first round** is
   always a "designated point alone" round, and it is the one that is
   discarded (§7.1). The **7 full rounds are placed at random** among the
   following 20 (published draw), not as a block at the end: the 21 values
   of the designated point come from two protocols (warmed up directly at
   the point, or reached after the lower points), and two successive blocks
   would confound the protocol gap with a drift of the machine over time.
   The pairing by round remains exact, since within a round all the
   contenders follow the same protocol.

Level 1 estimate, per configuration, warm-ups counted (1 min each, to
revise when §1.4 has measured the time to steady state):

- capacity: 21 × (1 min of warm-up + ~10 steps × 35 s, drain included) ≈
  21 × 7 min ≈ 150 min;
- points: 14 × (1 min of warm-up + 90 s) + 7 × (1 min + 4 × 90 s) ≈ 35 + 49
  ≈ 85 min (against ~150 min with 21 rounds at all the points);

that is **~3.9 h**; × 114 configurations ≈ 445 h. To which are added:

- the **compression**: 8 configurations at the fixed points only (~1.4 h
  each) ≈ 11 h;
- the **A/As** (§7.5): 3 per protocol, each = 2 configurations × 21 rounds
  of capacity ≈ 5 h, that is 3 × 5 × 3 protocols ≈ **45 h** per set of
  VMs;
- **+ 30%** for scouting, invalidated repetitions and a few extensions of
  the adaptive stop.

**Two figures, not one**:

| Case | Computation | One set of VMs | Three sets |
|---|---|---|---|
| **Nominal** | (445 + 11 + 45) × 1.3 | **~650 h, ~27 days** | ~11 days (the A/As are redone on each set) |
| **Worst case** | the adaptive stop pushes every scenario × primary metric to 40 rounds: capacity 40/21 × 150 ≈ 285 min, points 33 rounds at the designated point alone + 7 full ≈ 130 min, that is ~7 h per configuration; (114 × 7 + 11 + 45) × 1.3 | **~1,110 h, ~46 days** | ~17 days |

The worst case is not theoretical: a single unstable contender (JIT, GC)
restarts **all** of its scenario (§7.2). The budget is decided on the worst
case, or level 1 is reduced before starting. Each additional minute of
warm-up costs 42 min per configuration, ~70 h over level 1: .NET's JIT
(level 2) will weigh. This is the price of a statistic that can conclude
(§7.2): at 10 rounds the cost was half, but no verdict would have passed
Holm. Levers: more powerful VMs do not shorten the step durations; several
sets of three identical VMs in parallel do (~11 to ~17 days on three
sets), provided that a given scenario stays whole on one set (paired
rounds are not compared from one machine to another), and never two
configurations at the same time on the same machine; finally, 3 fixed
points instead of 4, which only plays on the 7 full rounds, ~−5%. The
generator and the analysis are automated: we launch, we read the report.

## 8. Internal study: which Sluice.go implementation to recommend

The comparison answers "Sluice.go against the others". This study answers
another question: **among its own implementations, which one to recommend,
for which use**. It has its own publication, its own statistical family and
its own report, and it **precedes the comparison**: it is what fixes the
optimised version of Sluice.go (§2.4).

**Bias to declare.** Sluice.go's optimised version comes out of a dedicated
study; those of the others come out of a day of work and their performance
documentation. The asymmetry is published in the comparison, with a pointer
to this study. Review by practitioners (§2.4) is the mitigation: they can
bring what the author does not know about their stack.

### 8.1 The available implementations

| Layer | Options | Status |
|---|---|---|
| HTTP transport | `web` on `net/http` (h1; h2 by ALPN); `httpstream` + `webstream`: `Serve` (h1, and h2 by preface or ALPN), `ServeH2Conn`, `ServeH3Conn` | `net/http`: **supported path**. `httpstream`: **experimental**, a parser exposed to hostile input that the project itself says it does not yet stand behind |
| QUIC (under HTTP/3) | Sluice.go's `quic`; external reference: `quic-go` | `quic`: **experimental**, not yet confronted with another implementation |
| PostgreSQL driver | `sluice/postgres`; `pgx` under the same `gateway` | `sluice/postgres`: the example's path |
| Batching | `gateway`: `Size` (max batch size), `Within` (max wait from the first key) | both mandatory, no default for `Within` |
| Batch on the transport side | `httpstream` `Config.MaxRequestsPerBatch` (default 64), `MaxConcurrentStreams` (h2/h3, default 64 in h3) | |
| Life of a connection | `httpstream` `Config.MaxRequestsPerConn`: **default 1,024**, then the connection is closed | see the pitfall below |
| Response | buffered (`web.WriteJSON`); streamed (`httpstream` `Response.Stream`, one batch at a time) | |
| Compression | none today; gzip and deflate possible through the stdlib | to prototype **in the `benchmarks/` module** before any API decision (§8.5) |

**Pitfall: `MaxRequestsPerConn = 1024`.** Under load, every `httpstream`
connection closes every 1,024 requests, and the client reconnects with a
TCP + TLS handshake. At 20,000 req/s, that makes ~20 new connections per
second that `net/http` does not make. `httpstream` would pay this cost in
the measurement, and it would be read as the cost of the protocol. It is
therefore measured as a separate axis (default, then unlimited or very
large if the configuration allows it, to check), and the reconnections are
counted and published, as for all the contenders (§1.2, "Closures by the
server"; Tomcat has the same pitfall at 100 requests, §3.3).

### 8.2 The reference configuration

**R** = the supported path: `web` on `net/http`, HTTP/1.1 with TLS,
`sluice/postgres`, `gateway` `Size: 64, Within: 2 ms`, buffered response,
no compression. Each axis is compared to R **changing only one thing at a
time**: a full factorial design (all the combinations) would cost months.
The interactions that really matter are measured separately (§8.4).

### 8.3 The axes, one factor at a time

Same scenarios as level 1 (small static, single read, list 1,000, mixed),
same measurement protocol (§3, §4, §7), same primary metrics: capacity and
p99 TTLB at the designated point.

| Axis | Variants compared to R | What the recommendation takes from it |
|---|---|---|
| **A: protocol and transport** | `net/http` h2; `httpstream` h1, h2, h3 (4) | which transport, and what moving to the experimental path costs or brings |
| **B: driver** | `pgx` under `gateway` (1) | what `sluice/postgres` is worth against the Go reference, at equal batching |
| **C: batching** | grid `Size` {16, 64, 256} × `Within` {0.5, 2, 8 ms}, R excluded (8) | the recommended values of `gateway`, **as a function of load**: `Within` costs at the low point (added latency) and pays near capacity. The recommendation is a rule (for example `Within` related to the web↔db RTT), not a single value |
| **D: transport batch** | `MaxRequestsPerBatch` {8, 256}; `MaxRequestsPerConn` unlimited (3) | whether the defaults of `httpstream` are the right ones |
| **E: response** | streamed against buffered, on list 1,000 and, at level 3, on list 100,000 (1) | when to serve as a stream: TTFB, peak RSS, and TTLB |
| **F: compression** | gzip level 1, gzip level 6 (default), deflate 6; streamed, flush per batch against end of response (3 + 1) | which algorithm, which level, from which size. List 1,000 only (§2.3 bis), with the measurement on a throttled link |
| **G: combination** | the best option of each axis, together, against R (1) | check that the gains add up; otherwise, the interaction is published |

That is, per scenario where everything applies (the list 1,000), 4 + 1 + 8
+ 3 + 1 + 4 + 1 = **22 comparisons** per metric, n = 44 over the two primary
metrics: m ≥ log₂(40 × 44) ≈ 10.8, and 20 rounds are enough (18/20:
p = 0.0004 < 0.05/44 ≈ 0.0011).
The axes that do not apply to a scenario leave it (no driver or batching on
the static, no compression under 2 KB); the family shrinks accordingly,
fixed before the campaign.

### 8.4 Protocols by network: where they really differ

On a clean network, with many connections carrying one request each,
HTTP/3 can only cost more: per-packet encryption, UDP in user space. Its
advantages only appear in three conditions: losses, new connections, and
many streams per connection (where HTTP/2 suffers TCP's head-of-line
blocking: one lost packet blocks **all** the streams of the connection,
whereas QUIC only blocks its own). Measuring only on the clean network of
§3 would publish a wrong verdict against HTTP/3. Hence a dedicated grid:

| Factor | Values |
|---|---|
| Implementation | `httpstream` h1, h2, h3; references `net/http` h1, h2 and `quic-go` h3 (6) |
| Losses | 0%; 1% in both directions |
| New connections | 0%; 10% |
| Traffic shape | **API**: many connections, ≤ 8 streams (§3.5); **browser**: 6 connections per virtual client in h1, 1 in h2/h3, 50 requests in flight |

RTT 20 ms, single-read scenario, then list 1,000; 6 × 2 × 2 × 2 = **48
configurations**, at the fixed points only: the capacity, for its part, is
measured at 0 ms and without losses (axis A).

**Losses, without touching web.** `netem` only delays and loses outgoing
traffic, and it never runs on web (§0). The losses in the web → client
direction are therefore injected **at the tester's input**, through an
`ifb` interface (the kernel redirects incoming traffic there to apply a
queue to it as if it were leaving). Both directions stay on the tester.

**The `dropped` check changes meaning.** With injected losses,
`dropped > 0` is intended, and the rule of §0 ("one loss invalidates the
step") can no longer be used. The check becomes: the maximum `backlog` of
the queue stays under its `limit`, and the measured loss rate stays within
±10% of the injected rate. A loss through overflow shows in the first, a
badly set injection in the second.

Verdicts on this grid: h3 against h2 and h2 against h1 **at equal
implementation** (`httpstream`), and `httpstream` h3 against `quic-go` h3,
per condition, on the p99 TTLB. It is a family of its own.

### 8.5 What the recommendation can say, and what it cannot

**A performance is not a production recommendation.** `httpstream` and
`quic` are experimental, and their own documentation says so: their parser
faces hostile input, they have no proven fuzzing corpus, and they have not
been confronted with other implementations nor reviewed by specialists. A
measured gain lifts none of these reservations. Every line of the report
therefore carries two columns, **measured gain** and **ready for
production**. The second only turns to yes on verifiable criteria:

| Component | Condition for "ready" |
|---|---|
| `httpstream` h1 / h2 | fuzzing corpus maintained and run in CI; `h2spec` (the HTTP/2 conformance suite) at 100%; external review |
| `quic` + `httpstream` h3 | the same, plus the **QUIC Interop Runner** (the interoperability matrix between QUIC implementations) green against at least two implementations that share no code |
| Compression | no maturity reservation (stdlib), but a security one: compressing a response that mixes a secret and attacker-controlled data exposes to **BREACH** (the attacker guesses the secret byte by byte by watching the compressed size). The recommendation says when not to compress |

**Brotli and zstd.** The Go stdlib has no encoder for them. Go 1.26
contains `internal/zstd`, but it is a **decompressor**, and it is internal.
The candidate modules (read on 2026-10-06):

| Module | Brings | Graph |
|---|---|---|
| `github.com/klauspost/compress` v1.20.1 | zstd, plus gzip/deflate faster than the stdlib (drop-in replacement), plus `gzhttp` | **no dependency** |
| `github.com/andybalholm/brotli` v1.2.6 | brotli, in pure Go (transposed from the reference C implementation) | pulls in `github.com/xyproto/randomstring`; one version retracted for "data corruption" (v1.0.1) |
| cgo bindings to the C libraries | brotli, zstd | **excluded**: cgo breaks static and cross compilation and the `scratch`/`distroless` images |

Neither enters the main module: its rule is `golang.org/x/crypto` alone,
and CI enforces it. If the gap measured against `tower-http` (§2.3 bis)
justifies it, the acceptable form is a **separate module** (its own
`go.mod`, for example `sluice/compress`), which the user adds if wanted: the
main module's graph does not move. A third way, the most common in
production: leave brotli and zstd to the proxy or the CDN in front of the
service, which already does it (nginx, Caddy, Envoy). For an API whose
clients are not browsers, gzip covers almost everything.

**Rewriting a codec: no.** An encoder does not parse hostile input, so the
risk is not that of a parser. But a bug corrupts every response of every
client, and a competitive encoder represents years of tuning (match
finding, entropy coding; `klauspost`'s zstd encoder is several thousand
lines). An in-house codec would be slower and would compress worse than
what it replaces.

**What is worth it, on the other hand: a streaming stage around the
existing codecs.** The stdlib encoders are already streaming `io.Writer`s.
"One batch = one write + one `Flush`" matches exactly Sluice.go's transport
unit: it is a few dozen lines, not a codec. Two points need care:
- the **`Accept-Encoding` negotiation**: it is a hostile header, with `q=`
  weights, hence a small parser, table-tested and fuzzed;
- the **choice of flushing per batch or at the end of the response** (axis
  F, §8.3).

The prototype lives in `benchmarks/`, it is measured by axis F, and it is
the measurement that decides whether it becomes an API of `web`.

**The shape of the report**: an "if your use is... then..." table, for
example API behind an internal load balancer, mobile clients on a lossy
network, large exports. Each line gives the configuration, the gain over R
with its interval, the condition that would change the advice, and the
"ready for production" column.

### 8.6 Cost

With the protocol of §7 (≈ 3.9 h per configuration, capacity and points),
on one set of VMs:

- axes A to G, R included: 9 configurations on the static (A, D, G), 18 on
  the single read and on the mixed (+ B, C), 23 on the list 1,000 (+ E,
  F), that is **68 configurations** ≈ 265 h;
- network grid (§8.4), at the fixed points only (≈ 1.4 h per
  configuration): 48 ≈ 70 h;
- A/As and 30% margin (§7.5, §7.6): ≈ (265 + 70 + 15) × 1.3 ≈ **455 h,
  ~19 days** nominal; the worst case of the adaptive stop roughly doubles
  the capacity part.

The study goes **before** the comparison, since it fixes the optimised
version of Sluice.go. It also serves as a dress rehearsal of the whole
chain (h1/h2/h3 generator, `ifb`, validation under compression) on a single
contender, before running fourteen.
