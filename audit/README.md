# audit — what this machine, this image and this binary were set to

This module reads settings and says what each one costs. It **changes
nothing**, ever: there is no apply, no fix, no write. Every recommendation
carries five things — the value found, the value proposed, the goal it
serves, the reason in plain words, and a link to the documentation that
reason comes from. A rule without that link does not start: a test refuses
it, because a reason nobody can check against a published document is an
opinion with a command-line flag.

It is a module of its own, on the standard library alone. It audits builds
and hosts, so it must not depend on what it audits, and it never widens
the dependency graph the root module records in its decision log.

```
go run -C audit ./cmd/audit -axes
go run -C audit ./cmd/audit -binary ./interop-endpoint
go run -C audit ./cmd/audit -module . -goals security -cve yes -sources tools
go run -C audit ./cmd/audit -dockerfile ../interop/Dockerfile
go run -C audit ./cmd/audit -binary ./x -sbom -output json
go run -C audit ./cmd/audit -container web -sources config,tools
go run -C audit ./cmd/audit -root / -kernel-log dmesg.txt -profile comparative
go run -C audit ./cmd/audit -pg-settings settings.csv -pg-hba pg_hba.conf -pg-log postgresql.log -answer pools=60
go run -C audit ./cmd/audit -pg-settings settings.csv -pg-roles roles.csv -firewall ruleset.txt -cve yes
go run -C audit ./cmd/audit -proxy-config nginx.conf -proxy-version "$(nginx -v 2>&1)"
go run -C audit ./cmd/audit -app-args start.txt -pg-settings settings.csv -profile comparative
go run -C audit ./cmd/audit -root / -netstat-before before.txt -netstat-after after.txt -sources logs,config
go run -C audit ./cmd/audit -root / -stat-before stat0.txt -stat-after stat1.txt -imds imds.json -profile comparative
```

## Axes, not modes

Every parameter is an axis, and axes are independent and multi-valued: a
run names the components it cares about, the goals it pursues, the profile
it reports under, whether advisories are judged, the sources it may read
and the output it wants. **Any combination is valid**, and an axis left
out means all of its values. "Security only, no advisories, on this
binary" is a selection like any other — not a mode, not a branch in the
program.

A rule declares the axis values it needs and runs only when the selection
covers them. So a selection that does not allow the `tools` source calls
no external tool, and one that asks for `performance` alone reports no
security finding. An axis named with no value, and a value the axes do not
know, are both refused: those are the two ways to ask for nothing by
accident, and a report that found nothing because it ran nothing is the
worst one there is.

Two questions have no answer in anything readable — what the machine does,
and whether it is reachable from the internet. Each has a default, so a
run in CI answers nothing and still gets a report; `-answer role=build`
sets one.

## What it reads today

| family | source | what it reads |
|---|---|---|
| `gobuild/*` | the binary | what the build fixed: `-trimpath`, `CGO_ENABLED`, `GOAMD64`, the profile, the toolchain, the revision stamped |
| `deps/*` | the module, and `govulncheck` | the requirements, their sums, and every advisory with its verdict |
| `docker/*` | the Dockerfile's text | stages, the bases and their digests, the runtime image, `USER`, secrets in layers, `.dockerignore` |
| `linux/*` | `/proc` and `/sys` under `-root` | the listen and device queues, the UDP buffers, the ephemeral ports, connection tracking, swap, hugepages, the CPU governor, and the process's own `nofile` under `-pid` |
| `container/*` | `docker inspect`, or its saved output | what a container was run with: `nofile`, pinned cores, a memory bound, the network, the log driver, capabilities, `no-new-privileges`, seccomp, AppArmor, a read-only root, the user, the Docker socket |
| `linux/listen-overflows` | two copies of `/proc/net/netstat`, or of `nstat -az`, taken before and after a load | connections the kernel dropped because an accept queue was full, put on the queue and its `somaxconn` — and nothing put on it for the retransmissions and timeouts a load also moves |
| `linux/reconnections` | the same two readings, and `-answer requests=N`, the requests the step served | fewer than 32 requests per new connection, put on what closes them: the server's `-max-requests-per-conn`, the proxy's keep-alive, else the clients |
| `postgres/connections-saturated`, `postgres/idle-in-transaction` | `pg_stat_activity` saved as CSV during a load, several readings appended, with `pg_settings` | every client slot taken at the peak; sessions idle in a transaction holding their connections |
| `host/*` | two copies of `/proc/stat`; `/sys/devices/system/cpu` under `-root`; Azure's instance metadata, saved | CPU time stolen by the hypervisor during a load, SMT on a machine of a comparison; the provider, the size, the region, the zone, the CPUs, cores and sockets in the system table |
| `linux/conntrack-full`, `linux/oom-kill` | the kernel's log, saved to a file | a connection tracking table that filled, a process the OOM killer took |
| `postgres/*` | `pg_settings` saved as CSV, `pg_hba.conf`, the server's log | connections against the clients' pools, `shared_buffers`, autovacuum, `password_encryption`, `ssl`, `trust`, `md5` and `password` rules; checkpoints forced by the WAL, clients refused for lack of slots |
| `postgres/superusers`, `postgres/cve` | `pg_roles` saved as CSV; `server_version` and `pg_hba.conf` | more than one superuser that logs in; each PostgreSQL advisory the audit knows, judged on the release and on the configuration it needs |
| `proxy/*` | `nginx.conf` or `haproxy.cfg`, saved; the release `nginx -v` printed | connections to the application not kept alive, nginx buffering a streamed response, HAProxy waits left infinite, TLS older than 1.2, `X-Forwarded-For` not set; nginx advisories judged on the release and the directive that reaches them |
| `app/*` | how the comparison's Sluice.go server was started, saved: its `NAME=value` environment and its command line | a connection closed every few requests, a remote database reached in clear and unauthenticated, more connections than the database's slots, TLS off on the internet, session resumption in a comparison; the server's flags in the system table |
| `firewall/postgres-exposed` | `nft list ruleset` or `iptables-save`, saved | PostgreSQL's port reachable from any address: accepted by an input chain — the chains it jumps to followed, ufw's and firewalld's — or by its policy, or published by a DNAT (Docker's `-p`, which never crosses INPUT); "undetermined" when a jump cannot be followed |
| `go/*` | the cgroup, the module's `go` directive, the environment | whether the runtime will read the CPU quota, or use every core of the machine; a `GODEBUG` that restores an older behaviour |
| `-sbom` | the binary | the bill of materials: the main module, the toolchain, every dependency linked |

**The build is read in the binary, not in the files that built it.** A
Dockerfile says what it meant to do; `debug/buildinfo` says what was done.
The gap between the two is exactly what an audit exists to find.

**Advisories are read by calling `govulncheck`, never by reimplementing
it.** The database, the call-graph analysis and the advisory format are the
Go team's and move daily; a second implementation here would be a second
thing to be wrong, and wrong quietly. Each advisory gets one of three
verdicts and no other — **applies** when a call path to the vulnerable
function was proved, **undetermined** when the package is linked but no
path was, **does not apply** when the module is only required — and each
verdict says what decided it. A machine without the tool produces a
finding that says so, because a report silent about vulnerabilities reads
as a report that found none.

**With no network, from a copy.** `-vulndb <directory>` names a copy of
the Go vulnerability database — `vuln.go.dev/vulndb.zip`, unzipped — and
`govulncheck` reads it through `file://`: the verdicts then need no
network, and the system table gives the copy and the date its
`index/db.json` records, so a reader knows how old the judgment is. The
audit never fetches or refreshes the copy.

**PostgreSQL's advisories are judged on the configuration, not on the
release alone.** A flaw present in a release is not a flaw reachable: each
advisory the audit knows is read from its page on postgresql.org, its
affected configurations included, and is judged on what was saved —
**applies** when the release is affected and the configuration reaches the
flaw, **does not apply** when nothing in it does (the upgrade stays due),
**undetermined** when the part it needs was not given. A fixed release
says nothing. Today the list holds one, CVE-2021-23214, reached through
`cert` authentication or `trust` with `clientcert`. nginx's are judged the
same way, on the release `-proxy-version` gives and the directive whose
module the flaw is in: CVE-2024-7347 needs `mp4`.

**The firewall is asked one question, the kernel's way.** Would a new
connection from anywhere reach the server's port: the first rule that
decides, chain by chain, the jumps into chains of your own followed —
ufw and firewalld keep every acceptance there, under a drop policy — then
the chain's policy. A rule matching on a connection state or a mark is
taken as not deciding. A jump the audit cannot follow — a chain the
ruleset does not define, a loop, a condition it does not read in front of a
chain that would accept — makes the answer *undetermined*, never a silence
that would read as a closed port. A server whose `listen_addresses` is the
loopback alone is not asked about.

Every reason says where the value shows up under load, because that is
what makes it actionable: a listen queue too short is `ListenOverflows` in
`nstat`, not an abstraction. And a knob the kernel does not have is not a
knob set to zero — a host that tracks no connection has no
`nf_conntrack_max`, and the rule stays silent rather than filling the
report with settings nobody set.

The host is read under `-root`, which is `/` on the machine itself and a
seeded directory in the tests. That is deliberate: the settings worth
reporting are the ones nobody would apply to their own machine to see the
report, so they are tried against a root written for the purpose.

**A container is read through `docker inspect`, never through the daemon's
socket.** The client follows `DOCKER_HOST`, contexts, a rootless daemon and
TLS; a second client here would be a second thing to get wrong. It is a
tool, so `-container` needs the `tools` source; the same record saved to a
file (`docker inspect web > web.json`) is read by `-container-json` with no
tool at all. The kernel's log is read from a file too — reading the ring
buffer takes a privilege the audit must not need.

**A PostgreSQL server is read from what it printed, never by connecting.**
The audit cannot borrow the connector it would be auditing, so the
settings come from `psql -X --csv -c 'SELECT name, setting, unit FROM
pg_settings' > settings.csv`, with `pg_hba.conf` and the log as they are.
What the clients' pools open together is read nowhere: `-answer pools=N`
gives it, and until then the rule that compares it with `max_connections`
stays silent.

**The comparative profile publishes a table.** A comparison does not ask
whether a value is good but whether every machine has the same one, so
under `-profile comparative` the report opens with the values read — the
kernel's queues, ports, buffers and huge pages, the process's `nofile`,
the PostgreSQL server's settings, the container's limits, network and log driver — and in JSON they are the
`system` part next to `findings`. Two machines of a comparison must print
the same table.

## In CI: two reports, and a gate

A report in JSON (`-output json`) is kept; the next run names it with
`-compare` and says what **appeared**, what **changed** — the same finding
with another value, `somaxconn = 128` become `512` — what was
**resolved**, and which lines of the system table moved. A finding is the
same from one report to the next when its rule, its documentation and its
text match with the numbers set aside; an advisory's identifier is kept
whole, so two CVEs of one rule stay two findings.

`-fail-on` turns the run into a gate: `new` trips on any finding that
appeared, `cve` on an advisory that appeared with the verdict *applies* —
or whose verdict became *applies* — and `security` on a new finding of
that goal. The run then exits with **2**, one line per cause, where a run
that could not read what it was given exits with 1. Without `-compare`,
every finding of the report counts as new.

```
go run -C audit ./cmd/audit -module . -cve yes -sources tools -output json > before.json
go run -C audit ./cmd/audit -module . -cve yes -sources tools -compare before.json -fail-on cve
```

## Fixes, written and never applied

`-propose <directory>` writes the fixes whose value is sure without
asking: `99-sluice-audit.conf` for the kernel's bounds, a `sysctl.d` file,
and `sluice-audit.sql` for PostgreSQL's settings, `ALTER SYSTEM`
statements that every installation accepts whichever file its
configuration lives in. Each file starts with the command that applies it,
each line with the rule that asked for it and its documentation. The audit
runs neither command, and does not write over a file that is there. A fix
that needs a judgment — a certificate for `ssl`, the machine's memory for
`shared_buffers` — is not written; the finding says what to decide. Once a
file is applied, the next report no longer has the finding, which is how a
fix is checked.

## What it does not do

It proposes; a person decides. A finding is not a failure: the interop
image runs as root on a simulator's base image on purpose, and the rules
that say so are right to say it — the answer is the deployer's.
