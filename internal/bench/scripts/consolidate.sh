#!/usr/bin/env bash
# Re-run every published figure in one session, on one machine, and write the
# result with the conditions attached.
#
#   internal/bench/scripts/consolidate.sh > docs/benchmarks.md
#
# This exists because the repository's own rule says absolute figures do not
# transfer across sessions or machines (docs/benchmarks.md, "Measuring on a
# laptop"), and the README had been assembled from a dozen separate runs over
# several hours. Every number in it was honestly obtained and no two of them
# were comparable.
#
# What this cannot fix is that a laptop under WSL2 is not a measurement rig.
# It fixes the part that is fixable: one session, one clock, one set of
# conditions, all of it stated.
#
# Requires a server. `eval "$(internal/bench/scripts/pgdev.sh start)"` first,
# then `internal/bench/scripts/pgdev.sh fixtures`.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BENCHTIME="${BENCHTIME:-1s}"
COUNT="${COUNT:-3}"
CORE="${CORE:-$(($(nproc) - 1))}"

# A loaded machine produces a table that is uniformly wrong rather than
# obviously wrong. One run of this script at load 3.3 came out 7-28% above the
# same code at load 0.4 — every row shifted, nothing flagged, and only a
# cross-check against the previous file caught it.
LOAD="$(awk '{print int($1)}' /proc/loadavg 2>/dev/null || echo 0)"
if [ "$LOAD" -ge 2 ]; then
  echo "consolidate: load average is $(awk '{print $1}' /proc/loadavg), which shifts every row." >&2
  echo "  Wait for the machine to settle, or set ALLOW_LOADED=1 to measure anyway." >&2
  [ -n "${ALLOW_LOADED:-}" ] || exit 1
fi

case "$BENCHTIME" in
  *ms)
    echo "consolidate: BENCHTIME=$BENCHTIME is too short for the network-bound rows." >&2
    echo "  A point read measured 127 µs and 318 µs in two runs of the same code at 300ms." >&2
    echo "  Use 1s or more, or set ALLOW_SHORT_BENCHTIME=1 to override." >&2
    [ -n "${ALLOW_SHORT_BENCHTIME:-}" ] || exit 1
    ;;
esac

if [ -z "${SLUICE_PG:-}" ]; then
  echo "SLUICE_PG is not set; run: eval \"\$(internal/bench/scripts/pgdev.sh start)\"" >&2
  exit 1
fi

# summarise turns `go test -bench` output into a table row per benchmark,
# reporting the median of the runs rather than the last one — the difference
# between a figure and whichever number came out of the machine most recently.
#
# Written in POSIX awk: this box has mawk, which has no multidimensional
# arrays, so the index is packed with SUBSEP the way paired.awk does it.
summarise() {
  awk '
    /ns\/op/ {
      name = $1; sub(/-[0-9]+$/, "", name)
      for (i = 1; i <= NF; i++) {
        if ($i == "ns/op")     { n[name]++; v[name SUBSEP n[name]] = $(i-1) }
        if ($i == "B/op")      b[name] = $(i-1)
        if ($i == "allocs/op") a[name] = $(i-1)
      }
      seen[name] = 1
    }
    END {
      for (nm in seen) {
        c = n[nm]
        for (i = 1; i <= c; i++) for (j = i+1; j <= c; j++)
          if (v[nm SUBSEP i] + 0 > v[nm SUBSEP j] + 0) {
            t = v[nm SUBSEP i]; v[nm SUBSEP i] = v[nm SUBSEP j]; v[nm SUBSEP j] = t
          }
        if (c % 2) med = v[nm SUBSEP int((c+1)/2)] + 0
        else       med = (v[nm SUBSEP int(c/2)] + v[nm SUBSEP int(c/2+1)]) / 2
        # The spread between the fastest and slowest run, which is the column a
        # reader needs before believing the median to three digits. A point
        # read measured 127 µs and 318 µs in two runs of the same code at
        # benchtime=300ms — a figure with no spread beside it invited both.
        lo = v[nm SUBSEP 1] + 0; hi = v[nm SUBSEP c] + 0
        spread = (med > 0) ? 100 * (hi - lo) / med : 0
        flag = (spread > 15) ? " **" : ""
        printf "| `%s` | %.0f ns | ±%.0f%%%s | %s B | %s |\n", nm, med, spread, flag, b[nm], a[nm]
      }
    }' | sort
}

# bench runs one group. The package pattern is explicit rather than "." because
# the root module's benchmarks live in gateway/ and database/postgres/, and a "." that
# quietly matched nothing produced two empty tables in the first run of this
# script — a report with a heading and no rows, which reads as "measured, found
# nothing" rather than "never ran".
bench() { # bench <module> <packages> <regex> [pinned]
  local module="$1" packages="$2" regex="$3" pinned="${4:-pin}"
  if [ "$pinned" = "pin" ]; then
    GOMAXPROCS=1 taskset -c "$CORE" go -C "$ROOT/$module" test -run='^$' \
      -bench="$regex" -benchmem -benchtime="$BENCHTIME" -count="$COUNT" $packages 2>/dev/null
  else
    go -C "$ROOT/$module" test -run='^$' -bench="$regex" -benchmem \
      -benchtime="$BENCHTIME" -count="$COUNT" $packages 2>/dev/null
  fi
}

# section runs one group and refuses to print an empty table, because a heading
# with no rows under it reads as "measured, found nothing" rather than "never
# ran" — which is how two sections of the first version of this file came out
# blank and nobody noticed.
#
# The command is run here rather than piped into a checker: `exit` inside a
# pipeline runs in a subshell and cannot stop the script, so the earlier
# spelling could only fail by way of `set -e` swallowing its own message.
section() { # section <name> <module> <packages> <regex> [pinned]
  local name="$1"
  shift
  local rows
  rows="$(bench "$@" | summarise)"
  if [ -z "$rows" ]; then
    echo "consolidate: section '$name' produced no rows; the benchmark pattern matched nothing" >&2
    exit 1
  fi
  printf '%s\n' "$rows"
}

# The hand-written head of the page — method, ceiling, how to read the
# tables — lives beside this script so that regenerating the figures never
# erases it.
cat "$ROOT/internal/bench/scripts/benchmarks-head.md"

cat <<HEADER
## Measured in one session

Produced by \`internal/bench/scripts/consolidate.sh\` in one session, on one
machine. That is the point of this file: the repository's rule is that absolute
figures do not transfer across sessions, and the tables it replaced had been
assembled from a dozen runs over several hours — each honest, none comparable
to the next.

## Conditions

| | |
|---|---|
| Date | $(date -u '+%Y-%m-%d %H:%M UTC') |
| Go | $(go version | awk '{print $3}') |
| CPU | $(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null || echo unknown) |
| Cores | $(nproc) |
| Kernel | $(uname -sr) |
| PostgreSQL | $(psql --version 2>/dev/null | awk '{print $3}' || echo n/a) |
| Server | $SLUICE_PG (loopback) |
| benchtime | $BENCHTIME × $COUNT, median reported, spread = (max-min)/median |
| Pinned | core $CORE, GOMAXPROCS=1, except where concurrency is the point |
HEADER

# Quoted, because everything below is prose with markdown code spans in it and
# an unquoted heredoc runs those as commands. The first version of this script
# did exactly that: `BENCHTIME` in the text executed a command of that name,
# left an empty string behind, and killed the run after the first table.
cat <<'PROSE'

**This is a laptop under WSL2, not a measurement rig.** Linux neither sees nor
controls the host clock here; two sequential runs of the same binary differ by
up to 14%, and the network-bound rows are worse than that at a short benchtime.
Read ratios rather than absolutes, and prefer the paired verdicts at the end
over any single row above them.

A **spread** over 15% is flagged with `**`. Those rows should not be quoted:
the run-to-run variation is larger than most of the differences the table is
being read for. Raise `BENCHTIME` and re-run before believing them — a point
read measured 127 µs in one run and 318 µs in another at `BENCHTIME=300ms`,
and settled at 285-298 µs across five runs of 1s.

## Connector, against pgx / lib/pq / database/sql

One connection each, since this connector has no pool and a comparison that
ignores that measures pooling.

| benchmark | median | spread | B/op | allocs/op |
|---|---|---|---|---|
PROSE
section connector benchmarks . 'BenchmarkPGPoint|BenchmarkPGAny|BenchmarkPGWide|BenchmarkPGCopy'

cat <<'SECTION'

## Codec, in isolation

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section codec benchmarks . 'BenchmarkPGEncodeInt8Array|BenchmarkPGDecodeInt8Array'

cat <<'SECTION'

## Web pipeline, in process

No socket, no kernel. See the socket table below before drawing anything from
the ordering here.

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section web benchmarks . 'BenchmarkWebNetHTTP$|BenchmarkWebSluice$|BenchmarkWebGin$|BenchmarkWebEcho$|BenchmarkWebChi$'

cat <<'SECTION'

## Web pipeline, over a socket

Not pinned: a socket benchmark needs a client and a server at once, and pinning
both to one core measures their contention.

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section socket benchmarks . 'BenchmarkSocket' free

cat <<'SECTION'

## The gateway, with a database behind it

Sixty-four concurrent requests, each needing one row. Not pinned, for the same
reason.

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section gateway-db benchmarks . 'BenchmarkWebDB' free

cat <<'SECTION'

## Partitioning by principal

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section partitioning . ./gateway/ 'BenchmarkPartition'

cat <<'SECTION'

## Statement cache

SECTION
echo '| benchmark | median | spread | B/op | allocs/op |'
echo '|---|---|---|---|---|'
section statement-cache . './database/postgres/ ./internal/bench/' 'BenchmarkStmtCache|BenchmarkPGPrepare'

cat <<'SECTION'

## Paired verdicts

A single run of each contender produces numbers in an order, which invites
ranking. The sign test says which of those differences survive interleaving.

```
SECTION
for pair in \
  "BenchmarkSocketNetHTTP BenchmarkSocketSluice" \
  "BenchmarkSocketGin BenchmarkSocketSluice" \
  "BenchmarkSocketNetHTTP BenchmarkSocketFiberFasthttp"; do
  "$ROOT/internal/bench/scripts/versus.sh" $pair 13 2>/dev/null | tail -1
done
cat <<'SECTION'
```
SECTION
