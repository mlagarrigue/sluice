#!/usr/bin/env bash
# Re-run every published figure in one session, on one machine, and write the
# result with the conditions attached.
#
#   internal/bench/scripts/consolidate.sh > /tmp/b.md && mv /tmp/b.md docs/benchmarks.md
#   internal/bench/scripts/readme-figures.sh   # the README's quoted figures, from the page
#
# It rewrites only the tables, each between two markers that name it, and
# prints the rest of docs/benchmarks.md as it found it:
#
#   <!-- consolidate: connector -->
#   | benchmark | median | ...
#   <!-- /consolidate -->
#
# The first version printed the whole page from a head file kept beside this
# script. The page was then edited where it is read and the head file was
# not, so the next run would have erased three sections measured by hand and
# reverted two of the method's paragraphs.
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
# then `go run ./internal/bench/cmd/pgfixtures`.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
BENCHTIME="${BENCHTIME:-1s}"
COUNT="${COUNT:-3}"
CORE="${CORE:-$(($(nproc) - 1))}"
PAGE="$ROOT/docs/benchmarks.md"
BLOCKS="conditions connector codec web socket gateway-db partitioning statement-cache verdicts"

# The page must hold each block once, closed before the next one opens, and
# no block this script does not produce. Checked here before anything is
# measured, so a lost marker costs a second rather than a run, and on every
# commit by TestBenchmarksPageHoldsEveryConsolidatedBlock, which reads
# BLOCKS from this file.
markers() {
  local status=0 name n
  for name in $BLOCKS; do
    n="$(grep -c "^<!-- consolidate: $name -->\$" "$PAGE" || true)"
    if [ "$n" != 1 ]; then
      echo "consolidate: docs/benchmarks.md has $n '<!-- consolidate: $name -->' markers, not one" >&2
      status=1
    fi
  done
  while IFS= read -r name; do
    case " $BLOCKS " in
      *" $name "*) ;;
      *)
        echo "consolidate: docs/benchmarks.md names a block this script does not produce: $name" >&2
        status=1
        ;;
    esac
  done < <(sed -n 's/^<!-- consolidate: \(.*\) -->$/\1/p' "$PAGE")
  awk '
    /^<!-- consolidate: / { if (open) { print "consolidate: line " NR " opens a block inside another"; bad = 1 } open = 1 }
    /^<!-- \/consolidate -->$/ { if (!open) { print "consolidate: line " NR " closes no block"; bad = 1 } open = 0 }
    END { if (open) { print "consolidate: the last block is never closed"; bad = 1 } exit bad }' "$PAGE" >&2 || status=1
  return $status
}
markers

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

# Each block is produced into a file of its own, then written into the page
# by the fill below; the prose around the tables belongs to the page.
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

table() { # table <name> <module> <packages> <regex> [pinned]
  {
    echo '| benchmark | median | spread | B/op | allocs/op |'
    echo '|---|---|---|---|---|'
    section "$@"
  } >"$OUT/$1"
}

cat >"$OUT/conditions" <<CONDITIONS
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
CONDITIONS

table connector benchmarks . 'BenchmarkPGPoint|BenchmarkPGAny|BenchmarkPGWide|BenchmarkPGCopy'
table codec benchmarks . 'BenchmarkPGEncodeInt8Array|BenchmarkPGDecodeInt8Array'
table web benchmarks . 'BenchmarkWebNetHTTP$|BenchmarkWebSluice$|BenchmarkWebGin$|BenchmarkWebEcho$|BenchmarkWebChi$'
table socket benchmarks . 'BenchmarkSocket' free
table gateway-db benchmarks . 'BenchmarkWebDB' free
table partitioning . ./gateway/ 'BenchmarkPartition'
table statement-cache . './database/postgres/ ./internal/bench/' 'BenchmarkStmtCache|BenchmarkPGPrepare'

# versus.sh ends on "A vs B  x -> y  <verdict>  wins=w/n (need k)"; the page
# reads it as a table, one pair per row.
{
  echo '| pair | first | second | verdict | sign test |'
  echo '|---|---|---|---|---|'
  for pair in \
    "BenchmarkSocketNetHTTP BenchmarkSocketSluice" \
    "BenchmarkSocketGin BenchmarkSocketSluice" \
    "BenchmarkSocketNetHTTP BenchmarkSocketFiberFasthttp"; do
    "$ROOT/internal/bench/scripts/versus.sh" $pair 13 2>/dev/null | tail -1
  done | awk '{
    v = ""
    for (i = 7; i <= NF && $i !~ /^wins=/; i++) v = v (v == "" ? "" : " ") $i
    w = $i; sub(/^wins=/, "", w); k = $(i + 2); sub(/\)$/, "", k)
    printf "| `%s` vs `%s` | %.0f ns | %.0f ns | %s | %s wins, %s needed |\n", $1, $3, $4, $6, v, w, k
  }'
} >"$OUT/verdicts"

# The fill: the page with what lies between each pair of markers replaced by
# the block of that name, and every other line as it was.
awk -v dir="$OUT" '
  /^<!-- consolidate: [a-z-]+ -->$/ {
    print
    file = dir "/" $3
    while ((getline line < file) > 0) print line
    close(file)
    skip = 1
    next
  }
  /^<!-- \/consolidate -->$/ { skip = 0 }
  !skip { print }' "$PAGE"
