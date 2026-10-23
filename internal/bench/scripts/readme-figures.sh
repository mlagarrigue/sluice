#!/usr/bin/env bash
# Rewrite the figures README.md quotes from docs/benchmarks.md, measuring
# nothing. Run it after consolidate.sh has regenerated the page:
#
#   internal/bench/scripts/readme-figures.sh
#
# Each quoted figure sits between two inline markers that name its row,
#
#   <!-- figure: BenchmarkWebDBSluiceOneConn -->898 µs<!-- /figure -->
#
# and becomes that row's median, in the unit and the three significant digits
# the README writes. The README used to copy them by hand, and a test caught
# them one run behind the page within an hour of the page being created;
# TestREADMEFiguresMatchTheConsolidatedMeasurements now reads the same markers
# and asks for the exact text this writes. A marker naming a row the page does
# not hold fails the run and leaves the README as it was.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
PAGE="$ROOT/docs/benchmarks.md"
README="$ROOT/README.md"

tmp="$(mktemp)"
trap 'rm -f "$tmp" "$tmp.medians"' EXIT
sed -n 's/^| `\(Benchmark[A-Za-z0-9_]*\)` | \([0-9]*\) ns |.*/\1 \2/p' "$PAGE" >"$tmp.medians"

# show is the README's spelling of a duration: ns, µs or ms, whichever keeps
# the number under 1000, to three significant digits.
awk -v medians="$tmp.medians" '
  BEGIN { while ((getline l < medians) > 0) { split(l, f, " "); m[f[1]] = f[2] } }
  function show(ns,   v, u) {
    v = ns; u = "ns"
    if (v >= 999.5) { v /= 1000; u = "µs" }
    if (v >= 999.5) { v /= 1000; u = "ms" }
    if (v >= 99.95) return sprintf("%.0f %s", v, u)
    if (v >= 9.995) return sprintf("%.1f %s", v, u)
    return sprintf("%.2f %s", v, u)
  }
  {
    out = ""
    while (match($0, /<!-- figure: [A-Za-z0-9_]+ -->[^<]*<!-- \/figure -->/)) {
      seg = substr($0, RSTART, RLENGTH)
      name = seg; sub(/^<!-- figure: /, "", name); sub(/ -->.*/, "", name)
      if (!(name in m)) {
        print "readme-figures: README.md:" NR " quotes " name ", which no table of docs/benchmarks.md holds" > "/dev/stderr"
        bad = 1
      }
      out = out substr($0, 1, RSTART - 1) "<!-- figure: " name " -->" show(m[name]) "<!-- /figure -->"
      $0 = substr($0, RSTART + RLENGTH)
    }
    print out $0
  }
  END { exit bad }' "$README" >"$tmp"
cat "$tmp" >"$README" # in place: the README keeps its own mode
