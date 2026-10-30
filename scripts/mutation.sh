#!/usr/bin/env bash
# Mutation testing: the one way gremlins runs in this repository, locally, in
# CI (the Mutation workflow) and in reviews. A test checks that nothing else
# calls gremlins (TestHeavyTestsRunThroughTheirScripts).
#
#   mutation.sh --out DIR [--shard K/N] [--wait MIN] [--dry-run] [PATH...]
#   mutation.sh --list [--shard K/N] [PATH...]
#   mutation.sh --merge --out DIR SHARD_DIR...
#   mutation.sh --print-version
#
# PATH is a package directory or a .go file; the default is every package of
# the main module (nested modules have their own go.mod and are not mutated).
#
# What the script holds, so that no caller has to:
#   - one configuration, mutation.yaml beside this script, passed with --config;
#   - one production file at a time: gremlins times the coverage run of what
#     it is given and derives every mutant's timeout from it, so a run over
#     the whole tree gives each mutant three times the whole suite, long
#     enough for a mutant that loops while allocating to fill the memory;
#   - a worktree of HEAD, so the working tree is never mutated (the code is
#     HEAD's; this script and its configuration are read where they are);
#   - an address-space cap on every process (sluice.memory-cap-kib), and as
#     many workers as the memory left under the limit can hold at that cap,
#     never more than unleash.workers;
#   - running alone: before each file, no compile, link, *.test, go
#     test/build/vet or other gremlins may run outside this script's own
#     processes. At the start the one-minute load must also be under a
#     quarter of the processors; --wait MIN waits up to MIN minutes for both,
#     otherwise the script refuses. Between files it waits as long as it
#     takes: it yields to other work instead of competing with it;
#   - one heavy run at a time (a lock shared with fuzzing), and a cache: a
#     file's result is reused while its package, the in-module packages its
#     tests build, go.mod, go.sum, the configuration, the Go and gremlins
#     versions are unchanged.
#     MUTATION_CACHE names the directory (default ~/.cache/sluice-mutation).
#
# DIR receives units/<file>.json and .log per file, summary.tsv (one line per
# file: killed, lived, timed out, not covered, efficacy, cached) and
# resultat.json (every file's mutants, merged). Exit status 1 if gremlins
# failed on a file (its coverage run failed, usually a red test).
set -euo pipefail

GREMLINS_VERSION=v0.5.1
CACHE_FORMAT=1

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
heavy_name=mutation
# shellcheck source=alone.sh
. "$here/alone.sh"
die() { heavy_die "$@"; }
say() { heavy_say "$@"; }

out="" shard="" wait_min=0 dry=() list=0 merge=0 paths=()
while [ $# -gt 0 ]; do
  case "$1" in
    --out) out=${2:?--out DIR}; shift ;;
    --shard) shard=${2:?--shard K/N}; shift ;;
    --wait) wait_min=${2:?--wait MIN}; shift ;;
    --dry-run) dry=(--dry-run) ;;
    --list) list=1 ;;
    --merge) merge=1 ;;
    --print-version) echo "$GREMLINS_VERSION"; exit 0 ;;
    -h | --help) sed -n '2,38p' "$0"; exit 0 ;;
    -*) die "unknown option $1" ;;
    *) paths+=("$1") ;;
  esac
  shift
done

root=$(git rev-parse --show-toplevel)
cfg=$here/mutation.yaml
[ -f "$cfg" ] || die "$cfg is missing"

# --- Configuration ------------------------------------------------------------
max_workers=$(yaml_value "$cfg" workers)
cap_kib=$(yaml_value "$cfg" memory-cap-kib)
margin_mib=$(yaml_value "$cfg" memory-margin-mib)
[[ $max_workers =~ ^[0-9]+$ && $cap_kib =~ ^[0-9]+$ && $margin_mib =~ ^[0-9]+$ ]] ||
  die "mutation.yaml: unleash.workers, sluice.memory-cap-kib and sluice.memory-margin-mib must be integers"
skip_re=$(awk '
  /^  skip:/ { on = 1; next }
  on && /^    - / { sub(/^    - /, ""); gsub(/"/, ""); r = r (r ? "|" : "") $0; next }
  on { on = 0 }
  END { print r }' "$cfg")

# --- Merge: shard outputs into one report -------------------------------------
summarize() { # $1 = output directory with units/
  local d=$1 j f
  printf 'file\tkilled\tlived\ttimed_out\tnot_covered\tefficacy\tcached\n' >"$d/summary.tsv"
  for j in "$d"/units/*.json; do
    [ -f "$j" ] || continue
    f=${j##*/}
    f=${f%.json}
    jq -r --arg file "${f//__//}" --arg cached "$(cat "${j%.json}.cached" 2>/dev/null || echo no)" '
      [.files[]?.mutations[]?.status] as $s
      | ($s | map(select(. == "KILLED")) | length) as $k
      | ($s | map(select(. == "LIVED")) | length) as $l
      | [ $file, $k, $l,
          ($s | map(select(. == "TIMED OUT")) | length),
          ($s | map(select(. == "NOT COVERED")) | length),
          (if $k + $l > 0 then ($k * 1000 / ($k + $l) | floor) / 10 else "-" end),
          $cached ] | @tsv' "$j" >>"$d/summary.tsv"
  done
  jq -s '{ go_module: (.[0].go_module // ""),
           files: (map(.files // []) | add),
           mutants_killed: (map(.files[]?.mutations[]? | select(.status == "KILLED")) | length),
           mutants_lived: (map(.files[]?.mutations[]? | select(.status == "LIVED")) | length),
           mutants_timed_out: (map(.files[]?.mutations[]? | select(.status == "TIMED OUT")) | length),
           mutants_not_covered: (map(.files[]?.mutations[]? | select(.status == "NOT COVERED")) | length) }
         | .test_efficacy = (if .mutants_killed + .mutants_lived > 0
                             then .mutants_killed * 100 / (.mutants_killed + .mutants_lived) else null end)' \
    "$d"/units/*.json >"$d/resultat.json" 2>/dev/null || echo '{}' >"$d/resultat.json"
  awk -F'\t' 'NR > 1 { k += $2; l += $3; t += $4; n++ }
    END { printf "mutation: %d files, %d killed, %d lived, %d timed out, efficacy %s\n", n, k, l, t, (k + l ? sprintf("%.1f%%", 100 * k / (k + l)) : "-") }' \
    "$d/summary.tsv" >&2
}

if [ "$merge" = 1 ]; then
  [ -n "$out" ] || die "--merge needs --out DIR"
  mkdir -p "$out/units"
  for d in "${paths[@]}"; do
    [ -d "$d/units" ] && cp "$d"/units/* "$out/units/" 2>/dev/null || true
  done
  summarize "$out"
  exit 0
fi

# --- Units: production files of the main module -------------------------------
pkgs=() files=()
for p in "${paths[@]}"; do
  p=${p#./}
  if [[ $p == *.go ]]; then files+=("$p"); pkgs+=("./$(dirname "$p")"); else pkgs+=("./${p%/}"); fi
done
[ ${#pkgs[@]} -gt 0 ] || pkgs=(./...)

units() {
  (cd "$root" && go list -f '{{.Dir}}{{range .GoFiles}} {{.}}{{end}}' "${pkgs[@]}") |
    while read -r dir names; do
      rel=${dir#"$root"}
      rel=${rel#/}
      for n in $names; do
        f=${rel:+$rel/}$n
        [ -n "$skip_re" ] && [[ $f =~ $skip_re ]] && continue
        head -5 "$root/$f" | grep -qE '^// Code generated .* DO NOT EDIT\.$' && continue
        printf '%s\n' "$f"
      done
    done | sort -u |
    if [ ${#files[@]} -gt 0 ]; then grep -xF -f <(printf '%s\n' "${files[@]}"); else cat; fi |
    if [ -n "$shard" ]; then
      # A file keeps its shard while the list grows (a hash of its path, not
      # its rank), so CI's per-shard cache still holds its result.
      awk -v s="$shard" '
        BEGIN { split(s, a, "/"); for (i = 32; i < 127; i++) ord[sprintf("%c", i)] = i }
        { h = 0; for (i = 1; i <= length($0); i++) h = (h * 31 + ord[substr($0, i, 1)]) % 1000003
          if (h % a[2] == a[1]) print }'
    else cat; fi
}

if [ "$list" = 1 ]; then units; exit 0; fi
[ -n "$out" ] || die "--out DIR is required"

# --- The pinned gremlins ------------------------------------------------------
command -v gremlins >/dev/null ||
  die "gremlins is not installed: go install github.com/go-gremlins/gremlins/cmd/gremlins@$GREMLINS_VERSION"
have=$(go version -m "$(command -v gremlins)" | awk '$1 == "mod" && $2 == "github.com/go-gremlins/gremlins" { print $3 }')
[ "$have" = "$GREMLINS_VERSION" ] ||
  die "gremlins ${have:-of unknown version} is installed, $GREMLINS_VERSION is required (go install github.com/go-gremlins/gremlins/cmd/gremlins@$GREMLINS_VERSION)"

# --- Alone, one heavy run at a time, workers from the memory left --------------
cache=${MUTATION_CACHE:-$HOME/.cache/sluice-mutation}
mkdir -p "$cache" "$out/units"
out=$(cd "$out" && pwd)
heavy_lock
heavy_start "$wait_min"
workers=$(heavy_workers "$cap_kib" "$max_workers" "$margin_mib")

# --- A worktree of HEAD -------------------------------------------------------
wt=${MUTATION_WORKTREE:-${TMPDIR:-/tmp}/sluice-mutation-wt}
git -C "$root" worktree remove --force "$wt" 2>/dev/null || rm -rf "$wt"
git -C "$root" worktree prune
git -C "$root" worktree add --quiet --detach "$wt" HEAD
trap 'git -C "$root" worktree remove --force "$wt" 2>/dev/null || true' EXIT
head_sha=$(git -C "$wt" rev-parse --short HEAD)

mapfile -t todo < <(units)
say "${#todo[@]} files at $head_sha, $workers worker(s), cap ${cap_kib} KiB a process, cache $cache"
{
  printf 'head\t%s\nworkers\t%s\nmemory-cap-kib\t%s\nshard\t%s\nstarted\t%s\n' \
    "$head_sha" "$workers" "$cap_kib" "${shard:-all}" "$(date -u +%FT%TZ)"
} >"$out/run.tsv"

# --- Cache key ----------------------------------------------------------------
pkgarg() { if [ "$1" = . ]; then echo .; else echo "./$1"; fi; }
declare -A closure=()
closure_hash() { # $1 = package directory, "." for the root
  local d=$1 dirs g
  if [ -z "${closure[$d]:-}" ]; then
    dirs=$(cd "$wt" && go list -deps -test -f '{{if .Module}}{{if .Module.Main}}{{.Dir}}{{end}}{{end}}' "$(pkgarg "$d")" | sort -u)
    closure[$d]=$(
      {
        printf '%s\n' "$CACHE_FORMAT" "$GREMLINS_VERSION" "$(go env GOVERSION)"
        cat "$cfg" "$wt/go.mod" "$wt/go.sum"
        for g in $dirs; do
          for g in "$g"/*.go; do [ -f "$g" ] && printf '%s\n' "${g#"$wt"/}" && cat "$g"; done
        done
      } | sha256sum | cut -d' ' -f1
    )
  fi
  printf '%s' "${closure[$d]}"
}

# --- One file at a time -------------------------------------------------------
failed=0 i=0
for f in "${todo[@]}"; do
  i=$((i + 1))
  d=$(dirname "$f")
  name=${f//\//__}
  json=$out/units/$name.json
  log=$out/units/$name.log
  key=$(printf '%s\n%s' "$(closure_hash "$d")" "$f" | sha256sum | cut -d' ' -f1)
  if [ ${#dry[@]} = 0 ] && [ -f "$cache/$key.json" ]; then
    cp "$cache/$key.json" "$json"
    echo yes >"$out/units/$name.cached"
    continue
  fi
  heavy_stay
  # Only this file: every file of a subdirectory, and every other file of the
  # package, by their paths from the package directory (what gremlins walks).
  excl=(-E /)
  others=$(cd "$wt/$d" && find . -maxdepth 1 -name '*.go' ! -name '*_test.go' ! -name "$(basename "$f")" -printf '%f\n' |
    sed 's/[.]/\\./g' | paste -sd'|')
  [ -n "$others" ] && excl+=(-E "^($others)\$")
  say "[$i/${#todo[@]}] $f"
  # -count=1: gremlins times its coverage run to set every mutant's timeout,
  # and a coverage run served from the test cache takes a tenth of a second.
  # 9>&-: the lock stays with this script, not with a test process gremlins
  # leaves behind for a moment.
  if (ulimit -v "$cap_kib" && cd "$wt" && GOFLAGS="${GOFLAGS:+$GOFLAGS }-count=1" exec gremlins unleash \
    --config "$cfg" --workers "$workers" "${dry[@]}" "${excl[@]}" -o "$json" "$(pkgarg "$d")" 9>&-) >"$log" 2>&1; then
    # A file without a mutant ends in "No results to report." and no file.
    [ -f "$json" ] || printf '{"go_module":"","files":[]}\n' >"$json"
    echo no >"$out/units/$name.cached"
    [ ${#dry[@]} = 0 ] && cp "$json" "$cache/$key.json"
  else
    failed=1
    say "gremlins failed on $f (log: $log)"
    rm -f "$json"
  fi
done
printf 'finished\t%s\n' "$(date -u +%FT%TZ)" >>"$out/run.tsv"
summarize "$out"
exit "$failed"
