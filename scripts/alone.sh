#!/usr/bin/env bash
# Sourced, not run: what the heavy test runs (mutation testing, fuzzing)
# share so that they run alone. Mutation testing run beside other builds and
# tests drove the development container to its memory limit three times in
# an afternoon (2026-10-30); fuzzing, as heavy, follows the same rules, the
# same locally and in CI.
#
#   heavy_lock          one heavy run at a time, mutation and fuzzing alike
#   heavy_start MIN     refuse unless alone; wait up to MIN minutes for it
#   heavy_stay          between two units of work, wait while others work
#   heavy_workers EST_KIB MAX MARGIN_MIB
#                       how many processes of EST_KIB the memory left holds
#   yaml_value FILE KEY the scalar of a "key: value" line
#
# The caller sets heavy_name (for messages) and runs under set -euo pipefail.

heavy_die() { printf '%s: %s\n' "${heavy_name:-heavy}" "$*" >&2; exit 2; }
heavy_say() { printf '%s: %s\n' "${heavy_name:-heavy}" "$*" >&2; }

yaml_value() { awk -v k="$2" '$1 == k ":" { print $2; exit }' "$1"; }

heavy_lock() {
  local dir=${XDG_CACHE_HOME:-$HOME/.cache}
  mkdir -p "$dir"
  exec 9>"$dir/sluice-heavy.lock"
  flock -n 9 || heavy_die "another mutation or fuzzing run holds $dir/sluice-heavy.lock"
}

# Processes that build or test, outside the caller's own process tree; a
# zombie (exited, not yet reaped) does nothing and is not counted.
heavy_foreign() {
  ps -eo pid=,ppid=,stat=,comm=,args= | awk -v me=$$ '
    { pid[NR] = $1; parent[$1] = $2; zombie[$1] = $3 ~ /^Z/; comm[$1] = $4; $1 = $2 = $3 = $4 = ""; args[pid[NR]] = $0 }
    END {
      for (i = 1; i <= NR; i++) {
        p = pid[i]; c = comm[p]
        if (zombie[p]) continue
        busy = c ~ /^(compile|link|asm|cgo|vet|gremlins)$/ || c ~ /\.test$/ ||
               (c == "go" && args[p] ~ /(^|[[:space:]])(test|build|vet|run|install)([[:space:]]|$)/)
        if (!busy) continue
        q = p; ours = 0
        while (q in parent && q > 1) { if (q == me) { ours = 1; break }; q = parent[q] }
        if (!ours) printf "%s(%s) ", c, p
      }
    }'
}

heavy_calm_load() {
  awk -v n="$(nproc)" '{ t = n / 4; if (t < 2) t = 2; exit !($1 < t) }' /proc/loadavg
}

# At the start: no foreign build or test, and a one-minute load under a
# quarter of the processors (two at least).
heavy_start() {
  local wait_min=${1:-0} waited=0 f
  while :; do
    f=$(heavy_foreign)
    if [ -z "$f" ] && heavy_calm_load; then return 0; fi
    if [ $((waited / 60)) -ge "$wait_min" ]; then
      heavy_die "not alone: ${f:-load $(cut -d' ' -f1 /proc/loadavg) on $(nproc) processors}; run it when nothing else builds or tests (--wait MIN to wait)"
    fi
    sleep 30
    waited=$((waited + 30))
  done
}

# Between units: yield to other work for as long as it lasts.
heavy_stay() {
  local f n=0
  while f=$(heavy_foreign) && [ -n "$f" ]; do
    [ $((n % 10)) = 0 ] && heavy_say "paused while other work runs: $f"
    sleep 30
    n=$((n + 1))
  done
}

# The memory limit is the container's (cgroup) when it has one, the
# machine's otherwise; what is used is anonymous memory, the page cache being
# reclaimable.
heavy_workers() {
  local est_kib=$1 max=$2 margin_mib=$3 limit used
  limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo max)
  if [[ $limit =~ ^[0-9]+$ ]]; then
    used=$(awk '$1 == "anon" { print $2 }' /sys/fs/cgroup/memory.stat)
  else
    limit=$(awk '$1 == "MemTotal:" { print $2 * 1024 }' /proc/meminfo)
    used=$(awk '$1 == "MemTotal:" { t = $2 } $1 == "MemAvailable:" { a = $2 } END { print (t - a) * 1024 }' /proc/meminfo)
  fi
  awk -v l="$limit" -v u="$used" -v m="$margin_mib" -v e="$est_kib" -v w="$max" \
    'BEGIN { n = int((l - u - m * 1048576) / (e * 1024)); if (n > w) n = w; if (n < 1) n = 1; print n }'
}
