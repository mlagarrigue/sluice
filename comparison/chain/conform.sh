#!/usr/bin/env bash
# A contender's image against R (CMP-09, comparison/README.md "Checking a
# contender against R"), from a container: the image runs on the Docker
# network where the database is, R and the generator are built from this
# tree and run beside it, and `loadgen -conformance` compares the two on
# CHECKS requests. One line, the generator's report; exit status 1 at the
# first difference, with the contender's last log lines.
#
#   comparison/chain/conform.sh node-express:standard-doc h1
#   comparison/chain/conform.sh go-nethttp:optimised-opt h3 gzip
#
# The machine running it must reach the container by name on NETWORK
# (the devcontainer, on sluice_default, does). The database is the one of
# the repository's tests (SLUICE_PG_USER, SLUICE_PG_PASSWORD, SLUICE_PG_DB),
# by its name on NETWORK (PGHOST); R reads SLUICE_PG itself. The contender
# uses its own certificate (generated, or carried by the image).
set -uo pipefail
cd "$(dirname "$0")/../.."
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
  echo "usage: conform.sh <image> <h1|h2|h3> [off|gzip|deflate|br|zstd]" >&2
  exit 2
fi
image=$1 proto=$2 comp=${3:-off}
NETWORK=${NETWORK:-sluice_default}
PGHOST=${PGHOST:-postgres}
CHECKS=${CHECKS:-300}
name=conform-$$

tmp=$(mktemp -d)
trap 'docker rm -f "$name" >/dev/null 2>&1; [ -n "${rpid:-}" ] && kill "$rpid" 2>/dev/null; rm -rf "$tmp"' EXIT
GOWORK=off go build -C comparison/sluice -o "$tmp/r" . || exit 2
GOWORK=off go build -C comparison/loadgen -o "$tmp/loadgen" . || exit 2

docker run -d -q --name "$name" --network "$NETWORK" \
  -e PGHOST="$PGHOST" -e PGPORT=5432 -e PGUSER="$SLUICE_PG_USER" \
  -e PGPASSWORD="$SLUICE_PG_PASSWORD" -e PGDATABASE="$SLUICE_PG_DB" \
  -e STUDY_PROTOCOL="$proto" -e STUDY_COMPRESSION="$comp" \
  "$image" >/dev/null || exit 2
STUDY_PROTOCOL=$proto STUDY_COMPRESSION=$comp STUDY_LISTEN=127.0.0.1:9443 "$tmp/r" 2>"$tmp/r.log" &
rpid=$!

args=(-no-verify -proto "$proto")
[ "$comp" != off ] && args+=(-encoding "$comp")
"$tmp/loadgen" "${args[@]}" -url https://127.0.0.1:9443 -ready 20s >/dev/null || { cat "$tmp/r.log" >&2; exit 2; }
"$tmp/loadgen" "${args[@]}" -url "https://$name:8443" -ready 60s >/dev/null || { docker logs "$name" 2>&1 | tail -8 >&2; exit 2; }
"$tmp/loadgen" "${args[@]}" -url "https://$name:8443" -conformance https://127.0.0.1:9443 -checks "$CHECKS"
rc=$?
[ $rc -eq 0 ] || docker logs "$name" 2>&1 | tail -8 >&2
exit $rc
