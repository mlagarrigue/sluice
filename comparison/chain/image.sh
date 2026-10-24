#!/usr/bin/env bash
# The containerisation of one image (§1.4), one JSON line: its size,
# compressed and not, and its layers; its build from nothing and after one
# line of code changed; then, on the local chain, its cold start to the
# first answer ok, its RSS at rest and its time to steady state
# (chain.sh boot). Local figures: they check the method, nothing is
# published.
#
#   comparison/chain/image.sh comparison/sluice/Dockerfile doc
#   comparison/chain/image.sh comparison/sluice/Dockerfile opt
#   TOUCH=main.go SCENARIO=unit RATE=1000 comparison/chain/image.sh <Dockerfile> <target>
#
# The build context is the committed tree (git archive HEAD), in a
# directory of its own: what a CI would build, the working tree untouched.
# The image is tagged study-measured:<target>; TOUCH names the file, under
# the Dockerfile's directory, where a line of code is added for the
# incremental build (main.go). The compressed size is the gzip of `docker
# save` — no registry here, the one a registry stores is of that order.
# The cold start runs from docker create, both networks attached, the
# database already up: what is timed is the server, not PostgreSQL.
set -euo pipefail
cd "$(dirname "$0")/../.."
if [ $# -ne 2 ]; then
  echo "usage: image.sh <Dockerfile> <target>" >&2
  exit 1
fi
dockerfile=$1 target=$2
tag=study-measured:$target
touch_file=$(dirname "$dockerfile")/${TOUCH:-main.go}
export SCENARIO=${SCENARIO:-unit} RATE=${RATE:-1000}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"; comparison/chain/chain.sh down' EXIT
git archive HEAD | tar -x -C "$tmp"
if [ ! -f "$tmp/$touch_file" ]; then
  echo "image: $touch_file is not in the committed tree" >&2
  exit 1
fi

ms() { echo $(( ($(date +%s%N) - $1) / 1000000 )); }
t=$(date +%s%N)
docker build -q --no-cache -f "$tmp/$dockerfile" --target "$target" -t "$tag" "$tmp" >/dev/null
cold=$(ms "$t")
# The line differs from run to run, or a second run would find the first
# run's incremental build in Docker's cache.
printf '\nvar _ = %s // one line of code changed (§1.4)\n' "$(date +%s%N)" >> "$tmp/$touch_file"
t=$(date +%s%N)
docker build -q -f "$tmp/$dockerfile" --target "$target" -t "$tag" "$tmp" >/dev/null
incremental=$(ms "$t")

size=$(docker image inspect "$tag" --format '{{.Size}}')
layers=$(docker image inspect "$tag" --format '{{len .RootFS.Layers}}')
gz=$(docker save "$tag" | gzip -c | wc -c)

comparison/chain/chain.sh up
start=$(WEB_IMAGE=$tag comparison/chain/chain.sh boot)
jq -c --arg dockerfile "$dockerfile" --arg target "$target" --arg rev "$(git rev-parse --short HEAD)" \
  --argjson cold "$cold" --argjson incr "$incremental" --argjson size "$size" \
  --argjson layers "$layers" --argjson gz "$gz" \
  '{dockerfile: $dockerfile, target: $target, rev: $rev,
    size: {bytes: $size, gzip_bytes: $gz, layers: $layers},
    build_ms: {cold: $cold, incremental: $incr}} + .' <<<"$start"
