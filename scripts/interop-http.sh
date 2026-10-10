#!/bin/sh
# net/httpstream against clients it did not write:
#
#   scripts/interop-http.sh
#
# Builds net/httpstream/internal/interopserver, points curl at it over
# HTTP/1.1 and HTTP/2, then runs h2spec (RFC 9113 conformance, 146 cases)
# and fails on any red case that net/httpstream/testdata/h2spec-exclusions.txt
# does not list — and on any listed case that passes, so the list stays
# honest. CI runs this same script (job interop-http).
#
# h2spec is built from a pinned commit of its repository into $TMPDIR
# unless $H2SPEC names a binary already on the machine. Built rather than
# downloaded: the release tarball carries no checksum, a commit hash does.
set -eu
cd "$(dirname "$0")/.."

H2SPEC_REPO=https://github.com/summerwind/h2spec
H2SPEC_COMMIT=70ac2294010887f48b18e2d64f5cccd48421fad1 # v2.6.0
EXCLUSIONS=net/httpstream/testdata/h2spec-exclusions.txt

work=$(mktemp -d)
server_pid=
cleanup() {
  [ -n "$server_pid" ] && kill "$server_pid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

fail() {
  echo "interop-http: $*" >&2
  exit 1
}

# --- h2spec -----------------------------------------------------------------

h2spec=${H2SPEC:-}
if [ -z "$h2spec" ]; then
  cache=${TMPDIR:-/tmp}/sluice-h2spec-$H2SPEC_COMMIT
  h2spec=$cache/h2spec
  if [ ! -x "$h2spec" ]; then
    echo "## building h2spec from $H2SPEC_REPO at $H2SPEC_COMMIT"
    rm -rf "$cache"
    git clone -q "$H2SPEC_REPO" "$cache/src"
    git -C "$cache/src" checkout -q "$H2SPEC_COMMIT"
    (cd "$cache/src" && GOFLAGS=-mod=mod GOWORK=off go build -o "$h2spec" ./cmd/h2spec)
    rm -rf "$cache/src"
  fi
fi

# --- the server -------------------------------------------------------------

echo "## building the server"
go build -o "$work/interopserver" ./net/httpstream/internal/interopserver
"$work/interopserver" >"$work/addr" 2>"$work/server.log" &
server_pid=$!
addr=
for _ in 1 2 3 4 5 6 7 8 9 10; do
  addr=$(head -n1 "$work/addr")
  [ -n "$addr" ] && break
  sleep 0.2
done
[ -n "$addr" ] || fail "the server printed no address: $(cat "$work/server.log")"
port=${addr##*:}
echo "server on $addr"

# --- curl -------------------------------------------------------------------

# expect <label> <want> <curl args...>: the body curl receives, byte for byte.
expect() {
  label=$1
  want=$2
  shift 2
  got=$(curl -sS --max-time 10 "$@") || fail "$label: curl failed"
  if [ "$got" != "$want" ]; then
    printf 'interop-http: %s: got\n%s\nwant\n%s\n' "$label" "$got" "$want" >&2
    exit 1
  fi
  echo "ok  $label"
}

stream='batch 1
batch 2
batch 3'

echo "## curl"
expect "HTTP/1.1 echo" "you asked for /hello" --http1.1 "http://$addr/hello"
expect "HTTP/1.1 streamed body" "$stream" --http1.1 "http://$addr/stream"
expect "HTTP/1.1 pipelined pair" "you asked for /ayou asked for /b" --http1.1 "http://$addr/a" "http://$addr/b"
expect "HTTP/2 echo" "you asked for /hello" --http2-prior-knowledge "http://$addr/hello"
expect "HTTP/2 streamed body" "$stream" --http2-prior-knowledge "http://$addr/stream"
# Two streamed responses multiplexed on one connection: the server answers
# them one after the other (see docs/guide/experimental.md), and both
# must arrive whole. curl 7.x cannot start a second transfer on a
# cleartext HTTP/2 connection it is still opening ("Error in the HTTP2
# framing layer", also against x/net's h2c server), so the case needs 8.0.
curl_major=$(curl --version | sed -n '1s/^curl \([0-9]*\)\..*/\1/p')
if [ "${curl_major:-0}" -ge 8 ]; then
  expect "HTTP/2 two streamed bodies in parallel" "$stream
$stream" --no-progress-meter --http2-prior-knowledge --parallel --parallel-max 2 \
    "http://$addr/stream" "http://$addr/stream"
else
  echo "--  HTTP/2 two streamed bodies in parallel: skipped, curl $curl_major cannot multiplex two transfers on a new h2c connection"
fi
# Without prior knowledge curl asks to upgrade from HTTP/1.1; the server
# speaks no upgrade dance and must answer the request as HTTP/1.1.
expect "HTTP/2 upgrade request answered as HTTP/1.1" "1.1 200" \
  --http2 -o /dev/null -w '%{http_version} %{http_code}' "http://$addr/hello"

# --- h2spec -----------------------------------------------------------------

echo "## h2spec"
# The JUnit report of v2.6.0 records no failures, so the text output is
# what gets parsed: the "Failures:" section at the end repeats every red
# case under its section heading.
set +e
"$h2spec" -h 127.0.0.1 -p "$port" -o 5 >"$work/h2spec.txt" 2>&1
set -e
tail -n1 "$work/h2spec.txt"

awk '
  /^Failures:/ { on = 1; next }
  !on { next }
  /^Generic tests/ { group = "generic"; next }
  /^Hypertext Transfer/ { group = "http2"; next }
  /^HPACK/ { group = "hpack"; next }
  /^ +[0-9]+(\.[0-9]+)*\. / {
    match($0, /[0-9]+(\.[0-9]+)*/); section = substr($0, RSTART, RLENGTH); next
  }
  index($0, "\303\227") {
    match($0, /[0-9]+:/); print group "/" section "/" substr($0, RSTART, RLENGTH - 1)
  }
' "$work/h2spec.txt" | sort -u >"$work/failed"
grep -v '^#' "$EXCLUSIONS" | grep -v '^$' | sort -u >"$work/excluded"

status=0
unexpected=$(comm -23 "$work/failed" "$work/excluded")
if [ -n "$unexpected" ]; then
  echo "interop-http: h2spec cases failing that $EXCLUSIONS does not list:" >&2
  echo "$unexpected" >&2
  status=1
fi
stale=$(comm -13 "$work/failed" "$work/excluded")
if [ -n "$stale" ]; then
  echo "interop-http: h2spec cases listed in $EXCLUSIONS that now pass; remove them:" >&2
  echo "$stale" >&2
  status=1
fi
if [ "$status" -ne 0 ]; then
  sed -n '/^Failures:/,$p' "$work/h2spec.txt" >&2
  exit "$status"
fi
echo "ok  h2spec: $(wc -l <"$work/failed" | tr -d ' ') expected failures, all listed in $EXCLUSIONS"
