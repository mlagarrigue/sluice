#!/bin/sh
# net/httpstream against clients it did not write:
#
#   scripts/interop-http.sh
#
# Builds net/httpstream/internal/interopserver, points curl at it over
# HTTP/1.1 and HTTP/2, loads it with h2load (10,000 HTTP/2 requests from
# ten connections, every one a 2xx), then runs h2spec (RFC 9113 conformance, 146 cases)
# and fails on any red case that net/httpstream/testdata/h2spec-exclusions.txt
# does not list — and on any listed case that passes, so the list stays
# honest. Everything runs twice: in clear text, and behind the TLS
# configuration docs/guide/experimental.md recommends (the server's -tls),
# where ALPN rather than prior knowledge is what opens HTTP/2. CI runs this
# same script (job interop-http).
#
# h2spec is built from a pinned commit of its repository into $TMPDIR
# unless $H2SPEC names a binary already on the machine. Built rather than
# downloaded: the release tarball carries no checksum, a commit hash does.
# h2load comes from the system (Debian and Ubuntu: nghttp2-client).
set -eu
cd "$(dirname "$0")/.."

H2SPEC_REPO=https://github.com/summerwind/h2spec
H2SPEC_COMMIT=70ac2294010887f48b18e2d64f5cccd48421fad1 # v2.6.0
EXCLUSIONS=net/httpstream/testdata/h2spec-exclusions.txt

work=$(mktemp -d)
clear_pid=
tls_pid=
cleanup() {
  [ -n "$clear_pid" ] && kill "$clear_pid" 2>/dev/null
  [ -n "$tls_pid" ] && kill "$tls_pid" 2>/dev/null
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

# start <name> <server flags...>: starts one server and sets $pid and $addr,
# the address read from its first line of output.
start() {
  name=$1
  shift
  "$work/interopserver" "$@" >"$work/$name.addr" 2>"$work/$name.log" &
  pid=$!
  addr=
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    addr=$(head -n1 "$work/$name.addr")
    [ -n "$addr" ] && break
    sleep 0.2
  done
  [ -n "$addr" ] || fail "the $name server printed no address: $(cat "$work/$name.log")"
  echo "$name server on $addr"
}

start clear
clear_pid=$pid
clear_addr=$addr
start tls -tls
tls_pid=$pid
tls_addr=$addr

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
two_streams=$(printf '%s\n%s' "$stream" "$stream")

echo "## curl, clear text"
addr=$clear_addr
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
  expect "HTTP/2 two streamed bodies in parallel" "$two_streams" \
    --no-progress-meter --http2-prior-knowledge --parallel --parallel-max 2 \
    "http://$addr/stream" "http://$addr/stream"
else
  echo "--  HTTP/2 two streamed bodies in parallel: skipped, curl $curl_major cannot multiplex two transfers on a new h2c connection"
fi
# Without prior knowledge curl asks to upgrade from HTTP/1.1; the server
# speaks no upgrade dance and must answer the request as HTTP/1.1.
expect "HTTP/2 upgrade request answered as HTTP/1.1" "1.1 200" \
  --http2 -o /dev/null -w '%{http_version} %{http_code}' "http://$addr/hello"

# Behind TLS there is no prior knowledge and no upgrade: curl offers h2 and
# http/1.1 by ALPN, the server's order picks h2, and %{http_version} says
# which one the connection actually spoke. -k: the certificate is the
# server's own, signed at start-up.
echo "## curl, TLS"
addr=$tls_addr
expect "HTTP/1.1 echo" "you asked for /hello" -k --http1.1 "https://$addr/hello"
expect "HTTP/1.1 streamed body" "$stream" -k --http1.1 "https://$addr/stream"
expect "HTTP/1.1 pipelined pair" "you asked for /ayou asked for /b" -k --http1.1 "https://$addr/a" "https://$addr/b"
expect "HTTP/1.1 by ALPN" "1.1 200" \
  -k --http1.1 -o /dev/null -w '%{http_version} %{http_code}' "https://$addr/hello"
expect "HTTP/2 by ALPN" "2 200" \
  -k --http2 -o /dev/null -w '%{http_version} %{http_code}' "https://$addr/hello"
expect "HTTP/2 echo" "you asked for /hello" -k --http2 "https://$addr/hello"
expect "HTTP/2 streamed body" "$stream" -k --http2 "https://$addr/stream"
expect "HTTP/2 two streamed bodies in parallel" "$two_streams" \
  --no-progress-meter -k --http2 --parallel --parallel-max 2 \
  "https://$addr/stream" "https://$addr/stream"

# --- h2load -----------------------------------------------------------------

# load <label> <url> <h2load flags...>: 10,000 HTTP/2 requests from ten
# connections, ten streams in flight on each, and every one must succeed
# with a 2xx — what one client at a time never shows: many batches at once
# on many connections, and the flow-control windows reopening under them.
LOAD_REQUESTS=10000
load() {
  label=$1
  url=$2
  shift 2
  out=$(h2load -n "$LOAD_REQUESTS" -c 10 -m 10 "$@" "$url" 2>&1) || {
    echo "$out" >&2
    fail "$label: h2load failed"
  }
  requests=$(echo "$out" | sed -n 's/^requests: .* \([0-9]*\) succeeded, \([0-9]*\) failed, \([0-9]*\) errored, \([0-9]*\) timeout.*/\1 \2 \3 \4/p')
  ok2xx=$(echo "$out" | sed -n 's/^status codes: \([0-9]*\) 2xx.*/\1/p')
  if [ "$requests" != "$LOAD_REQUESTS 0 0 0" ] || [ "$ok2xx" != "$LOAD_REQUESTS" ]; then
    echo "$out" >&2
    fail "$label: want $LOAD_REQUESTS requests succeeded with a 2xx, got succeeded/failed/errored/timeout $requests, 2xx $ok2xx"
  fi
  echo "ok  $label: $LOAD_REQUESTS requests, all 2xx"
}

echo "## h2load"
command -v h2load >/dev/null || fail "h2load is not installed (nghttp2-client); CI installs it, see the interop-http job"
load "HTTP/2 under load, clear text" "http://$clear_addr/hello"
load "HTTP/2 under load, TLS" "https://$tls_addr/hello"

# --- h2spec -----------------------------------------------------------------

# run_h2spec <mode> <port> <h2spec flags...>: runs the whole suite against
# one server and compares its red cases with the exclusion list. An entry
# of the list applies to both modes unless it starts with "<mode>:".
run_h2spec() {
  mode=$1
  port=$2
  shift 2
  echo "## h2spec, $mode"
  set +e
  "$h2spec" -h 127.0.0.1 -p "$port" -o 5 "$@" >"$work/h2spec-$mode.txt" 2>&1
  set -e
  tail -n1 "$work/h2spec-$mode.txt"

  awk -f scripts/interop-h2spec.awk "$work/h2spec-$mode.txt" | sort -u >"$work/failed"
  # Entries without a mode prefix, plus those of this mode with the prefix
  # dropped.
  grep -v '^#' "$EXCLUSIONS" | grep -v '^$' \
    | sed -n -e '/^[a-z]*:/!p' -e "s/^$mode://p" | sort -u >"$work/excluded"

  status=0
  unexpected=$(comm -23 "$work/failed" "$work/excluded")
  if [ -n "$unexpected" ]; then
    echo "interop-http: h2spec cases failing in $mode that $EXCLUSIONS does not list:" >&2
    echo "$unexpected" >&2
    status=1
  fi
  stale=$(comm -13 "$work/failed" "$work/excluded")
  if [ -n "$stale" ]; then
    echo "interop-http: h2spec cases listed in $EXCLUSIONS that now pass in $mode; remove them:" >&2
    echo "$stale" >&2
    status=1
  fi
  if [ "$status" -ne 0 ]; then
    sed -n '/^Failures:/,$p' "$work/h2spec-$mode.txt" >&2
    exit "$status"
  fi
  echo "ok  h2spec, $mode: $(wc -l <"$work/failed" | tr -d ' ') expected failures, all listed in $EXCLUSIONS"
}

run_h2spec clear "${clear_addr##*:}"
run_h2spec tls "${tls_addr##*:}" -t -k
