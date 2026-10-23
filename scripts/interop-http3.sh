#!/bin/sh
# net/httpstream over HTTP/3 against clients it did not write:
#
#   scripts/interop-http3.sh
#
# Builds net/httpstream/internal/interopserver and serves HTTP/3 with -h3,
# then points three QUIC implementations that share no code with sluice at
# it: curl (OpenSSL 3.5's QUIC stack and nghttp3) and aioquic's example
# client, both from the image scripts/interop-http3.Dockerfile describes,
# and h3spec (RFC 9000 and RFC 9114 conformance). The script fails on any
# h3spec case red that net/httpstream/testdata/h3spec-exclusions.txt does
# not list, and on any listed case that passes, so the list stays honest.
# CI runs this same script (job interop-http3).
#
# The clients run in Docker because no distribution curl on the CI runner
# speaks HTTP/3. They share the server's network: the host's in CI, and in
# the devcontainer, whose Docker daemon is not on its network, the
# devcontainer's own namespace; INTEROP_DOCKER_NETWORK overrides both.
#
# h3spec is a release binary, checked against the hash below and cached in
# $TMPDIR unless $H3SPEC names a binary already on the machine.
set -eu
cd "$(dirname "$0")/.."

H3SPEC_URL=https://github.com/kazu-yamamoto/h3spec/releases/download/v0.1.14/h3spec-linux-x86_64
H3SPEC_SHA256=7f2e3232715a3f6ff5212d408b2a3e74d6aa3ca06f96efca9a4e5dc55f3c6860
EXCLUSIONS=net/httpstream/testdata/h3spec-exclusions.txt
IMAGE=sluice-interop-http3

work=$(mktemp -d)
pid=
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

fail() {
  echo "interop-http3: $*" >&2
  exit 1
}

# --- h3spec and the clients' image ------------------------------------------

h3spec=${H3SPEC:-}
if [ -z "$h3spec" ]; then
  cache=${TMPDIR:-/tmp}/sluice-h3spec-$H3SPEC_SHA256
  h3spec=$cache/h3spec
  if [ ! -x "$h3spec" ]; then
    echo "## downloading h3spec from $H3SPEC_URL"
    mkdir -p "$cache"
    curl -sSfL -o "$cache/download" "$H3SPEC_URL"
    echo "$H3SPEC_SHA256  $cache/download" | sha256sum -c --quiet - \
      || fail "h3spec: the download does not match $H3SPEC_SHA256"
    chmod +x "$cache/download"
    mv "$cache/download" "$h3spec"
  fi
fi

echo "## building the clients' image"
docker build -q -t "$IMAGE" -f scripts/interop-http3.Dockerfile scripts >/dev/null

network=${INTEROP_DOCKER_NETWORK:-}
if [ -z "$network" ]; then
  if [ -f /.dockerenv ]; then
    network=container:$(hostname)
  else
    network=host
  fi
fi

# --- the server -------------------------------------------------------------

echo "## building the server"
go build -o "$work/interopserver" ./net/httpstream/internal/interopserver
"$work/interopserver" -h3 >"$work/addr" 2>"$work/log" &
pid=$!
addr=
for _ in 1 2 3 4 5 6 7 8 9 10; do
  addr=$(head -n1 "$work/addr")
  [ -n "$addr" ] && break
  sleep 0.2
done
[ -n "$addr" ] || fail "the server printed no address: $(cat "$work/log")"
echo "server on udp $addr"

# client <args...>: runs one command in the clients' image, on the server's
# network.
client() {
  docker run --rm --network "$network" "$IMAGE" "$@"
}

# expect <label> <want> <command...>: what the command prints, byte for byte.
expect() {
  label=$1
  want=$2
  shift 2
  got=$("$@") || fail "$label: the client failed"
  if [ "$got" != "$want" ]; then
    printf 'interop-http3: %s: got\n%s\nwant\n%s\n' "$label" "$got" "$want" >&2
    exit 1
  fi
  echo "ok  $label"
}

stream='batch 1
batch 2
batch 3'
two_streams=$(printf '%s\n%s' "$stream" "$stream")
url=https://$addr

# --- curl -------------------------------------------------------------------

# --http3-only: no fallback to TCP, where nothing listens. -k: the
# certificate is the server's own, signed at start-up.
echo "## curl"
expect "HTTP/3 echo" "you asked for /hello" \
  client curl -sS -k --http3-only --max-time 10 "$url/hello"
expect "HTTP/3 version and status" "3 200" \
  client curl -sS -k --http3-only --max-time 10 -o /dev/null -w '%{http_version} %{http_code}' "$url/hello"
expect "HTTP/3 streamed body" "$stream" \
  client curl -sS -k --http3-only --max-time 10 "$url/stream"
expect "HTTP/3 two streamed bodies on one connection" "$two_streams" \
  client curl -sS -k --http3-only --max-time 10 --parallel --parallel-max 2 --no-progress-meter \
  "$url/stream" "$url/stream"
expect "HTTP/3 POST with a body" "you asked for /posted" \
  client curl -sS -k --http3-only --max-time 10 --data-binary 'a request body' "$url/posted"

# --- aioquic ----------------------------------------------------------------

# aioquic <args...>: aioquic's client writes each response body into
# --output-dir, under the last segment of its path; the shell in the
# container prints them back, in the order of the URLs, and the client's
# log only when it failed.
echo "## aioquic"
aioquic() {
  client sh -c 'mkdir -p /tmp/out
    aioquic-client --insecure --output-dir /tmp/out "$@" >/tmp/log 2>&1 || { cat /tmp/log >&2; exit 1; }
    for a; do case $a in https://*) cat "/tmp/out/${a##*/}";; esac; done' aioquic "$@"
}
expect "aioquic echo" "you asked for /hello" aioquic "$url/hello"
expect "aioquic streamed body" "$stream" aioquic "$url/stream"
expect "aioquic POST with a body" "you asked for /posted" aioquic -d 'a request body' "$url/posted"
expect "aioquic two requests on one connection" "you asked for /ayou asked for /b" aioquic "$url/a" "$url/b"

# --- h3spec -----------------------------------------------------------------

# h3spec is hspec: its "Failures:" section numbers each red case and gives
# its full name, group included ("  2) HTTP/3 servers treats ..."). The
# count of names read must match the summary's count of failures, so that a
# change of format cannot turn red cases into an empty list.
echo "## h3spec"
set +e
"$h3spec" -n -t 2000 "${addr%:*}" "${addr##*:}" >"$work/h3spec.txt" 2>&1
set -e
summary=$(grep -E '^[0-9]+ examples?, [0-9]+ failures?' "$work/h3spec.txt") \
  || fail "h3spec printed no summary: $(tail -n 20 "$work/h3spec.txt")"
echo "$summary"

sed -n '/^Failures:/,$s/^  [0-9]*[)] \(.*[^ ]\) *$/\1/p' "$work/h3spec.txt" | sort -u >"$work/failed"
want=$(echo "$summary" | sed 's/^[0-9]* examples\{0,1\}, \([0-9]*\) failure.*/\1/')
[ "$(wc -l <"$work/failed" | tr -d ' ')" = "$want" ] \
  || fail "h3spec reports $want failures and the script read the names of $(wc -l <"$work/failed" | tr -d ' ')"
grep -v '^#' "$EXCLUSIONS" | grep -v '^$' | sort -u >"$work/excluded"

status=0
unexpected=$(comm -23 "$work/failed" "$work/excluded")
if [ -n "$unexpected" ]; then
  echo "interop-http3: h3spec cases failing that $EXCLUSIONS does not list:" >&2
  echo "$unexpected" >&2
  status=1
fi
stale=$(comm -13 "$work/failed" "$work/excluded")
if [ -n "$stale" ]; then
  echo "interop-http3: h3spec cases listed in $EXCLUSIONS that now pass; remove them:" >&2
  echo "$stale" >&2
  status=1
fi
if [ "$status" -ne 0 ]; then
  sed -n '/^Failures:/,$p' "$work/h3spec.txt" >&2
  exit "$status"
fi
echo "ok  h3spec: $(wc -l <"$work/failed" | tr -d ' ') expected failures, all listed in $EXCLUSIONS"
