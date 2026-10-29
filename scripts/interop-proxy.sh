#!/bin/sh
# net/httpstream behind the reverse proxies deployments put in front of it:
#
#   scripts/interop-proxy.sh
#
# Builds net/httpstream/internal/interopserver and starts nginx, HAProxy and
# Envoy in Docker in front of it, each from its official image pinned by
# digest and configured by the files in scripts/interop-proxy/. curl then
# speaks to each proxy, which speaks to the server over HTTP/1.1 and, for
# HAProxy and Envoy, over HTTP/2 in clear text as well. The cases are the
# ones a client alone never shows the server:
#
#   - a streamed response relayed by a proxy, and a chunked request body;
#   - Expect: 100-continue, answered by whichever side the proxy leaves it to;
#   - a hundred requests on the connections the proxy keeps to the server;
#   - the server closing an idle connection (on HTTP/2, after a GOAWAY)
#     that the proxy holds in its pool, and the request after it;
#   - response trailers, which each proxy relays or drops by its own rule;
#   - HAProxy's health check, OPTIONS * every 200 ms, one failure enough to
#     take the server out of rotation;
#   - last, the server's graceful stop (SIGTERM) with a slow response in
#     flight through each proxy: it finishes whole, and a request sent
#     during the stop is answered — by the server, or by the proxy with
#     502 or 503 — never cut.
#
# A weekly CI job runs this same script (workflow interop-proxy.yml). The
# containers share the server's network: the host's in CI and, in the
# devcontainer, whose Docker daemon is not on its network, the
# devcontainer's own namespace; INTEROP_DOCKER_NETWORK overrides both. The
# proxies listen on fixed ports, 18081 to 18085, on 127.0.0.1.
set -eu
cd "$(dirname "$0")/.."

NGINX_IMAGE=nginx@sha256:a7675fcee220f59b4a99f7098c06a0d5d999c2c0ee37c0211efb780536e5cb20         # 1.30.6
HAPROXY_IMAGE=haproxy@sha256:abc6a6dd00d67b796f593eb11473f61fe7ba5e29c4020fa5fb7bc6cf552e7ba9     # 3.4.7
ENVOY_IMAGE=envoyproxy/envoy@sha256:713bb146f1a7c97ebc70e13d259f77ff36f2c5d00f3145677816af5af9bd1530 # 1.40.1
IDLE=1s # the server's idle timeout; the idle case waits past it

work=$(mktemp -d)
pid=
containers=
cleanup() {
  [ -n "$pid" ] && kill "$pid" 2>/dev/null
  for c in $containers; do docker rm -f "$c" >/dev/null 2>&1; done
  rm -rf "$work"
}
trap cleanup EXIT INT TERM

fail() {
  echo "interop-proxy: $*" >&2
  for c in $containers; do
    echo "--- docker logs $c" >&2
    docker logs "$c" 2>&1 | tail -n 20 >&2
  done
  exit 1
}

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
"$work/interopserver" -idle "$IDLE" >"$work/addr" 2>"$work/log" &
pid=$!
addr=
for _ in 1 2 3 4 5 6 7 8 9 10; do
  addr=$(head -n1 "$work/addr")
  [ -n "$addr" ] && break
  sleep 0.2
done
[ -n "$addr" ] || fail "the server printed no address: $(cat "$work/log")"
echo "server on $addr"

# --- the proxies ------------------------------------------------------------

# proxy <name> <image> <config> <path in the image>: the configuration with
# the server's address written in, copied into a container created from the
# image, which then starts with the image's own command.
proxy() {
  name=sluice-interop-$1
  conf=$work/${3##*/}
  sed -e "s/UPSTREAM_HOST/${addr%:*}/g" -e "s/UPSTREAM_PORT/${addr##*:}/g" -e "s/UPSTREAM/$addr/g" "$3" >"$conf"
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker create -q --name "$name" --network "$network" "$2" >/dev/null
  containers="$containers sluice-interop-$1"
  docker cp -q "$conf" "$name:$4"
  docker start "$name" >/dev/null
}

echo "## starting nginx, HAProxy and Envoy"
proxy nginx "$NGINX_IMAGE" scripts/interop-proxy/nginx.conf /etc/nginx/nginx.conf
proxy haproxy "$HAPROXY_IMAGE" scripts/interop-proxy/haproxy.cfg /usr/local/etc/haproxy/haproxy.cfg
proxy envoy "$ENVOY_IMAGE" scripts/interop-proxy/envoy.yaml /etc/envoy/envoy.yaml

# ready <port>: waits for a 200 through the proxy. HAProxy answers 503 until
# its health check has passed, so this also waits on OPTIONS *.
ready() {
  for _ in $(seq 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$1/ready" || true)" = 200 ] && return 0
    sleep 0.5
  done
  fail "nothing answered 200 on port $1 within 30 seconds"
}
for port in 18081 18082 18083 18084 18085; do ready "$port"; done

# --- the cases --------------------------------------------------------------

# expect <label> <want> <command...>: what the command prints, byte for byte.
expect() {
  label=$1
  want=$2
  shift 2
  got=$("$@") || fail "$label: the client failed"
  if [ "$got" != "$want" ]; then
    printf 'interop-proxy: %s: got\n%s\nwant\n%s\n' "$label" "$got" "$want" >&2
    fail "$label: unexpected answer"
  fi
  echo "ok  $label"
}

get() { curl -sS --max-time 10 "$@"; }

# trailer <url>: the x-checksum field curl received after the body, if any.
trailer() {
  curl -sS --max-time 10 -D - -o /dev/null "$1" | tr -d '\r' | sed -n 's/^[Xx]-[Cc]hecksum: //p'
}

stream='batch 1
batch 2
batch 3'
hundred=$(for i in $(seq 100); do printf 'you asked for /k%d' "$i"; done)

# cases <label> <port> <trailer it relays, empty when it drops them>
cases() {
  url=http://127.0.0.1:$2
  expect "$1: GET" "you asked for /hello" get "$url/hello"
  expect "$1: streamed response" "$stream" get "$url/stream"
  expect "$1: chunked request body" "a chunked request body" \
    get -H 'Transfer-Encoding: chunked' --data-binary 'a chunked request body' "$url/echo"
  expect "$1: Expect: 100-continue" "an expected body" \
    get -H 'Expect: 100-continue' --data-binary 'an expected body' "$url/echo"
  expect "$1: a hundred requests on kept connections" "$hundred" get "$url/k[1-100]"
  # The server closes a connection idle for IDLE; the proxy still holds it.
  # The request after the close must not see it.
  for round in 1 2 3; do
    get -o /dev/null "$url/before-idle"
    sleep 1.5
    expect "$1: request after an idle close, round $round" "you asked for /after-idle" get "$url/after-idle"
  done
  expect "$1: body of a response with trailers" "$stream" get "$url/trailers"
  expect "$1: trailer relayed, or dropped where the proxy drops them" "$3" trailer "$url/trailers"
}

echo "## nginx, HTTP/1.1 to the server"
cases "nginx h1" 18081 ""
echo "## HAProxy, HTTP/1.1 to the server"
cases "haproxy h1" 18082 "three batches"
echo "## HAProxy, HTTP/2 to the server"
cases "haproxy h2" 18083 "three batches"
echo "## Envoy, HTTP/1.1 to the server"
cases "envoy h1" 18084 "three batches"
echo "## Envoy, HTTP/2 to the server"
cases "envoy h2" 18085 "three batches"

# The health check itself, straight to the server: the asterisk form
# (RFC 9112 §3.2.4) is answered, not refused.
expect "OPTIONS * straight to the server" "200" \
  curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -X OPTIONS --request-target '*' "http://$addr"

# --- the graceful stop ------------------------------------------------------

# SIGTERM is Serve's graceful stop: the listener closes, an idle HTTP/1.1
# connection is closed, an HTTP/2 one is told with its GOAWAY pair, and what
# is in flight is answered whole within the grace. One stop for the five
# ports, a slow response (/slow, two seconds) in flight on each; the
# requests sent during the stop may still be served — RFC 9113 §6.8 lets a
# stream the first GOAWAY did not exclude through — or be answered by the
# proxy, which then knows the server is going, but never cut.
echo "## the server stopped with a response in flight through each proxy"
ports="18081 18082 18083 18084 18085"
for port in $ports; do
  get -o "$work/slow-$port" "http://127.0.0.1:$port/slow" 2>"$work/slow-$port.err" &
  echo $! >"$work/slow-$port.pid"
done
sleep 1
kill -TERM "$pid"
sleep 0.3
for port in $ports; do
  code=$(curl -sS --max-time 10 -o "$work/during-$port" -w '%{http_code}' "http://127.0.0.1:$port/during-stop") ||
    fail "port $port: a request during the stop was cut"
  case $code in
    200) [ "$(cat "$work/during-$port")" = "you asked for /during-stop" ] ||
      fail "port $port: a request during the stop read $(cat "$work/during-$port")"
      echo "ok  port $port: a request during the stop, served" ;;
    502 | 503) echo "ok  port $port: a request during the stop, answered $code by the proxy" ;;
    *) fail "port $port: a request during the stop got $code" ;;
  esac
done
for port in $ports; do
  wait "$(cat "$work/slow-$port.pid")" || fail "port $port: the response in flight was cut: $(cat "$work/slow-$port.err")"
  [ "$(cat "$work/slow-$port")" = "$(printf 'slow 1\nslow 2')" ] ||
    fail "port $port: the response in flight read $(cat "$work/slow-$port")"
  echo "ok  port $port: the response in flight finished whole"
done
stopped=$pid
pid=
wait "$stopped" || fail "the server's stop was not clean: $(tail -n 5 "$work/log")"
echo "ok  the server stopped cleanly"
