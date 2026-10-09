#!/usr/bin/env bash
# A throwaway PostgreSQL for the integration suite and the comparisons.
#
# The suite needs a server that can disagree with this package, and for most of
# a session there was none — not because one was unavailable, but because
# nobody had asked whether initdb was installed. It is. This starts a cluster
# nobody has to own, on a port nothing else uses, with a role that is
# deliberately NOT a superuser: row-level security does not apply to one, so a
# suite run as postgres passes without proving anything.
#
#   eval "$(internal/bench/scripts/pgdev.sh start)"   # exports SLUICE_PG_*
#   go test ./database/postgres/ -run TestIntegration
#   internal/bench/scripts/pgdev.sh stop
#
# The data directory lives under ${TMPDIR:-/tmp}. Unix sockets are disabled
# rather than relocated: their path is capped at 107 bytes and a temporary
# directory blows past it, which fails with a message about socket paths rather
# than about the directory.
set -euo pipefail

PGBIN="${PGBIN:-$(dirname "$(command -v initdb 2>/dev/null || echo /usr/lib/postgresql/15/bin/initdb)")}"
PGPORT="${PGPORT:-5433}"
ROOT="${SLUICE_PGDEV_DIR:-${TMPDIR:-/tmp}/sluice-pgdev}"
DATA="$ROOT/data"
LOG="$ROOT/postgres.log"

case "${1:-}" in
start)
  if [ ! -d "$DATA" ]; then
    mkdir -p "$ROOT"
    printf 'devpass\n' > "$ROOT/pw"
    "$PGBIN/initdb" -D "$DATA" -U postgres --auth=scram-sha-256 \
      --pwfile="$ROOT/pw" -E UTF8 >/dev/null
    rm -f "$ROOT/pw"
  fi
  if ! "$PGBIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
    # Failing here must be loud. A start that quietly does not happen leaves
    # SLUICE_PG unset, and an integration suite with SLUICE_PG unset *skips* —
    # so the run goes green while testing nothing at all, which is the exact
    # failure this script exists to remove.
    if ! "$PGBIN/pg_ctl" -D "$DATA" -l "$LOG" \
      -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=" \
      -w start >/dev/null; then
      echo "pgdev: the server did not start; last lines of $LOG:" >&2
      tail -5 "$LOG" >&2 || true
      exit 1
    fi
  fi
  PGPASSWORD=devpass "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U postgres -q -v ON_ERROR_STOP=0 <<'SQL' >/dev/null 2>&1 || true
CREATE ROLE sluice LOGIN PASSWORD 'sluicepass' NOSUPERUSER;
CREATE DATABASE sluice OWNER sluice;
SQL
  # Printed rather than exported, so the caller decides: eval it, or read it.
  echo "export SLUICE_PG=127.0.0.1:$PGPORT"
  echo "export SLUICE_PG_USER=sluice"
  echo "export SLUICE_PG_DB=sluice"
  echo "export SLUICE_PG_PASSWORD=sluicepass"
  ;;
stop)
  "$PGBIN/pg_ctl" -D "$DATA" -m fast stop >/dev/null 2>&1 || true
  ;;
destroy)
  "$PGBIN/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$ROOT"
  ;;
fixtures)
  # The tables the comparison benchmarks read. Separate from start, because
  # loading 120 000 rows is not something a unit-test run should pay for.
  PGPASSWORD=sluicepass "$PGBIN/psql" -h 127.0.0.1 -p "$PGPORT" -U sluice -d sluice -q -v ON_ERROR_STOP=1 <<'SQL'
DROP TABLE IF EXISTS bench_orders, bench_wide, bench_copy;
CREATE TABLE bench_orders (id bigint PRIMARY KEY, total bigint NOT NULL,
  label text NOT NULL, created timestamptz NOT NULL DEFAULT now());
INSERT INTO bench_orders (id, total, label)
  SELECT g, g * 7, 'order-' || g FROM generate_series(1, 100000) g;
CREATE TABLE bench_wide (id bigint PRIMARY KEY,
  c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint,
  t1 text, t2 text, t3 text, t4 text, t5 text);
INSERT INTO bench_wide SELECT g, g, g+1, g+2, g+3, g+4,
  'a'||g, 'b'||g, 'c'||g, 'd'||g, 'e'||g FROM generate_series(1, 20000) g;
CREATE TABLE bench_copy (id bigint, label text);
ANALYZE bench_orders; ANALYZE bench_wide;
SQL
  ;;
*)
  echo "usage: pgdev.sh {start|stop|destroy|fixtures}" >&2
  exit 1
  ;;
esac
