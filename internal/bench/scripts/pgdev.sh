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
# The benchmark tables are loaded by go run ./internal/bench/cmd/pgfixtures,
# which reads the variables start prints.
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
*)
  echo "usage: pgdev.sh {start|stop|destroy}" >&2
  exit 1
  ;;
esac
