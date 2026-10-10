#!/bin/bash
# Entry point of sluice's image for the QUIC Interop Runner. The runner
# starts the container with ROLE, TESTCASE and (client) REQUESTS in the
# environment; /setup.sh and /wait-for-it.sh come from the simulator's
# endpoint base image and route the container through the simulated
# network. Everything else is the endpoint binary's: it exits 127 by itself
# for a case it does not implement.
set -eu
/setup.sh
if [ "${ROLE:-}" = client ]; then
  /wait-for-it.sh sim:57832 -s -t 30
fi
exec /endpoint
