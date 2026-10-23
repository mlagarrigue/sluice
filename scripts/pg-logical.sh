#!/usr/bin/env bash
# Turns logical replication on in the PostgreSQL container of the CI postgres
# job and restarts the server: wal_level is read at start only, and a service
# container takes no server flags, so it cannot be set where the container is
# declared. TestIntegrationReplicationReadsASlot needs it, and skips without
# it — which the job turns into a failure.
#
#   scripts/pg-logical.sh <container>
set -euo pipefail
container=$1

# ALTER SYSTEM survives the restart.
docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 \
	-c "ALTER SYSTEM SET wal_level = logical"
docker restart "$container" >/dev/null

for _ in $(seq 60); do
	if docker exec "$container" pg_isready -q -U postgres -h 127.0.0.1; then
		level=$(docker exec "$container" psql -U postgres -tAc "SHOW wal_level")
		if [ "$level" != logical ]; then
			echo "pg-logical.sh: the server restarted with wal_level = $level" >&2
			exit 1
		fi
		echo "pg-logical.sh: wal_level = logical"
		exit 0
	fi
	sleep 1
done
echo "pg-logical.sh: the server did not come back after the restart" >&2
docker logs --tail 30 "$container" >&2
exit 1
