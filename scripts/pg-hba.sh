#!/usr/bin/env bash
# Puts three authentication methods in front of the PostgreSQL container of
# the CI postgres job, one per throwaway role, and reloads the server:
#
#   scripts/pg-hba.sh <container>
#
#   sluice_hba_md5        md5: a challenge this connector declines
#   sluice_hba_password   password: the credential asked in clear
#   sluice_hba_trust      trust: no credential asked at all
#
# The lines go first in pg_hba.conf, so they decide for these roles and for
# no other: the rest of the suite runs unchanged on the same server.
# TestIntegrationAuthMethods creates the roles and reads SLUICE_PG_HBA=1 as
# the sign that this script ran. A service container takes no pg_hba.conf
# of its own, which is why the file is edited in place and reloaded.
set -euo pipefail
container=$1
hba=$(docker exec "$container" psql -U postgres -Atc 'SHOW hba_file')
docker exec "$container" sh -c "
  { printf 'host all sluice_hba_md5 all md5\n'
    printf 'host all sluice_hba_password all password\n'
    printf 'host all sluice_hba_trust all trust\n'
    cat '$hba'; } > /tmp/pg_hba.conf && cat /tmp/pg_hba.conf > '$hba'"
docker exec "$container" psql -U postgres -Atc 'SELECT pg_reload_conf()' >/dev/null
docker exec "$container" psql -U postgres -Atc \
  "SELECT count(*) FROM pg_hba_file_rules WHERE 'sluice_hba_md5' = ANY(user_name)" |
  grep -qx 1 || {
  echo "pg-hba.sh: the server did not load the md5 line" >&2
  exit 1
}
echo "pg-hba.sh: md5, password and trust in front of $hba"
