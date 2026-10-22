#!/usr/bin/env bash
# Turns TLS on in the PostgreSQL container of the CI postgres job, with a
# certificate made here, and restarts the server so that the next connection
# is sure to meet that certificate. A service container takes no server
# flags, so ssl = on cannot be set where the container is declared.
#
#   scripts/pg-tls.sh <container> <rsa|rsa-pss|rsa-pss-maxsalt|ecdsa|ed25519> <dir>
#
#   rsa              RSA key, PKCS #1 v1.5 signature with SHA-256: the common
#                    case.
#   rsa-pss          RSA key, RSASSA-PSS signature with SHA-384. The binding
#                    hash is taken from inside the PSS parameters, so this is
#                    the one where a client and the server can disagree
#                    (RFC 5929 §4.1).
#   rsa-pss-maxsalt  The same with OpenSSL's default salt, the longest that
#                    fits. crypto/x509 names a PSS signature only when the
#                    salt is the hash length, so here the client reads the
#                    hash from the parameters itself.
#   ecdsa            P-256 key, ECDSA with SHA-256.
#   ed25519          Ed25519 key: a signature with no hash of its own, for
#                    which RFC 5929 §4.1 defines no binding at all.
#
# Writes <dir>/server.crt, self-signed for 127.0.0.1: it is also the root the
# client trusts. The image's pg_hba.conf has host lines, which match TLS and
# plain TCP alike, so the rest of the suite runs unchanged on the same server.
set -euo pipefail

container=$1 kind=$2 dir=$3

key=(-newkey rsa:2048)
case $kind in
rsa) sign=(-sha256) ;;
ecdsa) key=(-newkey ec -pkeyopt ec_paramgen_curve:P-256) sign=(-sha256) ;;
# Ed25519 signs the message itself: no digest option is accepted.
ed25519) key=(-newkey ed25519) sign=() ;;
# The salt length is the hash length, the only one crypto/x509 names
# (SHA384-RSAPSS).
rsa-pss) sign=(-sha384 -sigopt rsa_padding_mode:pss -sigopt rsa_pss_saltlen:digest) ;;
rsa-pss-maxsalt) sign=(-sha384 -sigopt rsa_padding_mode:pss) ;;
*)
	echo "pg-tls.sh: unknown certificate kind $kind" >&2
	exit 2
	;;
esac

mkdir -p "$dir"
openssl req -x509 "${key[@]}" -nodes -days 2 \
	-keyout "$dir/server.key" -out "$dir/server.crt" \
	-subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1 ${sign[@]+"${sign[@]}"}

remote=/var/lib/postgresql
docker cp "$dir/server.crt" "$container:$remote/sluice-server.crt"
docker cp "$dir/server.key" "$container:$remote/sluice-server.key"
# The server refuses to start on a key others can read.
docker exec -u root "$container" chown postgres:postgres "$remote/sluice-server.crt" "$remote/sluice-server.key"
docker exec -u root "$container" chmod 600 "$remote/sluice-server.key"

# ALTER SYSTEM survives the restart; each -c is its own transaction, which
# ALTER SYSTEM requires.
docker exec "$container" psql -U postgres -v ON_ERROR_STOP=1 \
	-c "ALTER SYSTEM SET ssl_cert_file = '$remote/sluice-server.crt'" \
	-c "ALTER SYSTEM SET ssl_key_file = '$remote/sluice-server.key'" \
	-c "ALTER SYSTEM SET ssl = on"
docker restart "$container" >/dev/null

for _ in $(seq 60); do
	if docker exec "$container" pg_isready -q -U postgres -h 127.0.0.1; then
		ssl=$(docker exec "$container" psql -U postgres -tAc "SHOW ssl")
		if [ "$ssl" != on ]; then
			echo "pg-tls.sh: the server restarted with ssl = $ssl" >&2
			exit 1
		fi
		openssl x509 -in "$dir/server.crt" -noout -text | grep -m1 'Signature Algorithm'
		exit 0
	fi
	sleep 1
done
echo "pg-tls.sh: the server did not come back after the restart" >&2
docker logs --tail 30 "$container" >&2
exit 1
