# The HTTP/3 clients scripts/interop-http3.sh points at net/httpstream:
# Debian's curl (HTTP/3 over OpenSSL 3.5's QUIC stack and nghttp3) and
# aioquic's example client (aioquic from Debian, the example script from
# the matching release). Two QUIC implementations that share no code with
# sluice, nor with each other.
#
# The base image is pinned by digest and the example script by commit and
# checksum; the packages are those Debian 13 ships on the day of the build,
# security updates included.
FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad

RUN apt-get update -qq \
 && apt-get install -y -qq --no-install-recommends ca-certificates curl python3-aioquic python3-wsproto \
 && rm -rf /var/lib/apt/lists/*

# aioquic 1.2.0, the version Debian packages.
ADD --checksum=sha256:3db35a4985b226dc5322a4ff4bddc0a20ed005813213031adf1802be21273b24 \
    https://raw.githubusercontent.com/aiortc/aioquic/9bc1e43d13be3f06339841aca7c8560825053371/examples/http3_client.py \
    /opt/aioquic/http3_client.py
RUN printf '#!/bin/sh\nexec python3 /opt/aioquic/http3_client.py "$@"\n' >/usr/local/bin/aioquic-client \
 && chmod +x /usr/local/bin/aioquic-client
