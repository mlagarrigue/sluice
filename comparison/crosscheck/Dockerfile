# Two load tools independent of the study's generator, to cross-check a
# point of it (study §4, §2.3): oha for HTTP/1.1 and HTTP/2 (-q for a rate
# imposed, --latency-correction for the coordinated omission), and h2load
# for HTTP/3, nghttp2 built with ngtcp2 — a QUIC client that shares no
# code with quic-go, which the generator's HTTP/3 and the study's server
# both use, so that a bug symmetric between them cannot hide. Build from
# the repository root, like the other images:
#
#   docker build -f comparison/crosscheck/Dockerfile -t sluice-crosscheck .
#
# Alpine's own h2load shows the --h3 options but is built without ngtcp2,
# and its ngtcp2 has only the GnuTLS backend, where h2load speaks OpenSSL:
# so nghttp3, ngtcp2 on OpenSSL 3.5's QUIC API and nghttp2 are built here,
# each from its release tarball. Every piece is pinned: the base by digest,
# each tarball and oha's release binary (static, so it runs on musl) by the
# checksum its release publishes. They are tools in an image of the
# comparison, not code of the repository: nothing enters a go.mod.
FROM alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f AS h2load
RUN apk add --no-cache build-base pkgconf xz openssl-dev libev-dev c-ares-dev zlib-dev
ADD --checksum=sha256:aad782c23d3f01bd4bb52c8bac7a553b631ef8115fd1612703df6183449fef19 \
  https://github.com/ngtcp2/nghttp3/releases/download/v1.18.0/nghttp3-1.18.0.tar.xz /src/
ADD --checksum=sha256:2a34d2484ba17847a5d11965704e9dd0fac4c6d8efc75ffe1ec7de66d8c6b6fb \
  https://github.com/ngtcp2/ngtcp2/releases/download/v1.25.0/ngtcp2-1.25.0.tar.xz /src/
ADD --checksum=sha256:1fb324b6ec2c56f6bde0658f4139ffd8209fa9e77ce98fd7a5f63af8d0e508ad \
  https://github.com/nghttp2/nghttp2/releases/download/v1.69.0/nghttp2-1.69.0.tar.xz /src/
ENV PKG_CONFIG_PATH=/opt/h3/lib/pkgconfig
RUN cd /src && tar xf nghttp3-1.18.0.tar.xz && cd nghttp3-1.18.0 \
  && ./configure --prefix=/opt/h3 --enable-lib-only && make -j"$(nproc)" install
RUN cd /src && tar xf ngtcp2-1.25.0.tar.xz && cd ngtcp2-1.25.0 \
  && ./configure --prefix=/opt/h3 --enable-lib-only --with-openssl && make -j"$(nproc)" install
RUN cd /src && tar xf nghttp2-1.69.0.tar.xz && cd nghttp2-1.69.0 \
  && ./configure --prefix=/opt/h3 --enable-app --enable-http3 --disable-python-bindings \
  && make -j"$(nproc)" install && ldd /opt/h3/bin/h2load | grep -q ngtcp2_crypto_ossl

FROM alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f
RUN apk add --no-cache libstdc++ libev c-ares zlib openssl
COPY --from=h2load /opt/h3 /opt/h3
ENV PATH=/opt/h3/bin:$PATH LD_LIBRARY_PATH=/opt/h3/lib
ADD --checksum=sha256:620bb9e16fb53eabc9a3fc45f88bdb41fefa3fee5c05e75892011ce320391716 --chmod=755 \
  https://github.com/hatoo/oha/releases/download/v1.16.0/oha-linux-amd64 /usr/local/bin/oha
USER nobody
