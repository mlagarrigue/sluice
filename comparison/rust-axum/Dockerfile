# The Rust contender in containers (study §1.4): two images, as for the code
# (§2.4). Build from the repository root, like the study's other images:
#
#   docker build -f comparison/rust-axum/Dockerfile --target standard-doc -t rust-axum:standard .
#   docker build -f comparison/rust-axum/Dockerfile --target optimised-opt -t rust-axum:optimised .
#
# dataloader-doc is the documentation's image running the variant "with" a
# DataLoader (CMP-28).
#
# standard-doc is Docker's guide for Rust: the server built in the Rust
# image with its caches mounted, run from a slim base as a user with no
# rights. The guide's base is Alpine; this one is Debian slim, the C
# library the Rust image builds against (declared in comparison/README.md).
# optimised-opt is the `optimised` profile's build on a distroless image with
# the C runtime and nothing else. The server has no way to make a
# certificate at startup without one more crate, so the images carry one for
# a local run, made when they are built and named by STUDY_TLS_CERT and
# STUDY_TLS_KEY; a campaign names its own. The bases are pinned by digest.
FROM alpine:3@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS cert
RUN apk add --no-cache openssl && mkdir /cert && \
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 3650 \
      -subj /CN=study-contender -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
      -keyout /cert/key.pem -out /cert/cert.pem && chmod 644 /cert/key.pem /cert/cert.pem

FROM rust:1-slim@sha256:cc9edbcadf2c27013a642b1d1bb7504e26b1142d6b5ebe58f64cf2eb2bc128ae AS build
WORKDIR /app
COPY comparison/rust-axum/ ./
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --release && cp target/release/server target/release/dataloader /

FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2 AS standard-doc
RUN useradd --system --no-create-home --uid 10001 appuser
COPY --from=build /server /usr/local/bin/server
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
USER appuser
EXPOSE 8443
CMD ["/usr/local/bin/server"]

FROM standard-doc AS dataloader-doc
COPY --from=build /dataloader /usr/local/bin/dataloader
CMD ["/usr/local/bin/dataloader"]

FROM build AS build-opt
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --locked --profile optimised && cp target/optimised/server /server-opt

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cb87acc574abe719b6e5c547384defa2f7b1f24af9605d3f618cf852495d8be2 AS optimised-opt
COPY --from=build-opt /server-opt /server
COPY --from=cert /cert /cert
ENV STUDY_LISTEN=0.0.0.0:8443 STUDY_TLS_CERT=/cert/cert.pem STUDY_TLS_KEY=/cert/key.pem
EXPOSE 8443
CMD ["/server"]
