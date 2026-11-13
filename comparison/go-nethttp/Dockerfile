# The Go contenders in containers (study §1.4): two images per contender,
# as for the code (§2.4) — the documentation's image for the standard
# version and its variant "with", an optimised image for the optimised
# version. Build from the repository root, like the study's other images:
#
#   docker build -f comparison/go-nethttp/Dockerfile --target standard-doc -t go-nethttp:standard .
#   docker build -f comparison/go-nethttp/Dockerfile --target optimised-opt -t go-nethttp:optimised .
#
# The targets: standard-doc, dataloader-doc, gin-standard-doc (Docker's
# guide for Go: one stage on the Go image, the program built and run inside
# it); optimised-opt, gin-optimised-opt (the program static, on a distroless
# image with nothing else in it, run as a user with no rights). Every image
# reads the contract's variables (STUDY_*, PG*, comparison/README.md) and
# listens on STUDY_LISTEN, by default 0.0.0.0:8443 here, TCP and UDP. The
# bases are those of comparison/sluice/Dockerfile, pinned by digest.
FROM golang:1.26-bookworm@sha256:23ce71337b726608afb1bc00986e630a05034a87a87ea1fc2cd1ae5cf33387eb AS src
WORKDIR /src
COPY comparison/go-nethttp/go.mod comparison/go-nethttp/go.sum ./
RUN go mod download
COPY comparison/go-nethttp/ ./
ENV STUDY_LISTEN=0.0.0.0:8443
EXPOSE 8443/tcp 8443/udp

FROM src AS standard-doc
RUN go build -o /usr/local/bin/contender ./standard
ENTRYPOINT ["/usr/local/bin/contender"]

FROM src AS dataloader-doc
RUN go build -o /usr/local/bin/contender ./dataloader
ENTRYPOINT ["/usr/local/bin/contender"]

FROM src AS gin-standard-doc
RUN go build -o /usr/local/bin/contender ./gin-standard
ENTRYPOINT ["/usr/local/bin/contender"]

FROM src AS build-optimised
RUN CGO_ENABLED=0 go build -trimpath -o /contender ./optimised

FROM src AS build-gin-optimised
RUN CGO_ENABLED=0 go build -trimpath -o /contender ./gin-optimised

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS optimised-opt
COPY --from=build-optimised /contender /contender
ENV STUDY_LISTEN=0.0.0.0:8443
EXPOSE 8443/tcp 8443/udp
ENTRYPOINT ["/contender"]

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS gin-optimised-opt
COPY --from=build-gin-optimised /contender /contender
ENV STUDY_LISTEN=0.0.0.0:8443
EXPOSE 8443/tcp 8443/udp
ENTRYPOINT ["/contender"]
