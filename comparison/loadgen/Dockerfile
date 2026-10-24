# The study's load generator in a container, for the tester's role (study
# §0). Build from the repository root, like the server's image:
#
#   docker build -f comparison/loadgen/Dockerfile -t sluice-loadgen .
#   docker run --rm --network <net> sluice-loadgen -url https://<server>:8443 -scenario unit -rate 1000
#
# The module has no replace (it shares no code with the framework it
# measures), so only its own files enter the build. The binary is static,
# on a distroless image, run as a user with no rights; netem and ifb are
# set on the tester's interfaces by the chain, never from in here. Both
# bases are pinned by digest, like interop/Dockerfile's.
FROM golang:1.26-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS build
WORKDIR /src
COPY comparison/loadgen/go.mod comparison/loadgen/go.sum ./
RUN go mod download
COPY comparison/loadgen/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /loadgen .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /loadgen /loadgen
ENTRYPOINT ["/loadgen"]
