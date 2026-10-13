# sluice's endpoint for the QUIC Interop Runner. Build from the repository
# root, so that the interop module's replace directive finds the framework:
#
#   docker build -f interop/Dockerfile -t sluice-interop .
#
# The runtime image is the simulator's endpoint base, which provides
# /setup.sh and /wait-for-it.sh; the binary is static, so nothing else is
# installed on top of it. Both images are pinned by digest, so a release
# rebuilds the image it shipped; Dependabot moves the pins.
FROM golang:1.26-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY interop/go.mod interop/go.sum ./interop/
RUN cd interop && go mod download
COPY . .
RUN cd interop && CGO_ENABLED=0 go build -trimpath -o /endpoint ./cmd/endpoint

FROM martenseemann/quic-network-simulator-endpoint:latest@sha256:3b2b9e6fa317da238c8140a7b6d2bf8d4d45a8464b873eecac3ff2a32520b71a
COPY --from=build /endpoint /endpoint
COPY --chmod=755 interop/run_endpoint.sh /run_endpoint.sh
ENTRYPOINT ["/run_endpoint.sh"]
