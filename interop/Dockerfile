# sluice's endpoint for the QUIC Interop Runner. Build from the repository
# root, so that the interop module's replace directive finds the framework:
#
#   docker build -f interop/Dockerfile -t sluice-interop .
#
# The runtime image is the simulator's endpoint base, which provides
# /setup.sh and /wait-for-it.sh; the binary is static, so nothing else is
# installed on top of it.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY interop/go.mod interop/go.sum ./interop/
RUN cd interop && go mod download
COPY . .
RUN cd interop && CGO_ENABLED=0 go build -trimpath -o /endpoint ./cmd/endpoint

FROM martenseemann/quic-network-simulator-endpoint:latest
COPY --from=build /endpoint /endpoint
COPY --chmod=755 interop/run_endpoint.sh /run_endpoint.sh
ENTRYPOINT ["/run_endpoint.sh"]
