# The study's server in a container (study §1.4), in its two versions.
# Build from the repository root, so that the module's replace directive
# finds the framework the campaign measures:
#
#   docker build -f comparison/sluice/Dockerfile --target doc -t sluice-study:doc .
#   docker build -f comparison/sluice/Dockerfile --target opt -t sluice-study:opt .
#
# doc is what Docker's guide for Go writes: one stage on the Go image, the
# binary built and run inside it. opt is the optimised version (§2.4): the
# binary static, on a distroless image with nothing else in it, run as a
# user with no rights. The server reads the contract's variables
# (STUDY_*, PG*, comparison/README.md) and listens on STUDY_LISTEN, by
# default 0.0.0.0:8443 here, TCP and UDP. Both bases are pinned by digest,
# like interop/Dockerfile's: a campaign rebuilds the image it measured.
FROM golang:1.26-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af AS doc
WORKDIR /src
COPY go.mod go.sum ./
COPY comparison/sluice/go.mod comparison/sluice/go.sum ./comparison/sluice/
RUN cd comparison/sluice && go mod download
COPY . .
RUN cd comparison/sluice && go build -o /usr/local/bin/sluice-study .
ENV STUDY_LISTEN=0.0.0.0:8443
EXPOSE 8443/tcp 8443/udp
ENTRYPOINT ["/usr/local/bin/sluice-study"]

FROM doc AS build-opt
RUN cd comparison/sluice && CGO_ENABLED=0 go build -trimpath -o /sluice-study .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS opt
COPY --from=build-opt /sluice-study /sluice-study
ENV STUDY_LISTEN=0.0.0.0:8443
EXPOSE 8443/tcp 8443/udp
ENTRYPOINT ["/sluice-study"]
