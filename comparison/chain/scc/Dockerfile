# The line counter of the writing cost (study §1.1), scc at a fixed
# version, in its own image. cost.sh builds it with this directory as the
# only context, so no file of the repository enters the build, and runs it
# with no network on a copy of the files it counts:
#
#   docker build -t sluice-scc comparison/chain/scc
#
# The module is fetched through the Go proxy and checked against the
# checksum database; the binary is static, on a distroless image, run as a
# user with no rights. Both bases are pinned by digest.
FROM golang:1.26-bookworm@sha256:23ce71337b726608afb1bc00986e630a05034a87a87ea1fc2cd1ae5cf33387eb AS build
RUN CGO_ENABLED=0 GOBIN=/out go install -trimpath github.com/boyter/scc/v3@v3.7.0

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/scc /scc
ENTRYPOINT ["/scc"]
