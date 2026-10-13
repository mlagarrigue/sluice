# The official Go image on Debian bookworm. It already carries git, gcc and
# make (the race detector needs cgo), and nothing else is installed: no extra
# package, no third-party repository, no sudo, every tool pinned.
FROM golang:1.26-bookworm@sha256:d9c68c2c51161e12fd77e4c6320687c9cd86e1af1e3ad6e6cd63ff970641453c

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV GOPATH=/home/$USERNAME/go \
    GOTOOLCHAIN=local \
    PATH=/home/$USERNAME/go/bin:/home/$USERNAME/.local/bin:$PATH

# A non-root user whose ids match the usual host user, so the bind-mounted
# workspace stays writable on both sides.
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME

USER $USERNAME

# The formatter and linter scripts/check.sh runs, pinned to the versions CI
# runs: two versions of either disagree on what is clean.
RUN go install mvdan.cc/gofumpt@v0.12.0 \
    && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    && go clean -cache -modcache

# Anything personal — editor tools, credentials, mounts, CPU pinning —
# belongs in .devcontainer/local/, which git ignores, not here: how to layer
# it over this image, and why, is in CONTRIBUTING.md, "The development
# container".
