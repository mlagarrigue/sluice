# The official Go image on Debian bookworm. No Node toolchain.
FROM golang:1.26-bookworm

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV DEBIAN_FRONTEND=noninteractive \
    GOPATH=/home/$USERNAME/go \
    GOTOOLCHAIN=local \
    PATH=/home/$USERNAME/go/bin:/home/$USERNAME/.local/bin:$PATH

# Build essentials, git, a few terminal conveniences, and what the GitHub CLI
# repository needs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git gnupg jq less make \
        openssh-client ripgrep sudo unzip \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI, from its official repository: pull requests and releases are
# driven from the terminal in this project.
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# A non-root user with passwordless sudo.
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME \
    && echo "$USERNAME ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

# Go tooling: language server, debugger, the formatter and linter CI enforces
# (pinned to the versions CI runs), vulnerability scanning, import management.
RUN go install golang.org/x/tools/gopls@latest \
    && go install github.com/go-delve/delve/cmd/dlv@latest \
    && go install mvdan.cc/gofumpt@v0.12.0 \
    && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    && go install golang.org/x/vuln/cmd/govulncheck@latest \
    && go install golang.org/x/tools/cmd/goimports@latest \
    && go clean -cache -modcache

# Anything personal — extra tools, mounts, CPU pinning — belongs in
# .devcontainer/local/, which git ignores, not here.
