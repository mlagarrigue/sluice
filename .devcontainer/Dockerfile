# Image Go officielle (Debian bookworm), pas de toolchain Node.
FROM golang:1.26-bookworm

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

ENV DEBIAN_FRONTEND=noninteractive \
    GOPATH=/home/$USERNAME/go \
    GOTOOLCHAIN=local \
    PATH=/home/$USERNAME/go/bin:/home/$USERNAME/.local/bin:$PATH

# Outils système : build, git, gh, azure-cli et dépendances TLS pour les dépôts tiers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        curl \
        git \
        gnupg \
        jq \
        less \
        lsb-release \
        make \
        openssh-client \
        ripgrep \
        sudo \
        unzip \
    && rm -rf /var/lib/apt/lists/*

# GitHub CLI (dépôt officiel).
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
        -o /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
        > /etc/apt/sources.list.d/github-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Azure CLI (dépôt officiel Microsoft).
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && chmod go+r /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" \
        > /etc/apt/sources.list.d/azure-cli.list \
    && apt-get update && apt-get install -y --no-install-recommends azure-cli \
    && rm -rf /var/lib/apt/lists/*

# Utilisateur non-root avec sudo sans mot de passe.
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME \
    && echo "$USERNAME ALL=(root) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

# Outillage Go : langage, debug, qualité, sécurité, tests.
RUN go install golang.org/x/tools/gopls@latest \
    && go install github.com/go-delve/delve/cmd/dlv@latest \
    && go install honnef.co/go/tools/cmd/staticcheck@latest \
    && go install golang.org/x/vuln/cmd/govulncheck@latest \
    && go install mvdan.cc/gofumpt@latest \
    && go install golang.org/x/tools/cmd/goimports@latest \
    && go install github.com/segmentio/golines@latest \
    && go install gotest.tools/gotestsum@latest \
    && go install github.com/golang/mock/mockgen@latest \
    && go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 \
    && go clean -cache -modcache

# CLI Claude Code — installeur natif, aucune dépendance Node.
RUN curl -fsSL https://claude.ai/install.sh | bash

WORKDIR /workspaces/goframeworkweb
