#!/usr/bin/env bash
set -euo pipefail

if (( EUID != 0 )); then
    echo 'Run this script as root on the VPS.' >&2
    exit 1
fi

if ! command -v k3s >/dev/null; then
    installer=$(mktemp)
    trap 'rm -f -- "$installer"' EXIT
    curl -fsSL --retry 3 https://get.k3s.io -o "$installer"
    INSTALL_K3S_CHANNEL=stable sh "$installer" server \
        --disable traefik --disable servicelb \
        --secrets-encryption --write-kubeconfig-mode 600
fi

k3s kubectl wait --for=condition=Ready node --all --timeout=180s
k3s kubectl get nodes
