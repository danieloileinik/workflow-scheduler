#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == --help ]]; then
    cat <<'HELP'
Usage: SSH_HOST=host SSH_USER=user bash deploy/vps.sh

Build the scheduler, transfer the image over SSH and deploy it to a single-node k3s VPS.
The SSH user must be root or have passwordless sudo. The host key must already be trusted.

Optional environment variables:
  SSH_PORT              SSH port (default: 22)
  SSH_KEY               Private key file; otherwise use the SSH agent/config
  SSH_KNOWN_HOSTS_FILE  Dedicated known_hosts file; otherwise use the SSH defaults
  IMAGE_TAG             Unique Docker tag; otherwise use the Git revision and UTC timestamp
  BUILD_NETWORK         Docker build network (default: default); use host for restricted bridges

Run deploy/bootstrap-k3s.sh as root on a new VPS before the first deployment.
HELP
    exit 0
fi

: "${SSH_HOST:?Set SSH_HOST to the VPS hostname or SSH alias.}"
ssh_port=${SSH_PORT:-22}
if [[ ! "$SSH_HOST" =~ ^[a-zA-Z0-9][a-zA-Z0-9_.:@-]*$ ]] || \
   [[ ! "$ssh_port" =~ ^[0-9]{1,5}$ ]] || (( 10#$ssh_port < 1 || 10#$ssh_port > 65535 )); then
    echo 'Invalid SSH_HOST or SSH_PORT.' >&2
    exit 1
fi

for tool in docker git ssh tar; do
    command -v "$tool" >/dev/null || { printf 'Required tool: %s\n' "$tool" >&2; exit 1; }
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd "$script_dir/.." && pwd)
ssh_options=(-p "$ssh_port" -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
[[ -z "${SSH_USER:-}" ]] || ssh_options+=(-l "$SSH_USER")
[[ -z "${SSH_KEY:-}" ]] || ssh_options+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
[[ -z "${SSH_KNOWN_HOSTS_FILE:-}" ]] || ssh_options+=(-o "UserKnownHostsFile=$SSH_KNOWN_HOSTS_FILE")

architecture=$(ssh "${ssh_options[@]}" "$SSH_HOST" \
    'if [ "$(id -u)" -eq 0 ]; then set -- k3s; else set -- sudo -n k3s; fi
    "$@" kubectl get nodes -o custom-columns=ARCH:.status.nodeInfo.architecture --no-headers')
case "$architecture" in
    amd64|arm64) ;;
    *) echo 'Expected exactly one k3s node with amd64 or arm64 architecture.' >&2; exit 1 ;;
esac

revision=$(git -C "$repo_dir" rev-parse --short=12 HEAD)
image_tag=${IMAGE_TAG:-$revision-$(date -u +%Y%m%d%H%M%S)-$RANDOM}
if [[ ! "$image_tag" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]]; then
    echo 'Invalid IMAGE_TAG.' >&2
    exit 1
fi
image="docker.io/library/workflow-scheduler:$image_tag"
docker build --network "${BUILD_NETWORK:-default}" --platform "linux/$architecture" -t "$image" -f "$repo_dir/Dockerfile" "$repo_dir"

bundle=$(mktemp -d)
trap 'rm -rf -- "$bundle"' EXIT
cp -R "$script_dir/kubernetes" "$bundle/kubernetes"
cp "$script_dir/apply.sh" "$bundle/apply.sh"
printf '%s\n' "$image" > "$bundle/image.txt"
docker image save "$image" -o "$bundle/image.tar"

remote_command=$(cat <<'REMOTE'
if [ "$(id -u)" -eq 0 ]; then set -- bash; else set -- sudo -n bash; fi
"$@" -c '
set -euo pipefail
deploy_dir=$(mktemp -d /tmp/workflow-deploy.XXXXXXXXXX)
trap "rm -rf -- \"$deploy_dir\"" EXIT
tar -xzf - -C "$deploy_dir"
bash "$deploy_dir/apply.sh"
'
REMOTE
)
# The command is a fixed literal; its variables expand only on the VPS.
# shellcheck disable=SC2029
tar -C "$bundle" -czf - . | ssh "${ssh_options[@]}" "$SSH_HOST" "$remote_command"
