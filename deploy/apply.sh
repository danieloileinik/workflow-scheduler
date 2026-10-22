#!/usr/bin/env bash
set -euo pipefail
umask 077
cd "$(dirname "${BASH_SOURCE[0]}")"

if (( EUID != 0 )); then
    echo 'Run this script as root on the VPS.' >&2
    exit 1
fi

image=$(cat image.txt)
if [[ ! "$image" =~ ^docker\.io/library/workflow-scheduler:[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]]; then
    echo 'Invalid scheduler image reference.' >&2
    exit 1
fi

exec 9>/var/lock/workflow-scheduler-deploy.lock
flock -n 9 || { echo 'Another deployment is running.' >&2; exit 1; }

kubectl() { k3s kubectl "$@"; }
diagnostics() {
    kubectl -n workflow-scheduler get pods,pvc >&2 || true
    kubectl -n workflow-scheduler logs job/initialize-database --all-containers --tail=50 >&2 || true
    kubectl -n workflow-scheduler logs deployment/scheduler --all-containers --tail=50 >&2 || true
}
trap diagnostics ERR

k3s ctr --namespace k8s.io images import image.tar
kubectl apply -f kubernetes/namespace.yaml

secret=$(kubectl -n workflow-scheduler get secret postgres-credentials --ignore-not-found -o name)
if [[ -z "$secret" ]]; then
    claim=$(kubectl -n workflow-scheduler get pvc data-postgres-0 --ignore-not-found -o name)
    if [[ -n "$claim" ]]; then
        echo 'PostgreSQL data already exists. Restore postgres-credentials before deploying.' >&2
        exit 1
    fi

    credentials=$(mktemp -d)
    trap 'rm -rf -- "$credentials"' EXIT
    password=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
    printf '%s' "$password" > "$credentials/password"
    printf 'Host=postgres;Database=workflow;Username=workflow;Password=%s' "$password" > "$credentials/connection"
    unset password
    kubectl -n workflow-scheduler create secret generic postgres-credentials \
        --from-file=POSTGRES_PASSWORD="$credentials/password" \
        --from-file=ConnectionStrings__Postgres="$credentials/connection"
    rm -rf -- "$credentials"
    trap - EXIT
fi

kubectl apply -f kubernetes/configmap.yaml -f kubernetes/postgres.yaml -f kubernetes/kafka.yaml
kubectl -n workflow-scheduler rollout status statefulset/postgres --timeout=600s
kubectl -n workflow-scheduler rollout status statefulset/kafka --timeout=600s

deployment=$(kubectl -n workflow-scheduler get deployment scheduler --ignore-not-found -o name)
if [[ -n "$deployment" ]]; then
    kubectl -n workflow-scheduler scale deployment/scheduler --replicas=0
    kubectl -n workflow-scheduler rollout status deployment/scheduler --timeout=90s
    pods=$(kubectl -n workflow-scheduler get pods -l app=scheduler -o name)
    if [[ -n "$pods" ]]; then
        mapfile -t scheduler_pods <<< "$pods"
        kubectl -n workflow-scheduler wait --for=delete "${scheduler_pods[@]}" --timeout=90s
    fi
fi

kubectl -n workflow-scheduler delete job initialize-database --ignore-not-found --wait=true --timeout=60s
sed "s|workflow-scheduler:local|$image|g" kubernetes/initialize-database.yaml | kubectl apply -f -
kubectl -n workflow-scheduler wait --for=condition=complete job/initialize-database --timeout=330s

sed "s|workflow-scheduler:local|$image|g" kubernetes/scheduler.yaml | kubectl apply -f -
kubectl -n workflow-scheduler rollout status deployment/scheduler --timeout=300s
kubectl -n workflow-scheduler get pods,services
printf 'Deployed %s\n' "$image"
