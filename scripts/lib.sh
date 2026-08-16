#!/usr/bin/env bash
# Shared environment and helpers. Source this, do not execute it:
#
#   . "$(dirname "$0")/lib.sh"
#
# Everything else in scripts/ and experiments/ relies on it, so the settings
# live here once rather than being repeated and drifting.

# --- environment ------------------------------------------------------------

# k3s writes its kubeconfig here. Respect an existing KUBECONFIG so the scripts
# also work against a cluster that is not k3s.
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

export NS="${NS:-video-pipeline}"

# repo root, regardless of where the script was called from
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export REPO

# Where the five dataset_partition-*.csv files live ON THE NODE. Must be a real
# path on the machine running the kubelet, and on WSL it should be the native
# filesystem: /mnt/c goes through 9p and is far too slow to stream from.
export DATASET_DIR="${DATASET_DIR:-/opt/video-dataset}"

# laptop -> one replica of each store, which is what fits in ~11GB
# full   -> the replica counts declared in the manifests, needs ~20GB
export PROFILE="${PROFILE:-laptop}"

export TOPIC="${TOPIC:-youtube_videos}"
export KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-5}"

# --- output -----------------------------------------------------------------

say()  { printf '\n=== %s\n' "$*"; }
step() { printf '  - %s\n' "$*"; }
warn() { printf '  ! %s\n' "$*" >&2; }
die()  { printf '\nFAILED: %s\n' "$*" >&2; exit 1; }

# --- cluster helpers --------------------------------------------------------

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is not installed"; }

cluster_up() { kubectl get nodes >/dev/null 2>&1; }

require_cluster() {
  cluster_up || die "no reachable cluster. Run scripts/01_setup_cluster.sh first."
}

# k3s has no systemd under WSL, so it is started as a detached process and has
# to be restarted by hand after every `wsl --shutdown`. Cluster state and
# volumes survive on disk, so this is safe to call repeatedly.
start_k3s() {
  if pgrep -f 'k3s server' >/dev/null 2>&1; then
    step 'k3s already running'
  else
    step 'starting k3s'
    # the mode only applies when the file is created, so clear a root-only
    # leftover first or kubectl stays unusable without sudo
    sudo rm -f /etc/rancher/k3s/k3s.yaml
    sudo setsid nohup k3s server --write-kubeconfig-mode 644 \
      </dev/null >/tmp/k3s.log 2>&1 &
    sleep 10
  fi

  # Fix the kubeconfig mode BEFORE testing reachability, and do it whether or
  # not we started the server. k3s rewrites this file during its life and can
  # leave it root-only again, and --write-kubeconfig-mode only applies when the
  # file is created. If it is unreadable the check below can never pass, so the
  # loop would spin for its whole timeout and report the cluster as down while
  # it is in fact perfectly healthy.
  if [ -e /etc/rancher/k3s/k3s.yaml ] && [ ! -r /etc/rancher/k3s/k3s.yaml ]; then
    step 'kubeconfig was not readable, fixing its mode'
    sudo chmod 644 /etc/rancher/k3s/k3s.yaml 2>/dev/null || true
  fi

  for _ in $(seq 1 40); do
    cluster_up && return 0
    sleep 5
  done
  warn 'cluster did not become reachable, see /tmp/k3s.log'
  return 1
}

# wait_pods <label-selector> <expected ready count> [loops of 10s]
wait_pods() {
  local sel="$1" want="$2" loops="${3:-60}"
  for _ in $(seq 1 "$loops"); do
    sleep 10
    [ "$(kubectl get pods -n "$NS" -l "$sel" --no-headers 2>/dev/null \
         | grep -c '1/1.*Running')" = "$want" ] && return 0
  done
  return 1
}

# Cassandra ring membership, which is NOT the same as pod readiness: the
# readiness probe answers as soon as CQL is listening, which happens before the
# node has joined the ring. Anything that depends on the ring must use this.
ring_up() {
  kubectl exec cassandra-0 -n "$NS" -- nodetool status 2>/dev/null | grep -c '^UN'
}

wait_ring() {
  local want="$1"
  for _ in $(seq 1 "${2:-90}"); do
    sleep 10
    [ "$(ring_up)" = "$want" ] && return 0
  done
  return 1
}

cql() { kubectl exec cassandra-0 -n "$NS" -- cqlsh -e "$1" 2>/dev/null; }

# Emits a manifest with CRLF stripped.
#
# The YAML in this repo was written on Windows and carries \r\n. kubectl does
# not care, so this never shows up during a normal deploy. But any sed anchored
# with $ silently fails to match, because the \r sits between the value and the
# end of the line: `s/^  replicas: [0-9]+$/` will not touch `  replicas: 3\r`.
# It does not error, it just quietly changes nothing, so a replica override
# appears to work and the full topology gets deployed anyway.
manifest() { sed -e 's/\r$//' "$1"; }

# Applies a manifest, overriding the declared replica count when running the
# laptop profile. The manifests declare the intended 3-replica topology; this
# keeps that honest in the repo while still being deployable on one machine.
apply_scaled() {
  local file="$1" replicas="$2"
  if [ "$PROFILE" = "full" ]; then
    kubectl apply -f "$file"
  else
    manifest "$file" \
      | sed -E "s/^  replicas: [0-9]+$/  replicas: ${replicas}/" \
      | kubectl apply -f -
  fi
}

mem() { free -m 2>/dev/null | awk '/^Mem:/ {printf "memory: %sMi used, %sMi available\n", $3, $7}'; }
