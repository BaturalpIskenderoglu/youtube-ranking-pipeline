#!/usr/bin/env bash
# Checks the kernel prerequisites, installs k3s if needed, and starts it.
#
# The two kernel settings cannot be applied from here: they live in
# %USERPROFILE%\.wslconfig on the Windows side and need `wsl --shutdown`. This
# script verifies them and tells you exactly what to put there if they are
# wrong, because both failures are extremely confusing when hit blind.

set -u
. "$(dirname "$0")/lib.sh"

say 'kernel prerequisites'

CGROUP="$(stat -fc %T /sys/fs/cgroup/ 2>/dev/null)"
if [ "$CGROUP" = "cgroup2fs" ]; then
  step 'cgroup v2: ok'
else
  warn "cgroup is '$CGROUP', Kubernetes 1.36+ will not start a kubelet on v1"
  NEEDS_WSLCONFIG=1
fi

if ip -6 addr show 2>/dev/null | grep -q 'inet6'; then
  warn 'IPv6 is enabled. WSL advertises AAAA records without a working IPv6'
  warn 'route, so image pulls fail intermittently with "Try again".'
  NEEDS_WSLCONFIG=1
else
  step 'IPv6 disabled: ok'
fi

if [ -n "${NEEDS_WSLCONFIG:-}" ]; then
  cat <<'EOF'

  Put this in %USERPROFILE%\.wslconfig, then run `wsl --shutdown` from
  PowerShell and start a new shell:

      [wsl2]
      kernelCommandLine = cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1 ipv6.disable=1
      memory=11GB

EOF
  die 'kernel prerequisites not met'
fi

TOTAL_MB="$(free -m | awk '/^Mem:/ {print $2}')"
step "memory available to this VM: ${TOTAL_MB}Mi"
[ "$TOTAL_MB" -lt 7000 ] && warn 'below ~7GB the stack will not fit even at one replica per store'

say 'k3s'

if command -v k3s >/dev/null 2>&1; then
  step 'k3s already installed'
else
  step 'installing k3s'
  # SKIP_ENABLE/SKIP_START because WSL has no systemd: the unit the installer
  # writes would never fire, and the install would look successful while
  # nothing runs.
  curl -sfL https://get.k3s.io \
    | INSTALL_K3S_SKIP_ENABLE=true INSTALL_K3S_SKIP_START=true sh - \
    || die 'k3s install failed'
fi

start_k3s || die 'k3s did not come up, see /tmp/k3s.log'

say 'cluster'
kubectl get nodes
step "$(mem)"

cat <<EOF

Cluster is up. KUBECONFIG=$KUBECONFIG

k3s does not survive 'wsl --shutdown'. After a WSL restart, run this script
again: it will detect the existing install and just restart the server. All
cluster state and persistent volumes survive on disk.

Next:  scripts/02_build_images.sh
EOF
