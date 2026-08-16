#!/usr/bin/env bash
# Builds the three locally-built images and imports them into the cluster's
# containerd. There is no registry, so every manifest that uses them sets
# imagePullPolicy: Never.

set -u
. "$(dirname "$0")/lib.sh"

need docker
docker info >/dev/null 2>&1 || die 'the docker daemon is not reachable (start Docker Desktop)'

build_and_import() {  # build_and_import <tag> <context dir>
  local tag="$1" ctx="$2"
  step "building $tag"
  docker build -t "$tag" "$ctx" >/tmp/build.log 2>&1 \
    || { tail -20 /tmp/build.log; die "build failed for $tag"; }

  step "importing $tag into containerd"
  local tar="/tmp/$(echo "$tag" | tr ':/' '__').tar"
  docker save "$tag" -o "$tar" || die "docker save failed for $tag"

  # -n k8s.io is not optional. `k3s ctr` defaults to the `default` containerd
  # namespace, which the kubelet never looks in, so the import reports success
  # and the pod still cannot find the image.
  sudo k3s ctr -n k8s.io images import "$tar" >/dev/null || die "import failed for $tag"
  rm -f "$tar"
}

say 'building images'
build_and_import video-producer:latest        "$REPO/video_producer"
build_and_import spark-calculator:latest      "$REPO/spark"
build_and_import youtube-pipeline/superset:1.0 "$REPO/superset"

say 'present in the cluster'
sudo k3s ctr -n k8s.io images ls 2>/dev/null \
  | awk '{print $1}' \
  | grep -E 'video-producer|spark-calculator|youtube-pipeline/superset' \
  | sed 's/^/  /'

cat <<'EOF'

Note: the spark image carries all three connector jars and is used for the
master, the workers AND the driver. Executors deserialize lambdas that live in
those jars, so a driver holding jars the executors lack fails with
"ClassCastException: cannot assign instance of SerializedLambda", which says
nothing about a missing jar. One image everywhere avoids it by construction.

Next:  scripts/03_deploy_all.sh
EOF
