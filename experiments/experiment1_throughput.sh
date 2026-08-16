#!/usr/bin/env bash
#
# Experiment 1 - processing throughput against Spark worker count.
#
# Replays a fixed backlog that is already sitting in Kafka and times how long
# each micro-batch takes, for 1, 2 and 3 workers.
#
# The producer is deliberately NOT running. If it were, the measured rate would
# be min(producer rate, spark rate), and since the producer sleeps between
# chunks we would mostly be measuring its sleep timer. Adding workers would
# then change nothing and the result would read as "spark does not scale", which
# would be a statement about the producer. Kafka keeps the messages after the
# producer exits, so the backlog can be replayed as many times as we like.
#
# Prerequisites:
#   - the topic already holds the backlog (run the producer once beforehand)
#   - kafka partition count >= the largest worker count, otherwise consumer
#     parallelism is capped by partitioning and the curve is flat regardless
#   - NUMBER_OF_CORES_PER_EXECUTOR on the driver <= --cores on a worker.
#     An executor lives inside one worker and cannot span two, so if the driver
#     asks for more cores than a single worker advertises no executor is ever
#     placed, and the stream waits forever without printing an error.
#
# Usage:  ./experiment1_throughput.sh [batches_per_run]

set -u

NS=video-pipeline
BATCHES=${1:-3}
REPO="$(cd "$(dirname "$0")/.." && pwd)"
: "${KUBECONFIG:=/etc/rancher/k3s/k3s.yaml}"
export KUBECONFIG

cd "$REPO" || exit 1

# superset plays no part in the measurement and its memory is better spent on
# the extra workers
kubectl scale deployment/superset   -n $NS --replicas=0 >/dev/null 2>&1
kubectl scale statefulset/superset-db -n $NS --replicas=0 >/dev/null 2>&1

kubectl apply -f spark/spark-worker-deployment.yaml >/dev/null
kubectl apply -f spark/spark-driver-deployment.yaml >/dev/null
sleep 15

echo '=========== EXPERIMENT 1: throughput vs spark worker count ==========='
kubectl exec kafka-0 -n $NS -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-bootstrap:9092 --describe --topic youtube_videos 2>/dev/null | head -1
echo "batches sampled per run: $BATCHES"
echo 'each batch writes: videos + 3 ranking tables + elasticsearch'
echo

for N in 1 2 3; do
  kubectl scale deployment/spark-worker -n $NS --replicas=$N >/dev/null

  for _ in $(seq 1 24); do
    sleep 10
    r=$(kubectl get pods -n $NS -l app=spark-worker --no-headers 2>/dev/null | grep -c '1/1.*Running')
    [ "$r" = "$N" ] && break
  done
  ready=$(kubectl get pods -n $NS -l app=spark-worker --no-headers 2>/dev/null | grep -c '1/1.*Running')

  # a fresh checkpoint makes the stream replay the same backlog from offset 0,
  # so every run processes byte-identical input
  kubectl scale deployment/spark-driver -n $NS --replicas=0 >/dev/null
  sleep 10
  kubectl delete pvc spark-checkpoint -n $NS --ignore-not-found >/dev/null 2>&1
  sleep 5
  kubectl apply -f spark/spark-checkpoint-pvc.yaml >/dev/null
  kubectl scale deployment/spark-driver -n $NS --replicas=1 >/dev/null

  got=0
  POD=''
  for _ in $(seq 1 60); do
    sleep 15
    POD=$(kubectl get pods -n $NS -l app=spark-driver --sort-by=.metadata.creationTimestamp \
            -o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null)
    got=$(kubectl logs "$POD" -n $NS 2>/dev/null | grep -c 'METRIC batch=')
    [ "$got" -ge "$BATCHES" ] && break
  done

  echo "---- workers=$ready (requested $N) ----"
  if [ "$got" -eq 0 ]; then
    echo '  no batches completed within 15 minutes'
    echo '  check: kubectl logs -l app=spark-master -n '"$NS"' | grep "Launching executor"'
    echo '  zero launches means the executor core request cannot be satisfied'
  else
    kubectl logs "$POD" -n $NS 2>/dev/null | grep 'METRIC batch=' | head -"$BATCHES" | sed 's/^/  /'
    kubectl logs "$POD" -n $NS 2>/dev/null | grep 'METRIC batch=' | head -"$BATCHES" \
      | sed -E 's/.*rows_per_second=([0-9.]+).*/\1/' \
      | awk '{s+=$1; n++} END {if(n>0) printf "  MEAN rows_per_second = %.1f  (n=%d)\n", s/n, n}'
    echo '  note: batch 0 includes JIT warmup and first connection to both sinks,'
    echo '        so steady state is better read from the later batches'
  fi
  echo
done

echo '--- node memory at end ---'
kubectl top node 2>/dev/null | tail -1
