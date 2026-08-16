#!/usr/bin/env bash
#
# Experiment 2 - Cassandra replica failure, availability and recovery.
#
# Builds a 3 node ring at replication factor 3, then removes a replica two
# different ways:
#
#   Phase A - force delete the pod. The StatefulSet recreates it, so this
#             measures recovery time and whether any rows were lost.
#   Phase B - scale the ring down so the replica stays gone. Only a sustained
#             outage can demonstrate the availability claim, because recovery
#             in phase A takes about 25 seconds and a read issued after that
#             says nothing about behaviour while a replica is missing.
#
# Two things this script is careful about, both of which invalidated an earlier
# version of it:
#
#   1. Ring membership is read from nodetool, never from pod readiness. The
#      readiness probe is `nodetool statusbinary`, which answers as soon as CQL
#      is listening - which happens before the node has joined the ring.
#      Waiting on pod Ready ran the whole experiment against a 2 node cluster.
#
#   2. The baseline row count is asserted before anything is killed. Raising
#      the replication factor without a full repair leaves the new replicas
#      empty, and a quorum read then returns a fraction of the data with no
#      error at all. If the baseline is wrong the run is aborted, because
#      before/during/after counts that match each other while all being wrong
#      look exactly like a clean result.
#
# The cluster cannot hold three Cassandra replicas at the same time as Kafka,
# Spark and Elasticsearch, so those tiers are scaled to zero first. The data
# written by the pipeline stays on the persistent volumes.
#
# Usage:  ./experiment2_node_failure.sh <expected_row_count>

set -u

NS=video-pipeline
EXPECTED=${1:-}
: "${KUBECONFIG:=/etc/rancher/k3s/k3s.yaml}"
export KUBECONFIG

if [ -z "$EXPECTED" ]; then
  echo 'usage: experiment2_node_failure.sh <expected_row_count>'
  echo 'pass the row count the pipeline actually produced, so the script can'
  echo 'refuse to measure a half replicated ring'
  exit 1
fi

cql()      { kubectl exec cassandra-0 -n $NS -- cqlsh -e "$1" 2>/dev/null; }
count_at() { cql "CONSISTENCY $1; SELECT COUNT(*) FROM youtube_video_pipeline.videos;" \
             | grep -E '^ +[0-9]+' | head -1 | tr -d ' '; }
up_nodes() { kubectl exec cassandra-0 -n $NS -- nodetool status 2>/dev/null | grep -c '^UN'; }
ring()     { kubectl exec cassandra-0 -n $NS -- nodetool status 2>/dev/null | grep -E '^UN|^DN'; }

echo '========= EXPERIMENT 2: replica failure on a 3 node ring ========='
echo
echo 'NOTE: single host cluster. The three Cassandra nodes are three pods on'
echo 'one machine, so this exercises replica failure, not host failure.'
echo

echo '--- freeing memory: scaling the other tiers down ---'
kubectl scale deployment/spark-driver   -n $NS --replicas=0 >/dev/null 2>&1
kubectl scale deployment/spark-worker   -n $NS --replicas=0 >/dev/null 2>&1
kubectl scale deployment/spark-master   -n $NS --replicas=0 >/dev/null 2>&1
kubectl scale statefulset/kafka         -n $NS --replicas=0 >/dev/null 2>&1
kubectl scale statefulset/elasticsearch -n $NS --replicas=0 >/dev/null 2>&1
sleep 45

echo '--- growing the ring to 3 ---'
kubectl scale statefulset/cassandra -n $NS --replicas=3 >/dev/null
for i in $(seq 1 90); do
  sleep 10
  u=$(up_nodes)
  echo "  [$((i*10))s] nodes UN: $u/3"
  [ "$u" = "3" ] && break
done
if [ "$(up_nodes)" != "3" ]; then
  echo 'ABORT: ring never reached 3 nodes'
  exit 1
fi

echo
echo '--- replication factor 3 + full repair ---'
# without the repair the new replicas hold no data, and a quorum read silently
# returns a fraction of the rows
cql "ALTER KEYSPACE youtube_video_pipeline WITH replication = {'class':'NetworkTopologyStrategy','dc1':3};" >/dev/null
for p in 0 1 2; do
  kubectl exec cassandra-$p -n $NS -- nodetool repair -full youtube_video_pipeline >/dev/null 2>&1
done
ring

echo
echo '--- baseline ---'
BEFORE=$(count_at QUORUM)
echo "rows at QUORUM: $BEFORE  (expected $EXPECTED)"
if [ "$BEFORE" != "$EXPECTED" ]; then
  echo 'ABORT: baseline does not match, the ring is not fully replicated.'
  echo 'Measuring now would produce matching but meaningless numbers.'
  exit 1
fi

echo
echo '================= PHASE A: recovery from pod loss ================='
KILL_AT=$(date +%s)
kubectl delete pod cassandra-2 -n $NS --grace-period=0 --force >/dev/null 2>&1
echo 'cassandra-2 force deleted'

REJOIN_AT=''
for _ in $(seq 1 90); do
  sleep 5
  [ "$(up_nodes)" = "3" ] && { REJOIN_AT=$(date +%s); break; }
done
if [ -n "$REJOIN_AT" ]; then
  echo "recovery time: $((REJOIN_AT - KILL_AT)) seconds (kill -> 3 nodes UN again)"
else
  echo 'node did not rejoin within 7.5 minutes'
fi
ring
AFTER_A=$(count_at QUORUM)
echo "rows after recovery: $AFTER_A"

echo
echo '============ PHASE B: availability during a real outage ============'
# scaling down keeps the replica away, unlike deleting the pod which the
# StatefulSet immediately recreates
kubectl scale statefulset/cassandra -n $NS --replicas=2 >/dev/null
for _ in $(seq 1 30); do
  sleep 6
  [ "$(up_nodes)" = "2" ] && break
done
ring

echo
echo '--- QUORUM (needs 2 of 3) ---'
Q=$(count_at QUORUM)
if [ -n "$Q" ]; then echo "rows: $Q   -> SUCCEEDS"; else echo 'FAILED'; fi

echo '--- ALL (needs 3 of 3) ---'
cql "CONSISTENCY ALL; SELECT COUNT(*) FROM youtube_video_pipeline.videos;" 2>&1 \
  | grep -oE 'Cannot achieve consistency level ALL|[0-9]+' | head -1 \
  | sed 's/^/  /' || echo '  FAILED as expected'
kubectl exec cassandra-0 -n $NS -- cqlsh -e \
  "CONSISTENCY ALL; SELECT COUNT(*) FROM youtube_video_pipeline.videos;" 2>&1 \
  | grep -o "required_replicas.*alive_replicas[^}]*" | head -1 | sed 's/^/  /'

echo
echo '--- restoring the ring to 3 ---'
kubectl scale statefulset/cassandra -n $NS --replicas=3 >/dev/null
for _ in $(seq 1 60); do
  sleep 10
  [ "$(up_nodes)" = "3" ] && break
done
ring

echo
echo '=========================== RESULT ==========================='
echo "rows before failure    : $BEFORE"
echo "rows after recovery    : $AFTER_A"
echo "rows at QUORUM, 2 of 3 : $Q"
[ "$BEFORE" = "$AFTER_A" ] && echo 'data loss              : NONE' \
                           || echo 'data loss              : counts differ'
echo
echo 'To shrink the ring afterwards, lower the replication factor AFTER'
echo 'removing nodes, and remove them with nodetool decommission or'
echo 'nodetool removenode. kubectl scale alone deletes the pod without taking'
echo 'the node out of the ring, so its token ranges keep pointing at something'
echo 'that no longer exists.'
