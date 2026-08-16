#!/usr/bin/env bash
# Puts the cluster back to the demo topology after experiment 2, which leaves
# Cassandra at 3 replicas with RF=3 and everything else scaled to zero.
#
# The order is the whole point of this script.
#
# Shrinking a Cassandra ring is NOT `kubectl scale`. That deletes the pod
# without taking the node out of the ring, so Cassandra keeps believing it
# exists and is merely down. Its token ranges then point at nothing and reads
# fail with:
#
#   Cannot achieve consistency level ONE
#   info={'consistency':'ONE','required_replicas':1,'alive_replicas':0}
#
# even though every row is still on disk. Nodes must be removed from the ring
# first, and the replication factor lowered only afterwards.

set -u
. "$(dirname "$0")/../scripts/lib.sh"

require_cluster
say 'restoring demo topology'
step "$(mem)"

CURRENT_RING="$(ring_up)"
step "cassandra nodes currently in the ring: $CURRENT_RING"

if [ "$CURRENT_RING" -gt 1 ]; then
  say 'removing extra nodes from the ring'
  # everything except the node cassandra-0 is running as, identified by host id
  SELF_ID="$(kubectl exec cassandra-0 -n "$NS" -- nodetool info 2>/dev/null \
             | awk -F': *' '/^ID/ {print $2}' | tr -d ' \r')"
  step "keeping host id $SELF_ID"

  for id in $(kubectl exec cassandra-0 -n "$NS" -- nodetool status 2>/dev/null \
              | awk '/^UN|^DN/ {print $6}'); do
    [ "$id" = "$SELF_ID" ] && continue
    step "removenode $id"
    kubectl exec cassandra-0 -n "$NS" -- nodetool removenode "$id" >/dev/null 2>&1 \
      || warn "removenode $id did not complete cleanly"
  done

  say 'shrinking the statefulset'
  kubectl scale statefulset/cassandra -n "$NS" --replicas=1 >/dev/null
  sleep 30
fi

# only now, once the ring really is one node
say 'replication factor back to 1'
cql "ALTER KEYSPACE youtube_video_pipeline WITH replication = {'class':'NetworkTopologyStrategy','dc1':1};" >/dev/null
cql "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name='youtube_video_pipeline';" | sed -n '4p'

step "ring is now $(ring_up) node(s)"
ROWS="$(cql 'SELECT COUNT(*) FROM youtube_video_pipeline.videos;' | grep -E '^ +[0-9]+' | head -1 | tr -d ' ')"
step "videos table: ${ROWS:-unreadable} rows"
[ -z "$ROWS" ] && warn 'reads are failing; check nodetool status for leftover DN entries'

# --- bring the rest back ----------------------------------------------------
say 'restarting the other tiers'

step 'elasticsearch'
kubectl scale statefulset/elasticsearch -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=elasticsearch' 1 40 || warn 'elasticsearch not ready'

step 'kafka'
kubectl scale statefulset/kafka -n "$NS" --replicas=3 >/dev/null
wait_pods 'app=kafka' 3 40 || warn 'kafka not ready'

step 'spark master and worker'
kubectl scale deployment/spark-master -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=spark-master' 1 20 || warn 'spark master not ready'
kubectl scale deployment/spark-worker -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=spark-worker' 1 20 || warn 'spark worker not ready'

step 'superset'
kubectl scale statefulset/superset-db -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=superset-db' 1 30 || warn 'superset-db not ready'
kubectl scale deployment/superset -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=superset' 1 40 || warn 'superset not ready'

# last: heaviest, and it starts streaming as soon as it is up
step 'spark driver'
kubectl scale deployment/spark-driver -n "$NS" --replicas=1 >/dev/null
wait_pods 'app=spark-driver' 1 40 || warn 'spark driver not ready'

# --- report -----------------------------------------------------------------
say 'final state'
kubectl get pods -n "$NS" --no-headers 2>/dev/null | grep -v Completed | awk '{print "  "$1, $2, $3}'
echo
step "$(mem)"

ES_COUNT="$(kubectl exec elasticsearch-0 -n "$NS" -- \
  curl -s localhost:9200/"$TOPIC"/_count 2>/dev/null | grep -oE '"count":[0-9]+' | cut -d: -f2)"
step "elasticsearch documents: ${ES_COUNT:-unreadable}"

IP="$(hostname -I | awk '{print $1}')"
echo
echo "Superset: http://$IP:30088  (admin / admin)"
