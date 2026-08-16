#!/usr/bin/env bash
# Deploys the whole pipeline in dependency order and verifies each tier before
# moving on.
#
#   PROFILE=laptop (default)  one replica of Cassandra and Elasticsearch, fits ~11GB
#   PROFILE=full              the replica counts declared in the manifests, needs ~20GB
#
#   DATASET_DIR=/opt/video-dataset   where dataset_partition-0..4.csv live on the node
#
# Safe to re-run: every step is either idempotent or skipped when already done.

set -u
. "$(dirname "$0")/lib.sh"

require_cluster
say "profile: $PROFILE   namespace: $NS"
step "$(mem)"

# --- namespace --------------------------------------------------------------
say 'namespace'
kubectl apply -f "$REPO/video_pipeline_namespace.yaml"

# --- dataset ----------------------------------------------------------------
say 'dataset'
missing=0
for i in 0 1 2 3 4; do
  [ -f "$DATASET_DIR/dataset_partition-$i.csv" ] || missing=1
done
if [ "$missing" = "1" ]; then
  warn "expected dataset_partition-0..4.csv in $DATASET_DIR"
  warn 'generate them with dataset/dataset_sort.ipynb: split on video_id so all'
  warn 'observations of a video stay together, and fill empty numeric cells or'
  warn 'the producer dies on int(nan) partway through the stream.'
  die 'dataset not in place'
fi
step "found 5 partitions in $DATASET_DIR"

# the PV points at a fixed path; rewrite it to whatever DATASET_DIR is
sed -E "s#^( *path: ).*#\1$DATASET_DIR#" "$REPO/video_producer/video-dataset-pv.yaml" \
  | kubectl apply -f -
kubectl apply -f "$REPO/video_producer/video_dataset.yaml"

# --- cassandra --------------------------------------------------------------
say 'cassandra'
kubectl apply -f "$REPO/cassandra/Cassandra_headless_service.yaml"
apply_scaled "$REPO/cassandra/Cassandra_StatefulSet.yaml" 1
CASS_REPLICAS=$([ "$PROFILE" = "full" ] && echo 3 || echo 1)
wait_pods 'app=cassandra' "$CASS_REPLICAS" 90 || die 'cassandra did not become ready'
wait_ring "$CASS_REPLICAS" 60 || warn 'pods are ready but the ring is not fully formed yet'
step "ring: $(ring_up) node(s) UN"

# replication factor has to match the node count or every write fails for want
# of replicas that do not exist
say 'schema'
sed "s/'dc1': 3/'dc1': $CASS_REPLICAS/" "$REPO/cassandra/Cassandra_schema_configmap.yaml" \
  | kubectl apply -f -
kubectl delete job cassandra-schema-init -n "$NS" --ignore-not-found >/dev/null 2>&1
kubectl apply -f "$REPO/cassandra/Cassandra_schema_job.yaml"
kubectl wait --for=condition=complete job/cassandra-schema-init -n "$NS" --timeout=10m \
  || die 'schema job failed; the connector writes into existing tables and never creates them'
step 'keyspace and 4 tables created'

# --- elasticsearch ----------------------------------------------------------
say 'elasticsearch'
kubectl apply -f "$REPO/elasticsearch/Elasticsearch_headless_service.yaml"
kubectl apply -f "$REPO/elasticsearch/Elasticsearch_service.yaml"

ES_REPLICAS=$([ "$PROFILE" = "full" ] && echo 3 || echo 1)
if [ "$PROFILE" = "full" ]; then
  kubectl apply -f "$REPO/elasticsearch/Elasticsearch_StatefulSet.yaml"
else
  # dropping to one replica means initial_master_nodes must shrink too, or the
  # node waits forever for a 2-of-3 quorum while reporting 1/1 Running
  manifest "$REPO/elasticsearch/Elasticsearch_StatefulSet.yaml" \
    | sed -E -e 's/^  replicas: [0-9]+$/  replicas: 1/' \
             -e 's/^(          value: )elasticsearch-0,elasticsearch-1,elasticsearch-2$/\1elasticsearch-0/' \
    | kubectl apply -f -
fi
wait_pods 'app=elasticsearch' "$ES_REPLICAS" 90 || die 'elasticsearch did not become ready'

# the template must be registered BEFORE the first write: inference would make
# dates text and scores single precision, and mappings are immutable
say 'index template'
if [ "$PROFILE" = "full" ]; then
  kubectl apply -f "$REPO/elasticsearch/Elasticsearch_index_template_configmap.yaml"
else
  sed -e 's/"number_of_shards": 3/"number_of_shards": 1/' \
      -e 's/"number_of_replicas": 1/"number_of_replicas": 0/' \
      "$REPO/elasticsearch/Elasticsearch_index_template_configmap.yaml" | kubectl apply -f -
fi
kubectl delete job elasticsearch-index-template -n "$NS" --ignore-not-found >/dev/null 2>&1
kubectl apply -f "$REPO/elasticsearch/Elasticsearch_index_template_job.yaml"
kubectl wait --for=condition=complete job/elasticsearch-index-template -n "$NS" --timeout=10m \
  || die 'index template job failed'

# --- kafka ------------------------------------------------------------------
say 'kafka'
kubectl apply -f "$REPO/video_producer/Kafka_headless_service.yaml"
kubectl apply -f "$REPO/video_producer/Kafka_bootstrap_service.yaml"
kubectl apply -f "$REPO/video_producer/Kafka_StatefulSet.yaml"
wait_pods 'app=kafka' 3 90 || die 'kafka brokers did not become ready'

say 'topic'
if kubectl exec kafka-0 -n "$NS" -- /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server kafka-bootstrap:9092 --list 2>/dev/null | grep -qx "$TOPIC"; then
  step "topic $TOPIC already exists"
else
  kubectl exec kafka-0 -n "$NS" -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-bootstrap:9092 \
    --create --topic "$TOPIC" --partitions "$KAFKA_PARTITIONS" --replication-factor 3 2>&1 | tail -1
fi

# --- spark ------------------------------------------------------------------
say 'spark'
kubectl apply -f "$REPO/spark/spark-master-service.yaml"
kubectl apply -f "$REPO/spark/spark-master-deployment.yaml"
wait_pods 'app=spark-master' 1 30 || die 'spark master did not start'
kubectl apply -f "$REPO/spark/spark-worker-deployment.yaml"
wait_pods 'app=spark-worker' 1 30 || die 'spark worker did not start'
kubectl apply -f "$REPO/spark/spark-checkpoint-pvc.yaml"
kubectl apply -f "$REPO/spark/spark-driver-deployment.yaml"
wait_pods 'app=spark-driver' 1 40 || warn 'driver not ready yet, check its logs'

# Ask the master how many workers it currently has, rather than grepping its log
# for a registration line: on a master that has been up a while that line has
# scrolled away, and the check produces a misleading warning about a cluster
# that is working fine.
# parsed rather than grepped: the master pretty-prints its JSON as
# "aliveworkers" : 1, with spaces around the colon, which a naive pattern misses
WORKERS_ALIVE="$(kubectl exec deploy/spark-master -n "$NS" -- python3 -c \
  'import json,urllib.request; print(json.load(urllib.request.urlopen("http://localhost:8080/json/"))["aliveworkers"])' \
  2>/dev/null)"

if [ -n "$WORKERS_ALIVE" ] && [ "$WORKERS_ALIVE" -gt 0 ]; then
  step "spark master reports $WORKERS_ALIVE live worker(s)"
else
  warn 'the master reports no live workers.'
  warn 'if no batch ever completes, check that NUMBER_OF_CORES_PER_EXECUTOR is'
  warn 'not larger than a single worker --cores: an executor cannot span two'
  warn 'workers, so an oversized request is never placed and the stream waits'
  warn 'forever without printing an error.'
fi

# --- superset ---------------------------------------------------------------
say 'superset'
kubectl apply -f "$REPO/superset/Superset_secret.yaml"
kubectl apply -f "$REPO/superset/Superset_metadb_service.yaml"
kubectl apply -f "$REPO/superset/Superset_metadb_statefulset.yaml"
wait_pods 'app=superset-db' 1 40 || die 'superset metadata db did not start'
kubectl apply -f "$REPO/superset/Superset_deployment.yaml"
kubectl apply -f "$REPO/superset/Superset_service.yaml"
wait_pods 'app=superset' 1 60 || warn 'superset not ready yet'

# --- done -------------------------------------------------------------------
say 'deployed'
kubectl get pods -n "$NS" --no-headers 2>/dev/null | grep -v Completed | awk '{print "  "$1, $2, $3}'
step "$(mem)"

IP="$(hostname -I | awk '{print $1}')"
cat <<EOF

Superset:  http://$IP:30088   (admin / admin)
  On WSL, localhost forwarding to the NodePort does not work, use the address
  above. It changes when WSL restarts.

Start the stream:
  kubectl apply -f $REPO/video_producer/Producer_job.yaml

Then watch it land:
  kubectl logs -l app=spark-driver -n $NS | grep METRIC
  kubectl exec cassandra-0 -n $NS -- cqlsh -e 'SELECT COUNT(*) FROM youtube_video_pipeline.videos;'
  kubectl exec elasticsearch-0 -n $NS -- curl -s localhost:9200/$TOPIC/_count
EOF
