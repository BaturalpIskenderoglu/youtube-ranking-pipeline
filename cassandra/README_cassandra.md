# Cassandra Storage Service

This is the distributed storage layer of the pipeline. Spark writes the
processed video rows here and the ranking queries are served from it.

Runs as a 3 node StatefulSet. Each pod keeps its own data directory on its own
volume, so a pod that gets rescheduled comes back with the data it had before.

## Service

`cassandra` is a headless service. That is on purpose, a headless service makes
the DNS name resolve to all three pod IPs instead of a single virtual IP, and
that is exactly the contact point list the Cassandra driver expects. If it was a
normal ClusterIP service every connection would be load balanced to a random
node and token aware routing would not work.

The Spark job connects with the defaults from `spark_calculator.py`, so nothing
has to be passed to it:

```
CASSANDRA_HOST=cassandra
CASSANDRA_PORT=9042
```

Individual pods are also reachable one by one if needed:

```
cassandra-0.cassandra.video-pipeline.svc.cluster.local
cassandra-1.cassandra.video-pipeline.svc.cluster.local
cassandra-2.cassandra.video-pipeline.svc.cluster.local
```

## Volumes

The StatefulSet uses a `volumeClaimTemplates` entry, so Kubernetes creates one
PVC per pod automatically:

| Claim | Mount path | Size |
|:---|:---|:---|
| cassandra-storage-cassandra-0 | /var/lib/cassandra | 5Gi |
| cassandra-storage-cassandra-1 | /var/lib/cassandra | 5Gi |
| cassandra-storage-cassandra-2 | /var/lib/cassandra | 5Gi |

No storage class is set, so the cluster default is used. On minikube that is
`standard` and it provisions automatically.

Deleting the StatefulSet does **not** delete these PVCs. That is what makes the
node failure experiments work, you can kill a pod and it rejoins the ring with
its old data. To really start from scratch:

```
kubectl delete pvc -l app=cassandra -n video-pipeline
```

## Environment Variables

These are set inside the StatefulSet, they are listed here because they are the
ones worth changing.

**CASSANDRA_CLUSTER_NAME:** Name of the ring, all nodes must agree on it

**CASSANDRA_SEEDS:** Nodes a starting pod contacts to find the ring. Only the
first two pods, a seed list does not need every node in it

**CASSANDRA_ENDPOINT_SNITCH:** GossipingPropertyFileSnitch, needed for
NetworkTopologyStrategy to work

**CASSANDRA_DC / CASSANDRA_RACK:** dc1 / rack1. The keyspace replication refers
to `dc1`, so changing this means changing the schema too

**MAX_HEAP_SIZE / HEAP_NEWSIZE:** JVM heap. 1G is small for Cassandra but fine
for the dataset size in this project

**CASSANDRA_BROADCAST_ADDRESS:** Set in the start command to the pod's DNS name
instead of its IP. Pod IPs change on every restart, DNS names do not, so gossip
survives a reschedule

## Schema

`Cassandra_schema_job.yaml` creates the keyspace and the table. This has to run
before the Spark job, the Spark Cassandra connector writes into existing tables
but never creates them.

Keyspace `youtube_video_pipeline`, table `videos`, replication factor 3. RF 3
over 3 nodes means the default write consistency of the connector
(LOCAL_QUORUM, so 2 out of 3) still succeeds while one node is down, which is
the failure case the project wants to measure.

If you run fewer than 3 replicas, lower the replication factor in
`Cassandra_schema_configmap.yaml` first or every write will fail.

The partition key is `video_id` and the clustering key is `snapshot_date`
descending. So all snapshots of one video sit in one partition and the newest
one is the first row, which makes "latest state of this video" a single read.

Only the raw `videos` table is created here. The per language and per channel
ranking tables should be added to the same ConfigMap once the Spark job starts
writing them, since in Cassandra you model one table per query.

## Deploying

Order matters, the schema job waits for the ring but the Spark job does not
wait for the schema.

```
kubectl apply -f ../video_pipeline_namespace.yaml
kubectl apply -f Cassandra_headless_service.yaml
kubectl apply -f Cassandra_StatefulSet.yaml

kubectl rollout status statefulset/cassandra -n video-pipeline

kubectl apply -f Cassandra_schema_configmap.yaml
kubectl apply -f Cassandra_schema_job.yaml
```

The rollout takes a few minutes. Pods come up one at a time on purpose,
`podManagementPolicy: OrderedReady`, because two Cassandra nodes must never
bootstrap at the same time.

## Checking it

```
kubectl exec -it cassandra-0 -n video-pipeline -- nodetool status
```

All three should be `UN` (up, normal). Then:

```
kubectl exec -it cassandra-0 -n video-pipeline -- cqlsh -e "SELECT video_id, snapshot_date, engagement_score FROM youtube_video_pipeline.videos LIMIT 10;"
```

## Notes

Image is `cassandra:4.1.7`. The Spark job pulls
`com.datastax.spark:spark-cassandra-connector_2.12:3.5.1`, which is built and
tested against the 4.x line, so do not bump this to 5.x without checking the
connector first.

Resource requests are 500m CPU and 2Gi memory per pod, 6Gi total for the ring.
On a small laptop cluster drop `replicas` to 1 and set the keyspace replication
to 1, otherwise the pods will sit in Pending.
