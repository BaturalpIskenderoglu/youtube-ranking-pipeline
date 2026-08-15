# Elasticsearch Indexing Service

This is the search and indexing layer. Spark writes every processed video into
the `youtube_videos` index, and Superset reads its charts from the same index.

Runs as a 3 node StatefulSet, every node is master eligible and holds data.

## Services

There are two, they do different jobs.

**elasticsearch-headless** is the governing service of the StatefulSet. It is
headless and publishes not ready addresses, because the nodes have to resolve
each other while the cluster is still forming, which is before any of them
reports ready. Only Elasticsearch itself uses this one.

**elasticsearch** is a normal ClusterIP service on port 9200. This is what Spark
and Superset connect to. It only routes to ready pods, so a node that is
restarting is taken out of rotation on its own.

The Spark job connects with its defaults, nothing has to be passed:

```
ELASTICSEARCH_NODES=elasticsearch
ELASTICSEARCH_PORT=9200
ELASTICSEARCH_INDEX=youtube_videos
```

One thing worth knowing: the Elasticsearch Spark connector does not keep talking
to the service IP. It asks the cluster for its node list on the first call and
then writes straight to the pod IPs. That works inside the cluster and it is
what you want, the writes spread across all three nodes instead of going through
one service IP. It is also why `es.nodes.wan.only` is not set.

## Volumes

One PVC per pod through `volumeClaimTemplates`:

| Claim | Mount path | Size |
|:---|:---|:---|
| elasticsearch-storage-elasticsearch-0 | /usr/share/elasticsearch/data | 5Gi |
| elasticsearch-storage-elasticsearch-1 | /usr/share/elasticsearch/data | 5Gi |
| elasticsearch-storage-elasticsearch-2 | /usr/share/elasticsearch/data | 5Gi |

No storage class, the cluster default is used.

The pod runs as uid 1000 and the volume comes up owned by root, so the pod sets
`fsGroup: 1000`. Without it Elasticsearch cannot write to its own data
directory and crash loops on start.

PVCs stay after the StatefulSet is deleted. To wipe the indices:

```
kubectl delete pvc -l app=elasticsearch -n video-pipeline
```

## Environment Variables

**cluster.name:** youtube-ranking, all nodes must agree

**node.name:** taken from the pod name through the downward API

**discovery.seed_hosts:** `elasticsearch-headless`. One entry is enough, the
headless service resolves to every pod

**cluster.initial_master_nodes:** the three node names. This is only read the
very first time the cluster forms, it is ignored afterwards

**xpack.security.enabled:** false. No TLS and no passwords, the cluster is only
reachable inside the namespace. If this ever gets exposed outside the lab it
has to be turned back on

**ES_JAVA_OPTS:** `-Xms1g -Xmx1g`, min and max should always be equal

## Index Template

`Elasticsearch_index_template_job.yaml` registers the mapping for
`youtube_videos*` before anything writes to it.

This is not optional in practice. Elasticsearch would create the index by itself
on Spark's first write, but dynamic mapping turns `snapshot_date` into text and
the scores into float, and then sorting by date or aggregating on
`engagement_score` in Superset either fails or gives wrong numbers. Reindexing
afterwards is worse than doing it up front.

`title` and `channel_name` are mapped as `text` with a `.keyword` sub field.
The `text` side is what makes free text video search work, the `.keyword` side
is what Superset groups by. So `title` for searching, `title.keyword` for
grouping.

The template applies to any index matching `youtube_videos*`, so if the index is
later rolled over by date it keeps the same mapping.

## Deploying

```
kubectl apply -f ../video_pipeline_namespace.yaml
kubectl apply -f Elasticsearch_headless_service.yaml
kubectl apply -f Elasticsearch_service.yaml
kubectl apply -f Elasticsearch_StatefulSet.yaml

kubectl rollout status statefulset/elasticsearch -n video-pipeline

kubectl apply -f Elasticsearch_index_template_configmap.yaml
kubectl apply -f Elasticsearch_index_template_job.yaml
```

Run the template job before the Spark job.

## Checking it

```
kubectl exec -it elasticsearch-0 -n video-pipeline -- curl -s localhost:9200/_cluster/health?pretty
```

`status` should be `green` and `number_of_nodes` should be 3. Yellow with 3
nodes means replicas are not assigned yet, give it a minute.

Once Spark has written something:

```
kubectl exec -it elasticsearch-0 -n video-pipeline -- curl -s "localhost:9200/youtube_videos/_count?pretty"
```

Top videos by engagement score:

```
kubectl exec -it elasticsearch-0 -n video-pipeline -- curl -s "localhost:9200/youtube_videos/_search?pretty&size=5&sort=engagement_score:desc"
```

## Notes

Image is `docker.elastic.co/elasticsearch/elasticsearch:9.4.3`, matching the
`org.elasticsearch:elasticsearch-spark-30_2.12:9.4.3` connector the Spark job
pulls. These two should be bumped together.

The `raise-max-map-count` init container runs privileged and sets
`vm.max_map_count` to 262144. Elasticsearch refuses to start below that.
minikube and kind usually already have it high enough, but on a plain node they
do not, and the init container is cheaper than telling everyone to ssh into
their nodes. If the cluster blocks privileged pods, drop the init container and
set the sysctl on the node instead.

Requests are 500m CPU and 2Gi memory per pod. On a small cluster you can drop
`replicas` to 1, but three things have to change together or it will not work:

1. `replicas: 1` in the StatefulSet
2. `cluster.initial_master_nodes` down to just `elasticsearch-0`
3. `number_of_replicas` to 0 in the index template

Number 2 is the one that bites. Bootstrapping needs a majority of the nodes
listed in `cluster.initial_master_nodes`, so leaving all three names there with
only one pod running means the cluster waits forever for a quorum of 2 that is
never coming. The pod still goes `1/1 Running` while this happens, because the
readiness probe uses `?local=true` and that answers fine on a node with no
master. The give-away is `master not discovered yet` repeating in the pod log.

The probe is deliberately lenient like that: a stricter check would deadlock a
genuine 3 node bootstrap, since none of the pods can reach a quorum until all of
them are up and reachable. So on this StatefulSet, `Ready` means "the process is
answering", not "the cluster is usable". Check `_cluster/health` for the latter.
