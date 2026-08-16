# youtube-ranking-pipeline

This project aims to build an event-driven stream processing pipeline on Kubernetes to rank, store and search Youtube videos.

A static dataset of trending videos across 113 countries is replayed through
Kafka to simulate a live stream. Spark Structured Streaming computes engagement
metrics on each record and writes every batch to two stores at once: Cassandra
for ranking queries, Elasticsearch for full text search. Superset reads the
index and renders the ranking dashboards. Everything runs as containerised
services on Kubernetes.

```
  dataset (5 slices)
        |
        v
  producer Job  --->  Kafka  --->  Spark Structured Streaming
   (5 indexed pods)   (5 parts,     (master + workers + driver)
                       RF 3)               |
                                           +--> Cassandra   videos + 3 ranking tables
                                           |
                                           +--> Elasticsearch   youtube_videos index
                                                      |
                                                      v
                                                  Superset
```

## Why two stores

They answer different questions and neither can do the other's job.

**Cassandra** serves the rankings. It cannot sort or filter on non-key columns,
so the schema is built from the queries: one table per access pattern, each
partitioned on the ranking dimension and clustered by `engagement_score`
descending. Top-k is then a single partition read with a `LIMIT`, no sorting at
query time and no aggregation in the stream. It also keeps the full observation
history, one row per video per snapshot date.

**Elasticsearch** serves search and the dashboards. Titles are indexed as
analysed text with a `keyword` sub-field, so the same field supports free text
retrieval and exact grouping. It keeps only the latest state per video, because
the document id is `video_id`.

That difference is visible in the row counts: from 50,000 messages, Cassandra
holds 25,659 rows (distinct video/date pairs) and Elasticsearch 9,712 documents
(distinct videos). Nothing is lost; the keys collapse differently by design.

## Repository layout

| Path | |
|:---|:---|
| `scripts/` | cluster setup, image builds, full deploy |
| `experiments/` | the two measurements and a restore script |
| `video_producer/` | Kafka manifests and the dataset producer |
| `spark/` | Spark standalone manifests and the streaming job |
| `cassandra/` | StatefulSet, services, schema |
| `elasticsearch/` | StatefulSet, services, index template |
| `superset/` | image, Postgres metadata store, deployment |
| `dataset/` | notebook that slices the source CSV |
| `paper/` | the write-up |

Each component directory has its own README covering the design decisions and
the failure modes specific to it. `KUBERNETES_README.md` is the manual,
step-by-step deployment guide; the scripts below automate the same thing.

## Quick start

From a machine with nothing installed:

```bash
./scripts/01_setup_cluster.sh     # kernel checks, install and start k3s
./scripts/02_build_images.sh      # build the 3 local images, import to containerd
./scripts/03_deploy_all.sh        # deploy everything in dependency order
```

**Set `KUBECONFIG` in your own shell** before running `kubectl` by hand:

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

The scripts export it internally, so they work regardless, but a bare `kubectl`
does not inherit that. If Docker Desktop's WSL integration has been enabled at
any point it will have left a `~/.kube/config` pointing at its own cluster, and
without `KUBECONFIG` set every command goes there instead:

```
Get "https://127.0.0.1:64069/api?timeout=32s": connect: connection refused
```

Port 64069 is Docker Desktop; k3s serves 6443. Add the export to `~/.bashrc` to
make it stick.

Then start the stream:

```bash
kubectl apply -f video_producer/Producer_job.yaml
```

and watch it land:

```bash
kubectl logs -l app=spark-driver -n video-pipeline | grep METRIC
kubectl exec cassandra-0 -n video-pipeline -- cqlsh -e \
  'SELECT COUNT(*) FROM youtube_video_pipeline.videos;'
kubectl exec elasticsearch-0 -n video-pipeline -- curl -s localhost:9200/youtube_videos/_count
```

Superset is on the node's port 30088, `admin` / `admin`. On WSL use the VM
address rather than localhost:

```bash
echo "http://$(hostname -I | awk '{print $1}'):30088"
```

### Before you run it

**The dataset must be sliced first.** The producer is an indexed Job with five
pods, each reading `dataset_partition-N.csv`. Generate them with
`dataset/dataset_sort.ipynb`, split on `video_id` so all observations of a video
stay together, and put them in `/opt/video-dataset` on the node (override with
`DATASET_DIR`).

**Two profiles.** The manifests declare three replicas of Cassandra,
Elasticsearch and Kafka, which is the intended topology and needs about 20 GB.
`PROFILE=laptop` is the default and rewrites the replica counts on the way in,
along with the three settings that have to change with them. `PROFILE=full`
applies the manifests as written.

```bash
PROFILE=full ./scripts/03_deploy_all.sh
```

**k3s does not survive `wsl --shutdown`.** Re-run `scripts/01_setup_cluster.sh`
after a restart; it detects the existing install and just restarts the server.
Cluster state and volumes persist.

**If `kubectl` suddenly reports `permission denied` on the kubeconfig**, run
`scripts/01_setup_cluster.sh`. k3s rewrites `/etc/rancher/k3s/k3s.yaml` during
its lifetime and can leave it root-only again; `--write-kubeconfig-mode` only
applies when the file is created. The script repairs the mode. This is worth
knowing before a demo, because the cluster is healthy when it happens and the
error points at the config file rather than at anything obviously wrong.

## Experiments

```bash
./experiments/experiment1_throughput.sh          # ~15 min
./experiments/experiment2_node_failure.sh 25660  # ~15-20 min
./experiments/restore_demo_state.sh              # put the cluster back
```

Run them in that order. Experiment 2 scales Kafka, Spark and Elasticsearch to
zero to make room for a three node Cassandra ring, so running it first would
remove what Experiment 1 needs. The argument to Experiment 2 is the row count
the pipeline actually produced; it aborts on a mismatch rather than measuring a
half replicated ring.

### Results

**Throughput against Spark worker count**, replaying a fixed 50,000 message
backlog in 10,000 record batches:

| Workers | steady state |
|--------:|-------------:|
| 1 | ~1,215 rows/s |
| 2 | ~1,733 rows/s |
| 3 | out of memory, no data point |

Speedup 1.43x against a theoretical 2x. The shortfall is architectural: adding
processing replicas does not add sink capacity, and every batch commits to a
single Cassandra node and a single Elasticsearch node, so the system queues on
the write path rather than on compute.

**Replica failure**, three node ring at replication factor 3:

| | |
|:---|:---|
| Quorum read with one replica down | succeeds, all 25,660 rows |
| `ALL` read with one replica down | fails, `required_replicas: 3, alive_replicas: 2` |
| Recovery to full ring | 25 seconds |
| Data loss | none |

The contrast is the result: the cluster survived at the consistency level it was
configured for and failed explicitly at a stricter one.
