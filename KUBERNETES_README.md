# Deploying this project on Kubernetes

Start to finish, from a machine with nothing installed. Every command here was
run for real; the notes about what goes wrong are things that actually went
wrong.

The reference platform is a single node k3s cluster inside WSL2 on Windows. Any
Kubernetes cluster works, but the resource notes assume roughly 11 GB.

---

## 0. Platform setup

Skip this section if you already have a working cluster and `kubectl` context.

### 0.1 WSL kernel settings

Create `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
kernelCommandLine = cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1 ipv6.disable=1
memory=11GB
```

Both kernel flags are required, not optional:

- `cgroup_no_v1=all` Kubernetes 1.36 refuses to start the kubelet on a cgroup
  v1 hierarchy, and WSL mounts the hybrid v1 layout by default. Without this the
  control plane appears to start and then exits, complaining about the cgroup
  version but not about how to fix it.
- `ipv6.disable=1` WSL resolves AAAA records but has no working IPv6 route.
  Without this the container runtime keeps selecting IPv6 registry endpoints and
  image pulls fail intermittently with `Try again`, which looks like flaky
  networking rather than a configuration problem.

Then, from PowerShell:

```powershell
wsl --shutdown
```

Verify after restart (want `cgroup2fs`, and no IPv6 addresses):

```bash
stat -fc %T /sys/fs/cgroup/
ip -6 addr show
```

### 0.2 Install and start k3s

```bash
curl -sfL https://get.k3s.io | INSTALL_K3S_SKIP_ENABLE=true INSTALL_K3S_SKIP_START=true sh -
```

WSL has no systemd by default, so the service unit the installer writes will
never fire. Start the server directly instead:

```bash
sudo setsid nohup k3s server --write-kubeconfig-mode 644 </dev/null >/tmp/k3s.log 2>&1 &
```

`setsid` detaches it from the terminal so it survives closing the window.
`--write-kubeconfig-mode 644` makes the kubeconfig readable without root; it
only applies when the file is created, so delete `/etc/rancher/k3s/k3s.yaml`
first if it already exists with restrictive permissions.

```bash
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
kubectl get nodes          # expect one node, Ready
```

Put that export in `~/.bashrc`. Without it `kubectl` falls back to
`~/.kube/config`, and if Docker Desktop's WSL integration has ever been enabled
that file exists and points at Docker Desktop's own Kubernetes:

```
Get "https://127.0.0.1:64069/api?timeout=32s": connect: connection refused
```

Port 64069 is Docker Desktop, 6443 is k3s. The cluster is fine; `kubectl` is
simply talking to the wrong one.

If instead you get `error loading config file ... permission denied`, k3s has
rewritten the kubeconfig root-only. `--write-kubeconfig-mode` only applies when
the file is created, so it can revert at any point during the server's life:

```bash
sudo chmod 644 /etc/rancher/k3s/k3s.yaml
```

k3s does not survive `wsl --shutdown`. After a WSL restart, run the
`k3s server` command again all cluster state and volumes persist on disk.

---

## 1. Namespace

```bash
kubectl apply -f video_pipeline_namespace.yaml
```

---

## 2. Prepare the dataset

The producer runs as an indexed Job with five pods, and each reads its own
slice, so the data must be split into exactly five files named
`dataset_partition-0.csv` … `dataset_partition-4.csv`.

`dataset/dataset_sort.ipynb` does this. Two things to check:

- split on `video_id`, so every observation of a video lands in one file and
  per-video ordering survives Kafka
- fill empty numeric cells. The producer calls `int()` on the view, like and
  comment counts, and `int(nan)` raises, killing the producer partway through

Put the files on a path on the **node**, not in the repo. On WSL keep them on
the native filesystem; `/mnt/c` goes through 9p and is far too slow to stream
from.

```bash
sudo mkdir -p /opt/video-dataset && sudo chmod 777 /opt/video-dataset
cp dataset_partition-*.csv /opt/video-dataset/
```

Then set that path in `video_producer/video-dataset-pv.yaml`:

```yaml
hostPath:
  path: /opt/video-dataset
```

---

## 3. Dataset volume

```bash
kubectl apply -f video_producer/video-dataset-pv.yaml
kubectl apply -f video_producer/video_dataset.yaml
kubectl get pvc video-dataset -n video-pipeline    # expect Bound
```

If it stays `Pending`, the PV capacity is smaller than the claim, or
`storageClassName: ""` is missing on one of the two and the default storage
class has hijacked the claim.

---

## 4. Build the images

Two images are built locally. There is no registry, so they are imported
straight into the cluster's containerd, and both manifests use
`imagePullPolicy: Never`.

```bash
docker build -t video-producer:latest   ./video_producer
docker build -t spark-calculator:latest ./spark

for img in video-producer:latest spark-calculator:latest; do
  docker save "$img" -o /tmp/img.tar
  sudo k3s ctr -n k8s.io images import /tmp/img.tar
done
```

The `-n k8s.io` matters. `k3s ctr` defaults to the `default` containerd
namespace, which the kubelet never looks in, so the import appears to succeed
and the pod still fails to find the image.

The Spark image bakes in all three connector jars and is used for the master,
the workers **and** the driver. Executors deserialize lambdas that live in
those jars, so a driver holding jars the executors lack fails with
`ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda`,
which says nothing about a missing jar. One image everywhere removes the
problem by construction.

---

## 5. Cassandra

```bash
kubectl apply -f cassandra/Cassandra_headless_service.yaml
kubectl apply -f cassandra/Cassandra_StatefulSet.yaml
kubectl rollout status statefulset/cassandra -n video-pipeline

kubectl apply -f cassandra/Cassandra_schema_configmap.yaml
kubectl apply -f cassandra/Cassandra_schema_job.yaml
kubectl wait --for=condition=complete job/cassandra-schema-init -n video-pipeline --timeout=10m
```

The schema job creates the keyspace, the `videos` table and the three ranking
tables. It has to finish before Spark starts: the connector writes into tables
that already exist and never creates them.

**On a single node with ~11 GB**, run one replica and set the keyspace
replication factor to 1 to match, otherwise every write fails for want of
replicas that do not exist. See `cassandra/README_cassandra.md`.

Verify:

```bash
kubectl exec cassandra-0 -n video-pipeline -- nodetool status     # UN
```

---

## 6. Elasticsearch

```bash
kubectl apply -f elasticsearch/Elasticsearch_headless_service.yaml
kubectl apply -f elasticsearch/Elasticsearch_service.yaml
kubectl apply -f elasticsearch/Elasticsearch_StatefulSet.yaml
kubectl rollout status statefulset/elasticsearch -n video-pipeline

kubectl apply -f elasticsearch/Elasticsearch_index_template_configmap.yaml
kubectl apply -f elasticsearch/Elasticsearch_index_template_job.yaml
kubectl wait --for=condition=complete job/elasticsearch-index-template -n video-pipeline --timeout=10m
```

Register the template **before** anything writes. Elasticsearch will infer a
mapping on first write, and inference makes dates text and scores single
precision, which breaks sorting by time and degrades aggregation. Mappings are
immutable once an index exists, so the order is a correctness requirement.

If you reduce to one replica, `cluster.initial_master_nodes` must be cut to
`elasticsearch-0` as well. Left listing three names, the single node waits
forever for a 2-of-3 quorum while reporting `1/1 Running`; the giveaway is
`master not discovered yet` repeating in the pod log.

```bash
kubectl exec elasticsearch-0 -n video-pipeline -- curl -s localhost:9200/_cluster/health?pretty
```

---

## 7. Kafka

```bash
kubectl apply -f video_producer/Kafka_headless_service.yaml
kubectl apply -f video_producer/Kafka_bootstrap_service.yaml
kubectl apply -f video_producer/Kafka_StatefulSet.yaml
kubectl rollout status statefulset/kafka -n video-pipeline
```

The headless service must be named exactly `kafka`: the StatefulSet's
`serviceName`, the controller quorum voters and the advertised listeners all
resolve `kafka-N.kafka.video-pipeline.svc.cluster.local`. Rename it and the
brokers never form a quorum.

Create the topic explicitly rather than letting the first produce auto-create
it:

```bash
kubectl exec kafka-0 -n video-pipeline -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-bootstrap:9092 \
  --create --topic youtube_videos --partitions 5 --replication-factor 3
```

Auto-creation uses the broker default of one partition. A Kafka partition is
consumed by at most one consumer, so a single-partition topic caps Spark's
parallelism at one no matter how many workers exist additional workers then
change throughput by exactly zero. Changing the broker default afterwards does
not repartition an existing topic.

---

## 8. Spark

```bash
kubectl apply -f spark/spark-master-service.yaml
kubectl apply -f spark/spark-master-deployment.yaml
kubectl apply -f spark/spark-worker-deployment.yaml
kubectl apply -f spark/spark-checkpoint-pvc.yaml
kubectl apply -f spark/spark-driver-deployment.yaml
```

Three constraints are wired into these manifests and are easy to break:

- **`enableServiceLinks: false`.** Kubernetes injects an environment variable
  per Service into every pod, so a Service named `spark-master` produces
  `SPARK_MASTER_PORT=tcp://<ip>:7077`. Spark parses that as an integer before it
  reads `--port`, and the master dies with a `NumberFormatException`.
- **The master binds its pod IP, and its Service is headless.** A pod cannot
  bind a Service ClusterIP, and at startup a headless name has no ready
  endpoint to resolve, so it binds the pod IP from the downward API instead.
- **`NUMBER_OF_CORES_PER_EXECUTOR` ≤ the worker's `--cores`.** An executor lives
  inside one worker and cannot span two. Ask for more cores than a single worker
  advertises and no executor is ever placed: the application registers, waits
  forever, and prints nothing, because `setLogLevel("OFF")` suppresses the
  warning that would have explained it.

Check the worker registered:

```bash
kubectl logs -l app=spark-master -n video-pipeline | grep 'Registering worker'
```

---

## 9. Run the producer

```bash
kubectl apply -f video_producer/Producer_job.yaml
kubectl get pods -n video-pipeline -l job-name=video-producer
```

Five pods, one per dataset slice, each exiting `Completed`. For throughput
benchmarking set `CHUNK_READ_DELAY_SECOND` to `0`; the default sleeps between
chunks and would make the producer, not the pipeline, the bottleneck.

Watch it flow:

```bash
kubectl logs -l app=spark-driver -n video-pipeline | grep METRIC
kubectl exec cassandra-0 -n video-pipeline -- cqlsh -e \
  'SELECT COUNT(*) FROM youtube_video_pipeline.videos;'
kubectl exec elasticsearch-0 -n video-pipeline -- curl -s localhost:9200/youtube_videos/_count
```

Row counts will be lower than the number of messages produced, and that is
correct rather than data loss. Cassandra's key is
`(video_id, snapshot_date)` so repeated observations upsert, and Elasticsearch
uses `video_id` as the document id so it keeps only the latest state. For the
50,000 record sample: 25,659 Cassandra rows and 9,712 Elasticsearch documents.

---

## 10. Superset

```bash
cd superset && docker build -t youtube-pipeline/superset:1.0 .
docker save youtube-pipeline/superset:1.0 -o /tmp/superset.tar
sudo k3s ctr -n k8s.io images import /tmp/superset.tar
cd ..

kubectl apply -f superset/Superset_secret.yaml
kubectl apply -f superset/Superset_metadb_service.yaml
kubectl apply -f superset/Superset_metadb_statefulset.yaml
kubectl rollout status statefulset/superset-db -n video-pipeline

kubectl apply -f superset/Superset_deployment.yaml
kubectl apply -f superset/Superset_service.yaml
```

Reach it on the NodePort:

```bash
hostname -I | awk '{print $1}'      # then http://<that ip>:30088
```

On WSL, `localhost` forwarding to the NodePort does not work; use the VM's
address. It changes when WSL restarts.

Log in as `admin` / `admin`, then add a database with

```
elasticsearch+http://elasticsearch:9200/
```

and a dataset on `youtube_videos`. When charting, group by `title.keyword` and
`channel_name.keyword`, never the bare fields those are analysed, so grouping
on them returns individual words instead of whole titles.

Do **not** set `ELASTIC_CLIENT_APIVERSIONING`. See
`superset/README_superset.md`; it breaks the native search API against
Elasticsearch 9 while leaving SQL working, so the connection test passes and
only dataset creation fails.

---

## Resource reality on a single node

The manifests declare three replicas of Cassandra, Elasticsearch and Kafka,
which is the intended topology. All of it at once needs roughly 20 GB.

On ~11 GB, run one replica of Cassandra and Elasticsearch. Three Spark workers
alongside everything else will exhaust memory: the kernel OOM killer takes an
executor, and the failure cascades into the control plane, whose restart then
fails because the terminated process still holds the API server port. Two
workers is the practical ceiling.

`experiments/README_experiments.md` covers running the two measurements, and
which topology each needs.

---

## Troubleshooting

| Symptom | Cause |
|:---|:---|
| `kubectl` hits `127.0.0.1:64069`, connection refused | talking to Docker Desktop's cluster via `~/.kube/config`; set `KUBECONFIG` to the k3s one |
| `error loading config file … permission denied` | k3s reset the kubeconfig to root-only; `sudo chmod 644 /etc/rancher/k3s/k3s.yaml` or run `scripts/01_setup_cluster.sh` |
| a script's replica override appears to do nothing | the YAML has CRLF, so a `$`-anchored `sed` never matches; strip `\r` first (see `manifest()` in `scripts/lib.sh`) |
| kubelet exits mentioning cgroup v1 | `.wslconfig` kernel flags missing, section 0.1 |
| image pulls fail with `Try again` | IPv6 not disabled |
| pod cannot find a locally built image | imported into the wrong containerd namespace, use `-n k8s.io` |
| Spark master `NumberFormatException` on `tcp://…` | `enableServiceLinks: false` missing |
| stream runs but no batch ever completes | executor cores exceed a worker's cores; check for `Launching executor` in the master log |
| `ClassCastException … SerializedLambda` | executors lack the driver's jars; run one image everywhere |
| adding Spark workers changes nothing | topic has one partition |
| Elasticsearch `1/1 Running`, nothing works | `master not discovered yet`; `cluster.initial_master_nodes` does not match the replica count |
| Cassandra reads fail with `alive_replicas: 0` | ring shrunk with `kubectl scale`, which deletes pods without removing nodes; use `nodetool decommission` or `removenode` |
