# Experiments

Two measurements, matching the two questions in the project proposal:

1. how throughput changes as processing workers are added or removed
2. how the system behaves when a storage node fails

Both scripts assume the pipeline has already been run once, so that Kafka holds
the message backlog and Cassandra holds the processed rows.

## Platform the numbers came from

Single node k3s (Kubernetes v1.36.3) on WSL2, Linux kernel 5.15, 16 logical
CPUs, 11 GB allocated to the VM. Cassandra 4.1.7, Elasticsearch 9.4.3, Spark
3.5.0, Kafka in KRaft mode. Dataset: 50,000 records from the trending videos
dataset, partitioned into 5 files by `video_id`.

**Everything runs on one physical machine.** The three Cassandra nodes in
experiment 2 are three pods sharing one kernel, so it measures replica failure,
not host failure. Say "replica failure on a single host cluster", never "node
failure" without qualification.

## Experiment 1 - throughput vs worker count

```
./experiment1_throughput.sh [batches_per_run]
```

Replays the 50,000 message backlog already in Kafka for 1, 2 and 3 workers,
timing each micro-batch. Deletes the checkpoint before each run so every
configuration processes identical input.

The producer is **not** running during the measurement. With it running the
observed rate would be the slower of producer and Spark, and since the producer
sleeps between chunks the result would mostly describe its sleep timer. Adding
workers would then change nothing and the flat curve would look like "Spark does
not scale". Kafka retains messages after the producer exits, which is what makes
the replay possible.

### Results obtained

| Workers | batch 0 | batch 1 | batch 2 | steady state |
|--------:|--------:|--------:|--------:|-------------:|
| 1 | 555.9 | 1138.5 | 1290.5 | ~1,215 rows/s |
| 2 | 593.9 | 1682.1 | 1784.4 | ~1,733 rows/s |
| 3 | - | - | - | did not complete |

Speedup from 1 to 2 workers: **1.43x**, against a theoretical 2x.

The scaling is sublinear for an architectural reason. Doubling the processing
tier does not double the sinks: every batch commits to one Cassandra node and
one Elasticsearch node, so past a low degree of parallelism the system queues on
the write path rather than on compute.

Batch 0 is a consistent outlier in both runs. It carries JIT compilation and the
first connection to both sinks, costs paid once per driver lifetime. Report it,
do not quietly drop it.

**Three workers produced no data point.** Allocating a third worker exhausted
node memory, the kernel OOM killer terminated an executor holding ~1 GB, and the
failure cascaded into the control plane, whose restart then failed because a
terminated process still held the API server port. This is a capacity result,
not a gap: the platform admits two processing replicas alongside the other
tiers and no more.

## Experiment 2 - replica failure

```
./experiment2_node_failure.sh 25660
```

The argument is the row count the pipeline actually produced. The script refuses
to measure anything if the baseline does not match, for reasons below.

Builds a 3 node ring at RF=3 with a full repair, then removes a replica twice:

- **Phase A** force deletes the pod, which the StatefulSet recreates. Measures
  recovery time and data loss.
- **Phase B** scales the ring down so the replica stays gone, which is the only
  way to observe behaviour *during* an outage. Recovery in phase A takes about
  25 seconds, so a read issued afterwards says nothing about a degraded cluster.

### Results obtained

| Measurement | Result |
|:---|:---|
| Baseline, QUORUM, 3 replicas up | 25,660 rows |
| QUORUM read, 1 replica down | 25,660 rows, **succeeds** |
| ALL read, 1 replica down | **fails**, `required_replicas: 3, alive_replicas: 2` |
| Recovery, kill to 3 nodes UN | **25 seconds** |
| Data loss | **none** |
| Identity across restart | same host ID, new IP |

The QUORUM/ALL contrast is the substance of the result. The cluster did not
merely survive: it survived at the consistency level configured for it and
failed explicitly at a stricter one. That is a controlled trade-off, not luck.

The replacement pod returned on a different address but the same host ID,
confirming that broadcasting the stable DNS name rather than the pod IP keeps
ring membership intact across rescheduling. Without it a rescheduled pod looks
like a new node.

## Two traps that invalidated earlier runs

Both are worth knowing before trusting any number either script prints.

**Pod readiness is not cluster membership.** The Cassandra readiness probe is
`nodetool statusbinary`, which answers as soon as CQL is listening - before the
node has joined the ring. An earlier version waited on pod Ready and ran the
entire experiment against a 2 node cluster whose replication had never been
raised. Elasticsearch has the same property: its probe uses `?local=true`, which
answers on a node that has not yet elected a master. Both scripts read cluster
state from the cluster, never from Kubernetes.

**Raising the replication factor without a repair loses availability silently.**
New replicas start empty. A quorum read then returns a fraction of the rows -
in our case 12,608 of 25,659 - with no error anywhere. Nothing indicates a
problem unless you know the expected count, which is why experiment 2 takes it
as an argument and aborts on a mismatch. Matching before/during/after counts
that are all equally wrong look exactly like a clean result.

A related hazard when shrinking: `kubectl scale` deletes the pod without taking
the node out of the ring, so Cassandra keeps believing it exists and is merely
down. Its token ranges then point at nothing, and reads fail with
`alive_replicas: 0` even though the data is on disk. Use `nodetool decommission`
or `nodetool removenode`, and lower the replication factor **after** removing
nodes, not before.

## Restoring the stack

`experiment2_node_failure.sh` leaves Kafka, Spark and Elasticsearch scaled to
zero, since the 3 node ring needs their memory. Scale them back up afterwards,
and bring the Spark driver up last: it is the heaviest and begins streaming
immediately.
