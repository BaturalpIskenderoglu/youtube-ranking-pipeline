# Superset Visualization Service

The last step of the pipeline. Superset reads the indexed videos out of
Elasticsearch and turns them into the ranking dashboards.

It is a single Deployment plus its own small Postgres for storing dashboards.

## Why there is a second database here

Superset keeps its dashboards, charts, saved queries and users in a metadata
database. This has nothing to do with the video data, it is Superset's own
bookkeeping. SQLite would be the easy answer but Superset refuses to start on
it, so a one pod Postgres StatefulSet runs next to it with its own volume. If
that volume is lost, the dashboards are lost, the pipeline data is not.

## Building the image

The stock Superset image ships neither an Elasticsearch driver nor a PostgreSQL
one, so it has to be built once before deploying. Both are added in the
Dockerfile: `elasticsearch-dbapi` to read the video data, and `psycopg2-binary`
so Superset can reach its own metadata database at all.

```
docker build -t youtube-pipeline/superset:1.0 .
```

On minikube, build it into the cluster's own docker daemon, otherwise the pod
cannot find the image and sits in ErrImagePull:

```
eval $(minikube docker-env)
docker build -t youtube-pipeline/superset:1.0 .
```

`imagePullPolicy` is `IfNotPresent` for exactly this reason, the image is local
and there is no registry to pull it from.

## Environment Variables

**SUPERSET_SECRET_KEY:** Flask secret key, used to sign sessions. Comes from the
secret. Changing it after the fact logs everyone out and invalidates saved
database passwords

**ADMIN_PASSWORD:** password of the `admin` user created on first start

**METADB_USER / METADB_PASSWORD:** credentials of the metadata Postgres, shared
by the Postgres pod and Superset so they cannot drift apart

**METADB_HOST:** `superset-db`

**METADB_NAME:** `superset`

**ELASTIC_CLIENT_APIVERSIONING:** `1`. See the note at the bottom

All the secret values live in `Superset_secret.yaml` and are all placeholders.

## Volumes

| Claim | Mount path | Size |
|:---|:---|:---|
| superset-db-storage-superset-db-0 | /var/lib/postgresql/data | 2Gi |

Only the metadata Postgres has a volume. The Superset pod itself is stateless,
everything it knows is in Postgres, so it can be restarted or rescheduled
freely.

`PGDATA` points at a subdirectory of the mount instead of the mount itself,
because the postgres image refuses to initialise into a directory that already
has something in it, and a fresh volume usually does.

## Deploying

```
kubectl apply -f ../video_pipeline_namespace.yaml
kubectl apply -f Superset_secret.yaml
kubectl apply -f Superset_metadb_service.yaml
kubectl apply -f Superset_metadb_statefulset.yaml

kubectl rollout status statefulset/superset-db -n video-pipeline

kubectl apply -f Superset_deployment.yaml
kubectl apply -f Superset_service.yaml
```

The `bootstrap` init container waits for Postgres, runs `superset db upgrade`,
creates the admin user and runs `superset init`. All of it is safe to run again,
so a restarted pod just goes through it a second time. First start takes a
couple of minutes because the migrations are slow.

## Opening it

```
minikube service superset -n video-pipeline
```

or

```
kubectl port-forward svc/superset 8088:8088 -n video-pipeline
```

then http://localhost:8088, user `admin`, password whatever is in the secret.

## Connecting it to Elasticsearch

In the UI, Settings > Database Connections > + Database, pick "Other" and use:

```
elasticsearch+http://elasticsearch:9200/
```

Then add a dataset on the `youtube_videos` index and the charts can be built on
top of it.

Things to keep in mind when building the charts:

- group by `title.keyword` and `channel_name.keyword`, not `title` and
  `channel_name`. The plain fields are analysed text and grouping on them gives
  you individual words instead of whole titles
- `language` and `channel_id` are already keyword fields, they can be used
  directly
- `snapshot_date` is the time column
- global ranking is just `engagement_score` sorted descending, the language and
  channel rankings are the same thing with `language` or `channel_name.keyword`
  as the group by

## Notes

Superset talks to Elasticsearch over the `_sql` endpoint through
`elasticsearch-dbapi`. That package still pins the 7.x Elasticsearch client
library, and a 7.x client refuses to talk to a 9.x server by default, it
decides the server is "not Elasticsearch" and stops. Setting
`ELASTIC_CLIENT_APIVERSIONING=1` puts the client into compatibility mode and it
works. It is worth knowing this is the fragile part of the setup, it is the only
place where a version bump on the Elasticsearch side can break something that
is not obviously related.

Cassandra is deliberately not wired into Superset. There is no maintained
SQLAlchemy dialect for it and the usual way to do it is to put Trino in front,
which is another whole component. Since Elasticsearch holds the same rows and
is built for this kind of query, the dashboards read from there. Cassandra stays
what it is meant to be in this project, the durable storage layer.

`SUPERSET_PORT` is set explicitly in the Deployment and must stay there.
Kubernetes injects an environment variable per Service into every pod in the
namespace, so the Service named `superset` produces
`SUPERSET_PORT=tcp://10.43.x.x:8088`. The image's `run-server.sh` binds gunicorn
to `${SUPERSET_PORT:-8088}`, picks up that value, and the container dies with
`'tcp' is not a valid port number`. Setting it ourselves overrides the injected
one. Renaming the Service would also fix it, but the name is worth keeping.

`replicas` is 1. Superset scales horizontally fine, but with more than one pod
the async query results and the caches need Redis in front, and nothing here
needs that yet.
