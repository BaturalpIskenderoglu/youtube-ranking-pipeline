FROM apache/superset:4.1.1

USER root

# The stock image has no Elasticsearch driver. elasticsearch-dbapi talks to the
# _sql endpoint and still pins the 7.x client library. That talks to a 9.x
# server as-is; do not add ELASTIC_CLIENT_APIVERSIONING to "help" it, see the
# note in Superset_deployment.yaml.
# python -m pip rather than plain pip, so the packages land in the same
# interpreter superset runs under whatever the base image does with its paths
#
# psycopg2-binary is not optional: the stock image ships no PostgreSQL driver
# at all, so without this Superset cannot reach its own metadata database.
RUN python -m pip install --no-cache-dir \
        "elasticsearch-dbapi>=0.2.9" \
        "psycopg2-binary>=2.9"

COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
