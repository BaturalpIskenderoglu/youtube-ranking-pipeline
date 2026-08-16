FROM apache/superset:4.1.1

USER root

# The stock image has no Elasticsearch driver. elasticsearch-dbapi talks to
# the _sql endpoint and still pins the 7.x client library, which by default
# refuses to speak to a 9.x server. ELASTIC_CLIENT_APIVERSIONING (set in the
# Deployment) puts the client into compatibility mode so it works.
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
