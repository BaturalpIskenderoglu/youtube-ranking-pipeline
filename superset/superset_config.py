import os

# Superset picks this file up because the image already has
# /app/pythonpath on PYTHONPATH.

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Metadata database. Superset stores dashboards, charts and users here,
# not the video data itself. SQLite is not an option, Superset refuses to
# start on it, so a small Postgres runs next to it.
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ['METADB_USER']}:{os.environ['METADB_PASSWORD']}"
    f"@{os.environ.get('METADB_HOST', 'superset-db')}:5432"
    f"/{os.environ.get('METADB_NAME', 'superset')}"
)

# The pipeline keeps writing into youtube_videos while people are looking at
# dashboards, so nothing should be cached for long.
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 60,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

# Elasticsearch has no notion of a Superset "schema", queries go straight to
# the index. Without this the datasource editor keeps asking for one.
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_RBAC": False,
}

ROW_LIMIT = 5000
SUPERSET_WEBSERVER_TIMEOUT = 120

SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_PORT = 8088

# behind kubectl port-forward / NodePort there is no https
ENABLE_PROXY_FIX = True
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = True
