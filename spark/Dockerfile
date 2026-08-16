FROM apache/spark:3.5.0

USER root

WORKDIR /app

# ---------------------------------------------------------------------------
# All connector jars are baked into /opt/spark/jars, and this one image is used
# for the master, the workers and the driver.
#
# The reason is that executors deserialize lambdas that live in the connector
# jars. If the driver has them and the executors do not, the job dies with
#   ClassCastException: cannot assign instance of java.lang.invoke.SerializedLambda
# which says nothing at all about a missing jar. Shipping them through
# spark.jars is supposed to cover this, but running every JVM from one image
# means the classpaths are identical by construction and there is nothing to
# get wrong.
#
# It also means no Maven round trip at startup, so the driver comes up even if
# repo1.maven.org is unreachable.
# ---------------------------------------------------------------------------

# Resolve Kafka and Cassandra through Ivy once, at build time, by running a
# throwaway local job, then move the resolved jars onto the classpath.
RUN /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
        --master "local[1]" \
        /opt/spark/examples/src/main/python/pi.py 1 > /tmp/resolve.log 2>&1 \
    && cp /root/.ivy2/jars/*.jar /opt/spark/jars/ \
    && rm -rf /root/.ivy2 /tmp/resolve.log

# The Elasticsearch connector cannot come through --packages at all: its POM on
# Maven Central depends on org.elasticsearch:thirdparty and
# :elasticsearch-spark_2.12, neither of which Elastic ever published, so Ivy
# resolution always fails. The jar itself is self-contained (it already shades
# those classes), so fetch it straight from the repository.
ARG ES_HADOOP_VERSION=9.4.3
RUN curl -fsSL -o /opt/spark/jars/elasticsearch-spark-30_2.12-${ES_HADOOP_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/${ES_HADOOP_VERSION}/elasticsearch-spark-30_2.12-${ES_HADOOP_VERSION}.jar"

RUN chmod 644 /opt/spark/jars/*.jar

COPY spark_calculator.py /app/spark_calculator.py

# The image ships pyspark as zips that only spark-submit puts on the path, so a
# plain "python3 script.py" cannot import it.
ENV PYTHONPATH=/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark

CMD ["python3", "/app/spark_calculator.py"]
