from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, lit, log10, row_number, when
from pyspark.sql.types import LongType, StructType, StructField, StringType
import os
import socket
import time

# spark configs
NUMBER_OF_CORES_PER_EXECUTOR = int(os.getenv("NUMBER_OF_CORES_PER_EXECUTOR", "2"))
MASTER_NODE_ADDRESS = os.getenv("MASTER_NODE_ADDRESS", "local")

    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    # .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    # .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.4.3") \

# cassandra connection configs
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")

# No spark.jars.packages here. All three connectors are baked into
# /opt/spark/jars in the image, which the master, the workers and the driver
# all run from, so every JVM has the same classpath and nothing has to be
# resolved or shipped at run time. See the Dockerfile for why.

# Executors have to dial the driver back. By default Spark advertises the
# driver by hostname, and a pod's hostname is not in cluster DNS, so every
# executor fails to resolve it and exits immediately. The pod IP is routable,
# so advertise that instead. Falls back to the local address off-cluster.
DRIVER_HOST = os.getenv("POD_IP") or socket.gethostbyname(socket.gethostname())

spark = SparkSession.builder.appName("spark_calculator") \
    .config("spark.driver.host", DRIVER_HOST) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .master(MASTER_NODE_ADDRESS) \
    .config("spark.executor.cores", NUMBER_OF_CORES_PER_EXECUTOR) \
    .getOrCreate()

# cassandra configs
CASSANDRA_KEYSPACE_NAME = os.getenv("CASSANDRA_KEYSPACE_NAME", "youtube_video_pipeline")
CASSANDRA_TABLE_NAME = os.getenv("CASSANDRA_TABLE_NAME", "videos")

# function for spark to write a batch to cassandra
def write_to_cassandra(df, table=None):
    df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", CASSANDRA_KEYSPACE_NAME) \
        .option("table", table or CASSANDRA_TABLE_NAME) \
        .save()

# Ranking views.
#
# Nothing is ranked here. The ranking tables are keyed so that Cassandra keeps
# each partition ordered by engagement_score as rows are written, which makes
# top-k a single-partition read. Computing a rank per micro-batch would only
# order the rows that arrived in that window, so this stays a plain fan-out
# write and the stream remains stateless.
#
# The connector rejects a dataframe carrying columns the target table does not
# have, so each write projects down to exactly that table's columns.
RANKING_COMMON = ["snapshot_date", "engagement_score", "video_id", "title",
                  "channel_name", "view_count", "like_count", "comment_count",
                  "engagement_rate", "freshness_rate"]

RANKING_TABLES = {
    "videos_by_score":    RANKING_COMMON + ["language"],
    "videos_by_language": RANKING_COMMON + ["language"],
    "videos_by_channel":  RANKING_COMMON + ["language", "channel_id"],
}

def write_ranking_views(df):
    # One row per video per day before ranking. The dataset holds a separate
    # observation per country, so a single video arrives many times for the
    # same snapshot_date with slightly different counts. engagement_score is
    # part of the clustering key, so each of those becomes its own row and one
    # popular video ends up occupying the whole top of the ranking. Keeping the
    # highest-scoring observation makes the views one row per video.
    #
    # This is a window over the micro-batch, which is an ordinary dataframe
    # inside foreachBatch, so it carries no streaming state. It does mean
    # deduplication is per batch: the same video seen again in a later batch is
    # inserted again under its new score.
    newest = Window.partitionBy("video_id", "snapshot_date") \
                   .orderBy(col("engagement_score").desc())

    ranked = df.withColumn("_rank", row_number().over(newest)) \
               .filter(col("_rank") == 1) \
               .drop("_rank")

    ranked.persist()
    try:
        for table, columns in RANKING_TABLES.items():
            write_to_cassandra(ranked.select(*columns), table)
    finally:
        ranked.unpersist()

# Elasticsearch connection configs
ELASTICSEARCH_NODES = os.getenv("ELASTICSEARCH_NODES", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "youtube_videos")

# function for spark to write a batch to elastic search
def write_to_elasticsearch(df):
    df.write.format("org.elasticsearch.spark.sql") \
    .option("es.nodes", ELASTICSEARCH_NODES) \
    .option("es.port", ELASTICSEARCH_PORT) \
    .option("es.mapping.id", "video_id") \
    .mode("append") \
    .save(ELASTICSEARCH_INDEX)

# function for writing batch
def write_batch(batch, batch_id):

    # the batch now feeds five sinks, and without this it would be recomputed
    # from Kafka for each one
    batch.persist()

    started = time.time()
    try:
        rows = batch.count()

        write_to_cassandra(batch)

        write_ranking_views(batch)

        write_to_elasticsearch(batch)

        elapsed = time.time() - started
        rate = rows / elapsed if elapsed > 0 else 0

        # This line is the throughput measurement for the scaling experiment.
        # flush=True because stdout is a pipe here, not a terminal, so the
        # default block buffering would hide it until the buffer fills.
        print(
            f"METRIC batch={batch_id} rows={rows} "
            f"seconds={elapsed:.2f} rows_per_second={rate:.1f}",
            flush=True,
        )
    finally:
        batch.unpersist()


# for timestamps
spark.conf.set("spark.sql.session.timeZone", "UTC")

# show less log
spark.sparkContext.setLogLevel("OFF")

# message struct
mesaj_struct = StructType([
            StructField("title", StringType(), False),
            StructField("channel_name", StringType(), False),
            # "daily_rank":,
            # "daily_movement":,
            # "weekly_movement":,
            StructField("snapshot_date", StringType(), False),
            # "country":,
            StructField("view_count", LongType(), False),
            StructField("like_count", LongType(), False),
            StructField("comment_count", LongType(), False),
            # "description":,
            # "thumbnail_url":,
            StructField("video_id", StringType(), False),
            StructField("channel_id", StringType(), False),
            # "video_tags":,
            # "kind":,
            StructField("publish_date", StringType(), False),
            StructField("language", StringType(), True)
            ])


# Kafka topic
TOPIC = os.getenv("TOPIC","youtube_videos")

# BROKER_ADDRESSES = producer1_address, producer2_address, producer3_address 
broker_addresses_str = os.getenv("BROKER_ADDRESSES", "localhost:9092")

bootstrapservers = ",".join(address.strip() for address in broker_addresses_str.split(","))

# for index, address in enumerate(broker_addresses_str.split(","), start=1):
#     if index != 1:
#         bootstrapservers += f",broker{index}:{address.strip()}"
#     else:
#         bootstrapservers += f"broker{index}:{address.strip()}"

# read kafka stream
# MAX_OFFSETS_PER_TRIGGER caps how many records one micro-batch may take. Left
# unset the stream swallows whatever backlog exists in a single batch, which is
# fine in normal operation but useless for measurement, since one run produces
# exactly one timing sample of an unbounded size. Setting it makes every batch
# the same size, so a run yields several comparable samples.
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER")

kafka_reader = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapservers) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest")

if MAX_OFFSETS_PER_TRIGGER:
    kafka_reader = kafka_reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)

kafka_df = kafka_reader.load()

# preprocess kafka stream
video_df = kafka_df \
    .withColumn("kafka_value", col("value").cast("string")) \
    .withColumn("data", from_json(col("kafka_value"), mesaj_struct)) \
    .select("data.*") \

SECONDS_IN_DAY = 3600 * 24

# Calculate age of the video in terms of day
video_df_1 = video_df \
    .withColumn("snapshot_date", col("snapshot_date").cast("timestamp")) \
    .withColumn("publish_date", col("publish_date").cast("timestamp")) \
    .withColumn("age_in_days", ((col("snapshot_date").cast("long") - col("publish_date").cast("long")) / SECONDS_IN_DAY).cast("int") )

# Calculate engagement score, engagement rate, freshness rate
video_df_final = video_df_1.withColumn("engagement_score", 
                                   (log10(col("view_count") + 1) * 0.5) + 
                                   (log10(col("like_count") + 1) * 0.3) + 
                                   (log10(col("comment_count") + 1) * 0.2)
                            ) \
                            .withColumn("engagement_rate",
                                        when( col("view_count") > 0,
                            
                                            (col("like_count") + col("comment_count")) / col("view_count")
                                        ) \
                                        .otherwise(lit(0.0)
                            )) \
                            .withColumn("freshness_rate", col("engagement_score") / log10(col("age_in_days") + 2))

PROCESSING_TIME_SECONDS = os.getenv("PROCESSING_TIME_SECONDS", '10')

PROCESSING_TIME = f"{PROCESSING_TIME_SECONDS} seconds"

# Write to console for debug purpose
query = video_df_final.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/app/checkpoint") \
    .trigger(processingTime=PROCESSING_TIME) \
    .start()

# wait for termination
query.awaitTermination()