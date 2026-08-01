from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, log10
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import os

# spark configs
NUMBER_OF_CORES_PER_EXECUTOR = int(os.getenv("NUMBER_OF_CORES_PER_EXECUTOR", "2"))
MASTER_NODE_ADDRESS = os.getenv("MASTER_NODE_ADDRESS", "local")

    # .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    # .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
    # .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:9.4.3") \

# cassandra connection configs
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")

spark = SparkSession.builder.appName("spark_calculator") \
    .config("spark.jars.packages", ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                                             "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1",
                                             "org.elasticsearch:elasticsearch-spark-30_2.12:9.4.3"])) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .master(MASTER_NODE_ADDRESS) \
    .config("spark.executor.cores", NUMBER_OF_CORES_PER_EXECUTOR) \
    .getOrCreate()

# cassandra configs
CASSANDRA_KEYSPACE_NAME = os.getenv("CASSANDRA_KEYSPACE_NAME", "youtube_video_pipeline")
CASSANDRA_TABLE_NAME = os.getenv("CASSANDRA_TABLE_NAME", "videos")

# function for spark to write a batch to cassandra 
def write_to_cassandra(df):
    df.write.format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", CASSANDRA_KEYSPACE_NAME) \
        .option("table", CASSANDRA_TABLE_NAME) \
        .save()

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

    print(f"Batch {batch_id} is written")

    batch.show(truncate=False)

    write_to_cassandra(batch)

    write_to_elasticsearch(batch)


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
            StructField("view_count", IntegerType(), False),
            StructField("like_count", IntegerType(), False),
            StructField("comment_count", IntegerType(), False),
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

bootstrapservers = ""

for index, address in enumerate(broker_addresses_str.split(","), start=1):
    if index != 1:
        bootstrapservers += f",broker{index}:{address.strip()}"
    else:
        bootstrapservers += f"broker{index}:{address.strip()}"

# read kafka stream
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapservers) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

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
                                   (log10(col("comment_count") + 1) * 0.2)) \
                        .withColumn("engagement_rate", (col("like_count") + col("comment_count")) / col("view_count")) \
                        .withColumn("freshness_rate", col("engagement_score") / log10(col("age_in_days")))

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