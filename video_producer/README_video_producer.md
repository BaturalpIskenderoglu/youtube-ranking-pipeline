# Kafka Producer Service

This service reads a static YouTube dataset and publishes the records
to an Apache Kafka topic to simulate a real-time event stream.

## Environment Variables

**BROKER_ADDRESSES:** IP addresses of BROKERS seperated by comma
Examle: producer1_address, producer2_address, producer3_address

**MESSAGE_LINGER_DELAY_MS:** How many miliseconds to wait for streaming a batch

**MESSAGE_BATCH_SIZE_KB:** What is maximum batch size 

**CHUNK_SIZE:** How many rows should be read in every iteration

**TOPIC:** Name of the topic to send the data  

**CHUNK_READ_DELAY_SECOND:** How many seconds time to wait between
chunks when reading

## Volumes

- <dataset_relative_path>:/app/dataset.csv:ro

Example volume for this project: 

./dataset/trending_yt_videos_113_countries.csv:/app/dataset.csv:ro

This container should be run with broker containers.
In this project `apache/kafka:4.3.1` image used for broker container.

## Kafka Message Format

Message sent to brokers will be in json format.

Message format is given below:

{
    "title": str,
    "channel_name": str,
    "snapshot_date": str,
    "view_count": int,
    "like_count": int,
    "comment_count": int,
    "video_id": str,
    "channel_id": str,
    "publish_date": str,
    "langauge": str
}