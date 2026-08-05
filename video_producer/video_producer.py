import json
import time
from kafka import KafkaProducer
import pandas as pd
from datetime import date
import os

# Kafka config

# BROKER_ADDRESSES = producer1_address, producer2_address, producer3_address 
broker_addresses_str = os.getenv("BROKER_ADDRESSES", "localhost:9092")

broker_addresses = [address.strip() for address in broker_addresses_str.split(",")]

message_linger_delay_ms = float(os.getenv("MESSAGE_LINGER_DELAY_MS",30))
message_batch_size_kb = float(os.getenv("MESSAGE_BATCH_SIZE_KB", 100))

video_producer = KafkaProducer(
    bootstrap_servers= broker_addresses,
    value_serializer= lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    linger_ms=message_linger_delay_ms,
    batch_size=(message_batch_size_kb*1024)
)

print("------------Start Streaming------------")
print("Producer addresses are:")
for address in broker_addresses:
    print(address)

PRODUCER_INDEX = os.getenv("JOB_COMPLETION_INDEX")

dataset_path = f"/app/dataset_partition-{PRODUCER_INDEX}.csv"
chunk_size = int(os.getenv("CHUNK_SIZE", 1000))
topic = os.getenv("TOPIC","youtube_videos")
chunk_read_delay_second = float(os.getenv("CHUNK_READ_DELAY_SECOND",0.1))


# read the dataset and stream every row in json format
streamed_videos_num = 0
for chunk in pd.read_csv(dataset_path, chunksize=chunk_size):
    for row in chunk.itertuples():
        
        partition_key = row.video_id
        message_json = {
            "title": str(row.title),
            "channel_name": str(row.channel_name),
            # "daily_rank":,
            # "daily_movement":,
            # "weekly_movement":,
            "snapshot_date": str(row.snapshot_date),
            # "country":,
            "view_count": int(row.view_count),
            "like_count": int(row.like_count),
            "comment_count": int(row.comment_count),
            # "description":,
            # "thumbnail_url":,
            "video_id": str(row.video_id),
            "channel_id": str(row.channel_id),
            # "video_tags":,
            # "kind":,
            "publish_date": str(row.publish_date),
            "language": str(row.langauge) # 'langauge' is misspelled in the source data, typo handled 
        }

        video_producer.send(topic=topic, key=partition_key, value=message_json)
    
    streamed_videos_num += chunk_size
    print(f"{streamed_videos_num} videos are streamed")

    time.sleep(chunk_read_delay_second)

video_producer.flush()
print("Video Stream ended")

