# Spark Calculator Service

This service reads YouTube video data stream from Kafka topic and processes this data.
Then spark writes processed data to Cassandra and ElasticSearch. 

## What it calculates

**snapshot_date:** this column's type is cast into timestamp 
**publish_date:** this column's type is cast into timestamp 
**age_in_days:** difference between `snapshot_date` and `publish_date` in term of seconds
**engagement_score:** Measures how much this video drives engagement by combining view, like and comment count

formula:  
```
(log10(`view_count` + 1) * 0.5) + (log10(`like_count` + 1) * 0.3) + (log10(`comment_count` + 1) * 0.2)
```
**engagement_rate:** Measures how much engagement this video drives relative to its total view.

formula:
```
(`like_count` + `comment_count`) / `view_count`
```

**freshness_rate:** Measures how new and popular is this video  

formula:
```
`engagement_score` / log10(`age_in_days`)
```

## Environment Variables

**NUMBER_OF_CORES_PER_EXECUTOR:** How many cores to use per executor

**MASTER_NODE_ADDRESS:** Sparks' master node's address

**CASSANDRA_HOST:** Address of Cassandra host 

**CASSANDRA_PORT:** Target port of Cassandra

**CASSANDRA_KEYSPACE_NAME:** Cassandra's Keyspace name to write dataframes  

**CASSANDRA_TABLE_NAME:** Cassandra's Table name to write dataframes  

**ELASTICSEARCH_NODES:** Addresses of Elasticsearch nodes
**ELASTICSEARCH_PORT:** Target port of Elasticsearch 
**ELASTICSEARCH_INDEX:** Which index to write in Elasticsearch
**TOPIC:** Which topic to read the input stream in Kafka
**BROKER_ADDRESSES:** Kafka Brokers' adressess to listen
**PROCESSING_TIME_SECONDS:** How many per second to write data


## Volumes

No volumes needed

## Input Kafka Message Format

Message read from Kafka brokers will be in json format.

Input Message format is given below:

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
    "language": str
}

## Output Format

| Columns | Column_type |
|:---:|:---:|
| title | str |
| channel_name | str |
| snapshot_date | timestamp |
| view_count | int |
| like_count | int |
| comment_count | int |
| video_id | str |
| channel_id | str |
| publish_date | timestamp |
| language | str |
| age_in_days | int |
| engagement_score | float |
| engagement_rate | float |
| freshness_rate | float |

| Meyve | Renk | Fiyat |
| :--- | :---: | ---: |
