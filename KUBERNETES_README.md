# This document describes how to deploy this project on Kubernetes

## Follow this steps respectively

1) create namespace

```
kubectl apply -f namespace.yaml
```

2) Change the **hostPath** field in `video-dataset.yaml` with respect to your own dataset path.  
Make sure datasets are named as  
    dataset_partition-0.csv,    
    dataset_partition-1.csv,    
    dataset_partition-2.csv,    
    dataset_partition-3.csv,    
    dataset_partition-4.csv

```
hostPath:
    path: <path of the folder that contains your datasets>
    # change this path with respect to your own dataset path
```

3) Create PV and PVC
```
kubectl apply -f video_producer\video-dataset.yaml
kubectl apply -f video_producer\video-dataset-pv.yaml
```

4) Apply Kafka Services

```
kubectl apply -f Kafka_headless_service.yaml
kubectl apply -f Kafka_bootstrap_service.yaml
```

5) Apply Kafka StatefulSet
```
kubectl apply -f Kafka_StatefulSet.yaml
```

6) 