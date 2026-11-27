# Spark to Trainer with the Data Cache

This example shows the Kubeflow SDK as one flow from data engineering to training:
raw reviews in S3, enrichment with `SparkClient`, and a TrainJob that reads the
enriched data from the Trainer data cache.

There are two ways to get the enriched data into the cache.

**Baseline (pull):** Spark writes an Iceberg table, and the cache loads it.

```
raw reviews (S3) -> SparkClient enrichment -> Iceberg table (S3)
  -> DataCacheInitializer -> cache head plans files, workers load them -> TrainJob
```

**Write path (push):** Spark sends the enriched rows to the cache head, which splits
them into one contiguous row range per worker. No Iceberg table is written.

```
raw reviews (S3) -> SparkClient enrichment -> push to cache head
  -> head splits rows in fixed order, each worker stores its range -> TrainJob
```

## Files

| File | Purpose |
|---|---|
| `01_create_raw_reviews.py` | Creates noisy synthetic reviews in S3 (HTML, missing text, spam, duplicates) |
| `02_enrich_to_iceberg.py` | Baseline: enriches the reviews and writes an Iceberg table |
| `03_push_to_cache.py` | Write path: enriches the reviews and pushes them to the cache head |
| `spark_seaweed.py` | Shared Spark session settings for SeaweedFS and Iceberg |
| `spark_to_trainer_data_cache.ipynb` | Baseline flow in one notebook |
| `spark_push_to_data_cache.ipynb` | Write path in one notebook: creates its own data, pushes it to the cache, trains, and cleans up |
| `image/Dockerfile` | Spark image with the S3A and Iceberg jars |

The enrichment strips HTML, drops missing, short and spam rows, removes duplicates,
maps the rating (1-5) to a label (0-4), and shuffles the rows. The output columns
are `content` (string) and `label` (int64).

## Prerequisites

- A Kind cluster with the Trainer controller, the Spark Operator and SeaweedFS
  (bucket `mlpipeline`).
- A Spark image with the S3A and Iceberg jars, loaded into Kind. Downloading the jars
  at session start is too slow (the AWS SDK bundle is about 530 MB), so build them in:

  ```
  cd image
  M=https://repo1.maven.org/maven2
  curl -L -O $M/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar
  curl -L -O $M/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar
  curl -L -O $M/org/wildfly/openssl/wildfly-openssl/1.1.3.Final/wildfly-openssl-1.1.3.Final.jar
  curl -L -O $M/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar
  docker build -t spark-s3-iceberg:4.0.4 .
  kind load docker-image spark-s3-iceberg:4.0.4 --name kubeflow-local
  ```

- Run the scripts and notebooks from this folder, with an environment that has the Kubeflow SDK.

## Baseline

1. `python 01_create_raw_reviews.py` writes raw JSON to `s3a://mlpipeline/raw_reviews/`.
2. `python 02_enrich_to_iceberg.py` writes the Iceberg table `lake.default.reviews_enriched`
   and prints its metadata files.
3. Start a TrainJob with `DataCacheInitializer`, using the newest metadata file as
   `metadata_loc` and `cache://default/reviews_enriched` as `storage_uri`.
   `spark_to_trainer_data_cache.ipynb` runs all of this in one notebook.

## Write path

This needs a data cache image built from the Trainer branch that adds the write path
(`feat/data-cache-write-path`), set as `CACHE_IMAGE` in the runtime, with
`S3_PATH_STYLE_ACCESS=true` for SeaweedFS.

The quickest way is `spark_push_to_data_cache.ipynb`, which runs the whole flow and
cleans up its Spark session and TrainJob at the end. The steps below are the same
flow with the scripts.

1. Run `01_create_raw_reviews.py` once.
2. Start a TrainJob with `DataCacheInitializer`. The cache still starts from a small
   Iceberg table, and the push replaces its data.
3. Open a connection to the cache head:
   `kubectl port-forward pod/<train-job>-cache-0 50051:50051`.
4. `python 03_push_to_cache.py` enriches the data and pushes it to the head.
5. The training function waits until the head has data, then trains on the pushed rows.
   It polls the head and starts once it sees more rows than the placeholder table
   (see `train_func` in the notebook).

## Known limits

- The head holds all pushed rows in memory while it splits them.
- A push is not atomic: if a worker fails midway, some workers keep the new data while
  the head still has the old layout.
- A second push replaces the first one.
- The cache cannot start without an Iceberg table yet.
- The push collects the data on the driver and goes through a port-forward.
