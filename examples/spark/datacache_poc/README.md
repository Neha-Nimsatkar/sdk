# Baseline: raw data -> SparkClient -> Iceberg (SeaweedFS) -> Data Cache -> TrainJob

Run order (from this folder, Kind cluster `kubeflow-local` with Spark operator, Trainer and SeaweedFS):

1. Build the Spark image with the S3A and Iceberg jars (see image/Dockerfile) and load it:
   `kind load docker-image spark-s3-iceberg:4.0.4 --name kubeflow-local`
   Jars: hadoop-aws 3.4.1, iceberg-spark-runtime-4.0_2.13 1.11.0, wildfly-openssl 1.1.3.Final, awssdk bundle 2.24.6
   (downloading them at session start was too slow: the bundle jar is ~530 MB and the server restarted first).
2. `python 01_create_raw_reviews.py`  -> raw JSON at s3a://mlpipeline/raw_reviews/
3. `python 02_enrich_to_iceberg.py`   -> Iceberg table lake.default.reviews_enriched
4. In fine-tune-with-cache.ipynb set
   METADATA_LOC = s3a://mlpipeline/warehouse/default/reviews_enriched/metadata/v1.metadata.json
   STORAGE_URI  = cache://default/reviews_enriched

Notebook change: removed .squeeze(0) on input_ids / attention_mask (crashed training with a one-row batch).
