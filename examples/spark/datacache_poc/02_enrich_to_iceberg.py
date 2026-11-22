"""Enrich raw reviews with SparkClient and write an Iceberg table.

Output schema (what the TrainJob expects): content (string), label (int64, 0-4).
"""
from pyspark.sql import functions as F
from spark_seaweed import get_spark

client, spark = get_spark()
try:
    raw = spark.read.json("s3a://mlpipeline/raw_reviews/")
    clean = (
        raw
        .withColumn("content", F.trim(F.regexp_replace(F.regexp_replace("review_text", "<[^>]+>", " "), r"\s+", " ")))
        .filter(F.col("content").isNotNull() & (F.length("content") >= 10))
        .filter(~F.lower("content").rlike("http|buy now"))
        .dropDuplicates(["content"])
        .withColumn("label", (F.col("rating") - 1).cast("long"))
        .filter(F.col("label").between(0, 4))
        .select("content", "label")
        .orderBy(F.rand(7))
    )
    print("raw rows:", raw.count(), " enriched rows:", clean.count())
    clean.groupBy("label").count().orderBy("label").show()
    clean.writeTo("lake.default.reviews_enriched").createOrReplace()
    spark.sql("SELECT file FROM lake.default.reviews_enriched.metadata_log_entries").show(truncate=False)
finally:
    spark.stop()
    for s in client.list_sessions():
        client.delete_session(s.name)
