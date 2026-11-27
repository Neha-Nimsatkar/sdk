"""Enrich raw reviews with Spark and push them into the data cache head."""
import pyarrow as pa
import pyarrow.flight as flight
from pyspark.sql import functions as fn

from spark_seaweed import get_spark

HEAD_URL = "grpc://localhost:50051"  # kubectl port-forward to the cache head

client, spark = get_spark()
try:
    raw = spark.read.json("s3a://mlpipeline/raw_reviews/")
    clean = (
        raw.withColumn(
            "content",
            fn.trim(fn.regexp_replace(fn.regexp_replace("review_text", "<[^>]+>", " "), r"\s+", " ")),
        )
        .filter(fn.col("content").isNotNull() & (fn.length("content") >= 10))
        .filter(~fn.lower("content").rlike("http|buy now"))
        .dropDuplicates(["content"])
        .withColumn("label", (fn.col("rating") - 1).cast("long"))
        .filter(fn.col("label").between(0, 4))
        .select("content", "label")
        .orderBy(fn.rand(7))  # shuffle: each worker's range gets a mix of labels
    )
    try:
        table = clean.toArrow()
    except AttributeError:
        table = pa.Table.from_pandas(clean.toPandas(), preserve_index=False)
    table = table.cast(pa.schema([("content", pa.string()), ("label", pa.int64())]))
finally:
    spark.stop()
    for s in client.list_sessions():
        client.delete_session(s.name)

print("enriched rows:", table.num_rows)

flight_client = flight.connect(HEAD_URL)
writer, _ = flight_client.do_put(flight.FlightDescriptor.for_path("push"), table.schema)
writer.write_table(table, max_chunksize=500)
writer.close()
print("pushed", table.num_rows, "rows to the cache")
