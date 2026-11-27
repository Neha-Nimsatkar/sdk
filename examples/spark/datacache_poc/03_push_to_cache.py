# Copyright The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Enrich raw reviews with Spark and push them into the data cache head."""

import time

from data_cache_push import push_to_data_cache, to_arrow
import pyarrow as pa
from pyspark.sql import functions as fn
from spark_seaweed import get_spark

from kubeflow.spark import SparkClient

HEAD_URL = "grpc://localhost:50051"  # kubectl port-forward to the cache head

existing_sessions = {s.name for s in SparkClient().list_sessions()}
client, spark = get_spark()
new_sessions = [s.name for s in client.list_sessions() if s.name not in existing_sessions]
try:
    raw = spark.read.json("s3a://mlpipeline/raw_reviews/")
    clean = (
        raw.withColumn(
            "content",
            fn.trim(
                fn.regexp_replace(fn.regexp_replace("review_text", "<[^>]+>", " "), r"\s+", " ")
            ),
        )
        .filter(fn.col("content").isNotNull() & (fn.length("content") >= 10))
        .filter(~fn.lower("content").rlike("http|buy now"))
        .dropDuplicates(["content"])
        .withColumn("label", (fn.col("rating") - 1).cast("long"))
        .filter(fn.col("label").between(0, 4))
        .select("content", "label")
        .orderBy(fn.rand(7))  # shuffle: each worker's range gets a mix of labels
    )
    # Collect to Arrow before the session is closed (the DataFrame is lazy).
    start = time.perf_counter()
    table = to_arrow(clean).cast(pa.schema([("content", pa.string()), ("label", pa.int64())]))
    collect_s = time.perf_counter() - start
finally:
    spark.stop()
    for name in new_sessions:
        client.delete_session(name)

print("enriched rows:", table.num_rows)

start = time.perf_counter()
pushed = push_to_data_cache(table, HEAD_URL)
print(f"collect: {collect_s:.1f}s, push: {time.perf_counter() - start:.1f}s")
print("pushed", pushed, "rows to the cache")
