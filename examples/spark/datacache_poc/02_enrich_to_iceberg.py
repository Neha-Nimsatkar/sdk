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

"""Enrich raw reviews with SparkClient and write an Iceberg table.

Output schema (what the TrainJob expects): content (string), label (int64, 0-4).
"""

import time

from pyspark.sql import functions as fn
from spark_seaweed import get_spark

client, spark = get_spark()
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
        .orderBy(fn.rand(7))
    )
    print("raw rows:", raw.count(), " enriched rows:", clean.count())
    clean.groupBy("label").count().orderBy("label").show()
    start = time.perf_counter()
    clean.writeTo("lake.default.reviews_enriched").createOrReplace()
    print(f"iceberg write: {time.perf_counter() - start:.1f}s")
    spark.sql("SELECT file FROM lake.default.reviews_enriched.metadata_log_entries").show(
        truncate=False
    )
finally:
    spark.stop()
    for s in client.list_sessions():
        client.delete_session(s.name)
