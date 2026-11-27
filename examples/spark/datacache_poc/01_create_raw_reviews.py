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

"""Create noisy raw product reviews and write them to SeaweedFS as JSON."""

import os
import random

from spark_seaweed import get_spark

random.seed(7)
phrases = {
    1: ["Terrible quality, broke on day one.", "Worst purchase ever.", "Complete waste of money."],
    2: ["Not great, feels cheap.", "Disappointed, works only sometimes."],
    3: ["It's okay, nothing special.", "Average product for the price."],
    4: ["Good value, works well.", "Happy with this, would buy again."],
    5: ["Excellent! Exceeds expectations.", "Love it, works perfectly."],
}
rows = []
NUM_REVIEWS = int(os.environ.get("NUM_REVIEWS", "400"))
for i in range(NUM_REVIEWS):
    rating = random.choice([1, 2, 3, 4, 5])
    text = (
        random.choice(phrases[rating])
        + " "
        + random.choice(["Fast shipping.", "Packaging was fine.", "Arrived on time.", ""])
    )
    text = f"{text.strip()} #{i}"  # unique text, so deduplication keeps real reviews
    r = random.random()
    if r < 0.08:
        text = f"<p>{text}</p><br/>"  # HTML noise
    elif r < 0.12:
        text = None  # missing text
    elif r < 0.16:
        text = "BUY NOW cheap deals http://spam.example.com"  # spam
    elif r < 0.19:
        text = "ok"  # too short
    rows.append((f"r{i}", rating, text))
rows += [(f"d{i}", x[1], x[2]) for i, x in enumerate(rows[:10])]  # duplicates

client, spark = get_spark()
try:
    df = spark.createDataFrame(rows, "review_id string, rating int, review_text string")
    df.coalesce(1).write.mode("overwrite").json("s3a://mlpipeline/raw_reviews/")
    print("raw rows written:", df.count())
finally:
    spark.stop()
    for s in client.list_sessions():
        client.delete_session(s.name)
