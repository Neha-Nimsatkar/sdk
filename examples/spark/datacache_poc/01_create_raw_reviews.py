"""Create noisy raw product reviews and write them to SeaweedFS as JSON."""
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
for i in range(400):
    rating = random.choice([1, 2, 3, 4, 5])
    text = random.choice(phrases[rating]) + " " + random.choice(["Fast shipping.", "Packaging was fine.", "Arrived on time.", ""])
    r = random.random()
    if r < 0.08:
        text = f"<p>{text}</p><br/>"            # HTML noise
    elif r < 0.12:
        text = None                              # missing text
    elif r < 0.16:
        text = "BUY NOW cheap deals http://spam.example.com"  # spam
    elif r < 0.19:
        text = "ok"                              # too short
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
