# spark/stream_reviews.py
from pyspark.sql import SparkSession, functions as F, types as T

# --- Spark setup (Kafka connector) ---
SPARK_VER = "3.5.1"  # match your installed PySpark version
spark = (
    SparkSession.builder
    .appName("reviews-kafka-stream")
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VER}")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# --- Kafka source ---
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "reviews")
    # "latest" -> wait for new data; "earliest" -> replay everything in topic
    .option("startingOffsets", "latest")
    .load()
)

schema = T.StructType([
    T.StructField("review_id",   T.StringType()),
    T.StructField("user_id",     T.StringType()),
    T.StructField("business_id", T.StringType()),
    T.StructField("stars",       T.DoubleType()),
    T.StructField("text",        T.StringType()),
    T.StructField("event_time",  T.StringType()),
])

events = (
    raw.select(F.col("value").cast("string").alias("json"))
       .select(F.from_json("json", schema).alias("d")).select("d.*")
       .withColumn("event_time", F.to_timestamp("event_time"))
       .dropna(subset=["event_time", "user_id", "business_id", "stars"])
)

# --- Bronze sink (append raw rows) ---
bronze_q = (
    events.writeStream
    .format("parquet")
    .option("path", "delta/bronze_reviews")
    .option("checkpointLocation", "delta/_ckpt_bronze_kafka_v2")
    .outputMode("append")
    .trigger(processingTime="5 seconds")  # continuous
    .start()
)

# --- Metrics sink (watermark + 1-min tumbling window) ---
metrics_q = (
    events
    .withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "1 minute").alias("w"))
    .agg(
        F.count("*").alias("n_reviews"),
        F.avg("stars").alias("avg_stars"),
    )
    .select(
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "n_reviews",
        "avg_stars",
    )
    # in spark/stream_reviews.py for the metrics sink
    .writeStream \
    .format("parquet") \
    .option("path", "delta/metrics_1m") \
    .option("checkpointLocation", "delta/_ckpt_metrics_kafka_v3")  # <-- new name
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

)

# --- Bloom-filter duplicate detector (driver-held; demo-friendly) ---
from pybloom_live import BloomFilter

BF_CAPACITY = 200_000   # tune for your sample size
BF_ERROR = 0.01         # ~1% false-positive rate
_bloom = BloomFilter(capacity=BF_CAPACITY, error_rate=BF_ERROR)

# normalized text + ids -> dup key
norm_text = F.lower(F.regexp_replace(F.col("text"), r"\s+", " "))
dup_key_col = F.sha2(F.concat_ws("||", "user_id", "business_id", norm_text), 256).alias("dup_key")

def bloom_detect(batch_df, batch_id: int):
    cols = ["event_time", "review_id", "user_id", "business_id", "stars", "text"]
    pdf = batch_df.select(*cols, dup_key_col).toPandas()  # OK for small demo batches
    if pdf.empty:
        return
    dup_rows = []
    for _, r in pdf.iterrows():
        k = r["dup_key"]
        if k in _bloom:         # likely seen before
            dup_rows.append(r)
        _bloom.add(k)           # record as seen
    if dup_rows:
        import pandas as pd
        out = pd.DataFrame(dup_rows)[cols + ["dup_key"]]
        (spark.createDataFrame(out)
              .write.mode("append").parquet("delta/dup_bloom"))

dup_q = (
    events.writeStream
    .foreachBatch(bloom_detect)
    .option("checkpointLocation", "delta/_ckpt_dup_bloom_v2")
    .start()
)

# --- Keep app alive ---
for q in spark.streams.active:
    q.awaitTermination()
