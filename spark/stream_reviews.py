# spark/stream_reviews.py
import os
import sys

from pyspark.sql import SparkSession, functions as F, types as T

current_python = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", current_python)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", current_python)

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
    T.StructField("date",        T.StringType()),  # fallback field in Yelp source
])

events = (
    raw.select(F.col("value").cast("string").alias("json"))
       .select(F.from_json("json", schema).alias("d")).select("d.*")
       .withColumn(
           "event_time",
           F.to_timestamp(F.coalesce("event_time", "date"))
       )
       .drop("date")
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

from sketch_bloom import BloomConfig, BloomDuplicateTracker

BLOOM_CAPACITY = int(os.getenv("BLOOM_CAPACITY", "200000"))
BLOOM_ERROR_RATE = float(os.getenv("BLOOM_ERROR_RATE", "0.01"))
BLOOM_DUP_SINK = os.getenv("BLOOM_DUP_SINK", "delta/dup_bloom")
BLOOM_METRICS_SINK = os.getenv("BLOOM_METRICS_SINK", "delta/gold_dup_rate_1m")

dup_tracker = BloomDuplicateTracker(
    spark, BloomConfig(capacity=BLOOM_CAPACITY, error_rate=BLOOM_ERROR_RATE,
                       dup_sink=BLOOM_DUP_SINK, metrics_sink=BLOOM_METRICS_SINK)
)

dup_q = (
    events.select("event_time", "review_id", "user_id", "business_id", "stars", "text")
    .writeStream
    .foreachBatch(dup_tracker.process_batch)
    .option("checkpointLocation", "delta/_ckpt_dup_bloom_v3")
    .start()
)

# --- Keep app alive ---
for q in spark.streams.active:
    q.awaitTermination()
