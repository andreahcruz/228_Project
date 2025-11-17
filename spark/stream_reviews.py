# spark/stream_reviews.py
import os
import sys
import threading
import time

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

# --- Reservoir sampling (per-window uniform samples) ---
from sketch_reservoir import ReservoirConfig, ReservoirSampler

RESERVOIR_SAMPLE_SIZE = int(os.getenv("RESERVOIR_SAMPLE_SIZE", "100"))
RESERVOIR_SINK = os.getenv("RESERVOIR_SINK", "delta/reservoir_samples")

reservoir_sampler = ReservoirSampler(
    spark, ReservoirConfig(
        sample_size=RESERVOIR_SAMPLE_SIZE,
        sink=RESERVOIR_SINK,
        watermark_minutes=2  # Match the watermark from metrics_q
    )
)

reservoir_q = (
    events.select("event_time", "review_id", "user_id", "business_id", "stars", "text")
    .writeStream
    .foreachBatch(reservoir_sampler.process_batch)
    .option("checkpointLocation", "delta/_ckpt_reservoir_v1")
    .start()
)

# --- Flajolet-Martin distinct user counts (per window) ---
from sketch_fm import FMConfig, FlajoletMartinTracker

FM_NUM_HASHES = int(os.getenv("FM_NUM_HASHES", "5"))
FM_SINK = os.getenv("FM_SINK", "delta/fm_distinct_users")
FM_EXACT_THRESHOLD = int(os.getenv("FM_EXACT_THRESHOLD", "1000"))

fm_tracker = FlajoletMartinTracker(
    spark, FMConfig(
        num_hash_functions=FM_NUM_HASHES,
        sink=FM_SINK,
        watermark_minutes=2,  # Match the watermark from metrics_q
        compute_exact_for_windows_under=FM_EXACT_THRESHOLD
    )
)

fm_q = (
    events.select("event_time", "user_id")
    .writeStream
    .foreachBatch(fm_tracker.process_batch)
    .option("checkpointLocation", "delta/_ckpt_fm_v1")
    .start()
)

# --- Performance metrics (throughput, latency) ---
from metrics_performance import PerformanceConfig, PerformanceTracker

PERF_SINK = os.getenv("PERF_SINK", "delta/performance_metrics")

perf_tracker = PerformanceTracker(
    spark, PerformanceConfig(sink=PERF_SINK)
)

perf_q = (
    events.select("event_time")
    .writeStream
    .foreachBatch(perf_tracker.process_batch)
    .option("checkpointLocation", "delta/_ckpt_perf_v1")
    .trigger(processingTime="5 seconds")
    .start()
)
print("[Performance] Started performance metrics streaming query")

# --- Storage metrics (checkpoint size tracking) ---
from metrics_storage import StorageConfig, StorageTracker

STORAGE_SINK = os.getenv("STORAGE_SINK", "delta/storage_metrics")

storage_tracker = StorageTracker(
    spark, StorageConfig(sink=STORAGE_SINK)
)

def record_storage_metrics_periodically():
    """Background thread to record storage metrics every minute."""
    while True:
        try:
            storage_tracker.record_metrics()
        except Exception as e:
            print(f"[Storage] Error recording metrics: {e}")
        time.sleep(60)  # Record every minute

# Start background thread for storage tracking
storage_thread = threading.Thread(target=record_storage_metrics_periodically, daemon=True)
storage_thread.start()
print("[Storage] Started background thread for storage metrics tracking")

# --- Targeted filtering: High-rated reviews (stars ≥ 4) ---
# Filter the stream for high-rated reviews and run the same sketching algorithms
events_high_rated = events.filter(F.col("stars") >= 4.0)

# High-rated metrics
metrics_high_rated_q = (
    events_high_rated
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
    .writeStream
    .format("parquet")
    .option("path", "delta/metrics_1m_high_rated")
    .option("checkpointLocation", "delta/_ckpt_metrics_high_rated_v1")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# High-rated Bloom filter
dup_tracker_high_rated = BloomDuplicateTracker(
    spark, BloomConfig(
        capacity=BLOOM_CAPACITY,
        error_rate=BLOOM_ERROR_RATE,
        dup_sink="delta/dup_bloom_high_rated",
        metrics_sink="delta/gold_dup_rate_1m_high_rated"
    )
)

dup_high_rated_q = (
    events_high_rated.select("event_time", "review_id", "user_id", "business_id", "stars", "text")
    .writeStream
    .foreachBatch(dup_tracker_high_rated.process_batch)
    .option("checkpointLocation", "delta/_ckpt_dup_bloom_high_rated_v1")
    .start()
)

# High-rated FM distinct users
fm_tracker_high_rated = FlajoletMartinTracker(
    spark, FMConfig(
        num_hash_functions=FM_NUM_HASHES,
        sink="delta/fm_distinct_users_high_rated",
        watermark_minutes=2,
        compute_exact_for_windows_under=FM_EXACT_THRESHOLD
    )
)

fm_high_rated_q = (
    events_high_rated.select("event_time", "user_id")
    .writeStream
    .foreachBatch(fm_tracker_high_rated.process_batch)
    .option("checkpointLocation", "delta/_ckpt_fm_high_rated_v1")
    .start()
)

# High-rated performance metrics
perf_tracker_high_rated = PerformanceTracker(
    spark, PerformanceConfig(sink="delta/performance_metrics_high_rated")
)

perf_high_rated_q = (
    events_high_rated.select("event_time")
    .writeStream
    .foreachBatch(perf_tracker_high_rated.process_batch)
    .option("checkpointLocation", "delta/_ckpt_perf_high_rated_v1")
    .trigger(processingTime="5 seconds")
    .start()
)

print("[Targeted Filtering] Started high-rated (≥4 stars) sketching pipelines")

# --- Accuracy reporting (systematic FM error and Bloom FP/FN analysis) ---
from metrics_accuracy import AccuracyConfig, AccuracyReporter

accuracy_reporter_all = AccuracyReporter(
    spark, AccuracyConfig(
        fm_accuracy_sink="delta/accuracy_fm_summary",
        bloom_accuracy_sink="delta/accuracy_bloom_summary"
    )
)

accuracy_reporter_high_rated = AccuracyReporter(
    spark, AccuracyConfig(
        fm_accuracy_sink="delta/accuracy_fm_summary_high_rated",
        bloom_accuracy_sink="delta/accuracy_bloom_summary_high_rated"
    )
)

def generate_accuracy_reports_periodically():
    """Background thread to generate accuracy reports periodically."""
    # Wait a bit for data to accumulate
    time.sleep(300)  # Wait 5 minutes before first report
    
    while True:
        try:
            print("[Accuracy] Generating accuracy reports...")
            # Report for all reviews
            accuracy_reporter_all.generate_all_reports(suffix="")
            # Report for high-rated reviews
            accuracy_reporter_high_rated.generate_all_reports(suffix="_high_rated")
        except Exception as e:
            print(f"[Accuracy] Error generating reports: {e}")
        time.sleep(600)  # Generate reports every 10 minutes

# Start background thread for accuracy reporting
accuracy_thread = threading.Thread(target=generate_accuracy_reports_periodically, daemon=True)
accuracy_thread.start()
print("[Accuracy] Started background thread for accuracy reporting (reports every 10 minutes)")

# Note: For city/franchise filtering, you would need to:
# 1. Load business data (city, franchise info) as a static DataFrame
# 2. Join with events: events.join(business_df, "business_id", "left")
# 3. Filter: events.filter(F.col("city") == "Las Vegas") or events.filter(F.col("franchise") == "Starbucks")
# 4. Run the same sketching algorithms on the filtered stream

# --- Keep app alive ---
for q in spark.streams.active:
    q.awaitTermination()
