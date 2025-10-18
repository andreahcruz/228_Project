Layout of folder:
<img width="222" height="545" alt="image" src="https://github.com/user-attachments/assets/abe08133-6ada-4e0f-8345-05196ecf07f2" />

Prerequisites:

Python 3.9+ (uses venv)
Java 17 (required by Spark 3.5.x)
Docker + Docker Compose (for Kafka)

# 0) clone the repo, cd into it

# 1) Python env
python3 -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install pybloom-live               # for Bloom filter (or use the SimpleBloom fallback)

# 2) Java 17 in your PATH (macOS/Homebrew example)
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

# 3) Start Kafka (single broker)
docker compose up -d
docker compose ps                      # should show kafka + zookeeper

Create topic (only for first time):
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create --topic reviews --partitions 3 --replication-factor 1

Running the pipeline:
In terminal 1->
source .venv/bin/activate
export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

python spark/stream_reviews.py
Spark UI: http://localhost:4040
<img width="1115" height="936" alt="Screenshot 2025-10-17 at 4 28 02 PM" src="https://github.com/user-attachments/assets/72f66c51-c1aa-4c81-a5de-a3a3e7d65209" />


 <img width="1770" height="276" alt="Screenshot 2025-10-17 at 6 20 57 PM" src="https://github.com/user-attachments/assets/d8cac2a6-e04e-4045-bb99-2bed3808a184" />


In terminal 2-> #NOTE: this can take up to 6-7 mins to completely run
source .venv/bin/activate
python producer/send_reviews.py
# prints: "done" 
The localhost page should update so you see the blue ticks and you can run the lines in the data prep notebook to verify
<img width="712" height="324" alt="Screenshot 2025-10-17 at 6 21 20 PM" src="https://github.com/user-attachments/assets/8d59a3a1-abad-46b1-89e7-5ac01236e25f" />

Progress so far:

-Streaming stack is up and running. You stood up Kafka + Zookeeper via Docker and exposed the broker on localhost:9092.
-Data replay/ingest working. A Python Kafka producer (send_reviews.py) replays Yelp JSONL lines to the reviews topic at ~20 msgs/sec (0.05s sleep).
-Spark Structured Streaming job running stably. The consumer (stream_reviews.py) parses the JSON into typed columns, derives event_time, uses a 2-minute watermark and 1-minute tumbling windows,
writes metrics to Parquet/Delta with checkpoints, on a 5-second trigger, and also appends a bronze copy of raw events.
-Duplicate-review detection prototype. Implemented a driver-side Bloom filter using pybloom_live (capacity 200k, ~1% FP), emitting suspected duplicates to a Delta folder.

Operational evidence:
-Spark UI shows 549 completed jobs, 0 failed, and an active query with Avg Input ≈ 17/s and Avg Process ≈ 228/s (screenshots).
-A quick Pandas read of your delta/metrics_1m/*.parquet confirms the windowed output schema (window_start, window_end, n_reviews, avg_stars). The small n_reviews = 2 rows you saw are expected at your current input rate and window size.

Early findings / results:
-End-to-end streaming works from producer ⇒ Kafka ⇒ Spark ⇒ Delta.
-Windowed KPIs materialize correctly: you’re computing per-minute review counts and average stars with late-data handling via watermark.
-Throughput headroom: Spark processes well above ingest rate (Processing/sec >> Input/sec), so you have capacity to scale the producer rate.

Difficulties & how you’ll resolve them (Or maybe you can solve this for now)

-Limited message rate keeps windows tiny (e.g., n_reviews=2).
Plan: temporarily remove or reduce the producer sleep to stress the pipeline; optionally run multiple producers to hit target rates.

-Bloom filter is driver-held (demo-friendly, not distributed).
Plan: keep it for the prototype; document FP behavior and measure against exact duplicates on small windows.

Remaining tasks:

-Reservoir Sampling (uniform sample per window).
-Flajolet-Martin (distinct users per window) with small-window exact vs estimate error.
-Targeted sketching: pre-filters for cuisines/regions (as proposed).
-Evaluation suite: throughput, end-to-end latency, checkpoint/state size, and accuracy vs. exact on sampled windows.
-Notebook/dashboard: simple visuals over Delta tables (trends of n_reviews, avg_stars, distinct users, duplicate rate).

Bronze: the raw Kafka reviews you append to Delta (one file per micro-batch, partitioned by ingest_date).
Silver: parsed reviews with proper types, duplicates filtered (Bloom helps detect), late-data rules applied.
Gold: your 1-minute window metrics (window_start, window_end, n_reviews, avg_stars), plus future distinct-user counts (FM) and sampled subsets (reservoir) if you publish them as curated tables.
