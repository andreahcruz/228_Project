import json
import os
import pathlib
import time

from kafka import KafkaProducer

# Allow choosing the source file via environment variable so we can easily
# switch between the base sample and a duplicate-heavy variant for Bloom tests.
SOURCE_PATH = os.getenv(
    "YELP_SOURCE_FILE", "data/landing/yelp_small.jsonl"
)

p = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode(),
)
src = pathlib.Path(SOURCE_PATH)

with src.open() as f:
    for line in f:
        if not line.strip():
            continue
        p.send("reviews", json.loads(line))
        time.sleep(0.05)  # ~20 msgs/sec

p.flush()
print("done")
