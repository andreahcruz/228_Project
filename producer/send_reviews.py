import json, time, pathlib
from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers="localhost:9092",
                  value_serializer=lambda v: json.dumps(v).encode())
src = pathlib.Path("data/landing/yelp_small.jsonl")

for line in src.open():
    p.send("reviews", json.loads(line))
    time.sleep(0.05)  # ~20 msgs/sec
p.flush()
print("done")
