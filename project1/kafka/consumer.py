import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaConsumer
from dotenv import load_dotenv
from utils.file_saver import save_video_to_csv_and_json

# Kafka consumer script

load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

REGIONS = os.getenv("REGIONS").split(",")
TOPICS = [f"trending_{region.strip().lower()}" for region in REGIONS]

print(f" Connecting to Kafka broker: {KAFKA_BROKER}")
print(f" Subscribing to topic: {TOPICS}")

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    group_id="trending-consumer-group"
)

print("Listening for messages...\n")

buffer = []

for message in consumer:
    video = message.value
    print(f"Received message from topic: {message.topic}")
    region = message.topic.split("_")[-1]
    buffer.append(video)
    print(f"{video['title']} | {video['channel']} | {video['view_count']} views")

    if len(buffer) >= 50:
        save_video_to_csv_and_json(buffer, region)
        buffer = []


