import sys
import os
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaProducer
from dotenv import load_dotenv
from config import REGIONS
from data_ingestion.fetch_trending import fetch_trending_videos, clean_trending_data

# This script fetches trending video data by region and streams it to Kafka.

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
)

for region in REGIONS:
    print(f"\nSending trending videos for: {region}")
    
    df = fetch_trending_videos(region)
    df = clean_trending_data(df)

    topic = f"trending_{region.lower()}"
    for _, row in df.iterrows():
        producer.send(topic, row.to_dict())
    
    print(f"Sent {len(df)} videos to topic: {topic}")

producer.flush()
producer.close()