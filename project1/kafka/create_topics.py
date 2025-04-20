from kafka.admin import KafkaAdminClient, NewTopic
from config import REGIONS 

# Connects to a local Kafka broker and dynamically
# creates one Kafka topic for each region defined in config.py.
admin = KafkaAdminClient(bootstrap_servers="localhost:9092")

topics = [f"trending_{region.lower()}" for region in REGIONS]
new_topics = [
    NewTopic(
        name=topic, 
        num_partitions=1, 
        replication_factor=1
    ) for topic in topics
]

admin.create_topics(new_topics=new_topics, validate_only=False)