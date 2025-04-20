from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="/opt/airflow/.env")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
REGIONS = os.getenv("REGIONS").split(",")  # Converts comma-separated string to list
MAX_RESULTS = int(os.getenv("MAX_RESULTS"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER")