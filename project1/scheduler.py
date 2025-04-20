import os
import schedule
import time
from data_ingestion.fetch_trending import fetch_trending_videos
from config import REGIONS

# Simulates a real-time data stream by fetching trending YouTube videos
# at 2 minute intervals and saving them as CSV files.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def scheduler():
    for region in REGIONS:
        print(f"Fetching trending videos for {region}... ")
        df = fetch_trending_videos(region)
        file_path = os.path.join(DATA_DIR, f"trending_{region}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved {file_path} with {len(df)} records.")

schedule.every(2).minutes.do(scheduler)
print("Scheduler started. Press CTRL+C to stop.")

while True:
    schedule.run_pending()
    time.sleep(1)