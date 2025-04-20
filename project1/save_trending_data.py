import os
from datetime import datetime, timezone
from data_ingestion.fetch_trending import fetch_trending_videos
from config import REGIONS

# Saves trending YouTube video data as timestamped CSV and JSON files
# for each region listed in the config. Data is pulled via the YouTube API.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
output_dir = os.path.join(BASE_DIR, "data_test")

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M")

for region in REGIONS:
    df = fetch_trending_videos(region)

    # Save as CSV
    filename_csv = os.path.join(output_dir, f"trending_{region}_{timestamp}.csv")
    df.to_csv(filename_csv, index=False)

    # Save as JSON
    filename_json = os.path.join(output_dir, f"trending_{region}_{timestamp}.json")
    df.to_json(filename_json, orient="records", lines=True)

    print(f"[{region}] Saved {len(df)} videos to:")
    print(f"  CSV → {filename_csv}")
    print(f"  JSON → {filename_json}")
