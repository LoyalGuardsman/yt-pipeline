import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from googleapiclient.discovery import build 
from config import YOUTUBE_API_KEY, REGIONS, MAX_RESULTS
import pandas as pd
from datetime import datetime, timezone

# Retrieves trending video data from the YouTube API for 
# each region listed in the config.
# Selects relevant fields and statistics, 
# and returns a cleaned pandas data frame for further processing.
def fetch_trending_videos(region_code):
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    request = youtube.videos().list(  
        part = "snippet, statistics",
        chart = "mostPopular", 
        regionCode = region_code,
        maxResults = MAX_RESULTS
    )

    response = request.execute()
    if "items" not in response:
        raise ValueError("API response is missing list 'items'. Full response:\n" + str(response))
    items = response["items"]

    videos = []
    for item in items:
        video_data = {
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "channel": item["snippet"]["channelTitle"],
            "published": item["snippet"]["publishedAt"],
            "category": item["snippet"]["categoryId"],
            "region": region_code,
            "view_count": item["statistics"].get("viewCount", 0),
            "like_count": item["statistics"].get("likeCount", 0),
            "comment_count": item["statistics"].get("commentCount", 0),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        videos.append(video_data)

    return clean_trending_data(pd.DataFrame(videos))

# Cleans and standardizes the raw YouTube API video data.
# Selects the most relevant columns, converts numeric fields
# to integers, and parses datetime strings into pandas datetime objects 
# for analysis and plotting.
def clean_trending_data(df):
    df = df[[
        "video_id", "title", "channel", "published",
        "category", "region", "view_count", "like_count",
        "comment_count", "fetched_at"
    ]]

    df["view_count"] = df["view_count"].astype(int)
    df["like_count"] = df["like_count"].astype(int)
    df["comment_count"] = df["comment_count"].astype(int)

    df["published"] = pd.to_datetime(df["published"])
    df["fetched_at"] = pd.to_datetime(df["fetched_at"])

    return df

# Test block
if __name__ == "__main__":
    for region in REGIONS:
        print(f"Fetching trending videos for {region}...")
        df = fetch_trending_videos(region)
        print(df.head())
