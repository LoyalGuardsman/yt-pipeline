import os
import pandas as pd
import json
from datetime import datetime, timezone

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# If the cell contains a pandas Timestamp, convert it to ISO string format.
# Otherwise, returns the cell unchanged
def convert_timestamp(cell):
    if isinstance(cell, pd.Timestamp):
        return cell.isoformat()
    return cell

# Saves a buffer of video data to both CSV and JSON formats
def save_video_to_csv_and_json(region, **kwargs):

    task_instance = kwargs['ti']
    buffer = task_instance.xcom_pull(task_ids=f'fetch_trending_{region.lower()}')

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M")
    folder_path = os.path.join(project_root, "data_lake", region)
    os.makedirs(folder_path, exist_ok=True)

    filename_csv = f"trending_{region}_{timestamp}.csv"
    file_path_csv = os.path.join(folder_path, filename_csv)

    filename_json = f"trending_{region}_{timestamp}.json"
    file_path_json = os.path.join(folder_path, filename_json)
  
    df = pd.DataFrame(buffer)
    df = df.applymap(convert_timestamp)

    df.to_csv(file_path_csv, index=False)

    with open(file_path_json, "w", encoding="utf-8") as json_file:
        json.dump(df.to_dict(orient="records"), json_file, ensure_ascii=False, indent=2)

    print(f"Saved {len(df)} records to:")
    print(f"CSV  → {file_path_csv}")
    print(f"JSON → {file_path_json}")