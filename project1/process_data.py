import os
import pandas as pd

# Goes through each subfolder inside the data_lake directory.
# Finds the most recent .csv file inside each folder, 
# loads it, and combines them all into one DataFrame.
def load_latest_csv_files(data_lake_path="data_lake"):
    all_dfs = []

    for region_folder in os.listdir(data_lake_path):
        region_path = os.path.join(data_lake_path, region_folder)

        if os.path.isdir(region_path):
            csv_files = [file for file in os.listdir(region_path) if file.endswith(".csv")]

            if not csv_files:
                continue 

            csv_files.sort()
            latest_file = csv_files[-1]
            file_path = os.path.join(region_path, latest_file)

            df = pd.read_csv(file_path)
            if "region" not in df.columns:
                raise ValueError(f'Missing "region" column in file: {file_path}')
            all_dfs.append(df)

    if all_dfs:
        print(f"Successfully processed {len(all_dfs)} region files.")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        return combined_df
    else:
        raise ValueError("No CSV files were loaded"
        " — ensure producer and consumer are functioning.")

# Loads the most recent CSV file of each region from the data lake.
# Returns two tables:
# 1. Top 10 videos by view count
# 2. Top 5 categories by view count
# Compatible with both Airflow and local environments for testing.
def transform_data(**kwargs):
    region = kwargs['region']

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    if os.getenv("AIRFLOW_HOME"):
        data_folder = os.path.join("/opt/airflow/data_lake", region)
    else: # For running locally in case testing
        data_folder = os.path.join(project_root, "data_lake", region)

    all_csvs = sorted(
        [file for file in os.listdir(data_folder) if file .endswith(".csv")],
        reverse=True
    )

    if not all_csvs:
        raise ValueError(f"No CSV files found in {data_folder}")

    file_path = os.path.join(data_folder, all_csvs[0])
    df = pd.read_csv(file_path)

    print("\nTop 10 Videos by View Count:\n")
    top_videos = df.sort_values("view_count", ascending=False).head(10)
    print(top_videos[["title", "channel", "region", "view_count"]])

    print("\nTop 5 Categories by Average Views:\n")
    category_views = (
        df.groupby("category")["view_count"]
        .mean()
        .sort_values(ascending=False)
        .head(5)
    )
    print(category_views)

    top_videos.to_csv(os.path.join(data_folder, "top_10_videos.csv"), index=False)
    category_views.to_csv(os.path.join(data_folder, "top_5_categories.csv"))

    return (
        top_videos.to_dict(orient="records"),
        category_views.to_dict()
    )

# Test block
if __name__ == "__main__":
    from pathlib import Path
    from datetime import datetime

    data_lake_path = Path(__file__).resolve().parent / "data_lake" 
    df = load_latest_csv_files(data_lake_path)

    top_videos, category_views = transform_data(df)

    print(top_videos[["title", "channel", "region", "view_count"]])
    print(category_views)

    processed_path = Path(__file__).resolve().parent / "data_test" / "processed"
    if not processed_path.exists():
        raise FileNotFoundError(f"Output folder does not exist: {processed_path}")

    timestamp = datetime.now().strftime("%Y-%m-%d")

    top_videos.to_csv(processed_path / f"top_10_videos_{timestamp}.csv", index=False)
    category_views.to_csv(processed_path / f"top_5_categories_{timestamp}.csv")

    print(df.head())
    transform_data(df)