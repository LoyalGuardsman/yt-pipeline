import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Loads trending video data from CSV files into a PostgreSQL database.

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

# Config: Adjust these as needed
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = 5432

if os.path.exists("/opt/airflow/data_lake"):
    root_data_folder = "/opt/airflow/data_lake"
else:
    root_data_folder = "data_lake"  # For when you need to test

# Connects to a PostgreSQL database.
# Ensures required tables exist.
# Loops over each region folder inside the data lake directory.
# Reads two CSV files for each region: top_10_videos.csv and top_5_categories.csv.
# Selects and extracts relevant columns and inserts the data into the database, while avoiding duplicates.

def load_all_regions_to_postgres():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS airflow_test_table (
            note TEXT
        )
    """)
    cursor.execute("INSERT INTO airflow_test_table (note) VALUES ('Hello from Airflow')")
    conn.commit()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_10_videos (
            region TEXT,
            title TEXT,
            channel TEXT,
            view_count INTEGER,
            UNIQUE(region, title)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS top_5_categories (
            region TEXT,
            category TEXT,
            view_count DOUBLE PRECISION,
            UNIQUE(region, category)
        )
    """)

    for region in os.listdir(root_data_folder):
        region_folder = os.path.join(root_data_folder, region)

        if not os.path.isdir(region_folder):
            continue

        top_10_csv = os.path.join(region_folder, "top_10_videos.csv")
        top_5_csv = os.path.join(region_folder, "top_5_categories.csv")

        df_videos = pd.read_csv(top_10_csv)
        df_videos["region"] = region
        video_records = df_videos[["region", "title", "channel", "view_count"]].values.tolist()

        insert_videos = """
            INSERT INTO top_10_videos (region, title, channel, view_count)
            VALUES %s
            ON CONFLICT (region, title) DO UPDATE SET
                channel = EXCLUDED.channel,
                view_count = EXCLUDED.view_count
        """
        print("SAMPLE video record being inserted:", video_records[0])
        execute_values(cursor, insert_videos, video_records)

        df_categories = pd.read_csv(top_5_csv)
        df_categories["region"] = region
        cat_records = df_categories[["region", "category", "view_count"]].values.tolist()
        insert_categories = """
            INSERT INTO top_5_categories (region, category, view_count)
            VALUES %s
            ON CONFLICT (region, category) DO UPDATE SET
                view_count = EXCLUDED.view_count
        """
        print(f"Inserting {len(video_records)} videos and {len(cat_records)} categories")
        print("SAMPLE category record being inserted:", cat_records[0])
        execute_values(cursor, insert_categories, cat_records)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded into PostgreSQL successfully.")

if __name__ == "__main__":
    load_all_regions_to_postgres()