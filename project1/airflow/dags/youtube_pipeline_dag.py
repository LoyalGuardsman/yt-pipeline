from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from data_ingestion.fetch_trending import fetch_trending_videos
from utils.file_saver import save_video_to_csv_and_json
from process_data import transform_data
from load_to_postgres import load_all_regions_to_postgres
from config import REGIONS 

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2025, 4, 18),
}

# Defines the Airflow DAG.
# Dynamically generates a set of tasks per region
with DAG(
    dag_id="youtube_data_pipeline",
    default_args=default_args,
    description="Fetch, process and store YouTube trending data",
    schedule_interval="@hourly",
    start_date=datetime(2025, 4, 18),
    catchup=False,
) as dag:
    process_tasks = []

    for region in REGIONS:
        fetch_trending_task = PythonOperator(
            task_id=f"fetch_trending_{region.lower()}",
            python_callable=fetch_trending_videos,
            op_args=[region],
        )

        save_data_task = PythonOperator(
            task_id=f"save_data_{region.lower()}",
            python_callable=save_video_to_csv_and_json,
            op_args=[region],
            provide_context=True,
        )

        process_data_task = PythonOperator(
            task_id=f"process_data_{region.lower()}",
            python_callable=transform_data,
            op_kwargs={'region': region},
        )

        process_tasks.append(process_data_task)
        fetch_trending_task >> save_data_task >> process_data_task

    load_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_all_regions_to_postgres
    )

    for task in process_tasks:
        task >> load_postgres_task