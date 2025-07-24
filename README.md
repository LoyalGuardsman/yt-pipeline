# project1-yt-pipeline
An end-to-end Youtube Trending data pipeline built with Python and containerized using Docker.

This project automates the process of ingesting, processing, storing, and visualizing trending video data
across multiple regions using:

Python - Core language for data ingestion, processing and visualization (pandas, psycopg2, kafka)

Docker - Manages isolated environments for all services (Airflow, Kafka, PostgreSQL, Streamlit)

Airflow - Orchestrates and schedules each pipeline step

Kafka - Streams real-time data between components

PostgreSQL - Stores structured video and category data

Streamlit - Provides an interactive dashboard to explore top videos and categories

The pipeline fetches live data from the Youtube API, processes it with Python scripts managed by Airflow, streams it via Kafka,
stores it in PostgreSQL, and presents it via a dockerized Streamlit dashboard.
