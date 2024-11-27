# scripts/setup_folders.py
import os

def create_directories():
    directories = [
        "logs",
        "raw_data",
        "raw_data/sql_data",
        "raw_data/parquet_data",
        "raw_data/parquet_data/images",
        "raw_data/zip_data",
        "raw_data/nlp_data",
        "csv_data",
        "csv_data/transformed_sql",
        "csv_data/transformed_parquet",
        "csv_data/transformed_zip",
        "csv_data/transformed_nlp"
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
