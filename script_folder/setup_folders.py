# script_folder/setup_folders.py
import os

def create_directories():
    directories = [
        "logs",
        "csv_folder",
        "csv_folder/parquet_csv",
        "csv_folder/csv",
        # Ajoutez d'autres dossiers si nécessaire
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
