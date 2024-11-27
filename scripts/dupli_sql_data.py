# scripts/dupli_sql_data.py
import os
import shutil
import logging
from scripts.utils import setup_logger

def duplicate_sql_data(folders):
    raw_dir = os.path.join("raw_data", "sql_data")
    csv_dir = os.path.join("csv_data", "sql_data")
    os.makedirs(csv_dir, exist_ok=True)

    for folder in folders:
        source_folder = os.path.join(raw_dir, folder)
        target_folder = os.path.join(csv_dir, folder)
        os.makedirs(target_folder, exist_ok=True)

        for file_name in os.listdir(source_folder):
            if file_name.endswith(".csv"):
                shutil.copy(os.path.join(source_folder, file_name), target_folder)
                logging.info(f"Fichier SQL copié : {file_name} -> {target_folder}")
            else:
                logging.warning(f"Fichier ignoré (non CSV) : {file_name}")

if __name__ == "__main__":
    setup_logger(log_file="logs/pipeline.log")
    logging.info("Démarrage du traitement SQL Data...")
    folders_to_copy = ["Person", "Production", "Purchasing", "Sales"]
    duplicate_sql_data(folders_to_copy)
    logging.info("Traitement SQL Data terminé.")
