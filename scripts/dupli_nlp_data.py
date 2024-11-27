import os
import shutil
import logging
from scripts.utils import setup_logger

def duplicate_nlp_data():
    raw_dir = os.path.join("raw_data", "nlp_data")
    csv_dir = os.path.join("csv_data", "nlp_data")
    os.makedirs(csv_dir, exist_ok=True)

    for folder_name, _, files in os.walk(raw_dir):
        relative_folder = os.path.relpath(folder_name, raw_dir)
        output_folder = os.path.join(csv_dir, relative_folder)
        os.makedirs(output_folder, exist_ok=True)

        for file_name in files:
            if file_name.endswith(".csv"):
                shutil.copy(os.path.join(folder_name, file_name), output_folder)
                logging.info(f"Fichier NLP copié : {file_name} -> {output_folder}")
            else:
                logging.warning(f"Fichier ignoré (non CSV) : {file_name}")

if __name__ == "__main__":
    setup_logger(log_file="logs/pipeline.log")
    logging.info("Démarrage du traitement NLP Data...")
    duplicate_nlp_data()
    logging.info("Traitement NLP Data terminé.")
