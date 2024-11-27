import os
import tarfile
import zipfile
import shutil
import logging
from scripts.utils import setup_logger

def extract_and_convert_zip():
    raw_dir = os.path.join("raw_data", "zip_data")
    csv_dir = os.path.join("csv_data", "zip_data")
    os.makedirs(csv_dir, exist_ok=True)

    for file_name in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file_name)
        try:
            if file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
                with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(csv_dir)
                logging.info(f"Archive TGZ extraite : {file_name}")
            elif file_name.endswith(".zip"):
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(csv_dir)
                logging.info(f"Archive ZIP extraite : {file_name}")
            elif file_name.endswith(".csv"):
                shutil.copy(file_path, csv_dir)
                logging.info(f"Fichier CSV copié : {file_name}")
            else:
                logging.warning(f"Fichier ignoré (non traité) : {file_name}")
        except Exception as e:
            logging.error(f"Erreur lors de l'extraction/conversion : {file_name} - {e}")

if __name__ == "__main__":
    setup_logger(log_file="logs/pipeline.log")
    logging.info("Démarrage du traitement ZIP Data...")
    extract_and_convert_zip()
    logging.info("Traitement ZIP Data terminé.")
