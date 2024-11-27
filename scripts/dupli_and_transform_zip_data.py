# scripts/dupli_and_transform_zip_data.py
import os
import tarfile
import zipfile
import shutil
import logging
from scripts.utils import setup_logger

def extract_and_convert_zip():
    """
    Extracts zip and tgz files from raw_data/zip_data to csv_data/transformed_zip.
    For zip files containing tgz, extract tgz to obtain csv files.
    Only csv files are kept in csv_data/transformed_zip.
    Archive files are not retained in csv_data/transformed_zip.
    
    :return: Bool indicating if any files were extracted/converted
    """
    transformed = False
    pipeline_logger = logging.getLogger('pipeline')
    
    raw_dir = os.path.join("raw_data", "zip_data")
    csv_dir = os.path.join("csv_data", "transformed_zip")
    os.makedirs(csv_dir, exist_ok=True)
    
    for file_name in os.listdir(raw_dir):
        file_path = os.path.join(raw_dir, file_name)
        if not (file_name.endswith(".zip") or file_name.endswith(".tgz") or file_name.endswith(".tar.gz")):
            pipeline_logger.warning(f"Fichier ignoré (non zip/tgz) : {file_name}")
            continue
        try:
            if file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
                with tarfile.open(file_path, "r:gz") as tar:
                    tar.extractall(csv_dir)
                pipeline_logger.info(f"Archive TGZ extraite : {file_name}")
                transformed = True
            elif file_name.endswith(".zip"):
                with zipfile.ZipFile(file_path, "r") as zip_ref:
                    zip_ref.extractall(csv_dir)
                pipeline_logger.info(f"Archive ZIP extraite : {file_name}")
                transformed = True
            else:
                pipeline_logger.warning(f"Fichier ignoré (extension non supportée) : {file_name}")
        except Exception as e:
            pipeline_logger.error(f"Erreur lors de l'extraction/conversion : {file_name} - {e}")
            continue
    
    # Après extraction initiale, rechercher des fichiers tgz dans transformed_zip et les extraire
    for root, dirs, files in os.walk(csv_dir):
        for file_name in files:
            if file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
                file_path = os.path.join(root, file_name)
                try:
                    with tarfile.open(file_path, "r:gz") as tar:
                        tar.extractall(os.path.dirname(file_path))
                    pipeline_logger.info(f"Archive TGZ extraite : {file_name}")
                    transformed = True
                    # Supprimer le fichier tgz après extraction
                    os.remove(file_path)
                    pipeline_logger.info(f"Archive TGZ supprimée après extraction : {file_name}")
                except Exception as e:
                    pipeline_logger.error(f"Erreur lors de l'extraction du tgz : {file_name} - {e}")
    
    # Optionnel : Supprimer tout fichier zip/tgz restant dans transformed_zip
    for root, dirs, files in os.walk(csv_dir):
        for file_name in files:
            if file_name.endswith(".zip") or file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
                file_path = os.path.join(root, file_name)
                try:
                    os.remove(file_path)
                    pipeline_logger.info(f"Archive supprimée de csv_data : {file_name}")
                except Exception as e:
                    pipeline_logger.error(f"Erreur lors de la suppression de l'archive : {file_name} - {e}")
    
    return transformed

if __name__ == "__main__":
    setup_logger('pipeline', 'logs/pipeline.log')
    logging.getLogger('pipeline').info("Démarrage du traitement ZIP Data...")
    extract_and_convert_zip()
    logging.getLogger('pipeline').info("Traitement ZIP Data terminé.")
