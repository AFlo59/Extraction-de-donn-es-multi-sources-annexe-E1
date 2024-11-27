# scripts/dupli_sql_data.py
import os
import shutil
import logging
from scripts.utils import get_env_variable

def duplicate_sql_data(folders):
    """
    Duplique les données SQL de raw_data/sql_data vers csv_data/sql_data.

    :param folders: Liste des noms de schémas à dupliquer
    :return: Booléen indiquant si des fichiers ont été dupliqués
    """
    duplicated = False
    pipeline_logger = logging.getLogger('pipeline')

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
                pipeline_logger.info(f"Fichier SQL copié : {file_name} -> {target_folder}")
                duplicated = True
            else:
                pipeline_logger.warning(f"Fichier ignoré (non CSV) : {file_name}")

    return duplicated
