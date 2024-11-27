# scripts/dupli_nlp_data.py
import os
import shutil
import logging
from scripts.utils import setup_logger

def duplicate_nlp_data():
    """
    Duplique les fichiers CSV de raw_data/nlp_data vers csv_data/transformed_nlp,
    en conservant la structure des sous-dossiers.
    
    :return: Booléen indiquant si des fichiers ont été dupliqués
    """
    duplicated = False
    pipeline_logger = logging.getLogger('pipeline')

    raw_dir = os.path.join("raw_data", "nlp_data")
    csv_dir = os.path.join("csv_data", "transformed_nlp")
    os.makedirs(csv_dir, exist_ok=True)

    for folder_name, _, files in os.walk(raw_dir):
        relative_folder = os.path.relpath(folder_name, raw_dir)
        output_folder = os.path.join(csv_dir, relative_folder)
        os.makedirs(output_folder, exist_ok=True)

        for file_name in files:
            if file_name.endswith(".csv"):
                source_file = os.path.join(folder_name, file_name)
                target_file = os.path.join(output_folder, file_name)
                shutil.copy(source_file, target_file)
                pipeline_logger.info(f"Fichier NLP copié : {file_name} -> {output_folder}")
                duplicated = True
            else:
                pipeline_logger.warning(f"Fichier ignoré (non CSV) : {file_name}")

    return duplicated

if __name__ == "__main__":
    # Pour les tests manuels, mais normalement non utilisé
    setup_logger('pipeline', 'logs/pipeline.log')
    pipeline_logger = logging.getLogger('pipeline')
    pipeline_logger.info("Démarrage du traitement NLP Data...")
    duplicate_nlp_data()
    pipeline_logger.info("Traitement NLP Data terminé.")
