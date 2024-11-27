# scripts/extract_nlp_data.py
import os
import logging
import fsspec
from scripts.utils import setup_logger, get_env_variable
from scripts.generate_sas_token import generate_sas_token

# Initialiser le logger pour l'extraction NLP
extraction_logger = setup_logger('extraction_nlp', 'logs/extraction.log')

def extract_nlp_data():
    """Extrait tous les fichiers des sous-dossiers du data lake tout en préservant l'architecture."""
    updated = False  # Indicateur de mise à jour

    try:
        # Variables d'environnement
        account_name = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("NLP_FOLDER")  # 'nlp_data'

        # Générer le SAS token
        sas_token = generate_sas_token(container_name)

        # Configuration du système de fichiers
        fs = fsspec.filesystem(
            'az',
            account_name=account_name,
            sas_token=sas_token
        )

        # Lister tous les fichiers dans le dossier NLP et ses sous-dossiers
        nlp_files = fs.glob(f"{container_name}/{folder_name}/**/*")

        if not nlp_files:
            extraction_logger.warning(f"Aucun fichier trouvé dans {folder_name}/")
            return updated

        # Dossier de sortie
        output_dir = os.path.join("raw_data", "nlp_data")
        os.makedirs(output_dir, exist_ok=True)

        for nlp_file in nlp_files:
            # Vérifier si c'est un fichier
            if not fs.isfile(nlp_file):
                extraction_logger.info(f"Skipping directory {nlp_file}")
                continue

            # Obtenir le chemin relatif pour conserver la structure
            relative_path = os.path.relpath(nlp_file, f"{container_name}/{folder_name}")
            local_file_path = os.path.join(output_dir, relative_path)

            # Créer les sous-dossiers locaux si nécessaire
            local_folder_path = os.path.dirname(local_file_path)
            os.makedirs(local_folder_path, exist_ok=True)

            if os.path.exists(local_file_path):
                extraction_logger.info(f"Le fichier {local_file_path} existe déjà. Vérification des mises à jour...")

                # Comparer les tailles de fichier pour détecter les changements
                remote_size = fs.size(nlp_file)
                local_size = os.path.getsize(local_file_path)

                if remote_size == local_size:
                    extraction_logger.info(f"Aucune mise à jour pour {local_file_path}.")
                    continue
                else:
                    extraction_logger.info(f"Mise à jour détectée pour {local_file_path}. Téléchargement du nouveau fichier.")
                    updated = True
            else:
                extraction_logger.info(f"Fichier {local_file_path} non trouvé localement. Téléchargement en cours...")
                updated = True

            # Télécharger le fichier
            with fs.open(nlp_file, 'rb') as remote_file:
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(remote_file.read())

            extraction_logger.info(f"Téléchargé {local_file_path}")
            updated = True

    except Exception as e:
        extraction_logger.error(f"Erreur lors de l'extraction des données NLP : {e}")

    return updated

if __name__ == "__main__":
    extract_nlp_data()
