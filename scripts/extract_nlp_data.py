# scripts/extract_nlp_data.py

import os
import logging
import fsspec
from scripts.utils import get_env_variable
from scripts.generate_sas_token import generate_sas_token

def extract_nlp_data():
    """Extrait les données NLP de tous les sous-dossiers du data lake."""
    try:
        logger = logging.getLogger(__name__)

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
        nlp_files = fs.glob(f"{container_name}/{folder_name}/**")

        # Dossier de sortie
        output_dir = os.path.join("raw_data", "nlp_data")
        os.makedirs(output_dir, exist_ok=True)

        for nlp_file in nlp_files:
            file_name = os.path.basename(nlp_file)
            local_file_path = os.path.join(output_dir, file_name)

            # Ignorer les dossiers
            if nlp_file.endswith('/'):
                continue

            # Vérifier si le fichier existe déjà
            if os.path.exists(local_file_path):
                logger.info(f"Le fichier {file_name} existe déjà. Vérification des mises à jour...")

                # Comparer les tailles de fichier pour détecter les changements
                remote_size = fs.size(nlp_file)
                local_size = os.path.getsize(local_file_path)

                if remote_size == local_size:
                    logger.info(f"Aucune mise à jour pour {file_name}.")
                    continue
                else:
                    logger.info(f"Mise à jour détectée pour {file_name}. Téléchargement du nouveau fichier.")

            # Télécharger le fichier
            with fs.open(nlp_file, 'rb') as remote_file:
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
            logger.info(f"Téléchargé {file_name} vers {local_file_path}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des données NLP : {e}")
