# scripts/extract_zip.py

import logging
import os
import fsspec
import zipfile
from scripts.utils import get_env_variable
from scripts.generate_sas_token import generate_sas_token

def extract_zip():
    """Extrait le fichier ZIP du data lake et sauvegarde les données CSV."""
    try:
        logger = logging.getLogger(__name__)

        # Variables d'environnement
        account_name = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("ZIP_FOLDER")  # 'machine_learning'
        zip_file_name = get_env_variable("ZIP_FILE_NAME")  # 'reviews.zip'

        # Générer le SAS token
        sas_token = generate_sas_token(container_name)

        # Configuration du système de fichiers
        fs = fsspec.filesystem(
            'az',
            account_name=account_name,
            sas_token=sas_token
        )

        # Chemin vers le fichier ZIP dans le data lake
        zip_path = f"{container_name}/{folder_name}/{zip_file_name}"

        # Dossier de sortie
        output_dir = os.path.join("raw_data", "zip_data")
        os.makedirs(output_dir, exist_ok=True)

        # Vérifier si le fichier ZIP existe déjà localement
        local_zip_path = os.path.join(output_dir, zip_file_name)
        if os.path.exists(local_zip_path):
            logger.info(f"Le fichier ZIP {zip_file_name} existe déjà localement. Vérification des mises à jour...")

            # Comparer les tailles de fichier pour détecter les changements
            remote_size = fs.size(zip_path)
            local_size = os.path.getsize(local_zip_path)

            if remote_size == local_size:
                logger.info(f"Aucune mise à jour pour {zip_file_name}.")
            else:
                logger.info(f"Mise à jour détectée pour {zip_file_name}. Téléchargement du nouveau fichier.")
                # Télécharger le fichier ZIP
                with fs.open(zip_path, 'rb') as remote_file:
                    with open(local_zip_path, 'wb') as local_file:
                        local_file.write(remote_file.read())
                logger.info(f"Téléchargé le fichier ZIP vers {local_zip_path}")
        else:
            # Télécharger le fichier ZIP
            with fs.open(zip_path, 'rb') as remote_file:
                with open(local_zip_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
            logger.info(f"Téléchargé le fichier ZIP vers {local_zip_path}")

        # Extraire le fichier ZIP
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_dir)
            logger.info(f"Fichier ZIP {zip_file_name} extrait dans {output_dir}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
