# scripts/extract_parquet.py
import os
import logging
import fsspec
from scripts.utils import get_env_variable
from scripts.generate_sas_token import generate_sas_token

def extract_parquet():
    """Extrait les fichiers Parquet du data lake et sauvegarde les images et métadonnées."""
    updated = False  # Indicateur de mise à jour

    try:
        # Initialiser le logger
        logger = logging.getLogger(__name__)

        # Variables d'environnement
        account_name = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("PARQUET_FOLDER")  # 'product_eval'
        storage_account_key = get_env_variable("STORAGE_ACCOUNT_KEY")

        # Générer le SAS token
        sas_token = generate_sas_token(container_name)

        # Configuration du système de fichiers
        fs = fsspec.filesystem(
            'az',
            account_name=account_name,
            sas_token=sas_token
        )

        # Lister tous les fichiers Parquet dans le dossier
        parquet_files = fs.glob(f"{container_name}/{folder_name}/*.parquet")

        if not parquet_files:
            logger.error(f"Aucun fichier Parquet trouvé dans {folder_name}/")
            return

        # Dossier de sortie
        output_dir = os.path.join("raw_data", "parquet_data")
        images_dir = os.path.join(output_dir, "images")
        os.makedirs(images_dir, exist_ok=True)

        for parquet_path in parquet_files:
            file_name = os.path.basename(parquet_path)
            local_parquet_path = os.path.join(output_dir, file_name)

            if os.path.exists(local_parquet_path):
                logger.info(f"Le fichier {file_name} existe déjà. Vérification des mises à jour...")
                remote_size = fs.size(parquet_path)
                local_size = os.path.getsize(local_parquet_path)

                if remote_size == local_size:
                    logging.info(f"Aucune mise à jour pour {file_name}.")
                    continue
                else:
                    logging.info(f"Mise à jour détectée pour {file_name}. Téléchargement du nouveau fichier.")
                    updated = True
            else:
                updated = True

            with fs.open(parquet_path, 'rb') as remote_file:
                with open(local_parquet_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
            logger.info(f"Téléchargé {file_name} vers {local_parquet_path}")

    except Exception as e:
        logging.error(f"Erreur lors de l'extraction Parquet : {e}")

    return updated
