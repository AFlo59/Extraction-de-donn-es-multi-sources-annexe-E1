import os
import logging
import pyarrow.parquet as pq
import fsspec
from scripts.utils import get_env_variable
from scripts.generate_sas_token import generate_sas_token

def extract_parquet():
    """Extrait les fichiers Parquet du data lake et sauvegarde les images et métadonnées."""
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

            # Vérifier si le fichier existe déjà
            if os.path.exists(local_parquet_path):
                logger.info(f"Le fichier {file_name} existe déjà. Vérification des mises à jour...")

                # Comparer les tailles de fichier pour détecter les changements
                remote_size = fs.size(parquet_path)
                local_size = os.path.getsize(local_parquet_path)

                if remote_size == local_size:
                    logger.info(f"Aucune mise à jour pour {file_name}.")
                    continue
                else:
                    logger.info(f"Mise à jour détectée pour {file_name}. Téléchargement du nouveau fichier.")

            # Télécharger le fichier Parquet
            with fs.open(parquet_path, 'rb') as remote_file:
                with open(local_parquet_path, 'wb') as local_file:
                    local_file.write(remote_file.read())
            logger.info(f"Téléchargé {file_name} vers {local_parquet_path}")

            # Traitement du fichier Parquet si nécessaire

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction Parquet : {e}")
