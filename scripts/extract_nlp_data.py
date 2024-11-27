# scripts/extract_nlp_data.py
import os
import logging
import fsspec
from scripts.utils import get_env_variable
from scripts.generate_sas_token import generate_sas_token

def extract_nlp_data():
    """Extrait les fichiers .csv et .xlsx des sous-dossiers du data lake tout en préservant l'architecture."""
    updated = False  # Indicateur de mise à jour

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
            # Vérifier si c'est un dossier
            if fs.isdir(nlp_file):
                logger.info(f"Ignoré : {nlp_file} est un dossier.")
                continue

            # Filtrer les fichiers pour ne garder que les .csv et .xlsx
            if not (nlp_file.endswith('.csv')):
                logger.info(f"Fichier ignoré (extension non prise en charge) : {nlp_file}")
                continue

            # Obtenir le chemin relatif pour conserver la structure
            relative_path = os.path.relpath(nlp_file, f"{container_name}/{folder_name}")
            local_file_path = os.path.join(output_dir, relative_path)

            # Créer les sous-dossiers locaux si nécessaire
            local_folder_path = os.path.dirname(local_file_path)
            os.makedirs(local_folder_path, exist_ok=True)

            if os.path.exists(local_file_path):
                logger.info(f"Le fichier {local_file_path} existe déjà. Vérification des mises à jour...")

                # Comparer les tailles de fichier pour détecter les changements
                remote_size = fs.size(nlp_file)
                local_size = os.path.getsize(local_file_path)

                if remote_size == local_size:
                    logging.info(f"Aucune mise à jour pour {local_file_path}.")
                    continue
                else:
                    logging.info(f"Mise à jour détectée pour {local_file_path}. Téléchargement du nouveau fichier.")
                    updated = True
            else:
                updated = True

            # Télécharger le fichier
            with fs.open(nlp_file, 'rb') as remote_file:
                with open(local_file_path, 'wb') as local_file:
                    local_file.write(remote_file.read())

            logger.info(f"Téléchargé {local_file_path}")

    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données NLP : {e}")

    return updated

