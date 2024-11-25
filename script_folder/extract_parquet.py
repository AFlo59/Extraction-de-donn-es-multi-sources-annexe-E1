# script_folder/extract_parquet.py
import os
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import pyarrow.fs as fs
from script_folder.utils import get_env_variable

def extract_parquet():
    """Extrait des fichiers Parquet les métadonnées et images."""
    try:
        # Variables d'environnement
        storage_account = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("DOSSIER_1")  # product_eval
        sas_token = get_env_variable("SAS_TOKEN")

        # Construction de l'URL du blob
        blob_url = f"https://{storage_account}.blob.core.windows.net/{container_name}/{folder_name}/evaluations.parquet{sas_token}"

        # Création du système de fichiers Azure
        azure_fs = fs.PyFileSystem(fs.AzureBlobFileSystem(
            account_name=storage_account,
            container_name=container_name,
            sas_token=sas_token.strip('?')
        ))

        # Lecture du fichier Parquet
        with azure_fs.open_input_file(f"{folder_name}/evaluations.parquet") as parquet_file:
            table = pq.read_table(parquet_file)
            df = table.to_pandas()

        # Traiter les données comme précédemment
        # ... (votre code pour extraire les images et métadonnées)

        print("Extraction Parquet réussie.")
    except Exception as e:
        print(f"Erreur lors de l'extraction Parquet : {e}")
