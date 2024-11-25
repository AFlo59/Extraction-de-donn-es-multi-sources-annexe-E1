# script_folder/extract_csv.py
import os
from azure.storage.blob import BlobServiceClient, ContainerClient
import polars as pl
from script_folder.utils import get_env_variable
import io

def extract_csv():
    """Extrait les fichiers CSV compressés et enregistre dans un dossier."""
    try:
        # Variables d'environnement
        storage_account = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("DOSSIER_2")  # machine_learning
        sas_token = get_env_variable("SAS_TOKEN")

        # Création du client Blob
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=sas_token.strip('?')
        )

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=f"{folder_name}/train.csv.gz"
        )

        # Télécharger le contenu du blob dans un BytesIO
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        csv_bytes = io.BytesIO(data)

        # Charger les données CSV compressées
        df = pl.read_csv(csv_bytes, compression='gzip')

        # Sélectionner les colonnes pertinentes
        df_filtered = df.select(["customer_id", "review", "rating"])

        # Sauvegarder dans un CSV local
        output_csv = "csv_folder/csv_data.csv"
        df_filtered.write_csv(output_csv)

        print(f"Extraction CSV réussie : {output_csv}")
    except Exception as e:
        print(f"Erreur lors de l'extraction CSV : {e}")

