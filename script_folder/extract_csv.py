# script_folder/extract_csv.py
import os
import polars as pl
import fsspec
import zipfile
from script_folder.utils import get_env_variable
from script_folder.generate_sas_token import generate_sas_token

def extract_csv():
    """Extrait les fichiers CSV compressés et enregistre dans un dossier."""
    try:
        # Variables d'environnement
        storage_account = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("DOSSIER_2")  # machine_learning

        # Générer le SAS token
        sas_token = generate_sas_token()

        # Configuration du système de fichiers avec le conteneur spécifié
        fs = fsspec.filesystem('abfs', account_name=storage_account, sas_token=sas_token, container_name=container_name)

        # Chemin du fichier ZIP (relatif au conteneur)
        zip_path = f"/{folder_name}/reviews.zip"

        # Vérifier si le fichier existe
        if not fs.exists(zip_path):
            print(f"Le fichier {zip_path} n'existe pas.")
            return

        # Lire le fichier ZIP
        with fs.open(zip_path, 'rb') as f:
            with zipfile.ZipFile(f) as z:
                # Lister les fichiers dans le ZIP
                file_list = z.namelist()
                print("Fichiers dans le ZIP :", file_list)
                
                # Trouver le fichier CSV dans le ZIP
                csv_filename = None
                for filename in file_list:
                    if filename.endswith('.csv'):
                        csv_filename = filename
                        break

                if csv_filename is None:
                    print("Aucun fichier CSV trouvé dans le ZIP.")
                    return

                # Lire le fichier CSV à l'intérieur du ZIP
                with z.open(csv_filename) as csvfile:
                    # Lire le CSV avec polars
                    df = pl.read_csv(csvfile)

        # Sélectionner les colonnes pertinentes
        df_filtered = df.select(["customer_id", "review", "rating"])

        # Dossier de sortie
        output_dir = os.path.join("csv_folder", "csv")
        os.makedirs(output_dir, exist_ok=True)

        # Sauvegarder dans un CSV local (écrasement)
        output_csv = os.path.join(output_dir, "csv_data.csv")
        df_filtered.write_csv(output_csv)

        print("Extraction CSV réussie.")
    except Exception as e:
        print(f"Erreur lors de l'extraction CSV : {e}")
