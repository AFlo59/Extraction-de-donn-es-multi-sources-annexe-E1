# scripts/transform_parquet_data.py
import os
import logging
import pyarrow.parquet as pq

def transform_parquet_to_csv():
    """
    Transforme les fichiers Parquet en CSV et extrait les images.

    :return: Booléen indiquant si des fichiers ont été transformés
    """
    transformed = False
    pipeline_logger = logging.getLogger('pipeline')

    raw_dir = os.path.join("raw_data", "parquet_data")
    csv_dir = os.path.join("csv_data", "parquet_data")
    images_dir = os.path.join(csv_dir, "images")
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)

    for file_name in os.listdir(raw_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(raw_dir, file_name)
            try:
                table = pq.read_table(file_path)
                df = table.to_pandas()

                # Traiter les colonnes "image"
                if "image" in df.columns:
                    for idx, row in df.iterrows():
                        image_data = row["image"]
                        image_name = f"image_{idx}.png"
                        image_path = os.path.join(images_dir, image_name)
                        
                        # Sauvegarder l'image
                        with open(image_path, "wb") as img_file:
                            img_file.write(image_data)

                        # Mettre à jour le chemin dans le DataFrame
                        df.at[idx, "image_path_name"] = os.path.join("images", image_name)

                    # Supprimer la colonne "image"
                    df = df.drop(columns=["image"])

                # Enregistrer en CSV
                csv_file_name = f"{os.path.splitext(file_name)[0]}.csv"
                output_csv_path = os.path.join(csv_dir, csv_file_name)
                df.to_csv(output_csv_path, index=False)
                pipeline_logger.info(f"Fichier Parquet transformé : {file_name} -> {output_csv_path}")
                transformed = True
            except Exception as e:
                pipeline_logger.error(f"Erreur lors du traitement de {file_name}: {e}")

    return transformed
