# scripts/transform_parquet_data.py
import os
import logging
import pyarrow.parquet as pq
import pandas as pd
from PIL import Image
import io

def transform_parquet_to_csv():
    """
    Transforme les fichiers Parquet en un seul metadata CSV et extrait les images en PNG.

    :return: Booléen indiquant si des fichiers ont été transformés
    """
    transformed = False
    pipeline_logger = logging.getLogger('pipeline')

    raw_dir = os.path.join("raw_data", "parquet_data")
    csv_dir = os.path.join("csv_data", "transformed_parquet")
    images_dir = os.path.join(csv_dir, "images")
    metadata_csv_path = os.path.join(csv_dir, "metadata_fichier.csv")
    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(images_dir, exist_ok=True)

    metadata_records = []

    for file_name in os.listdir(raw_dir):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(raw_dir, file_name)
            try:
                table = pq.read_table(file_path)
                df = table.to_pandas()

                # Traiter les colonnes "image"
                if "image" in df.columns:
                    for idx, row in df.iterrows():
                        image_info = row["image"]
                        # Supposons que 'image' est un dict avec 'type' et 'pass'
                        image_bytes = image_info.get("byte") or image_info.get("bytes") or image_info.get("data")
                        image_name_webp = image_info.get("pass") or image_info.get("path") or f"image_{idx}.webp"

                        if not image_bytes or not image_name_webp:
                            pipeline_logger.warning(f"Image data manquante pour la ligne {idx} dans {file_name}")
                            continue

                        # Convertir webp en png
                        try:
                            image = Image.open(io.BytesIO(image_bytes))
                            # Convertir en PNG
                            image = image.convert("RGBA")
                            image_name_png = os.path.splitext(image_name_webp)[0] + ".png"
                            image_path = os.path.join(images_dir, image_name_png)
                            image.save(image_path, format="PNG")
                        except Exception as e:
                            pipeline_logger.error(f"Erreur lors de la conversion de l'image {image_name_webp}: {e}")
                            continue

                        # Mettre à jour le chemin dans le DataFrame
                        image_path_relative = os.path.join("images", image_name_png)
                        df.at[idx, "image_path_name"] = image_path_relative

                    # Supprimer la colonne "image"
                    df = df.drop(columns=["image"])

                # Accumuler les données pour le metadata CSV
                metadata_records.append(df)

                pipeline_logger.info(f"Fichier Parquet transformé : {file_name} -> {file_name}.csv")
                transformed = True
            except Exception as e:
                pipeline_logger.error(f"Erreur lors du traitement de {file_name}: {e}")

    if metadata_records:
        metadata_df = pd.concat(metadata_records, ignore_index=True)
        # Enregistrer en un seul metadata CSV
        metadata_df.to_csv(metadata_csv_path, index=False)
        pipeline_logger.info(f"Métadata CSV créé : {metadata_csv_path}")
        transformed = True

    return transformed
