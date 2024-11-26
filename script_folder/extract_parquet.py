# script_folder/extract_parquet.py

import os
import logging
import pandas as pd
import pyarrow.parquet as pq
import fsspec
import base64
from script_folder.utils import get_env_variable
from script_folder.generate_sas_token import generate_sas_token

def extract_parquet():
    """Extrait les données des fichiers Parquet, en sauvegardant les images, les métadonnées et les fichiers CSV complets."""
    try:
        # Initialiser le logger
        logger = logging.getLogger(__name__)

        # Variables d'environnement
        account_name = get_env_variable("DATALAKE")
        container_name = get_env_variable("CONTAINER")
        folder_name = get_env_variable("DOSSIER_1")
        sas_token = generate_sas_token()

        logger.info(f"Account Name: {account_name}")
        logger.info(f"Container Name: {container_name}")
        logger.info(f"Folder Name: {folder_name}")

        # Configuration du système de fichiers
        fs = fsspec.filesystem(
            'az',
            account_name=account_name,
            sas_token=sas_token
        )
        logger.info("Système de fichiers fsspec configuré.")

        # Lister tous les fichiers Parquet dans le dossier
        parquet_files = fs.glob(f"{container_name}/{folder_name}/*.parquet")
        logger.info(f"Fichiers Parquet trouvés : {parquet_files}")

        if not parquet_files:
            logger.error(f"Aucun fichier Parquet trouvé dans {folder_name}/")
            return

        # Dossier de sortie pour les métadonnées, les images et les CSV complets
        output_dir = os.path.join("csv_folder", "parquet_csv")
        os.makedirs(output_dir, exist_ok=True)

        output_images_dir = os.path.join(output_dir, "images")
        os.makedirs(output_images_dir, exist_ok=True)

        output_full_csv_dir = os.path.join(output_dir, "full_csv")
        os.makedirs(output_full_csv_dir, exist_ok=True)

        all_metadata = []

        for parquet_path in parquet_files:
            logger.info(f"Traitement du fichier Parquet : {parquet_path}")

            # Lecture du fichier Parquet
            with fs.open(parquet_path, 'rb') as f:
                table = pq.read_table(f)
                df = table.to_pandas()
                logger.info(f"Fichier {parquet_path} lu avec succès.")

            # Sauvegarder le DataFrame complet en CSV sans modifications
            full_csv_filename = os.path.basename(parquet_path).replace('.parquet', '_full.csv')
            full_csv_path = os.path.join(output_full_csv_dir, full_csv_filename)
            df.to_csv(full_csv_path, index=False)
            logger.info(f"Fichier CSV complet sauvegardé dans {full_csv_path}")

            # Identifier les colonnes pertinentes
            columns = df.columns.tolist()
            logger.info(f"Colonnes trouvées : {columns}")

            # Trouver la colonne d'image
            image_column = next((col for col in columns if 'image' in col.lower()), None)
            if not image_column:
                logger.warning(f"Aucune colonne d'image trouvée dans {parquet_path}. Le fichier sera traité sans extraire d'images.")

            # Identifier la colonne d'ID unique
            possible_id_columns = ['id', 'item_ID', 'item_id', 'ID', 'ItemID', 'ItemId']
            id_column = next((col for col in possible_id_columns if col in columns), None)
            if not id_column:
                logger.error(f"Aucune colonne d'identifiant unique trouvée dans {parquet_path}. Impossible de générer des noms d'images uniques.")
                continue
            else:
                logger.info(f"Colonne d'identifiant unique trouvée : {id_column}")

            # Extraire les images et préparer les métadonnées
            metadata = []
            for index, row in df.iterrows():
                row_dict = row.to_dict()

                # Extraire l'image si la colonne d'image est présente
                if image_column:
                    image_data = row_dict.pop(image_column, None)  # Exclure 'image_column' des métadonnées
                    image_id = row_dict.get(id_column)

                    if image_data and image_id:
                        image_name = f"{image_id}.png"
                        image_path = os.path.join("images", image_name)  # Chemin relatif pour 'image_path' dans le CSV
                        full_image_path = os.path.join(output_images_dir, image_name)  # Chemin complet pour sauvegarder l'image

                        # Vérifier le type de 'image_data'
                        if isinstance(image_data, str):
                            try:
                                # Si c'est une chaîne, essayer de la décoder en base64
                                image_bytes = base64.b64decode(image_data)
                            except Exception as e:
                                logger.error(f"Erreur lors du décodage de l'image pour l'ID {image_id}: {e}")
                                continue
                        elif isinstance(image_data, bytes):
                            image_bytes = image_data
                        else:
                            logger.error(f"Type de 'image_data' inattendu pour l'ID {image_id}: {type(image_data)}")
                            continue

                        # Sauvegarder l'image au format PNG
                        try:
                            with open(full_image_path, "wb") as img_file:
                                img_file.write(image_bytes)
                            logger.info(f"Image {image_name} sauvegardée avec succès.")
                            # Ajouter le chemin de l'image aux métadonnées
                            row_dict['image_path'] = image_path
                        except Exception as e:
                            logger.error(f"Erreur lors de la sauvegarde de l'image {image_name}: {e}")
                            row_dict['image_path'] = None
                    else:
                        row_dict['image_path'] = None
                else:
                    row_dict['image_path'] = None
                    logger.debug(f"Aucune image à extraire pour la ligne {index}")

                metadata.append(row_dict)

            # Sauvegarder les métadonnées du fichier courant
            file_base_name = os.path.basename(parquet_path).replace('.parquet', '')
            metadata_filename = f"metadata_{file_base_name}.csv"
            output_csv = os.path.join(output_dir, metadata_filename)
            metadata_df = pd.DataFrame(metadata)
            metadata_df.to_csv(output_csv, index=False)
            logger.info(f"Métadonnées sauvegardées dans {output_csv}")

            all_metadata.extend(metadata)

        # Sauvegarder toutes les métadonnées combinées
        combined_metadata_df = pd.DataFrame(all_metadata)
        combined_metadata_csv = os.path.join(output_dir, "metadata_files.csv")
        combined_metadata_df.to_csv(combined_metadata_csv, index=False)
        logger.info(f"Toutes les métadonnées combinées sauvegardées dans {combined_metadata_csv}")

        logger.info("Extraction Parquet réussie.")
        print("Extraction Parquet réussie.")
    except Exception as e:
        logger.error(f"Erreur lors de l'extraction Parquet : {e}")
        print(f"Erreur lors de l'extraction Parquet : {e}")
