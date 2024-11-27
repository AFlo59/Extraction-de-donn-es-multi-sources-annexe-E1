# script.py
import logging
import traceback
import os
from scripts.extract_sql import extract_sql
from scripts.extract_parquet import extract_parquet
from scripts.extract_zip import extract_zip
from scripts.extract_nlp_data import extract_nlp_data
from scripts.utils import setup_logger
from scripts.setup_folders import create_directories

# Importer les scripts de duplication/transformation
from scripts.dupli_sql_data import duplicate_sql_data
from scripts.transform_parquet_data import transform_parquet_to_csv
from scripts.dupli_and_transform_zip_data import extract_and_convert_zip
from scripts.dupli_nlp_data import duplicate_nlp_data

def main():
    # Création des dossiers nécessaires
    create_directories()

    # Initialisation du logger pour le pipeline
    pipeline_logger = setup_logger('pipeline', 'logs/pipeline.log')

    pipeline_logger.info("Démarrage de l'extraction de données multisource.")

    # Variables pour suivre les mises à jour
    sql_updated = False
    parquet_updated = False
    zip_updated = False
    nlp_updated = False

    try:
        sql_updated = extract_sql()
    except Exception as e:
        pipeline_logger.error(f"Erreur lors de l'extraction SQL : {e}")
        traceback.print_exc()

    try:
        parquet_updated = extract_parquet()
    except Exception as e:
        pipeline_logger.error(f"Erreur lors de l'extraction Parquet : {e}")
        traceback.print_exc()

    try:
        zip_updated = extract_zip()
    except Exception as e:
        pipeline_logger.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
        traceback.print_exc()

    try:
        nlp_updated = extract_nlp_data()
    except Exception as e:
        pipeline_logger.error(f"Erreur lors de l'extraction des données NLP : {e}")
        traceback.print_exc()

    pipeline_logger.info("Processus d'extraction terminé.")

    # Logique pour vérifier les données manquantes dans csv_data
    # Initialiser les flags à False
    run_sql_duplication = False
    run_parquet_transformation = False
    run_zip_transformation = False
    run_nlp_duplication = False

    # Vérifier les données SQL
    required_sql_folders = ["Person", "Production", "Purchasing", "Sales"]
    for folder in required_sql_folders:
        target_folder = os.path.join("csv_data", "transformed_sql", folder)
        if not os.path.exists(target_folder):
            run_sql_duplication = True
            pipeline_logger.warning(f"Dossier SQL manquant : {target_folder}")
            break
        # Vérifier si au moins un CSV est présent dans chaque dossier de schéma
        has_csv = any(file.endswith(".csv") for file in os.listdir(target_folder))
        if not has_csv:
            run_sql_duplication = True
            pipeline_logger.warning(f"Aucun fichier CSV dans {target_folder}")
            break

    # Vérifier les données Parquet
    metadata_csv = os.path.join("csv_data", "transformed_parquet", "metadata_fichier.csv")
    if not os.path.exists(metadata_csv):
        run_parquet_transformation = True
        pipeline_logger.warning(f"Métadata CSV manquant : {metadata_csv}")

    # Vérifier les données ZIP
    csv_zip_dir = os.path.join("csv_data", "transformed_zip")
    has_zip_csv = False
    for root, dirs, files in os.walk(csv_zip_dir):
        for file in files:
            if file.endswith(".csv"):
                has_zip_csv = True
                break
    if not has_zip_csv:
        run_zip_transformation = True
        pipeline_logger.warning(f"Aucun fichier CSV dans {csv_zip_dir}")

    # Vérifier les données NLP
    csv_nlp_dir = os.path.join("csv_data", "transformed_nlp")
    has_nlp_csv = False
    for root, dirs, files in os.walk(csv_nlp_dir):
        for file in files:
            if file.endswith(".csv"):
                has_nlp_csv = True
                break
    if not has_nlp_csv:
        run_nlp_duplication = True
        pipeline_logger.warning(f"Aucun fichier CSV dans {csv_nlp_dir}")

    # Définir les flags pour lancer les transformations
    if sql_updated or run_sql_duplication:
        pipeline_logger.info("Les données SQL ont été mises à jour ou manquantes. Lancement du script de duplication SQL.")
        sql_dup_result = duplicate_sql_data(required_sql_folders)
        if sql_dup_result:
            pipeline_logger.info("Duplication SQL réussie.")
        else:
            pipeline_logger.warning("Aucune duplication SQL effectuée.")

    if parquet_updated or run_parquet_transformation:
        pipeline_logger.info("Les données Parquet ont été mises à jour ou manquantes. Lancement du script de transformation Parquet.")
        parquet_transform_result = transform_parquet_to_csv()
        if parquet_transform_result:
            pipeline_logger.info("Transformation Parquet réussie.")
        else:
            pipeline_logger.warning("Aucune transformation Parquet effectuée.")

    if zip_updated or run_zip_transformation:
        pipeline_logger.info("Les données ZIP ont été mises à jour ou manquantes. Lancement du script de transformation ZIP.")
        zip_transform_result = extract_and_convert_zip()
        if zip_transform_result:
            pipeline_logger.info("Transformation ZIP réussie.")
        else:
            pipeline_logger.warning("Aucune transformation ZIP effectuée.")

    if nlp_updated or run_nlp_duplication:
        pipeline_logger.info("Les données NLP ont été mises à jour ou manquantes. Lancement du script de duplication NLP.")
        nlp_dup_result = duplicate_nlp_data()
        if nlp_dup_result:
            pipeline_logger.info("Duplication NLP réussie.")
        else:
            pipeline_logger.warning("Aucune duplication NLP effectuée.")

    pipeline_logger.info("Pipeline complet terminé.")

if __name__ == "__main__":
    main()
