# script.py
import logging
import traceback
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

    # Initialisation du logger
    setup_logger()

    logging.info("Démarrage de l'extraction de données multisource.")

    # Variables pour suivre les mises à jour
    sql_updated = False
    parquet_updated = False
    zip_updated = False
    nlp_updated = False

    try:
        sql_updated = extract_sql()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction SQL : {e}")
        traceback.print_exc()

    try:
        parquet_updated = extract_parquet()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction Parquet : {e}")
        traceback.print_exc()

    try:
        zip_updated = extract_zip()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
        traceback.print_exc()

    try:
        nlp_updated = extract_nlp_data()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données NLP : {e}")
        traceback.print_exc()

    logging.info("Processus d'extraction terminé.")

    # Lancer les scripts de duplication/transformation si les données ont été mises à jour
    if sql_updated:
        logging.info("Les données SQL ont été mises à jour. Lancement du script de duplication SQL.")
        duplicate_sql_data(["Person", "Production", "Purchasing", "Sales"])

    if parquet_updated:
        logging.info("Les données Parquet ont été mises à jour. Lancement du script de transformation Parquet.")
        transform_parquet_to_csv()

    if zip_updated:
        logging.info("Les données ZIP ont été mises à jour. Lancement du script de transformation ZIP.")
        extract_and_convert_zip()

    if nlp_updated:
        logging.info("Les données NLP ont été mises à jour. Lancement du script de duplication NLP.")
        duplicate_nlp_data()

    logging.info("Pipeline complet terminé.")

if __name__ == "__main__":
    main()
