# script.py
import logging
import traceback
from scripts.extract_sql import extract_sql
from scripts.extract_parquet import extract_parquet
from scripts.extract_zip import extract_zip
from scripts.extract_nlp_data import extract_nlp_data
from scripts.utils import setup_logger
from scripts.setup_folders import create_directories

def main():
    # Création des dossiers nécessaires
    create_directories()

    # Initialisation du logger
    setup_logger()

    logging.info("Démarrage de l'extraction de données multisource.")

    try:
        extract_sql()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction SQL : {e}")
        traceback.print_exc()

    try:
        extract_parquet()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction Parquet : {e}")
        traceback.print_exc()

    try:
        extract_zip()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction du fichier ZIP : {e}")
        traceback.print_exc()

    try:
        extract_nlp_data()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction des données NLP : {e}")
        traceback.print_exc()

    logging.info("Processus d'extraction terminé.")

if __name__ == "__main__":
    main()
