import logging
import traceback
from script_folder.extract_sql import extract_sql
from script_folder.extract_parquet import extract_parquet
from script_folder.extract_csv import extract_csv
from script_folder.utils import setup_logger

def main():
    # Initialisation du logger
    setup_logger()

    logging.info("Démarrage de l'extraction multisource.")

    try:
        extract_sql()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction SQL : {e}")
        traceback.print_exc()

    # try:
    #     extract_parquet()
    # except Exception as e:
    #     logging.error(f"Erreur lors de l'extraction Parquet : {e}")
    #     traceback.print_exc()

    # try:
    #     extract_csv()
    # except Exception as e:
    #     logging.error(f"Erreur lors de l'extraction CSV : {e}")
    #     traceback.print_exc()

    logging.info("Processus d'extraction terminé.")

if __name__ == "__main__":
    main()
