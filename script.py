import logging
from extraction_sql import extract_sql
from extraction_parquet import extract_parquet
from extraction_csv import extract_csv

def main():
    # Initialisation des logs
    logging.basicConfig(filename='extraction.log', level=logging.INFO)
    logging.info("Démarrage de l'extraction multi-sources.")

    try:
        # Étape 1 : Extraction SQL
        extract_sql()

        # Étape 2 : Extraction Parquet
        extract_parquet()

        # Étape 3 : Extraction CSV
        extract_csv()

        logging.info("Extraction terminée avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction : {e}")

if __name__ == "__main__":
    main()
