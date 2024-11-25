# script_folder/extract_sql.py
import logging
import os
from dotenv import load_dotenv
import pandas as pd
import pyodbc
from script_folder.utils import get_env_variable

# Charger les variables d'environnement
load_dotenv()

# Configuration du logger
LOGS_DIRECTORY = "./logs"
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)

logging.basicConfig(
    filename="./logs/sql_extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def connect_to_sql_server():
    """Établit la connexion au serveur SQL."""
    try:
        server = get_env_variable("SQL_SERVER")
        database = get_env_variable("SQL_DB")
        login = get_env_variable("SQL_ID")
        password = get_env_variable("SQL_PW")
        
        logging.info(f"Connexion au serveur : {server}, base de données : {database}")
        
        connection = pyodbc.connect(
            f"Driver={get_env_variable('DRIVER')};"
            f"Server={server};"
            f"Database={database};"
            f"UID={login};"
            f"PWD={password};"
            f"Encrypt={get_env_variable('ENCRYPT')};"
            f"TrustServerCertificate={get_env_variable('TrustServerCertificate')};"
            f"Connection Timeout={get_env_variable('Connection_Timeout')};"
        )
        logging.info("Connexion réussie au serveur SQL.")
        return connection
    except Exception as e:
        logging.error(f"Erreur de connexion au serveur SQL : {e}")
        raise

def extract_data(connection, query, output_file):
    """
    Extrait les données de la base de données Azure SQL grâce à une requête SQL.
    Enregistre les données dans un fichier CSV.
    """
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Dossier {output_dir} créé.")
    
    try:
        df = pd.read_sql_query(query, connection)
        logging.info(f"Les 5 premières lignes des données extraites :\n{df.head()}")
        df.to_csv(output_file, index=False)
        logging.info(f"Données extraites et sauvegardées dans {output_file}.")
    except Exception as e:
        logging.error(f"Une erreur s'est produite lors de l'extraction des données : {e}")
        raise

def extract_sql():
    """Fonction principale pour l'extraction SQL."""
    try:
        # Vérifier que toutes les variables d'environnement nécessaires sont présentes
        required_vars = ["SQL_SERVER", "SQL_DB", "SQL_ID", "SQL_PW"]
        for var in required_vars:
            if not os.getenv(var):
                logging.error(f"La variable d'environnement {var} est manquante.")
                raise ValueError(f"La variable d'environnement {var} est manquante.")
        
        connection = connect_to_sql_server()
        query = "SELECT * FROM Sales.Customer"
        output_file = "csv_folder/sql_data.csv"
        extract_data(connection, query, output_file)
        connection.close()
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction SQL : {e}")
        print(f"Erreur lors de l'extraction SQL : {e}")

if __name__ == "__main__":
    extract_sql()
