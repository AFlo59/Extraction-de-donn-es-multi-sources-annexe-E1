# script_folder/extract_sql.py
import logging
import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from script_folder.utils import get_env_variable

def connect_to_sql_server():
    """Établit la connexion au serveur SQL via SQLAlchemy."""
    try:
        server = get_env_variable("SQL_SERVER")
        database = get_env_variable("SQL_DB")
        login = get_env_variable("SQL_ID")
        password = get_env_variable("SQL_PW")
        driver = get_env_variable("DRIVER").strip('{}')
        encrypt = get_env_variable("ENCRYPT")
        trust_cert = get_env_variable("TrustServerCertificate")

        # Construire la chaîne de connexion
        connection_string = (
            f"mssql+pyodbc://{login}:{quote_plus(password)}@{server}/{database}"
            f"?driver={driver}"
            f"&Encrypt={encrypt}"
            f"&TrustServerCertificate={trust_cert}"
        )

        engine = create_engine(connection_string, fast_executemany=True)
        logging.info("Connexion réussie au serveur SQL via SQLAlchemy.")
        return engine
    except Exception as e:
        logging.error(f"Erreur de connexion au serveur SQL : {e}")
        raise

def get_all_tables(engine):
    """Récupère les schémas et tables spécifiés de la base de données."""
    try:
        # Liste des schémas à inclure
        included_schemas = ("Production", "Person", "Purchasing", "Sales", "HumanResources")

        # Convertir la liste en chaîne pour la requête SQL
        schemas_list = "', '".join(included_schemas)

        query = f"""
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA IN ('{schemas_list}')
        """
        df_tables = pd.read_sql_query(query, engine)
        return df_tables
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des tables : {e}")
        raise

def extract_table_data(engine, schema, table, output_file):
    """Extrait les données d'une table en excluant les types non supportés."""
    try:
        # Obtenir les colonnes et leurs types à partir de INFORMATION_SCHEMA.COLUMNS
        columns_query = f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """
        columns_df = pd.read_sql_query(columns_query, engine)

        if columns_df.empty:
            logging.warning(f"Aucune colonne trouvée dans {schema}.{table}.")
            return

        # Construire la liste des colonnes pour la requête SQL
        select_clauses = []
        unsupported_types = ['datetimeoffset', 'hierarchyid', 'xml', 'geometry', 'geography', 'image', 'sql_variant']
        for _, row in columns_df.iterrows():
            column_name = row['COLUMN_NAME']
            data_type = row['DATA_TYPE'].lower()
            if data_type == 'datetimeoffset':
                # Convertir la colonne datetimeoffset en VARCHAR
                select_clauses.append(f"CONVERT(VARCHAR(50), [{column_name}], 121) AS [{column_name}]")
                logging.warning(f"Colonne datetimeoffset convertie ({schema}.{table}): {column_name}")
            elif data_type in unsupported_types:
                # Exclure les colonnes de types non supportés
                logging.warning(f"Colonne exclue ({schema}.{table}): {column_name} (type: {data_type})")
                continue
            else:
                select_clauses.append(f"[{column_name}]")

        if not select_clauses:
            logging.warning(f"Aucune colonne supportée trouvée dans {schema}.{table}. Table ignorée.")
            return

        columns_str = ', '.join(select_clauses)
        query = f"SELECT {columns_str} FROM [{schema}].[{table}]"

        # Exécuter la requête et enregistrer les données
        df = pd.read_sql_query(text(query), engine)
        df.to_csv(output_file, index=False)
        logging.info(f"Données extraites de {schema}.{table} vers {output_file}")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction de {schema}.{table} : {e}")
        # Ne pas lever l'exception pour permettre au script de continuer avec les autres tables

def extract_sql():
    """Fonction principale pour l'extraction des tables spécifiées."""
    try:
        # Vérifier que toutes les variables d'environnement nécessaires sont présentes
        required_vars = ["SQL_SERVER", "SQL_DB", "SQL_ID", "SQL_PW", "DRIVER", "ENCRYPT", "TrustServerCertificate"]
        for var in required_vars:
            if not get_env_variable(var):
                logging.error(f"La variable d'environnement {var} est manquante.")
                raise ValueError(f"La variable d'environnement {var} est manquante.")

        engine = connect_to_sql_server()
        df_tables = get_all_tables(engine)
        logging.info(f"{len(df_tables)} tables trouvées pour l'extraction.")

        # Dossier de base pour les fichiers CSV
        base_output_dir = "csv_folder"

        # Parcourir chaque table et extraire les données
        for index, row in df_tables.iterrows():
            schema = row['TABLE_SCHEMA']
            table = row['TABLE_NAME']

            # Créer le dossier du schéma s'il n'existe pas
            schema_dir = os.path.join(base_output_dir, schema)
            if not os.path.exists(schema_dir):
                os.makedirs(schema_dir)
                logging.info(f"Dossier créé : {schema_dir}")

            # Définir le chemin du fichier de sortie
            output_file = os.path.join(schema_dir, f"{table}.csv")

            # Extraire les données de la table
            extract_table_data(engine, schema, table, output_file)

        engine.dispose()
        logging.info("Toutes les tables spécifiées ont été extraites avec succès.")
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction SQL : {e}")
        print(f"Erreur lors de l'extraction SQL : {e}")

if __name__ == "__main__":
    extract_sql()
