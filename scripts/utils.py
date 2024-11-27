# scripts/utils.py
import logging
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

def setup_logger(name, log_file, level=logging.INFO):
    """
    Configure et retourne un logger avec le nom spécifié et le fichier de log.

    :param name: Nom du logger (ex: 'extraction_sql', 'pipeline', 'schedule')
    :param log_file: Chemin vers le fichier de log
    :param level: Niveau de log (par défaut INFO)
    :return: Logger configuré
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Éviter d'ajouter plusieurs handlers si le logger est déjà configuré
    if not logger.handlers:
        # Handler pour le fichier de log
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Handler pour la console (optionnel)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

def get_env_variable(key):
    """Récupère une variable d'environnement ou retourne une erreur."""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"La variable d'environnement {key} est introuvable.")
    return value
