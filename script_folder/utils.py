import logging
from dotenv import load_dotenv
import os

# Charger les variables d'environnement
load_dotenv()

def setup_logger(log_file="logs/extraction.log"):
    """Configure le logger."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def get_env_variable(key):
    """Récupère une variable d'environnement ou retourne une erreur."""
    value = os.getenv(key)
    if not value:
        raise ValueError(f"La variable d'environnement {key} est introuvable.")
    return value
