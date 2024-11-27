# scripts/generate_sas_token.py
import logging
from azure.storage.blob import generate_container_sas, ContainerSasPermissions
from datetime import datetime, timedelta
from .utils import get_env_variable  # Import relatif

def generate_sas_token(container_name):
    """Génère un SAS token pour le conteneur spécifié."""
    try:
        logger = logging.getLogger(__name__)

        account_name = get_env_variable("DATALAKE")
        account_key = get_env_variable("STORAGE_ACCOUNT_KEY")

        logger.info(f"Génération du SAS token pour le conteneur {container_name}...")

        # Définir les permissions
        permissions = ContainerSasPermissions(read=True, list=True)

        # Définir la date d'expiration
        expiry_time = datetime.utcnow() + timedelta(hours=1)

        # Générer le SAS token pour le conteneur
        sas_token = generate_container_sas(
            account_name=account_name,
            container_name=container_name,
            account_key=account_key,
            permission=permissions,
            expiry=expiry_time
        )

        logger.info("SAS token généré avec succès.")
        return sas_token
    except Exception as e:
        logger.error(f"Erreur lors de la génération du SAS token : {e}")
        raise
