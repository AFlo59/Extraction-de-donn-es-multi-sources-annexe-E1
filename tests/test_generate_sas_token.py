import pytest
from unittest.mock import patch
from scripts.generate_sas_token import generate_sas_token

def test_generate_sas_token_success():
    with patch('scripts.generate_sas_token.get_env_variable') as mock_get_env:
        mock_get_env.side_effect = lambda key: {
            "DATALAKE": "test_account",
            "STORAGE_ACCOUNT_KEY": "test_key"
        }[key]
        
        with patch('azure.storage.blob.generate_container_sas') as mock_sas:
            mock_sas.return_value = "test_sas_token"
            sas_token = generate_sas_token("test_container")
            assert sas_token == "test_sas_token"

def test_generate_sas_token_missing_env():
    with patch('scripts.generate_sas_token.get_env_variable') as mock_get_env:
        mock_get_env.side_effect = ValueError("La variable d'environnement DATALAKE est introuvable.")
        
        with pytest.raises(ValueError):
            generate_sas_token("test_container")
