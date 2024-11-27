import pytest
from unittest.mock import patch, MagicMock
from scripts.extract_nlp_data import extract_nlp_data

def test_extract_nlp_data_no_files():
    with patch('scripts.extract_nlp_data.get_env_variable') as mock_get_env:
        mock_get_env.side_effect = lambda key: {
            "DATALAKE": "test_account",
            "CONTAINER": "test_container",
            "NLP_FOLDER": "nlp_data"
        }[key]
        
        with patch('scripts.extract_nlp_data.generate_sas_token') as mock_sas:
            mock_sas.return_value = "test_sas_token"
            with patch('fsspec.filesystem') as mock_fs:
                mock_fs_instance = MagicMock()
                mock_fs_instance.glob.return_value = []
                mock_fs.return_value = mock_fs_instance
                
                result = extract_nlp_data()
                assert result == False

def test_extract_nlp_data_with_files():
    with patch('scripts.extract_nlp_data.get_env_variable') as mock_get_env:
        mock_get_env.side_effect = lambda key: {
            "DATALAKE": "test_account",
            "CONTAINER": "test_container",
            "NLP_FOLDER": "nlp_data"
        }[key]
        
        with patch('scripts.extract_nlp_data.generate_sas_token') as mock_sas:
            mock_sas.return_value = "test_sas_token"
            with patch('fsspec.filesystem') as mock_fs:
                mock_fs_instance = MagicMock()
                mock_fs_instance.glob.return_value = [
                    "test_container/nlp_data/file1.csv",
                    "test_container/nlp_data/subdir/file2.xlsx"
                ]
                mock_fs_instance.isfile.side_effect = lambda x: not x.endswith("subdir")
                mock_fs_instance.size.side_effect = lambda x: 1000
                mock_fs.return_value = mock_fs_instance
                
                with patch('builtins.open', new_callable=MagicMock) as mock_open:
                    result = extract_nlp_data()
                    assert result == True
