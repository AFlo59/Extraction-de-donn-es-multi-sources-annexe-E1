# Extraction-de-donn-es-multi-sources-annexe-E1

# Extraction de données multisource

## Structure du projet
- **script.py** : Point d'entrée pour exécuter tous les scripts.
- **script_folder/** : Contient les scripts modulaires pour l'extraction SQL, Parquet, CSV, et les utilitaires.
- **csv_folder/** : Contient les fichiers extraits.
- **logs/** : Stocke les fichiers de logs.
- **requirements.txt** : Liste des dépendances Python.

## Installation
1. Créez un environnement virtuel :
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows

project/
│
├── script.py                 # Script principal orchestrant les extractions
├── .env                      # Variables d'environnement (confidentiales)
├── .gitignore                # Ignore venv et fichiers sensibles
├── README.md                 # Documentation du projet
├── requirements.txt          # Dépendances Python
├── venv/                     # Environnement virtuel
├── script_folder/            # Scripts modulaires pour les extractions
│   ├── extract_sql.py        # Extraction SQL
│   ├── extract_parquet.py    # Extraction Parquet
│   ├── extract_csv.py        # Extraction CSV
│   ├── setup_folders.py        
│   └── utils.py              # Fonctions utilitaires communes
├── csv_folder/               # Dossier de destination des fichiers CSV extraits
│   ├── sql_data/
|   |   └── xschema/
|   |       └── xtable.csv
│   ├── parquet_metadata/
|   |   ├── metadata_xx.csv
|   |   └── images/xxx.png 
│   └── csv_data/
|       └── xx.csv 
└── logs/                     # Dossier pour les fichiers de logs
    └── extraction.log
