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
├── script.py                 # Script principal orchestrant les extractions
├── install.sh                # Script pour l'installation automatique des dépendances
├── schedule.sh               # Script pour planifier et lancer les extractions
├── .gitlab-ci.yml            # Configuration CI/CD pour valider les scripts sur la branche develop
├── .env                      # Variables d'environnement (confidentielles)
├── .gitignore                # Ignore venv et fichiers sensibles
├── README.md                 # Documentation du projet
├── requirements.txt          # Dépendances Python
├── venv/                     # Environnement virtuel
├── scripts/                  # Scripts modulaires pour les extractions
│   ├── generate_sas_token.py # Script pour générer des tokens SAS
│   ├── extract_sql.py        # Extraction des tables SQL convertie en csv
│   ├── extract_parquet.py    # Extraction Parquet
│   ├── extract_zip.py        # Extraction des fichiers ZIP
│   ├── extract_nlp_data.py   # Extraction des données NLP
│   ├── setup_folders.py      # Création des dossiers nécessaires
│   ├── dupli_and_transform_zip_data.py
│   ├── dupli_nlp_data.py
│   ├── dupli_sql_data.py
│   ├── transform_parquet_data.py
│   └── utils.py              # Fonctions utilitaires communes
├── raw_data/                 # Dossier pour les données brutes
│   ├── sql_data/
│   │   └── schema/
│   │       └── table.csv
│   ├── parquet_data/
│   │   └── fichier.parquet    # télécharge juste les fichiers .parquet
│   ├── zip_data/
│   │   └── fichier.zip
│   └── nlp_data/
│       └── sous_dossier/
│           └── fichier.csv ou xlsx ou sous_dossier/fichier.csv ou xlsx
├── csv_data/                 # Dossier pour les données transformées
│   ├── transformed_sql/
│   │   └── schema/
│   │       └── table.csv      # sélectionner
│   ├── transformed_parquet/
│   │   ├── images/            # chaque image en png extraite des parquet
│   │   └── metadata_fichier.csv # convertir les .parquet en plusieurs parties en un csv 
│   │                            # metadata correspondant à l'image extraite en png 
│   ├── transformed_nlp/
│   │   └── sous_dossier/
│   │       └── fichier.csv ou sous_dossier/fichier.csv
│   └── transformed_zip/       # tous les fichiers du zip extrait, si il y a un fichier tgz dedans
│                               # extrait tous les csv qu'il contient
└── logs/                     # Dossier pour les fichiers de logs
    ├── extraction.log
    ├── pipeline.log
    └── schedule.log


pour lire les log du cron
grep CRON /var/log/syslog
