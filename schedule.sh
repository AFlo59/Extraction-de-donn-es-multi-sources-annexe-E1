#!/bin/bash

# Dossier des logs
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/schedule.log"

# Création du dossier logs s'il n'existe pas
if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Création du dossier $LOG_DIR." >> "$LOG_FILE"
fi

# Création du fichier de log s'il n'existe pas
if [ ! -f "$LOG_FILE" ]; then
    touch "$LOG_FILE"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Création du fichier $LOG_FILE." >> "$LOG_FILE"
fi

# Redirection des logs de schedule.sh uniquement vers le fichier log
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$(date '+%Y-%m-%d %H:%M:%S') - Début de l'exécution de schedule.sh."

# Vérification du système d'exploitation
OS=$(uname)
PYTHON="python3"

if [[ "$OS" == "Linux" ]]; then
    PYTHON="python3"
elif [[ "$OS" == "MINGW"* || "$OS" == "CYGWIN"* ]]; then
    PYTHON="python"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Système d'exploitation non pris en charge : $OS"
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Vérification de l'environnement virtuel."

# Vérification et installation via install.sh si nécessaire
if [ ! -d "venv" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Environnement virtuel non trouvé. Exécution de install.sh."
    chmod +x ./install.sh
    ./install.sh
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Environnement virtuel trouvé."
fi

# Activation de l'environnement virtuel
echo "$(date '+%Y-%m-%d %H:%M:%S') - Activation de l'environnement virtuel."
source venv/bin/activate

# Exécution du script Python principal
echo "$(date '+%Y-%m-%d %H:%M:%S') - Exécution du script principal script.py."
$PYTHON script.py

if [ $? -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Script principal terminé avec succès."
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Erreur lors de l'exécution de script.py."
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Fin de l'exécution de schedule.sh."

# Archivage des logs
echo "$(date '+%Y-%m-%d %H:%M:%S') - Lancement de l'archivage des logs." >> "$LOG_FILE"
chmod +x ./archive_logs.sh
./archive_logs.sh
echo "$(date '+%Y-%m-%d %H:%M:%S') - Archivage des logs terminé." >> "$LOG_FILE"

deactivate
