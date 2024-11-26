#!/bin/bash

# Chemin vers le projet
PROJECT_DIR="/home/utilisateur/Bureau/E1/Extraction-de-donn-es-multi-sources-annexe-E1"

# Détecter la plateforme
OS=$(uname -s)

if [[ "$OS" == "Linux" ]]; then
    PYTHON_CMD="python3"
    ACTIVATE_CMD="source $PROJECT_DIR/venv/bin/activate"
elif [[ "$OS" == "MINGW"* || "$OS" == "CYGWIN"* || "$OS" == "Windows_NT" ]]; then
    PYTHON_CMD="python"
    ACTIVATE_CMD="source $PROJECT_DIR/venv/Scripts/activate"
else
    echo "Système d'exploitation non supporté."
    exit 1
fi

# Vérifier si install.sh doit être exécuté
if [[ ! -d "$PROJECT_DIR/venv" || ! -f "$PROJECT_DIR/requirements.txt" ]]; then
    echo "Installation ou mise à jour des dépendances..."
    bash "$PROJECT_DIR/install.sh"
fi

# Activer l'environnement virtuel
echo "Activation de l'environnement virtuel..."
eval "$ACTIVATE_CMD"

# Exécuter le script principal
echo "Exécution du script principal..."
$PYTHON_CMD "$PROJECT_DIR/script.py" >> "$PROJECT_DIR/logs/schedule.log" 2>&1

# Désactiver l'environnement virtuel
deactivate
