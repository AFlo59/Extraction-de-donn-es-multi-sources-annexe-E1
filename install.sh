#!/bin/bash

# Détecter la plateforme
OS=$(uname -s)

if [[ "$OS" == "Linux" ]]; then
    echo "Détection : Linux"
    PYTHON_CMD="python3"
    PIP_CMD="pip"
elif [[ "$OS" == "MINGW"* || "$OS" == "CYGWIN"* || "$OS" == "Windows_NT" ]]; then
    echo "Détection : Windows"
    PYTHON_CMD="python"
    PIP_CMD="pip"
else
    echo "Système d'exploitation non supporté."
    exit 1
fi

# Mise à jour des paquets (uniquement sur Linux)
if [[ "$OS" == "Linux" ]]; then
    echo "Mise à jour des paquets..."
    sudo apt-get update -y

    echo "Installation des dépendances système..."
    sudo apt-get install -y unixodbc-dev libpq-dev build-essential curl python3-venv python3-pip
fi

# Installation du pilote ODBC pour SQL Server (Linux uniquement)
if [[ "$OS" == "Linux" && ! -f /usr/bin/msodbcsql18 ]]; then
    echo "Installation du pilote ODBC pour SQL Server..."
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
    sudo curl -o /etc/apt/sources.list.d/mssql-release.list https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list
    sudo apt-get update -y
    sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18

    # Ajouter les outils SQL Server au PATH
    if ! grep -q '/opt/mssql-tools18/bin' ~/.bashrc; then
        echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
        source ~/.bashrc
    fi
fi

# Création de l'environnement virtuel
if [[ ! -d "venv" ]]; then
    echo "Création de l'environnement virtuel..."
    $PYTHON_CMD -m venv venv
else
    echo "L'environnement virtuel existe déjà."
fi

# Activation de l'environnement virtuel
echo "Activation de l'environnement virtuel..."
if [[ "$OS" == "Linux" ]]; then
    source venv/bin/activate
else
    source venv/Scripts/activate
fi

# Mise à jour de pip
echo "Mise à jour de pip..."
$PIP_CMD install --upgrade pip

# Installation des dépendances Python
if [[ -f "requirements.txt" ]]; then
    echo "Installation des dépendances Python..."
    $PIP_CMD install -r requirements.txt
else
    echo "Fichier requirements.txt introuvable."
fi

echo "Installation terminée !"
