#!/bin/bash

# Mise à jour des paquets
echo "Mise à jour des paquets..."
sudo apt-get update -y

# Installation des dépendances système
echo "Installation des dépendances système..."
sudo apt-get install -y python3-pip python3-venv unixodbc-dev libpq-dev build-essential curl

# Installation du pilote ODBC pour SQL Server
echo "Installation du pilote ODBC pour SQL Server..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo curl -o /etc/apt/sources.list.d/mssql-release.list https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list
sudo apt-get update -y
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18

# Ajouter les outils SQL Server au PATH
echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc
source ~/.bashrc

# Création de l'environnement virtuel
echo "Création de l'environnement virtuel..."
python3 -m venv venv

# Activation de l'environnement virtuel
echo "Activation de l'environnement virtuel..."
source venv/bin/activate

# Mise à jour de pip
pip install --upgrade pip

# Installation des dépendances Python
echo "Installation des dépendances Python..."
pip install -r requirements.txt

echo "Installation terminée !"
