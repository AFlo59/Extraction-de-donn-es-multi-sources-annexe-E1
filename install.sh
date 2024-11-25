#!/bin/bash

# Mise à jour des paquets
echo "Mise à jour des paquets..."
sudo apt-get update -y

# Installation des dépendances système
echo "Installation des dépendances système..."
sudo apt-get install -y python3-pip python3-venv unixodbc-dev libpq-dev build-essential

# Création de l'environnement virtuel
echo "Création de l'environnement virtuel..."
python3 -m venv venv

# Activation de l'environnement virtuel
echo "Activation de l'environnement virtuel..."
source venv/bin/activate

# Installation des dépendances Python
echo "Installation des dépendances Python..."
pip install -r requirements.txt

echo "Installation terminée !"

