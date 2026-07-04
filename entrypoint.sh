#!/bin/bash
set -e

echo "=== DÉMARRAGE DU SCRIPT D'ENTRÉE ==="

# Migration de la base Neon
echo "Initialisation et migration de la base Neon..."
airflow db migrate

# Création de l'utilisateur Admin (Version Moderne d'Airflow)
echo "Création de l'utilisateur admin..."
airflow users create-admin \
    --username admin \
    --password adminpassword \
    --email admin@example.com || echo "L'utilisateur admin existe déjà ou la commande a été ignorée."

# Lancement du scheduler en arrière-plan
echo "Démarrage du Scheduler..."
airflow scheduler &

# Lancement du webserver au premier plan
echo "Démarrage du Webserver sur le port 7860..."
exec airflow webserver