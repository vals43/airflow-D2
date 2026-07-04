#!/bin/bash
set -e

echo "=== DÉMARRAGE DU SCRIPT D'ENTRÉE ==="

# Migration de la base de données (équivalent moderne de db init)
echo "Initialisation et migration de la base Neon..."
airflow db migrate

# Création de l'utilisateur Admin
echo "Création de l'utilisateur admin..."
airflow users create \
    --username admin \
    --firstname Teddy \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password adminpassword

# Lancement du scheduler en arrière-plan
echo "Démarrage du Scheduler..."
airflow scheduler &

# Lancement du webserver au premier plan
echo "Démarrage du Webserver sur le port 7860..."
exec airflow webserver