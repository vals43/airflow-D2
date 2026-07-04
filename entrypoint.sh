#!/bin/bash
set -e

echo "=== DÉMARRAGE DU SCRIPT D'ENTRÉE ==="

# Initialisation de la base Airflow
airflow db init

# Création de l'admin
airflow users create \
    --username admin \
    --firstname Teddy \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# Lancement du scheduler en tâche de fond
airflow scheduler &

# Lancement du webserver (garde le conteneur actif)
exec airflow webserver