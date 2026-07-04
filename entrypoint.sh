#!/bin/bash

echo "=== INITIALISATION DE LA BASE DE DONNÉES ==="
airflow db init

echo "=== CRÉATION DE L'UTILISATEUR ADMIN ==="
# Cette commande crée ton compte admin s'il n'existe pas déjà
airflow users create \
    --username admin \
    --firstname Teddy \
    --lastname Andri \
    --role Admin \
    --email admin@example.com \
    --password adminpassword

echo "=== DÉMARRAGE D'AIRFLOW EN MODE STANDALONE ==="
exec airflow standalone