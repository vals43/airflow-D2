#!/bin/bash

echo "=== MIGRATION DE LA BASE DE DONNÉES ==="
airflow db upgrade

echo "=== CRÉATION DE L'UTILISATEUR ADMIN ==="
if ! airflow users list | grep -q "admin"; then
    airflow users create \
        --username admin \
        --firstname Teddy \
        --lastname Andri \
        --role Admin \
        --email admin@example.com \
        --password adminpassword
fi

echo "=== DÉMARRAGE D'AIRFLOW EN MODE STANDALONE ==="
exec airflow standalone