#!/usr/bin/env bash

# Attendre que les configurations système soient prêtes si nécessaire
clignote_drapeau=0

echo "=== DÉMARRAGE D'AIRFLOW EN MODE STANDALONE ==="

# exec permet à Airflow de récupérer le PID 1 et de gérer proprement le cycle de vie du conteneur
exec airflow standalone