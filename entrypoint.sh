#!/bash/bin
# (Le shell par défaut dans l'image Airflow est souvent bash, mais on sécurise)
#!/bin/bash

# Initialisation de la base de données Airflow locale (SQLite par défaut pour l'instant)
echo "Initialisation de la base de données Airflow..."
airflow db init

# Création de l'utilisateur Admin pour te connecter à l'interface
echo "Création de l'utilisateur admin..."
airflow users create \
    --username admin \
    --firstname Teddy \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password admin

# Lancer le Scheduler en arrière-plan (&) pour qu'il gère l'exécution des DAGs
echo "Démarrage du Scheduler..."
airflow scheduler &

# Lancer le Webserver au premier plan (via exec) sur le port obligatoire 7860
# C'est ce processus qui va maintenir le conteneur Hugging Face en vie
echo "Démarrage du Webserver sur le port 7860..."
exec airflow webserver