FROM apache/airflow:2.8.1-python3.11

USER root

# Installer Git au niveau système (utile pour cloner des DAGs plus tard)
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

# Variables d'environnements obligatoires pour forcer le mode mono-conteneur et le port HF
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:7860

# Base de données SQLite locale pour valider que l'installation fonctionne
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db

# Copier et rendre exécutable le script d'entrée
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Exposer le port requis par l'infrastructure Hugging Face
EXPOSE 7860

ENTRYPOINT ["/entrypoint.sh"]