FROM apache/airflow:2.8.1-python3.11

USER root

# Installer Git et dos2unix pour nettoyer les scripts
RUN apt-get update && apt-get install -y git dos2unix && apt-get clean

# Définir le répertoire de travail
WORKDIR /opt/airflow

# Copier le script d'entrée
COPY entrypoint.sh ./entrypoint.sh

# Forcer les droits d'exécution et le format Linux (LF)
RUN chmod +x ./entrypoint.sh && dos2unix ./entrypoint.sh

USER airflow

# Configurations Airflow obligatoires pour Hugging Face
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:7860

# Injection de ta base de données Neon avec le driver psycopg2
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://neondb_owner:npg_S3nxyH9aOPke@ep-hidden-fog-at6pfq19-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require

EXPOSE 7860

ENTRYPOINT ["./entrypoint.sh"]