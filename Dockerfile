FROM apache/airflow:2.10.2-python3.11

# Force le serveur FAB/Gunicorn à écouter sur le port Hugging Face
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:7860

# Désactive les exemples pour économiser la RAM de Neon et HF
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Ta chaîne de connexion Neon
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://neondb_owner:npg_S3nxyH9aOPke@ep-hidden-fog-at6pfq19-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require