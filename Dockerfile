FROM apache/airflow:2.10.2-python3.11

# --- CONFIGURATION DU PORT HUGGING FACE ---
# On force Airflow (et le serveur Flask/Gunicorn sous-jacent) à écouter sur 7860
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:7860

# --- CONFIGURATION DE LA BASE DE DONNÉES (NEON) ---
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://neondb_owner:npg_S3nxyH9aOPke@ep-hidden-fog-at6pfq19-pooler.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require

# --- CONFIGURATION CORE OPTIMISÉE ---
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# --- CORRECTIFS POUR LE PROXY DE HUGGING FACE ---
ENV AIRFLOW__WEBSERVER__COOKIE_SECURE=True
ENV AIRFLOW__WEBSERVER__SESSION_COOKIE_SAMESITE=None
ENV AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True

# Copie du script d'entrée
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]