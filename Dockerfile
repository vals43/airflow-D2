FROM apache/airflow:2.10.2-python3.11

# --- DRIVER POSTGRESQL (NÉCESSAIRE POUR NEON) ---
RUN pip install --no-cache-dir psycopg2-binary

# --- CONFIGURATION DU PORT HUGGING FACE ---
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=7860
ENV AIRFLOW__WEBSERVER__BASE_URL=http://localhost:7860

# --- CONFIGURATION CORE COMPATIBLE SQLITE/NEON ---
# SequentialExecutor accepte SQLite ET Postgres sans broncher
ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# --- CORRECTIFS POUR LE PROXY DE HUGGING FACE ---
ENV AIRFLOW__WEBSERVER__COOKIE_SECURE=True
ENV AIRFLOW__WEBSERVER__SESSION_COOKIE_SAMESITE=None
ENV AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX=True

# --- SYNCHRONISATION DES DOSSIERS DAGS ET SCRIPTS ---
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

# Copie du script d'entrée
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]