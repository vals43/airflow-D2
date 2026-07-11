# Plan : Migration de SQLite vers Neon (PostgreSQL)

## Problème
Les users créés via l'UI Airflow disparaissent à chaque crash/redéploiement car SQLite est stocké dans le conteneur éphémère.

## Solution
PostgreSQL externe via Neon + modifications du code pour utiliser une vraie base persistante.

---

## Fichiers à modifier

### 1. `Dockerfile`
**Ajouter** après `FROM ...` :
```dockerfile
RUN pip install --no-cache-dir psycopg2-binary
```

### 2. `entrypoint.sh`
**Remplacer** :
```bash
airflow db init
```
par :
```bash
airflow db upgrade
```

**Remplacer** la création admin par une version idempotente :
```bash
if ! airflow users list | grep -q "admin"; then
    airflow users create \
        --username admin \
        --firstname Teddy \
        --lastname Andri \
        --role Admin \
        --email admin@example.com \
        --password adminpassword
fi
```

---

## Configuration manuelle (par toi)

### 3. Créer un compte Neon
- Va sur https://neon.tech
- Sign up avec GitHub
- Crée un projet → copie la connection string :
  ```
  postgresql://user:password@ep-xxx.us-east-2.aws.neon.tech/neondb?sslmode=require
  ```

### 4. Ajouter le secret dans Hugging Face Spaces
- Va dans Settings de ton Space → Repository Secrets
- Ajoute :
  - **Nom** : `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`
  - **Valeur** : ta connection string Neon

### 5. Déployer
- Commit + push → GitHub Actions déploie automatiquement sur HF Spaces
- La variable d'environnement sera automatiquement utilisée par Airflow

---

## Vérification
- Après déploiement, créer un user via l'UI
- Redémarrer le Space (ou attendre un crash)
- Vérifier que le user est toujours là
