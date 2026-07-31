# Rapport de projet — DONNEES2

Pipeline de collecte horaire de la qualité de l'air (AQI) pour 5 villes, déployé avec Apache Airflow, avec livraison d'un data warehouse dimensionnel.

## Composition du groupe

| Membre  | STD      |
|---------|----------|
| Teddy   | STD24041 |
| Manda   | STD24083 |
| Miahy   | STD24123 |
| Anah    | STD24207 |
| Amboara | STD24115 |

## Méthode de travail

- Travail collaboratif sur GitHub avec des branches `feat/*` et des pull requests revues avant fusion dans `main`.
- Intégration continue via GitHub Actions : chaque push sur `main` redéploie automatiquement le pipeline sur Hugging Face Spaces.
- Déploiement et validation dans l'UI Airflow (inspection des runs, des logs et de la table fact).
- Documentation maintenue au fil de l'eau dans `ARCHITECTURE.md` et `README.md`.

## Répartition des tâches

| Membre | Tâches |
|--------|--------|
| Teddy Andria | Mise en place du conteneur Airflow (Dockerfile, entrypoint), déploiement HF Spaces, migration de la base Airflow de SQLite vers PostgreSQL (Neon), `ARCHITECTURE.md` et `README.md` initiaux |
| Manda Tiavina | DAG de backfill sur 12 mois, `load_warehouse.py` (chargement du schéma en étoile), passage en insertion par lots (`execute_values`) et cache des dimensions pour réduire les appels base |
| Miahy | `aqi_utils.py` (appels API courant + historique), DAG `aqi_pipeline` horaire, pipeline de bout en bout (extract → raw → clean → warehouse) |
| Anah Antonerrie | `build_clean.py` : reconstruction complète de `clean/qualite_air.csv` depuis `raw/`, déduplication ville + heure, tri |
| _À compléter_ | _À compléter_ |

## Architecture retenue

```
OpenWeatherMap Air Pollution API
        │  collecte horaire + backfill 12 mois
        ▼
Apache Airflow 2.10.2 (Hugging Face Spaces)
        ▼
raw/    fichiers JSON bruts par ville et par appel (jamais modifiés)
clean/  un CSV unique reconstruit à chaque run
        ▼
Data Warehouse PostgreSQL (Neon) — schéma en étoile
```

Justifications des choix techniques dans `ARCHITECTURE.md`.

## Difficultés rencontrées et solutions

1. **Persistance de la base Airflow** : SQLite vivait dans le conteneur éphémère, les utilisateurs et l'historique disparaissaient à chaque redéploiement. Solution : migration vers PostgreSQL (Neon) via `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, migration de `airflow db init` vers `airflow db upgrade` et création idempotente de l'admin.
2. **Écrasement de `raw/` à chaque redéploiement** : la zone brute vit dans le conteneur. Solution : réexécution du backfill (DAG `backfill_aqi` rejouable) après chaque déploiement ; la reconstruction de `clean/` reste possible tant que `raw/` est présent.
3. **Lenteur du chargement du warehouse** : insertions ligne par ligne avec un appel base par ligne. Solution : insertion par lots (`execute_values`), cache des clés `dim_ville` et `dim_temps`, `connect_timeout` pour éviter les blocages.
4. **Doublons ville + heure** : plusieurs appels (horaire + backfill) pouvaient produire la même mesure. Solution : déduplication dans `build_clean.py` (une ligne par ville + heure, la dernière lecture écrase) et `ON CONFLICT (id_temps, id_ville) DO UPDATE` dans la table de faits.
5. **Cohérence de l'AQI** : l'AQI retourné par l'API n'était pas fiable selon la période. Solution : recalcul de l'AQI selon les breakpoints US EPA à partir des concentrations des polluants (`calculer_aqi_epa`), l'AQI final étant le maximum des sous-indices.
6. **Backfill partiel** : l'API historique est découpée en tranches. Solution : découpage mois par mois sur 12 mois, arrêt à l'heure courante, et script rejouable en cas de trou.

## Choix techniques justifiés

| Choix | Justification |
|-------|---------------|
| Apache Airflow | Standard industriel de l'orchestration, DAGs versionnés en Python, scheduling riche, UI de suivi des exécutions |
| OpenWeatherMap Air Pollution API | Gratuite, couvre les 5 villes cibles, fournit AQI + polluants (CO, NO, NO₂, O₃, SO₂, PM2.5, PM10, NH₃) |
| Hugging Face Spaces (Docker) | Hébergement gratuit accessible 24/24, redéploiement automatique via GitHub Actions |
| Stockage raw/ en fichiers JSON | Format brut issu de l'API, jamais modifié, un fichier par ville et par appel = sauvegarde rejouable |
| Stockage clean/ en CSV unique | Format universel et lisible par IA1, reconstruit intégralement depuis raw/ à chaque exécution |
| PostgreSQL (Neon) | Base cloud gratuite et persistante en dehors du conteneur, survive aux redéploiements |
| Schéma en étoile | Simple pour ce volume (5 villes), requêtes analytiques directes sans jointures multiples |

## Période couverte et trous connus

- **Backfill** : de juillet 2025 à la date d'exécution du DAG `backfill_aqi` (12 mois).
- **Collecte horaire** : depuis le déploiement du DAG `aqi_pipeline` (horaire, `start_date` le 1er juillet 2026).
- **Trous connus** : sur l'offre gratuite de Hugging Face Spaces, le conteneur peut hiberner après inactivité ; les heures manquantes pendant l'hibernation ne sont pas rattrapées (`catchup=False`). Les trous éventuels sont visibles dans `clean/` et récupérables en relançant `backfill_aqi`.

## Consommation par IA1

- `data/clean/qualite_air.csv` : fichier unique, une ligne par ville et par heure, trié, sans doublon. Colonnes et unités documentées dans `README.md`.
- Data warehouse Neon : tables `dim_ville`, `dim_temps`, `fact_air_quality`. Infos de connexion via la Variable Airflow `WAREHOUSE_DSN` (secrète, non versionnée).

## Livrables

- Code source complet : `dags/`, `scripts/`, `Dockerfile`, `entrypoint.sh`
- `ARCHITECTURE.md` : stack et justifications
- `README.md` : documentation du stockage (villes, colonnes, unités, schéma du warehouse, période, trous, connexion)
- `RAPPORT.md` : présent document
- `data/raw/` et `data/clean/` : zones de stockage (exportées séparément)
- Vidéo de démonstration et captures de l'historique des exécutions : transmises par email
