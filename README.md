---
title: Airflow D2
emoji: 🐠
colorFrom: purple
colorTo: purple
sdk: docker
pinned: false
---

# Airflow-D2

Pipeline ETL de collecte horaire de la qualité de l'air (AQI) pour 5 villes, orchestré avec Apache Airflow et déployé sur Hugging Face Spaces. Le projet livre un data warehouse dimensionnel (schéma en étoile) dans PostgreSQL (Neon).

## Stack

| Technologie | Version |
|-------------|---------|
| Apache Airflow | 2.10.2 |
| Python | 3.11 |
| PostgreSQL (Neon) | 18 |
| Docker | HF Spaces |
| API | OpenWeatherMap (Air Pollution API) |

## Villes couvertes

| Ville | Pays | Latitude | Longitude |
|-------|------|----------|-----------|
| Antananarivo | MG | -18.8792 | 47.5079 |
| London | GB | 51.5074 | -0.1278 |
| New York | US | 40.7128 | -74.0060 |
| Paris | FR | 48.8566 | 2.3522 |
| Tokyo | JP | 35.6895 | 139.6917 |

## DAGs

| DAG | Description |
|-----|-------------|
| `aqi_pipeline` | Collecte horaire de l'AQI des 5 villes, reconstruction de `clean/` et chargement du warehouse |
| `backfill_aqi` | Backfill rejouable : 12 mois d'historique (juillet 2025 → aujourd'hui), mois par mois |
| `hello_etl` | DAG de test ETL (legacy) |
| `dag_deploye_via_github` | DAG de validation du déploiement CI/CD (legacy) |

## Stockage

### raw/ — Zone brute (intouchable)

`data/raw/` — un fichier JSON brut par ville et par appel API, jamais modifié après écriture.

```
data/raw/
├── antananarivo_20260701T000000Z.json
├── london_20260701T000000Z.json
├── new_york_20260701T000000Z.json
├── paris_20260701T000000Z.json
└── tokyo_20260701T000000Z.json
```

Nom de fichier : `{ville}_{YYYYMMDDTHHMMSSZ}.json` (horodatage de l'appel).

### clean/ — Contrat de données

`data/clean/qualite_air.csv` — un fichier unique, toutes villes réunies, une ligne par ville et par heure, sans doublon. Reconstruit intégralement depuis `raw/` à chaque exécution (`scripts/build_clean.py`).

| Colonne | Type | Unité | Description |
|---------|------|-------|-------------|
| `ville` | texte | — | Nom de la ville |
| `pays` | texte | — | Code pays ISO 3166-1 alpha-2 |
| `latitude` | nombre | degrés | Latitude (WGS84) |
| `longitude` | nombre | degrés | Longitude (WGS84) |
| `horodatage_utc` | datetime | UTC | `YYYY-MM-DD HH:MM:SS`, heure de la mesure |
| `aqi` | entier | indice | AQI calculé selon les breakpoints US EPA (0-500) |
| `co_ug_m3` | nombre | µg/m³ | Monoxyde de carbone |
| `no_ug_m3` | nombre | µg/m³ | Monoxyde d'azote |
| `no2_ug_m3` | nombre | µg/m³ | Dioxyde d'azote |
| `o3_ug_m3` | nombre | µg/m³ | Ozone |
| `so2_ug_m3` | nombre | µg/m³ | Dioxyde de soufre |
| `pm2_5_ug_m3` | nombre | µg/m³ | Particules fines PM2.5 |
| `pm10_ug_m3` | nombre | µg/m³ | Particules PM10 |
| `nh3_ug_m3` | nombre | µg/m³ | Ammoniac |

L'AQI est le maximum des sous-indices EPA calculés à partir des concentrations (facteurs de conversion µg/m³ → ppm/ppb documentés dans `dags/aqi_utils.py`).

Le fichier est validé par `scripts/validate_clean.py` (colonnes, ≥ 5 villes, pas de doublon, tri chronologique, formats et unités) :

```bash
python scripts/validate_clean.py
```

## Data Warehouse (Neon PostgreSQL)

Schéma en étoile :

```
dim_temps 1 ──── * fact_air_quality * ──── 1 dim_ville
```

- **`dim_ville`** : `id_ville` (PK), `nom`, `pays`, `latitude`, `longitude`
- **`dim_temps`** : `id_temps` (PK), `date_entiere`, `annee`, `mois`, `jour`, `heure`, `jour_semaine`, `weekend`
- **`fact_air_quality`** : `id_fait` (PK), `id_temps` (FK), `id_ville` (FK), `aqi`, `co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3` (concentrations en µg/m³), contrainte d'unicité `(id_temps, id_ville)`

Cohérence attendue : nombre de lignes de `fact_air_quality` ≈ nombre de villes (5) × nombre d'heures couvertes. Les écarts viennent des heures manquantes (hibernation du conteneur HF Spaces, erreurs API) et des créneaux hors backfill ; ils sont visibles dans `clean/` et récupérables en relançant `backfill_aqi`.

### Connexion à la base

Le DSN PostgreSQL est stocké dans la Variable Airflow `WAREHOUSE_DSN` (secrète, jamais versionnée). Les tables sont créées automatiquement par `dags/load_warehouse.py` à chaque chargement.

## Variables Airflow requises

Créer ces Variables dans l'UI (`Admin → Variables`) avant de lancer les DAGs :

| Clé | Valeur |
|-----|--------|
| `OPENWEATHER_API_KEY` | Clé API OpenWeatherMap |
| `WAREHOUSE_DSN` | DSN PostgreSQL Neon, ex. `postgresql://user:password@ep-xxx.neon.tech/neondb?sslmode=require` |

## Déploiement

Automatique via GitHub Actions (`Deploy to Hugging Face Spaces`) → push sur Hugging Face Spaces à chaque commit sur `main`.

## Accès

- **URL** : [https://vals43-airflow-d2.hf.space](https://vals43-airflow-d2.hf.space)
- **Admin** : créé automatiquement au démarrage (`admin` / `adminpassword`)

## Période couverte et trous connus

- **Backfill** : juillet 2025 → date d'exécution du DAG `backfill_aqi` (12 mois).
- **Collecte horaire** : depuis le déploiement du DAG `aqi_pipeline`.
- **Trous possibles** : sur l'offre gratuite HF Spaces, le conteneur peut hiberner après inactivité ; les heures manquantes ne sont pas rattrapées automatiquement (`catchup=False`). Réexécuter `backfill_aqi` pour combler les trous.

## Documentation complémentaire

- `ARCHITECTURE.md` : architecture, choix techniques et schéma du warehouse
- `RAPPORT.md` : rapport de projet (méthode, répartition, difficultés)
