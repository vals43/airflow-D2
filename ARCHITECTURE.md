# Architecture du pipeline Airflow-D2

Pipeline de collecte horaire de la qualité de l'air (AQI) pour 5 villes, livrant un data warehouse dimensionnel (schéma en étoile).

## Stack technique

| Composant | Technologie | Justification |
|-----------|------------|---------------|
| **API** | OpenWeatherMap (Air Pollution API) | Gratuite, couvre les 5 villes cibles, fournit AQI + polluants (CO, NO, NO₂, O₃, SO₂, PM2.5, PM10, NH₃) |
| **Orchestrateur** | Apache Airflow 2.10.2 | Standard industriel du data engineering ; DAGs Python versionnés, scheduling horaire, UI intégrée, large communauté |
| **Stockage raw** | Système de fichiers (conteneur Docker) | Fichiers JSON bruts par ville et par appel, jamais modifiés — garanti par le code (écriture en mode `w` unique, pas de réécriture) |
| **Stockage clean** | Fichier CSV unique (reconstruit à chaque run) | Format universel, lisible par tout outil, facile à valider et à consommer par IA1 |
| **Data Warehouse** | PostgreSQL 18 (Neon Serverless) | Base relationnelle cloud gratuite, persistante en dehors du conteneur, survit aux crashs et redéploiements |
| **Déploiement** | Hugging Face Spaces (Docker) | Plateforme gratuite avec builder CI, idéale pour héberger une application Airflow accessible 24/24 |
| **CI/CD** | GitHub Actions | Automatisation du push vers HF Spaces à chaque commit sur `main` |

## Diagramme de flux

```
┌─────────────────────────────────────────────────────────────────────┐
│                      OpenWeatherMap API                              │
│  /data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}            │
│  /data/2.5/air_pollution/history?lat={lat}&lon={lon}                │
└────────┬────────────┬────────────┬────────────┬────────────────────┘
         │            │            │            │
         ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Airflow 2.10.2 (Hugging Face Spaces)               │
│                                                                      │
│  DAG aqi_pipeline (@hourly)                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐         │
│  │ Extract  │  │ Extract  │  │ Extract  │  │ Extract      │         │
│  │ Antanan. │  │  London  │  │ New York │  │ Paris / Tokyo│         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘         │
│       └──────┬──────┘     ┌───────┘                │                 │
│              ▼            ▼                        ▼                 │
│  ┌────────────────────────────────────────────────────────┐          │
│  │           reconstruire_clean (scripts/build_clean.py)   │          │
│  └──────────────────────────┬─────────────────────────────┘          │
│                             ▼                                        │
│  ┌────────────────────────────────────────────────────────┐          │
│  │            charger_warehouse (load_warehouse.py)        │          │
│  └────────────────────────────────────────────────────────┘          │
│                                                                      │
│  DAG backfill_aqi (manuel) : 12 mois d'historique, mois par mois     │
└───────────────┬──────────────────────────────────────────────────────┘
                │
  ┌─────────────┼─────────────┐
  ▼             ▼             ▼
┌──────────────────┐ ┌──────────┐ ┌──────────────┐
│  data/raw/       │ │ clean/   │ │  Data        │
│  fichiers JSON   │ │ CSV      │ │  Warehouse   │
│  bruts           │ │ unique   │ │  PostgreSQL  │
│  (conteneur)     │ │          │ │  (Neon)      │
└──────────────────┘ └──────────┘ └──────────────┘
```

## Organisation du stockage

### raw/ — Zone brute (intouchable)

```
/opt/airflow/data/raw/
├── antananarivo_20260701T000000Z.json
├── london_20260701T000000Z.json
├── new_york_20260701T000000Z.json
├── paris_20260701T000000Z.json
└── tokyo_20260701T000000Z.json
```

- **Un fichier par ville et par appel API**, jamais modifié après écriture
- Format : JSON brut retourné par l'API
- Nom : `{ville}_{YYYYMMDDTHHMMSSZ}.json` (horodatage de l'appel)

### clean/ — Zone nettoyée (reconstruite à chaque run)

```
/opt/airflow/data/clean/qualite_air.csv
```

- Un seul fichier CSV, toutes villes réunies
- Une ligne par (ville × heure), sans doublons
- Reconstruit intégralement depuis `raw/` à chaque exécution
- Colonnes : `ville`, `pays`, `latitude`, `longitude`, `horodatage_utc`, `aqi`, `co_ug_m3`, `no_ug_m3`, `no2_ug_m3`, `o3_ug_m3`, `so2_ug_m3`, `pm2_5_ug_m3`, `pm10_ug_m3`, `nh3_ug_m3` (unités documentées dans le README)

### Data Warehouse (Neon PostgreSQL)

Schéma en étoile :

```
┌──────────────────────┐
│   fact_air_quality    │
│──────────────────────│
│ id_fait         PK   │
│ id_temps        FK   │──┐
│ id_ville        FK   │──┼────────────────┐
│ aqi                  │  │                │
│ co                   │  │                │
│ no                   │  │                │
│ no2                  │  │                │
│ o3                   │  │                │
│ so2                  │  │                │
│ pm2_5                │  │                │
│ pm10                 │  │                │
│ nh3                  │  │                │
│ UNIQUE(id_temps,     │  │                │
│        id_ville)     │  │                │
└──────────────────────┘  │                │
                           │                │
┌──────────────────────┐  │                │
│   dim_temps           │  │                │
│──────────────────────│  │                │
│ id_temps         PK  │◄─┘                │
│ date_entiere         │                   │
│ annee                │                   │
│ mois                 │                   │
│ jour                 │                   │
│ heure                │                   │
│ jour_semaine         │                   │
│ weekend              │                   │
└──────────────────────┘                   │
                                            │
┌──────────────────────┐                   │
│   dim_ville           │                   │
│──────────────────────│                   │
│ id_ville         PK  │◄───────────────────┘
│ nom                  │
│ pays                 │
│ latitude             │
│ longitude            │
└──────────────────────┘
```

- Les concentrations des polluants (`co`, `no`, `no2`, `o3`, `so2`, `pm2_5`, `pm10`, `nh3`) sont en µg/m³, l'AQI est un indice EPA sans unité.
- `UNIQUE(id_temps, id_ville)` garantit une seule ligne par ville et par heure (déduplication au chargement).

**Justification du schéma en étoile :**
- Plus simple qu'un flocon pour ce volume de données (5 villes)
- Requêtes analytiques directes sans jointures multiples
- Pas de hiérarchie naturelle dans les dimensions (une ville n'a pas de sous-niveaux)

## Déploiement

```
Développeur (commit)
  │
  ▼
GitHub (branche main)
  │
  ▼
GitHub Actions (force-push)
  │
  ▼
Hugging Face Spaces (build Docker)
  │
  ▼
Conteneur Airflow 2.10.2 (démarrage)
  │
  ├── airflow db upgrade (migration)
  ├── Création user admin (idempotent)
  └── Démarrage standalone (scheduler + webserver + triggerer)
```

## Variables d'environnement et Variables Airflow

| Variable | Rôle | Source |
|----------|------|--------|
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Connexion PostgreSQL (Neon) pour la base interne Airflow | Secret HF Spaces |
| `OPENWEATHER_API_KEY` | Clé OpenWeatherMap (Air Pollution API) | Variable Airflow (UI) |
| `WAREHOUSE_DSN` | DSN PostgreSQL (Neon) du data warehouse | Variable Airflow (UI) |

## Périodicité

- **Collecte** : horaire (`@hourly`) via le DAG `aqi_pipeline`
- **Backfill** : DAG `backfill_aqi` rejouable, 12 mois (juillet 2025 → aujourd'hui), découpé mois par mois
- **clean/** : reconstruit à chaque run du pipeline

## Limites connues

- `raw/` vit dans le conteneur éphémère : il est effacé à chaque redéploiement. Le DAG de backfill est rejouable pour reconstruire l'historique.
- Sur l'offre gratuite de Hugging Face Spaces, le conteneur peut hiberner après inactivité ; les heures manquantes ne sont pas rattrapées (`catchup=False`).
