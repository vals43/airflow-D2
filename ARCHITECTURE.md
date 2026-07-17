# Architecture du pipeline Airflow-D2

## Stack technique

| Composant | Technologie | Justification |
|-----------|------------|---------------|
| **API** | OpenWeatherMap (Air Pollution API) | Gratuite, accessible sans authentification lourde, couvre les 5 villes cibles, fournit AQI + polluants (CO, NO₂, O₃, PM2.5, PM10) |
| **Orchestrateur** | Apache Airflow 2.10.2 | Standard industriel du data engineering ; DAGs Python versionnés, scheduling riche, UI intégrée, large communauté |
| **Stockage raw** | Système de fichiers (conteneur Docker) | Fichiers JSON bruts par appel API, jamais modifiés — garanti par le code (écriture en mode `w` unique, pas de réécriture) |
| **Stockage clean** | Fichier CSV unique (reconstruit à chaque run) | Format universel, lisible par tout outil, facile à valider et à consommer par IA1 |
| **Data Warehouse** | PostgreSQL 18 (Neon Serverless) | Base relationnelle cloud gratuite (500 Mo), persistante en dehors du conteneur, survit aux crashs et redéploiements |
| **Déploiement** | Hugging Face Spaces (Docker) | Plateforme gratuite avec builder CI, idéale pour héberger une application Airflow accessible 24/7 |
| **CI/CD** | GitHub Actions | Automatisation du push vers HF Spaces à chaque commit sur `main` |

## Diagramme de flux

```
┌─────────────────────────────────────────────────────────────────────┐
│                      OpenWeatherMap API                              │
│  /data/2.5/air_pollution?lat={lat}&lon={lon}&appid={key}            │
└────────┬────────────┬────────────┬────────────┬────────────────────┘
         │            │            │            │
         ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  Airflow 2.10.2 (Hugging Face Spaces)               │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                  DAG : aggregate_meteo                        │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │   │
│  │  │ Extract  │  │ Extract  │  │ Extract  │  │   Extract    │ │   │
│  │  │ Antanan. │  │  London  │  │ New York │  │ Paris / Tokyo│ │   │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬───────┘ │   │
│  │       └──────┬──────┘     ┌───────┘                │         │   │
│  │              ▼            ▼                        ▼         │   │
│  │  ┌────────────────────────────────────────────────────────┐  │   │
│  │  │              Aggregate (classement + save)             │  │   │
│  │  └─────────────────────┬──────────────────────────────────┘  │   │
│  └────────────────────────┼─────────────────────────────────────┘   │
└───────────────────────────┼─────────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
┌──────────────────┐ ┌──────────┐ ┌──────────────┐
│  raw/ (JSON)      │ │ clean/   │ │  Data        │
│  fichiers bruts   │ │ CSV      │ │  Warehouse   │
│  par ville/appel  │ │ unique   │ │  PostgreSQL  │
│  (dans conteneur) │ │          │ │  (Neon)      │
└──────────────────┘ └──────────┘ └──────────────┘
```

## Organisation du stockage

### raw/ — Zone brute (intouchable)
```
/opt/airflow/raw/
├── Antananarivo/
│   ├── 2026-07-01_00:00:00.json
│   ├── 2026-07-01_01:00:00.json
│   └── ...
├── London/
├── New_York/
├── Paris/
└── Tokyo/
```

- **Un fichier par ville et par appel API**, jamais modifié après écriture
- Format : JSON brut retourné par l'API
- Nom : `YYYY-MM-DD_HH:MM:SS.json`

### clean/ — Zone nettoyée (reconstruite à chaque run)
```
/opt/airflow/data/meteo_villes.csv
```

- Un seul fichier CSV, toutes villes réunies
- Une ligne par (ville × heure)
- Tri chronologique, sans doublons
- Reconstruit intégralement depuis raw/ à chaque exécution
- Colonnes : ville, latitude, longitude, horodatage, AQI, CO, NO₂, O₃, PM2.5, PM10, température, humidité, vent

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
│ no₂                  │  │                │
│ o₃                   │  │                │
│ pm2_5                │  │                │
│ pm10                 │  │                │
│ temperature_c        │  │                │
│ humidite_pct         │  │                │
│ vent_m_s             │  │                │
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

## Variables d'environnement

| Variable | Rôle | Source |
|----------|------|--------|
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Connexion PostgreSQL (Neon) | Secret HF Spaces |
| `OPENWEATHER_API_KEY` | Clé OpenWeatherMap | Variable Airflow (UI) |

## Périodicité

- **Scheduling** : quotidien (`@daily`) à 00:00 UTC
- **Backfill** : script dédié pour rattraper les 12 derniers mois
- **Données** : chaque run produit un snapshot des 5 villes à H + quelques minutes
