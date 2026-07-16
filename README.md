---
title: Airflow D2
emoji: 🐠
colorFrom: purple
colorTo: purple
sdk: docker
pinned: false
---

# Airflow-D2

Pipeline ETL de données météo orchestré avec Apache Airflow, déployé sur Hugging Face Spaces.

## Stack

| Technologie | Version |
|-------------|---------|
| Apache Airflow | 2.10.2 |
| Python | 3.11 |
| PostgreSQL (Neon) | 18 |
| Docker | HF Spaces |
| API | OpenWeatherMap |

## Architecture

Voir [ARCHITECTURE.md](./ARCHITECTURE.md)

## DAGs

| DAG | Description |
|-----|-------------|
| `aggregate_meteo` | Extrait 5 villes (Antananarivo, Londres, New York, Paris, Tokyo), agrège et classe les données |
| `hello_etl` | DAG de test ETL |
| `test_github` | DAG de validation CI/CD |

## Déploiement

Automatique via GitHub Actions → push sur Hugging Face Spaces à chaque commit sur `main`.

## Accès

- **URL** : [https://vals43-airflow-d2.hf.space](https://vals43-airflow-d2.hf.space)
- **Admin** : créé automatiquement au démarrage

## Prérequis

Avant de lancer les DAGs, créer la Variable Airflow suivante dans l'UI (`Admin → Variables`) :

| Clé | Valeur |
|-----|--------|
| `OPENWEATHER_API_KEY` | Ta clé API OpenWeatherMap |

## Licence

Projet étudiant — ISPM
