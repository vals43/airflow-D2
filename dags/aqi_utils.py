from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# --- Les 5 villes du groupe (mêmes villes que aggregate_meteo.py) ---
VILLES = [
    {"nom": "Antananarivo", "pays": "MG", "latitude": -18.8792, "longitude": 47.5079},
    {"nom": "London",       "pays": "GB", "latitude": 51.5074,  "longitude": -0.1278},
    {"nom": "New York",     "pays": "US", "latitude": 40.7128,  "longitude": -74.0060},
    {"nom": "Paris",        "pays": "FR", "latitude": 48.8566,  "longitude": 2.3522},
    {"nom": "Tokyo",        "pays": "JP", "latitude": 35.6895,  "longitude": 139.6917},
]

BASE_URL = "https://api.openweathermap.org/data/2.5/air_pollution"
BASE_URL_HISTORY = "https://api.openweathermap.org/data/2.5/air_pollution/history"

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
CLEAN_DIR = Path(__file__).resolve().parent.parent / "data" / "clean"
CLEAN_CSV = CLEAN_DIR / "qualite_air.csv"


def extraire_aqi_courant(ville: dict, cle_api: str) -> dict:
    """Appelle l'API pour l'AQI courant d'une ville. Retourne le JSON brut."""
    params = {"lat": ville["latitude"], "lon": ville["longitude"], "appid": cle_api}
    reponse = requests.get(BASE_URL, params=params, timeout=15)
    reponse.raise_for_status()
    return reponse.json()


def extraire_aqi_historique(ville: dict, cle_api: str, start_ts: int, end_ts: int) -> dict:
    """Appelle l'API historique (backfill) entre deux timestamps unix (UTC)."""
    params = {
        "lat": ville["latitude"],
        "lon": ville["longitude"],
        "start": start_ts,
        "end": end_ts,
        "appid": cle_api,
    }
    reponse = requests.get(BASE_URL_HISTORY, params=params, timeout=30)
    reponse.raise_for_status()
    return reponse.json()


def sauvegarder_raw(ville_nom: str, payload: dict) -> Path:
    """
    Sauvegarde le JSON brut, un fichier par ville et par appel.
    Zone raw/ : jamais modifiee ensuite.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    horodatage = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    nom_fichier = f"{ville_nom.replace(' ', '_').lower()}_{horodatage}.json"
    chemin = RAW_DIR / nom_fichier
    chemin.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return chemin


def transformer_mesure(ville: dict, mesure_brute: dict) -> dict:
    """Transforme un element 'list' de la reponse OWM en ligne plate pour clean/."""
    composants = mesure_brute["components"]
    dt_utc = datetime.fromtimestamp(mesure_brute["dt"], tz=timezone.utc)
    return {
        "ville": ville["nom"],
        "pays": ville["pays"],
        "latitude": ville["latitude"],
        "longitude": ville["longitude"],
        "horodatage_utc": dt_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "aqi": mesure_brute["main"]["aqi"],  # 1 a 5
        "co_ug_m3": composants.get("co"),
        "no_ug_m3": composants.get("no"),
        "no2_ug_m3": composants.get("no2"),
        "o3_ug_m3": composants.get("o3"),
        "so2_ug_m3": composants.get("so2"),
        "pm2_5_ug_m3": composants.get("pm2_5"),
        "pm10_ug_m3": composants.get("pm10"),
        "nh3_ug_m3": composants.get("nh3"),
    }