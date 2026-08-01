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
    Structure : raw/{Ville}/{YYYY-MM-DD_HH:MM:SS}.json
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    dossier_ville = RAW_DIR / ville_nom.replace(" ", "_")
    dossier_ville.mkdir(parents=True, exist_ok=True)
    horodatage = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H:%M:%S")
    nom_fichier = f"{horodatage}.json"
    chemin = dossier_ville / nom_fichier
    chemin.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return chemin


def calculer_aqi_epa(composants: dict) -> int:
    """
    Calcule l'AQI US EPA (0-500) a partir des concentrations des polluants.
    L'AQI final est le MAX des sous-indices (norme EPA).

    L'API OWM renvoie les valeurs en ug/m3. On convertit en ppm/ppb
    pour les gaz (CO, NO2, O3, SO2) selon les breakpoints EPA.
    """
    # Facteurs de conversion ug/m3 -> ppm (CO) ou ppb (NO2, O3, SO2) a 25°C, 1 atm
    # 1 ppm CO  = 1145 ug/m3
    # 1 ppb NO2 = 1.88 ug/m3
    # 1 ppb O3  = 1.96 ug/m3
    # 1 ppb SO2 = 2.62 ug/m3
    # PM2.5 et PM10 sont deja en ug/m3

    def _sous_indice(conc: float | None, breakpoints: list[tuple]) -> int:
        if conc is None or conc < 0:
            return 0
        for c_low, c_high, i_low, i_high in breakpoints:
            if c_low <= conc <= c_high:
                return round((i_high - i_low) / (c_high - c_low) * (conc - c_low) + i_low)
        dernier = breakpoints[-1]
        if conc > dernier[1]:
            return dernier[3]
        return 0

    # Breakpoints EPA : (C_low, C_high, I_low, I_high)
    # PM2.5 (ug/m3, 24h)
    bp_pm25 = [
        (0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 500.4, 301, 500),
    ]
    # PM10 (ug/m3, 24h)
    bp_pm10 = [
        (0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
        (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500),
    ]
    # CO (ppm, 8h)
    bp_co = [
        (0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
        (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 50.4, 301, 500),
    ]
    # NO2 (ppb, 1h)
    bp_no2 = [
        (0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
        (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 2049, 301, 500),
    ]
    # O3 (ppb, 8h)
    bp_o3 = [
        (0, 54, 0, 50), (55, 70, 51, 100), (71, 85, 101, 150),
        (86, 105, 151, 200), (106, 200, 201, 300),
    ]
    # SO2 (ppb, 1h)
    bp_so2 = [
        (0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150),
        (186, 304, 151, 200), (305, 604, 201, 300), (605, 1004, 301, 500),
    ]

    pm25 = composants.get("pm2_5")
    pm10 = composants.get("pm10")
    co_ppm = composants.get("co") / 1145 if composants.get("co") else None
    no2_ppb = composants.get("no2") / 1.88 if composants.get("no2") else None
    o3_ppb = composants.get("o3") / 1.96 if composants.get("o3") else None
    so2_ppb = composants.get("so2") / 2.62 if composants.get("so2") else None

    sous_indices = [
        _sous_indice(pm25, bp_pm25),
        _sous_indice(pm10, bp_pm10),
        _sous_indice(co_ppm, bp_co),
        _sous_indice(no2_ppb, bp_no2),
        _sous_indice(o3_ppb, bp_o3),
        _sous_indice(so2_ppb, bp_so2),
    ]

    return max(sous_indices)


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
        "aqi": calculer_aqi_epa(mesure_brute.get("components", {})),
        "co_ug_m3": composants.get("co"),
        "no_ug_m3": composants.get("no"),
        "no2_ug_m3": composants.get("no2"),
        "o3_ug_m3": composants.get("o3"),
        "so2_ug_m3": composants.get("so2"),
        "pm2_5_ug_m3": composants.get("pm2_5"),
        "pm10_ug_m3": composants.get("pm10"),
        "nh3_ug_m3": composants.get("nh3"),
    }