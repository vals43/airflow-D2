from __future__ import annotations

import csv
from pathlib import Path

import requests


def extraire_meteo(ville: str, cle_api: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    parametres = {
        "q": ville,
        "appid": cle_api,
        "units": "metric",
        "lang": "fr",
    }
    reponse = requests.get(url, params=parametres, timeout=10)
    reponse.raise_for_status()
    return reponse.json()


def transformer_donnees(data: dict) -> dict:
    return {
        "ville": data["name"],
        "temperature_C": data["main"]["temp"],
        "ressenti_C": data["main"]["feels_like"],
        "humidite_%": data["main"]["humidity"],
        "description": data["weather"][0]["description"],
        "vent_m_s": data["wind"]["speed"],
    }


def sauvegarder_csv(ligne: dict, csv_path: str):
    chemin = Path(csv_path)
    ecrire_entete = not chemin.exists()
    with open(chemin, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ligne.keys())
        if ecrire_entete:
            writer.writeheader()
        writer.writerow(ligne)
