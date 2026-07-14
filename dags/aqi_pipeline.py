from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from aqi_utils import VILLES, extraire_aqi_courant, sauvegarder_raw


@dag(
    dag_id="aqi_pipeline",
    description="Collecte horaire de l'AQI pour 5 villes + reconstruction clean/ + chargement warehouse",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["aqi", "warehouse"],
)
def aqi_pipeline():

    @task
    def extraire_et_sauvegarder(ville: dict):
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        payload = extraire_aqi_courant(ville, cle_api)
        chemin = sauvegarder_raw(ville["nom"], payload)
        return str(chemin)

    @task
    def reconstruire_clean(_chemins_raw: list[str]):
        from build_clean import reconstruire_clean_csv
        nb_lignes = reconstruire_clean_csv()
        print(f"clean/qualite_air.csv reconstruit : {nb_lignes} lignes")
        return nb_lignes

    @task
    def charger_warehouse(nb_lignes: int):
        from load_warehouse import charger_warehouse as _charger
        dsn = Variable.get("WAREHOUSE_DSN")
        _charger(dsn)

    chemins = [extraire_et_sauvegarder(ville) for ville in VILLES]
    nb = reconstruire_clean(chemins)
    charger_warehouse(nb)


aqi_pipeline()