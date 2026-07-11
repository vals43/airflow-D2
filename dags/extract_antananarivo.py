from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.datasets import Asset
from airflow.models import Variable

from utils import extraire_meteo, transformer_donnees, sauvegarder_csv

CSV_PATH = "/opt/airflow/data/meteo_villes.csv"
csv_asset = Asset("meteo://villes.csv")


@dag(
    dag_id="extract_antananarivo",
    description="ETL meteo OpenWeather pour Antananarivo",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
    catchup=True,
    tags=["meteo", "antananarivo"],
)
def extract_antananarivo():

    @task
    def extract():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return extraire_meteo("Antananarivo", cle_api)

    @task
    def transform(data: dict) -> dict:
        return transformer_donnees(data)

    @task(outlets=[csv_asset])
    def load(ligne: dict):
        context = get_current_context()
        ligne["date_extraction"] = context["logical_date"].format(
            "YYYY-MM-DD HH:mm:ss"
        )
        sauvegarder_csv(ligne, CSV_PATH)

    load(transform(extract()))


extract_antananarivo()
