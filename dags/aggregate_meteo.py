from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from utils import extraire_meteo, transformer_donnees


@dag(
    dag_id="aggregate_meteo",
    description="ETL meteo : extrait 5 villes, aggrege et affiche le classement",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
    catchup=False,
    tags=["meteo"],
)
def aggregate_meteo():

    @task
    def extract_antananarivo():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return transformer_donnees(extraire_meteo("Antananarivo", cle_api))

    @task
    def extract_london():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return transformer_donnees(extraire_meteo("London", cle_api))

    @task
    def extract_newyork():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return transformer_donnees(extraire_meteo("New York", cle_api))

    @task
    def extract_paris():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return transformer_donnees(extraire_meteo("Paris", cle_api))

    @task
    def extract_tokyo():
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        return transformer_donnees(extraire_meteo("Tokyo", cle_api))

    @task
    def aggregate(antananarivo, london, newyork, paris, tokyo):
        villes = [antananarivo, london, newyork, paris, tokyo]
        classement = sorted(
            villes, key=lambda x: x["temperature_C"], reverse=True
        )

        print("========================================")
        print("  RESUME METEO - CLASSEMENT")
        print("========================================")
        for i, v in enumerate(classement, 1):
            print(
                f"  {i}. {v['ville']:20s} "
                f"{v['temperature_C']:5.1f}C  "
                f"{v['humidite_%']:3.0f}%  "
                f"{v['vent_m_s']:4.1f}m/s  "
                f"{v['description']}"
            )
        print("----------------------------------------")
        print(
            f"  + Chaude : {classement[0]['ville']} "
            f"({classement[0]['temperature_C']}C)"
        )
        print(
            f"  + Froide : {classement[-1]['ville']} "
            f"({classement[-1]['temperature_C']}C)"
        )
        print("========================================")

        return classement

    aggregate(
        extract_antananarivo(),
        extract_london(),
        extract_newyork(),
        extract_paris(),
        extract_tokyo(),
    )


aggregate_meteo()
