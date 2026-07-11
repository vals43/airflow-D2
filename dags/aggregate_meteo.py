from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.datasets import Dataset

CSV_PATH = "/opt/airflow/data/meteo_villes.csv"
RESUME_PATH = "/opt/airflow/data/meteo_resume.csv"
csv_asset = Dataset("meteo://villes.csv")


@dag(
    dag_id="aggregate_meteo",
    description="Agrege les donnees meteo des 5 villes",
    schedule=[csv_asset],
    start_date=pendulum.datetime(2026, 6, 30, tz="UTC"),
    catchup=False,
    tags=["meteo", "aggregate"],
)
def aggregate_meteo():

    @task
    def lire_donnees() -> pd.DataFrame:
        df = pd.read_csv(CSV_PATH)
        return df

    @task
    def classer_villes(df: pd.DataFrame) -> dict:
        dernieres = df.groupby("ville").last().reset_index()
        plus_chaude = dernieres.loc[dernieres["temperature_C"].idxmax()]
        plus_froide = dernieres.loc[dernieres["temperature_C"].idxmin()]
        moyenne = df.groupby("ville")["temperature_C"].mean().round(2)

        resume = []
        for _, row in dernieres.iterrows():
            resume.append(
                {
                    "ville": row["ville"],
                    "temperature_actuelle_C": row["temperature_C"],
                    "humidite_%": row["humidite_%"],
                    "vent_m_s": row["vent_m_s"],
                    "description": row["description"],
                }
            )

        return {
            "classement": sorted(resume, key=lambda x: x["temperature_actuelle_C"], reverse=True),
            "plus_chaude": plus_chaude["ville"],
            "temp_plus_chaude": plus_chaude["temperature_C"],
            "plus_froide": plus_froide["ville"],
            "temp_plus_froide": plus_froide["temperature_C"],
            "moyennes": moyenne.to_dict(),
        }

    @task
    def afficher_resume(stats: dict):
        print("========================================")
        print("  RESUME METEO - CLASSEMENT")
        print("========================================")
        for i, ville in enumerate(stats["classement"], 1):
            print(
                f"  {i}. {ville['ville']:20s} "
                f"{ville['temperature_actuelle_C']:5.1f}C  "
                f"{ville['humidite_%']:3.0f}%  "
                f"{ville['vent_m_s']:4.1f}m/s  "
                f"{ville['description']}"
            )
        print("----------------------------------------")
        print(
            f"  + Chaude : {stats['plus_chaude']} "
            f"({stats['temp_plus_chaude']}C)"
        )
        print(
            f"  + Froide : {stats['plus_froide']} "
            f"({stats['temp_plus_froide']}C)"
        )
        print("----------------------------------------")
        print("  Moyennes par ville :")
        for ville, temp in stats["moyennes"].items():
            print(f"    {ville:20s} {temp:5.1f}C")
        print("========================================")

        df = pd.DataFrame(stats["classement"])
        df["date_resume"] = pendulum.now("UTC").to_iso8601_string()
        df.to_csv(RESUME_PATH, index=False)
        print(f"Resume sauvegarde dans {RESUME_PATH}")

    afficher_resume(classer_villes(lire_donnees()))


aggregate_meteo()
