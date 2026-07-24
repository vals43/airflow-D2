from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

from aqi_utils import VILLES, extraire_aqi_historique, sauvegarder_raw


@dag(
    dag_id="backfill_aqi",
    description="Backfill AQI : 12 mois (juil 2025 → aujourd'hui) mois par mois",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["aqi", "backfill"],
)
def backfill_aqi():

    @task
    def extraire_tout_l_historique() -> int:
        cle_api = Variable.get("OPENWEATHER_API_KEY")
        now = datetime.now(timezone.utc)
        total = 0

        for decalage in range(13):
            debut = pendulum.datetime(2025, 7, 1, tz="UTC").add(months=decalage)
            if debut > now:
                break
            fin = min(debut.add(months=1), now)

            start_ts = int(debut.timestamp())
            end_ts = int(fin.timestamp())

            for ville in VILLES:
                try:
                    payload = extraire_aqi_historique(ville, cle_api, start_ts, end_ts)
                    sauvegarder_raw(ville["nom"], payload)
                    total += 1
                    time.sleep(1.2)
                except Exception as e:
                    print(f"Erreur {ville['nom']} ({debut.date()}): {e}")

            print(f"  {debut.date()} -> {fin.date()} : {len(VILLES)} villes OK")

        print(f"Fichiers raw crees : {total}")
        return total

    @task
    def reconstruire_clean(fichiers: int):
        from build_clean import reconstruire_clean_csv

        nb_lignes = reconstruire_clean_csv()
        print(f"clean/qualite_air.csv : {nb_lignes} lignes depuis {fichiers} fichiers raw")
        return nb_lignes

    @task
    def charger_warehouse(nb_lignes: int):
        from load_warehouse import charger_warehouse as _charger

        dsn = Variable.get("WAREHOUSE_DSN")
        _charger(dsn)

    fichiers = extraire_tout_l_historique()
    nb = reconstruire_clean(fichiers)
    charger_warehouse(nb)


backfill_aqi()
