from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="recover_6days",
    description="Recupere les 6 derniers jours depuis l'API, rebuild clean, charge warehouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    tags=["aqi", "recovery"],
)
def recover_6days():

    @task
    def extraire() -> int:
        from aqi_utils import VILLES, extraire_aqi_historique, sauvegarder_raw

        cle_api = Variable.get("OPENWEATHER_API_KEY")
        now = datetime.now(timezone.utc)
        debut = int(now.timestamp()) - 6 * 24 * 3600
        total = 0

        for ville in VILLES:
            try:
                sauvegarder_raw(
                    ville["nom"],
                    extraire_aqi_historique(ville, cle_api, debut, int(now.timestamp())),
                )
                total += 1
                time.sleep(1.2)
            except Exception as e:
                print(f"Erreur {ville['nom']}: {e}")

        return total

    @task
    def clean(fichiers: int) -> int:
        from build_clean import reconstruire_clean_csv

        nb = reconstruire_clean_csv()
        print(f"clean : {nb} lignes depuis {fichiers} fichiers")
        return nb

    @task
    def warehouse(nb: int):
        from load_warehouse import charger_warehouse as _charger

        _charger(Variable.get("WAREHOUSE_DSN"))

    warehouse(clean(extraire()))


recover_6days()