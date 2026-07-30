from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from aqi_utils import VILLES, RAW_DIR, extraire_aqi_historique, sauvegarder_raw
from build_clean import reconstruire_clean_csv
from load_warehouse import charger_warehouse


def recuperer_6_jours(cle_api: str) -> int:
    now = datetime.now(timezone.utc)
    debut = int(now.timestamp()) - 6 * 24 * 3600
    fin = int(now.timestamp())
    total = 0

    print(f"Recuperation AQI du {(datetime.fromtimestamp(debut, tz=timezone.utc)).strftime('%Y-%m-%d %H:%M')} a {now.strftime('%Y-%m-%d %H:%M')} UTC")

    for ville in VILLES:
        print(f"  {ville['nom']}...")
        try:
            payload = extraire_aqi_historique(ville, cle_api, debut, fin)
            sauvegarder_raw(ville["nom"], payload)
            total += 1
            time.sleep(1.2)
        except Exception as e:
            print(f"  ERREUR {ville['nom']}: {e}")

    print(f"Fichiers raw crees : {total}")
    return total


if __name__ == "__main__":
    cle_api = os.environ.get("OPENWEATHER_API_KEY") or sys.argv[1] if len(sys.argv) > 1 else None
    dsn = os.environ.get("WAREHOUSE_DSN") or sys.argv[2] if len(sys.argv) > 2 else None

    if not cle_api:
        print("Usage: OPENWEATHER_API_KEY=... WAREHOUSE_DSN=... python scripts/recover_6days.py")
        print("   ou: python scripts/recover_6days.py <api_key> <dsn>")
        sys.exit(1)
    if not dsn:
        print("WAREHOUSE_DSN manquant. Les donnees seront sauvegardees dans raw/ et clean/ mais pas chargees en warehouse.")
        print("Passez le DSN en 2e argument ou variable d'environnement pour charger le warehouse.")

    fichiers = recuperer_6_jours(cle_api)

    nb_lignes = reconstruire_clean_csv()
    print(f"clean/qualite_air.csv : {nb_lignes} lignes depuis {fichiers} appels API")

    if dsn:
        charger_warehouse(dsn)
        print("Warehouse charge avec les 6 derniers jours.")
    else:
        print("Warehouse non charge (pas de DSN). Les donnees sont dans raw/ et clean/.")