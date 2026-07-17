from __future__ import annotations

import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
from aqi_utils import VILLES, RAW_DIR, CLEAN_DIR, CLEAN_CSV, transformer_mesure  # noqa: E402

VILLES_PAR_COORD = {(v["latitude"], v["longitude"]): v for v in VILLES}
COLONNES = [
    "ville", "pays", "latitude", "longitude", "horodatage_utc", "aqi",
    "co_ug_m3", "no_ug_m3", "no2_ug_m3", "o3_ug_m3", "so2_ug_m3",
    "pm2_5_ug_m3", "pm10_ug_m3", "nh3_ug_m3",
]


def ville_depuis_coord(lat: float, lon: float) -> dict | None:
    """Retrouve la ville connue la plus proche des coordonnees d'un fichier raw."""
    for (v_lat, v_lon), ville in VILLES_PAR_COORD.items():
        if abs(v_lat - lat) < 0.01 and abs(v_lon - lon) < 0.01:
            return ville
    return None


def lire_fichier_raw(chemin: Path) -> list[dict]:
    """
    Lit un fichier JSON brut (current OU historique, meme structure OWM :
    {"coord": {...}, "list": [...]}), et retourne des lignes transformees.
    """
    data = json.loads(chemin.read_text(encoding="utf-8"))
    coord = data.get("coord", {})
    ville = ville_depuis_coord(coord.get("lat"), coord.get("lon"))
    if ville is None:
        print(f"  [ignore] {chemin.name} : coordonnees non reconnues")
        return []
    return [transformer_mesure(ville, mesure) for mesure in data.get("list", [])]


def reconstruire_clean_csv() -> int:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    if not RAW_DIR.exists():
        raise SystemExit(f"Erreur : {RAW_DIR} n'existe pas, lancez le backfill / le DAG d'abord")

    lignes_par_cle: dict[tuple[str, str], dict] = {}
    fichiers = sorted(RAW_DIR.glob("*.json"))
    print(f"Lecture de {len(fichiers)} fichiers dans {RAW_DIR}")

    for chemin in fichiers:
        for ligne in lire_fichier_raw(chemin):
            cle = (ligne["ville"], ligne["horodatage_utc"])  # dedup : ville+heure
            lignes_par_cle[cle] = ligne  # la derniere lecture ecrase (idempotent)

    lignes_triees = sorted(lignes_par_cle.values(), key=lambda x: (x["ville"], x["horodatage_utc"]))

    with open(CLEAN_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
        writer.writeheader()
        writer.writerows(lignes_triees)

    print(f"clean/qualite_air.csv ecrit : {len(lignes_triees)} lignes")
    return len(lignes_triees)


if __name__ == "__main__":
    reconstruire_clean_csv()