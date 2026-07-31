from __future__ import annotations

import csv
import sys
from datetime import datetime
from pathlib import Path

CLEAN_CSV = Path(__file__).resolve().parent.parent / "data" / "clean" / "qualite_air.csv"

COLONNES_ATTENDUES = [
    "ville", "pays", "latitude", "longitude", "horodatage_utc", "aqi",
    "co_ug_m3", "no_ug_m3", "no2_ug_m3", "o3_ug_m3", "so2_ug_m3",
    "pm2_5_ug_m3", "pm10_ug_m3", "nh3_ug_m3",
]

CHAMPS_REQUIS = ["ville", "pays", "latitude", "longitude", "horodatage_utc", "aqi"]
POLLUANTS = ["co_ug_m3", "no_ug_m3", "no2_ug_m3", "o3_ug_m3", "so2_ug_m3",
             "pm2_5_ug_m3", "pm10_ug_m3", "nh3_ug_m3"]
VILLES_MIN = 5
FORMAT_HORODATAGE = "%Y-%m-%d %H:%M:%S"


def valider() -> int:
    if not CLEAN_CSV.exists():
        print(f"ERREUR : {CLEAN_CSV} introuvable")
        return 1

    erreurs: list[str] = []
    lignes = 0
    villes: set[str] = set()
    vus: set[tuple[str, str]] = set()
    horodatages_precedents: list[str] = []
    aqi_ok = 0
    aqi_ko = 0
    champs_requis_ko = 0
    polluants_ko = 0
    coordonnees_ko = 0
    format_ko = 0
    doublons = 0
    tri_ko = 0

    with open(CLEAN_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        if reader.fieldnames != COLONNES_ATTENDUES:
            erreurs.append(
                f"En-tete invalide : {reader.fieldnames} (attendu : {COLONNES_ATTENDUES})"
            )
            return 1

        for row in reader:
            lignes += 1

            for champ in CHAMPS_REQUIS:
                if not row.get(champ):
                    champs_requis_ko += 1
                    erreurs.append(
                        f"ligne {lignes} : champ requis vide '{champ}'"
                    )

            ville = row.get("ville", "")
            horodatage = row.get("horodatage_utc", "")
            villes.add(ville)

            cle = (ville, horodatage)
            if cle in vus:
                doublons += 1
                erreurs.append(f"doublon (ville, horodatage) : {ville} | {horodatage}")
            vus.add(cle)

            try:
                dt = datetime.strptime(horodatage, FORMAT_HORODATAGE)
            except (ValueError, TypeError):
                format_ko += 1
                erreurs.append(f"ligne {lignes} : horodatage invalide '{horodatage}'")
                dt = None

            if dt is not None:
                if horodatages_precedents and dt < horodatages_precedents[-1]:
                    tri_ko += 1
                    erreurs.append(
                        f"ligne {lignes} : horodatage decroissant '{horodatage}'"
                    )
                horodatages_precedents.append(dt)

            for champ in ("latitude", "longitude"):
                try:
                    float(row.get(champ, ""))
                except ValueError:
                    coordonnees_ko += 1
                    erreurs.append(
                        f"ligne {lignes} : coordonnee invalide '{champ}' = '{row.get(champ)}'"
                    )

            try:
                aqi = int(row.get("aqi", ""))
                if not 0 <= aqi <= 500:
                    aqi_ko += 1
                    erreurs.append(f"ligne {lignes} : aqi hors [0, 500] : {aqi}")
                else:
                    aqi_ok += 1
            except (ValueError, TypeError):
                aqi_ko += 1
                erreurs.append(
                    f"ligne {lignes} : aqi invalide '{row.get('aqi')}'"
                )

            for polluant in POLLUANTS:
                valeur = row.get(polluant, "")
                if valeur == "":
                    continue
                try:
                    float(valeur)
                except ValueError:
                    polluants_ko += 1
                    erreurs.append(
                        f"ligne {lignes} : {polluant} invalide '{valeur}'"
                    )

    if len(villes) < VILLES_MIN:
        erreurs.append(
            f"nombre de villes insuffisant : {len(villes)} (minimum {VILLES_MIN})"
        )

    print(f"Fichier           : {CLEAN_CSV}")
    print(f"Lignes            : {lignes}")
    print(f"Villes distinctes : {len(villes)} ({', '.join(sorted(villes))})")
    print(f"AQI valides       : {aqi_ok}")
    print(f"AQI invalides     : {aqi_ko}")
    print(f"Coordonnees KO    : {coordonnees_ko}")
    print(f"Champs requis KO  : {champs_requis_ko}")
    print(f"Polluants KO      : {polluants_ko}")
    print(f"Horodatages KO    : {format_ko}")
    print(f"Doublons          : {doublons}")
    print(f"Non chronologique : {tri_ko}")

    if erreurs:
        print("\n" + "\n".join(erreurs))
        print("\nFAIL : clean/qualite_air.csv non conforme au contrat de donnees")
        return 1

    print("\nPASS : clean/qualite_air.csv conforme au contrat de donnees")
    return 0


if __name__ == "__main__":
    sys.exit(valider())
