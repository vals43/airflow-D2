from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

CLEAN_CSV = (
    Path(__file__).resolve().parent.parent / "data" / "clean" / "qualite_air.csv"
)

DDL_DIM_VILLE = """
    CREATE TABLE IF NOT EXISTS dim_ville (
        id_ville SERIAL PRIMARY KEY,
        nom TEXT NOT NULL UNIQUE,
        pays TEXT NOT NULL,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL
    )
"""

DDL_DIM_TEMPS = """
    CREATE TABLE IF NOT EXISTS dim_temps (
        id_temps SERIAL PRIMARY KEY,
        date_entiere DATE NOT NULL,
        annee INTEGER NOT NULL,
        mois INTEGER NOT NULL,
        jour INTEGER NOT NULL,
        heure INTEGER NOT NULL,
        jour_semaine TEXT NOT NULL,
        weekend BOOLEAN NOT NULL,
        UNIQUE(date_entiere, heure)
    )
"""

DDL_FACT = """
    CREATE TABLE IF NOT EXISTS fact_air_quality (
        id_fait SERIAL PRIMARY KEY,
        id_temps INTEGER NOT NULL REFERENCES dim_temps(id_temps),
        id_ville INTEGER NOT NULL REFERENCES dim_ville(id_ville),
        aqi INTEGER,
        co DOUBLE PRECISION,
        no DOUBLE PRECISION,
        no2 DOUBLE PRECISION,
        o3 DOUBLE PRECISION,
        so2 DOUBLE PRECISION,
        pm2_5 DOUBLE PRECISION,
        pm10 DOUBLE PRECISION,
        nh3 DOUBLE PRECISION,
        UNIQUE(id_temps, id_ville)
    )
"""


def _creer_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_DIM_VILLE)
        cur.execute(DDL_DIM_TEMPS)
        cur.execute(DDL_FACT)
    conn.commit()


def _int_or_none(val):
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _float_or_none(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _dernier_horodatage_warehouse(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT dt.date_entiere, dt.heure
            FROM fact_air_quality f
            JOIN dim_temps dt ON f.id_temps = dt.id_temps
            ORDER BY dt.date_entiere DESC, dt.heure DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            return row[0], row[1]
        return None, None


def charger_warehouse(dsn: str, force: bool = False) -> int:
    if not CLEAN_CSV.exists():
        logger.warning("Fichier clean introuvable : %s", CLEAN_CSV)
        return 0

    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        _creer_tables(conn)

        if not force:
            derniere_date, derniere_heure = _dernier_horodatage_warehouse(conn)
        else:
            derniere_date, derniere_heure = None, None
        logger.info("Dernier horodatage en warehouse : %s %s", derniere_date, derniere_heure)

        lignes_csv: list[dict] = []
        villes_set: dict[str, dict] = {}
        temps_set: dict[str, datetime] = {}

        with open(CLEAN_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if derniere_date is not None:
                    dt = datetime.strptime(row["horodatage_utc"], "%Y-%m-%d %H:%M:%S")
                    if dt.date() < derniere_date or (
                        dt.date() == derniere_date and dt.hour < derniere_heure
                    ):
                        continue

                lignes_csv.append(row)
                villes_set[row["ville"]] = row
                temps_set[row["horodatage_utc"]] = (
                    datetime.strptime(row["horodatage_utc"], "%Y-%m-%d %H:%M:%S")
                )

        if not lignes_csv:
            logger.info("Aucune nouvelle ligne a charger")
            conn.commit()
            return 0

        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO dim_ville (nom, pays, latitude, longitude)
                VALUES %s
                ON CONFLICT (nom) DO UPDATE SET
                    pays = EXCLUDED.pays,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude
            """, [
                (v["ville"], v["pays"], v["latitude"], v["longitude"])
                for v in villes_set.values()
            ])
            cur.execute("SELECT nom, id_ville FROM dim_ville")
            cache_ville = dict(cur.fetchall())

        temps_batch = []
        for ts_str, dt in temps_set.items():
            jour_semaine = dt.strftime("%A")
            weekend = jour_semaine in ("Saturday", "Sunday")
            temps_batch.append((dt.date(), dt.year, dt.month, dt.day, dt.hour, jour_semaine, weekend))

        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO dim_temps
                    (date_entiere, annee, mois, jour, heure, jour_semaine, weekend)
                VALUES %s
                ON CONFLICT (date_entiere, heure) DO NOTHING
            """, temps_batch)
            cur.execute("SELECT date_entiere, heure, id_temps FROM dim_temps")
            cache_temps = {(d, h): id_t for d, h, id_t in cur.fetchall()}

        rows: list[tuple] = []
        for row in lignes_csv:
            dt = datetime.strptime(row["horodatage_utc"], "%Y-%m-%d %H:%M:%S")
            id_ville = cache_ville[row["ville"]]
            id_temps = cache_temps[(dt.date(), dt.hour)]
            rows.append((
                id_temps, id_ville,
                _int_or_none(row.get("aqi")),
                _float_or_none(row.get("co_ug_m3")),
                _float_or_none(row.get("no_ug_m3")),
                _float_or_none(row.get("no2_ug_m3")),
                _float_or_none(row.get("o3_ug_m3")),
                _float_or_none(row.get("so2_ug_m3")),
                _float_or_none(row.get("pm2_5_ug_m3")),
                _float_or_none(row.get("pm10_ug_m3")),
                _float_or_none(row.get("nh3_ug_m3")),
            ))

        seen = set()
        unique_rows = []
        for r in rows:
            key = (r[0], r[1])
            if key not in seen:
                seen.add(key)
                unique_rows.append(r)

        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO fact_air_quality
                    (id_temps, id_ville, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                VALUES %s
                ON CONFLICT (id_temps, id_ville) DO UPDATE SET
                    aqi = EXCLUDED.aqi, co = EXCLUDED.co, no = EXCLUDED.no,
                    no2 = EXCLUDED.no2, o3 = EXCLUDED.o3, so2 = EXCLUDED.so2,
                    pm2_5 = EXCLUDED.pm2_5, pm10 = EXCLUDED.pm10, nh3 = EXCLUDED.nh3
            """, unique_rows)

        conn.commit()
        logger.info("Warehouse charge : %s nouvelles lignes", len(unique_rows))
        return len(unique_rows)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
