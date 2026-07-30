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


def _upsert_dim_ville(cur, row):
    cur.execute(
        """
        INSERT INTO dim_ville (nom, pays, latitude, longitude)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (nom) DO UPDATE SET
            pays = EXCLUDED.pays,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        RETURNING id_ville
        """,
        (row["ville"], row["pays"], row["latitude"], row["longitude"]),
    )
    return cur.fetchone()[0]


def _upsert_dim_temps(cur, horodatage_utc):
    dt = datetime.strptime(horodatage_utc, "%Y-%m-%d %H:%M:%S")
    jour_semaine = dt.strftime("%A")
    weekend = jour_semaine in ("Saturday", "Sunday")
    cur.execute(
        """
        INSERT INTO dim_temps
            (date_entiere, annee, mois, jour, heure, jour_semaine, weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_entiere, heure) DO NOTHING
        RETURNING id_temps
        """,
        (
            dt.date(),
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            jour_semaine,
            weekend,
        ),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT id_temps FROM dim_temps WHERE date_entiere = %s AND heure = %s",
        (dt.date(), dt.hour),
    )
    return cur.fetchone()[0]


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


def charger_warehouse(dsn: str) -> int:
    if not CLEAN_CSV.exists():
        logger.warning("Fichier clean introuvable : %s", CLEAN_CSV)
        return 0

    conn = None
    try:
        conn = psycopg2.connect(dsn, connect_timeout=5)
        _creer_tables(conn)

        derniere_date, derniere_heure = _dernier_horodatage_warehouse(conn)
        logger.info("Dernier horodatage en warehouse : %s %s", derniere_date, derniere_heure)

        rows: list[tuple] = []
        with open(CLEAN_CSV, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if derniere_date is not None:
                    dt = datetime.strptime(row["horodatage_utc"], "%Y-%m-%d %H:%M:%S")
                    if dt.date() < derniere_date or (
                        dt.date() == derniere_date and dt.hour < derniere_heure
                    ):
                        continue

                with conn.cursor() as cur:
                    id_ville = _upsert_dim_ville(cur, row)
                    id_temps = _upsert_dim_temps(cur, row["horodatage_utc"])
                    rows.append((
                        id_temps,
                        id_ville,
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

                if len(rows) % 500 == 0:
                    logger.info("Warehouse : %s lignes preparees...", len(rows))

        if rows:
            with conn.cursor() as cur:
                execute_values(cur, """
                    INSERT INTO fact_air_quality
                        (id_temps, id_ville, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
                    VALUES %s
                    ON CONFLICT (id_temps, id_ville) DO UPDATE SET
                        aqi = EXCLUDED.aqi, co = EXCLUDED.co, no = EXCLUDED.no,
                        no2 = EXCLUDED.no2, o3 = EXCLUDED.o3, so2 = EXCLUDED.so2,
                        pm2_5 = EXCLUDED.pm2_5, pm10 = EXCLUDED.pm10, nh3 = EXCLUDED.nh3
                """, rows)
                logger.info("Batch fact_air_quality : %s lignes inserees", len(rows))

        conn.commit()
        logger.info("Warehouse charge : %s nouvelles lignes", len(rows))
        return len(rows)
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
