# -*- coding: utf-8 -*-
"""
watermark.py
------------
Module générique pour gérer des watermarks (incrémental) via app.job_watermark.

Objectifs:
- Un seul mécanisme pour tous les jobs/sources
- Lecture du watermark + lookback (anti-perte)
- Marquage RUNNING / SUCCESS / FAIL
- Support watermark type 'ts' (timestamp) et 'id' (bigint)

Pré-requis DB:
- Table app.job_watermark (Option A)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union

import psycopg2


@dataclass(frozen=True)
class WatermarkState:
    job_name: str
    watermark_type: str  # 'ts' | 'id'
    watermark_ts: datetime
    watermark_id: int
    lookback_sec: int

    def since_timestamp(self) -> datetime:
        """
        Timestamp "since" à utiliser côté source (watermark_ts - lookback).
        Idéal pour write_date > since.
        """
        return self.watermark_ts - timedelta(seconds=self.lookback_sec)

    def since_id(self) -> int:
        """
        ID "since" à utiliser côté source (watermark_id - lookback not applicable).
        Généralement id > watermark_id.
        """
        return self.watermark_id


# ---------------------------------------------------------------------
# SQL constants
# ---------------------------------------------------------------------
SQL_GET = """
SELECT
  watermark_type,
  COALESCE(watermark_ts, timestamp '2000-01-01 00:00:00') AS watermark_ts,
  COALESCE(watermark_id, 0) AS watermark_id,
  lookback_sec
FROM app.job_watermark
WHERE job_name = %s
  AND is_enabled = true
"""

SQL_INIT = """
INSERT INTO app.job_watermark (
  job_name, source_name, entity_name, watermark_field, watermark_type,
  watermark_ts, watermark_id, lookback_sec, last_run_status
)
VALUES (
  %s, %s, %s, %s, %s,
  %s, %s, %s, 'INIT'
)
ON CONFLICT (job_name) DO NOTHING
"""

SQL_MARK_RUNNING = """
UPDATE app.job_watermark
SET
  last_run_started = %s,
  last_run_status  = 'RUNNING',
  last_error       = NULL
WHERE job_name = %s
"""

SQL_MARK_SUCCESS = """
UPDATE app.job_watermark
SET
  -- Avance le watermark uniquement si (new_ts,new_id) est supérieur au tuple courant
  watermark_ts = CASE
    WHEN (%s, %s) > (COALESCE(watermark_ts, timestamp '2000-01-01'), COALESCE(watermark_id, 0))
      THEN %s
    ELSE watermark_ts
  END,
  watermark_id = CASE
    WHEN (%s, %s) > (COALESCE(watermark_ts, timestamp '2000-01-01'), COALESCE(watermark_id, 0))
      THEN %s
    ELSE watermark_id
  END,

  last_run_started = %s,
  last_run_ended   = %s,
  last_run_status  = 'SUCCESS',
  last_run_rows    = %s,
  last_error       = NULL
WHERE job_name = %s
"""

SQL_MARK_FAIL = """
UPDATE app.job_watermark
SET
  last_run_started = %s,
  last_run_ended   = %s,
  last_run_status  = 'FAIL',
  last_run_rows    = %s,
  last_error       = LEFT(%s, 4000)
WHERE job_name = %s
"""


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------
def ensure_job(
    conn: psycopg2.extensions.connection,
    job_name: str,
    source_name: str,
    entity_name: str,
    watermark_field: str = "write_date",
    watermark_type: str = "ts",
    watermark_ts: Optional[datetime] = None,
    watermark_id: int = 0,
    lookback_sec: int = 7200,
) -> None:
    """
    Crée l'entrée watermark pour un job si elle n'existe pas.

    À appeler une fois (ou au démarrage du job en mode safe).
    """
    if watermark_type not in ("ts", "id"):
        raise ValueError("watermark_type must be 'ts' or 'id'")

    if watermark_ts is None:
        watermark_ts = datetime(2000, 1, 1, 0, 0, 0)
    
    with conn.cursor() as cur:
        # CORRECTION : Utiliser SQL_INIT au lieu de SQL_MARK_SUCCESS
        cur.execute(
            SQL_INIT,  # <-- CHANGER ICI
            (
                job_name,
                source_name,
                entity_name,
                watermark_field,
                watermark_type,
                watermark_ts,
                watermark_id,
                lookback_sec,
            ),
        )
    conn.commit()


def get_state(conn: psycopg2.extensions.connection, job_name: str) -> WatermarkState:
    """
    Lit l'état watermark d'un job.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_GET, (job_name,))
        row = cur.fetchone()

    if not row:
        raise RuntimeError(
            f"Job watermark introuvable ou désactivé: {job_name}. "
            f"Appelle ensure_job() ou vérifie app.job_watermark."
        )

    wm_type, wm_ts, wm_id, lookback = row
    wm_type = (wm_type or "ts").strip().lower()

    if wm_type not in ("ts", "id"):
        raise RuntimeError(f"watermark_type invalide pour {job_name}: {wm_type}")

    # garantir types
    if wm_ts is None:
        wm_ts = datetime(2000, 1, 1, 0, 0, 0)
    if wm_id is None:
        wm_id = 0
    if lookback is None:
        lookback = 7200

    return WatermarkState(
        job_name=job_name,
        watermark_type=wm_type,
        watermark_ts=wm_ts,
        watermark_id=int(wm_id),
        lookback_sec=int(lookback),
    )


def mark_running(
    conn: psycopg2.extensions.connection,
    job_name: str,
    started_at: Optional[datetime] = None,
) -> datetime:
    """
    Marque le job comme RUNNING.
    Retourne started_at utilisé.
    """
    if started_at is None:
        started_at = datetime.now()

    with conn.cursor() as cur:
        cur.execute(SQL_MARK_RUNNING, (started_at, job_name))
    conn.commit()
    return started_at

def mark_success(
    conn: psycopg2.extensions.connection,
    job_name: str,
    *,
    started_at: datetime,
    ended_at: Optional[datetime] = None,
    rows: Optional[int] = None,
    new_watermark_ts: Optional[datetime] = None,
    new_watermark_id: Optional[int] = None,
) -> None:
    """
    Marque SUCCESS + avance watermark.
    - Pour watermark_type='ts': fournir new_watermark_ts
    - Pour watermark_type='id': fournir new_watermark_id
    """
    if ended_at is None:
        ended_at = datetime.now()

    # Valeurs par défaut (ne pas casser l'UPDATE CASE)
    if new_watermark_ts is None:
        new_watermark_ts = datetime(2000, 1, 1, 0, 0, 0)
    if new_watermark_id is None:
        new_watermark_id = 0

    with conn.cursor() as cur:
        cur.execute(
            SQL_MARK_SUCCESS,
            (
                new_watermark_ts, int(new_watermark_id), new_watermark_ts,          # watermark_ts CASE
                new_watermark_ts, int(new_watermark_id), int(new_watermark_id),     # watermark_id CASE
                started_at,
                ended_at,
                rows,
                job_name,
            ),
        )
    conn.commit()


def mark_fail(
    conn: psycopg2.extensions.connection,
    job_name: str,
    *,
    started_at: datetime,
    ended_at: Optional[datetime] = None,
    rows: Optional[int] = None,
    error: str = "Unknown error",
) -> None:
    """
    Marque FAIL sans avancer watermark.
    """
    if ended_at is None:
        ended_at = datetime.now()

    with conn.cursor() as cur:
        cur.execute(
            SQL_MARK_FAIL,
            (
                started_at,
                ended_at,
                rows,
                error,
                job_name,
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def compute_new_watermark_ts_id_from_rows(
    rows: list,
    ts_field: str = "write_date",
    id_field: str = "id",
) -> Optional[Tuple[datetime, int]]:
    """
    Retourne le max tuple (write_date, id) pour avancer le watermark composite.
    rows: liste dict Odoo (search_read)
    """
    best: Optional[Tuple[datetime, int]] = None

    for r in rows:
        vts = r.get(ts_field)
        vid = r.get(id_field)

        if not vts or vid is None:
            continue

        try:
            ts = datetime.strptime(vts, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

        tup = (ts, int(vid))
        if best is None or tup > best:
            best = tup

    return best


def odoo_domain_since_write_date_id(
    base_domain: list,
    since_ts_str: str,
    since_id: int,
) -> list:
    """
    Domaine Odoo: write_date > since_ts OR (write_date = since_ts AND id > since_id)
    Compatible avec la notation préfixe Odoo.
    """
    # OR condition:
    # ['|', ('write_date','>',ts), '&', ('write_date','=',ts), ('id','>',id)]
    cond = [
        "|",
        ("write_date", ">", since_ts_str),
        "&",
        ("write_date", "=", since_ts_str),
        ("id", ">", since_id),
    ]

    if not base_domain:
        return cond

    # AND(base_domain, cond) en notation préfixe
    # ['&', <base_domain>, <cond>] mais base_domain est une liste de conditions implicites AND,
    # donc on la garde telle quelle.
    return ["&", cond] + base_domain

def format_odoo_dt(dt: datetime) -> str:
    """
    Formate un datetime en string compatible Odoo: 'YYYY-MM-DD HH:MM:SS'
    (utilise timestamp without tz)
    """
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def compute_new_watermark_ts_from_rows(
    rows: list,
    field_name: str = "write_date",
) -> Optional[datetime]:
    """
    Calcule le max datetime sur un champ Odoo ('write_date') pour avancer le watermark.
    rows: liste de dict Odoo (search_read)
    Retourne None si non calculable.
    """
    max_ts: Optional[datetime] = None
    for r in rows:
        v = r.get(field_name)
        if not v:
            continue
        try:
            ts = datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
        if (max_ts is None) or (ts > max_ts):
            max_ts = ts
    return max_ts
