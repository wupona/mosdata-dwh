#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
job_08_etl_fct_sm_stock_movement_4.py - CORRIGÉ (curseur nommé)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

LOG = logging.getLogger("job_08_etl_fct_sm_stock_movement")


# -------------------- Logging / Env --------------------
def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def pg_connect():
    dsn = os.getenv("DWH_DSN")
    if dsn:
        return psycopg2.connect(dsn)

    return psycopg2.connect(
        host=env("DB_HOST"),
        dbname=env("DB_NAME"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432")),
    )


# -------------------- Helpers --------------------
def safe_float(v: Any) -> Optional[float]:
    if v is None or v is False or v is True:
        return None
    try:
        return float(v)
    except Exception:
        return None


def date_key(d: date) -> int:
    return int(d.strftime("%Y%m%d"))


# -------------------- Upsert --------------------
def upsert_fact_update_insert(cur, fact_table: str, rows: List[Tuple]) -> int:
    if not rows:
        return 0

    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS tmp_sm_fact_load (
            sm_fct_id               UUID,
            sm_movement_day         DATE,
            sm_odoo_move_line_id    INT,
            sm_odoo_move_id         INT,
            sm_odoo_picking_id      INT,
            sm_odoo_write_date      TIMESTAMP,
            sm_product_id_odoo      INT,
            sm_location_id_odoo     INT,
            sm_date_key             INT,
            sm_qty                  NUMERIC(16,4),
            sm_signed_qty           NUMERIC(16,4),
            sm_uom_name             TEXT,
            sm_movement_side        TEXT,
            sm_location_usage       TEXT,
            sm_is_internal_location BOOLEAN,
            sm_etl_loaded_at        TIMESTAMP,
            sm_barcode              TEXT,
            sm_unit_sale_price      NUMERIC(16,4),
            sm_unit_cost            NUMERIC(16,4),
            sm_value_sale           NUMERIC(18,4),
            sm_value_cost           NUMERIC(18,4),
            sm_movement_ref         TEXT,
            sm_movement_type        TEXT
        ) ON COMMIT DELETE ROWS;
    """)

    expected = 23

    # Validation des lignes
    for i, r in enumerate(rows[:10]):
        if not isinstance(r, (tuple, list)):
            raise RuntimeError(f"Row[{i}] is not tuple/list: type={type(r)} value={r!r}")
        if len(r) != expected:
            raise RuntimeError(f"Row[{i}] length mismatch: expected {expected}, got {len(r)}. Row: {r}")

    execute_values(cur, """
        INSERT INTO tmp_sm_fact_load (
            sm_fct_id, sm_movement_day, sm_odoo_move_line_id, sm_odoo_move_id, sm_odoo_picking_id, sm_odoo_write_date,
            sm_product_id_odoo, sm_location_id_odoo, sm_date_key, sm_qty, sm_signed_qty, sm_uom_name,
            sm_movement_side, sm_location_usage, sm_is_internal_location, sm_etl_loaded_at,
            sm_barcode, sm_unit_sale_price, sm_unit_cost, sm_value_sale, sm_value_cost,
            sm_movement_ref, sm_movement_type
        ) VALUES %s
    """, rows, page_size=5000)

    # UPDATE existing
    cur.execute(f"""
        UPDATE {fact_table} p
        SET
            sm_odoo_move_id         = t.sm_odoo_move_id,
            sm_odoo_picking_id      = t.sm_odoo_picking_id,
            sm_odoo_write_date      = t.sm_odoo_write_date,
            sm_product_id_odoo      = t.sm_product_id_odoo,
            sm_date_key             = t.sm_date_key,
            sm_qty                  = t.sm_qty,
            sm_signed_qty           = t.sm_signed_qty,
            sm_uom_name             = t.sm_uom_name,
            sm_location_usage       = t.sm_location_usage,
            sm_is_internal_location = t.sm_is_internal_location,
            sm_barcode              = t.sm_barcode,
            sm_unit_sale_price      = t.sm_unit_sale_price,
            sm_unit_cost            = t.sm_unit_cost,
            sm_value_sale           = t.sm_value_cost,
            sm_value_cost           = t.sm_value_cost,
            sm_movement_ref         = t.sm_movement_ref,
            sm_movement_type        = t.sm_movement_type,
            sm_etl_loaded_at        = t.sm_etl_loaded_at
        FROM tmp_sm_fact_load t
        WHERE p.sm_movement_day      = t.sm_movement_day
          AND p.sm_odoo_move_line_id = t.sm_odoo_move_line_id
          AND p.sm_movement_side     = t.sm_movement_side
          AND p.sm_location_id_odoo  = t.sm_location_id_odoo
    """)

    # INSERT missing
    cur.execute(f"""
        INSERT INTO {fact_table} (
            sm_fct_id, sm_movement_day,
            sm_odoo_move_line_id, sm_odoo_move_id, sm_odoo_picking_id, sm_odoo_write_date,
            sm_product_id_odoo, sm_location_id_odoo, sm_date_key,
            sm_qty, sm_signed_qty, sm_uom_name,
            sm_movement_side, sm_location_usage, sm_is_internal_location,
            sm_etl_loaded_at,
            sm_barcode, sm_unit_sale_price, sm_unit_cost, sm_value_sale, sm_value_cost,
            sm_movement_ref, sm_movement_type
        )
        SELECT
            t.sm_fct_id, t.sm_movement_day,
            t.sm_odoo_move_line_id, t.sm_odoo_move_id, t.sm_odoo_picking_id, t.sm_odoo_write_date,
            t.sm_product_id_odoo, t.sm_location_id_odoo, t.sm_date_key,
            t.sm_qty, t.sm_signed_qty, t.sm_uom_name,
            t.sm_movement_side, t.sm_location_usage, t.sm_is_internal_location,
            t.sm_etl_loaded_at,
            t.sm_barcode, t.sm_unit_sale_price, t.sm_unit_cost, t.sm_value_sale, t.sm_value_cost,
            t.sm_movement_ref, t.sm_movement_type
        FROM tmp_sm_fact_load t
        WHERE NOT EXISTS (
            SELECT 1
            FROM {fact_table} p
            WHERE p.sm_movement_day      = t.sm_movement_day
              AND p.sm_odoo_move_line_id = t.sm_odoo_move_line_id
              AND p.sm_movement_side     = t.sm_movement_side
              AND p.sm_location_id_odoo  = t.sm_location_id_odoo
        )
    """)

    return len(rows)


def post_update_movement_type(cur, fact_table: str, d_from: date, d_to: date) -> int:
    sql = f"""
        UPDATE {fact_table} f
        SET sm_movement_type = r.mt_movement_type_code
        FROM core.ref_mt_movement_type r
        WHERE r.mt_is_active = true
          AND f.sm_movement_day >= %s
          AND f.sm_movement_day <= %s
          AND f.sm_movement_ref IS NOT NULL
          AND f.sm_movement_ref LIKE r.mt_ref_pattern
          AND (f.sm_movement_type IS NULL OR f.sm_movement_type = '')
          AND r.mt_priority = (
              SELECT MIN(r2.mt_priority)
              FROM core.ref_mt_movement_type r2
              WHERE r2.mt_is_active = true
                AND f.sm_movement_ref LIKE r2.mt_ref_pattern
          );
    """
    cur.execute(sql, (d_from, d_to))
    return cur.rowcount


# -------------------- Main ETL --------------------
def main():
    setup_logging()

    stg_table = os.getenv("SM_STG_TABLE", "core.stg_sm_stock_move_line")
    fact_table = os.getenv("SM_FACT_TABLE", "core.fct_sm_stock_movement")

    window_days = int(os.getenv("SM_WINDOW_DAYS", "3"))
    batch_size = int(os.getenv("SM_BATCH", "5000"))

    filter_done = os.getenv("SM_FILTER_DONE", "1") == "1"
    use_date_field = os.getenv("SM_USE_DATE_FIELD", "write_date").lower().strip()

    d_to = datetime.utcnow().date()
    d_from = d_to - timedelta(days=window_days)

    LOG.info("STG=%s | FACT=%s", stg_table, fact_table)
    LOG.info("Window: %s -> %s | batch=%s | filter_done=%s | use_date_field=%s",
             d_from, d_to, batch_size, filter_done, use_date_field)

    where_clauses = ["sm_part_day >= %s", "sm_part_day <= %s"]
    params: List[Any] = [d_from, d_to]
    if filter_done:
        where_clauses.append("COALESCE(sm_odoo_state,'') = 'done'")
    where_sql = " AND ".join(where_clauses)

    stg_select = f"""
        SELECT
            sm_odoo_move_line_id,
            sm_odoo_move_id,
            sm_odoo_picking_id,
            sm_product_id_odoo,
            sm_location_id_odoo,
            sm_location_dest_id_odoo,
            sm_qty_done,
            sm_uom_name,
            sm_barcode,
            sm_unit_sale_price,
            sm_unit_cost,
            sm_odoo_date,
            sm_odoo_write_date,
            COALESCE(
              sm_movement_ref,
              CASE
                WHEN jsonb_typeof((sm_payload::jsonb)->'raw'->'picking_id') = 'array'
                THEN (sm_payload::jsonb)->'raw'->'picking_id'->>1
                ELSE NULL
              END
            ) AS sm_movement_ref
        FROM {stg_table}
        WHERE {where_sql}
        ORDER BY sm_part_day, sm_odoo_move_line_id
    """

    total_fact_rows = 0

    with pg_connect() as conn:
        # DÉSACTIVER autocommit pour utiliser les transactions correctement
        conn.autocommit = False
        
        # SOLUTION 1: Utiliser un curseur simple au lieu d'un curseur nommé
        with conn.cursor() as stg_cur:  # CHANGEMENT ICI: pas de name="stg_cur"
            stg_cur.execute(stg_select, params)
            
            with conn.cursor() as cur2:
                buffer: List[Tuple] = []

                def flush():
                    nonlocal buffer, total_fact_rows
                    if not buffer:
                        return
                    total_fact_rows += upsert_fact_update_insert(cur2, fact_table, buffer)
                    buffer = []

                # Lire toutes les lignes en une fois (pour de petits volumes)
                # ou par batch pour de gros volumes
                rows = stg_cur.fetchall()
                
                for (ml_id, move_id, picking_id, prod_id, loc_src, loc_dst,
                     qty_done, uom, barcode, unit_sale_price, unit_cost,
                     odoo_date, odoo_write_date, movement_ref) in rows:

                    # Déterminer le mouvement day
                    movement_day: date
                    if use_date_field == "write_date" and odoo_write_date is not None:
                        movement_day = odoo_write_date.date()
                    elif odoo_date is not None:
                        movement_day = odoo_date.date()
                    elif odoo_write_date is not None:
                        movement_day = odoo_write_date.date()
                    else:
                        continue

                    qty = safe_float(qty_done)
                    if qty is None:
                        continue
                    qty_abs = abs(qty)

                    usp = safe_float(unit_sale_price) or 0.0
                    ucost = safe_float(unit_cost) or 0.0

                    value_sale = qty_abs * usp
                    value_cost = qty_abs * ucost

                    # Récupérer les usages des locations
                    cur2.execute("""
                        SELECT l_usage
                        FROM core.ref_l_location
                        WHERE l_location_id_odoo = %s
                        LIMIT 1
                    """, (loc_src,))
                    src_usage = cur2.fetchone()
                    src_usage = src_usage[0] if src_usage else None
                    src_is_internal = (src_usage == "internal")

                    cur2.execute("""
                        SELECT l_usage
                        FROM core.ref_l_location
                        WHERE l_location_id_odoo = %s
                        LIMIT 1
                    """, (loc_dst,))
                    dst_usage = cur2.fetchone()
                    dst_usage = dst_usage[0] if dst_usage else None
                    dst_is_internal = (dst_usage == "internal")

                    # récupérer les usages pour déterminer le signe
                    sm_signed_qty_src = -qty_abs if src_is_internal else 0

                    sm_signed_qty_dst = qty_abs if dst_is_internal else 0
                    now_ts = datetime.utcnow()

                    # SRC row
                    buffer.append((
                        str(uuid4()),  # sm_fct_id
                        movement_day,  # sm_movement_day
                        ml_id,  # sm_odoo_move_line_id
                        move_id,  # sm_odoo_move_id
                        picking_id,  # sm_odoo_picking_id
                        odoo_write_date,  # sm_odoo_write_date
                        prod_id,  # sm_product_id_odoo
                        loc_src,  # sm_location_id_odoo
                        date_key(movement_day),  # sm_date_key
                        qty_abs,  # sm_qty
                        sm_signed_qty_src,  # LA VARIABLE CALCULÉE
                        uom,  # sm_uom_name
                        "SRC",  # sm_movement_side
                        src_usage,  # sm_location_usage
                        src_is_internal,  # sm_is_internal_location
                        now_ts,  # sm_etl_loaded_at
                        barcode,  # sm_barcode
                        usp,  # sm_unit_sale_price
                        ucost,  # sm_unit_cost
                        value_sale,  # sm_value_sale
                        value_cost,  # sm_value_cost
                        movement_ref,  # sm_movement_ref
                        None  # sm_movement_type
                    ))

                    # DST row
                    buffer.append((
                        str(uuid4()),  # sm_fct_id
                        movement_day,  # sm_movement_day
                        ml_id,  # sm_odoo_move_line_id
                        move_id,  # sm_odoo_move_id
                        picking_id,  # sm_odoo_picking_id
                        odoo_write_date,  # sm_odoo_write_date
                        prod_id,  # sm_product_id_odoo
                        loc_dst,  # sm_location_id_odoo
                        date_key(movement_day),  # sm_date_key
                        qty_abs,  # sm_qty
                        sm_signed_qty_dst,  # LA VARIABLE CALCULÉE
                        uom,  # sm_uom_name
                        "DST",  # sm_movement_side
                        dst_usage,  # sm_location_usage
                        dst_is_internal,  # sm_is_internal_location
                        now_ts,  # sm_etl_loaded_at
                        barcode,  # sm_barcode
                        usp,  # sm_unit_sale_price
                        ucost,  # sm_unit_cost
                        value_sale,  # sm_value_sale
                        value_cost,  # sm_value_cost
                        movement_ref,  # sm_movement_ref
                        None  # sm_movement_type
                    ))

                    if len(buffer) >= batch_size:
                        flush()

                flush()

                # Mettre à jour les types de mouvement
                typed = post_update_movement_type(cur2, fact_table, d_from, d_to)
                conn.commit()

    LOG.info("DONE: fact_rows_processed=%s | movement_type_filled=%s", total_fact_rows, typed)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.exception("FAILED: %s", e)
        sys.exit(1)
