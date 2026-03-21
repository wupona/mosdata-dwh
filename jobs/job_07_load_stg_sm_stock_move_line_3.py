#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import gzip
import glob
import shutil
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# -------------------------------------------------------------------
# Configuration & Logging
# -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
LOG = logging.getLogger("job_07_load_stg_sm_stock_move_line")

# -------------------------------------------------------------------
# Utilitaires
# -------------------------------------------------------------------
def db_connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432"))
    )

def clean_false(v: Any) -> Any:
    """ Convertit False (Odoo) en None (SQL NULL) pour les colonnes Integer/Text """
    return None if v is False else v

def md5_text(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def parse_odoo_dt(v: Any) -> Optional[datetime]:
    if not v or v is False: return None
    try:
        return datetime.strptime(str(v).strip(), "%Y-%m-%d %H:%M:%S")
    except:
        return None

def get_movement_ref(obj: Dict[str, Any]) -> Optional[str]:
    """ Extrait la référence depuis le payload raw généré par Job 06 """
    raw = obj.get("raw") or {}
    picking = raw.get("picking_id")
    # 1. Si picking_id est une liste [id, name]
    if isinstance(picking, list) and len(picking) >= 2:
        return str(picking[1])
    # 2. Sinon fallback sur reference ou display_name
    return clean_false(raw.get("reference")) or clean_false(raw.get("display_name"))

# -------------------------------------------------------------------
# Coeur du Job
# -------------------------------------------------------------------
def process_file(cur, stg_table: str, data_file: str, extract_ts: datetime, batch_size: int) -> int:
    source_file = os.path.basename(data_file)
    
    sql = f"""
        INSERT INTO {stg_table} (
            sm_part_day, sm_odoo_move_line_id, sm_odoo_move_id, sm_odoo_picking_id,
            sm_odoo_date, sm_odoo_write_date, sm_odoo_state,
            sm_product_id_odoo, sm_location_id_odoo, sm_location_dest_id_odoo,
            sm_qty_done, sm_uom_name, sm_barcode, sm_unit_sale_price, sm_unit_cost,
            sm_dup_key, sm_dup_flag, sm_extract_ts, sm_source_file,
            sm_movement_ref, sm_payload
        ) VALUES %s
    """

    rows = []
    inserted = 0

    with gzip.open(data_file, "rt", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line.strip())
            
            # Application de clean_false pour éviter l'erreur DatatypeMismatch (Boolean vs Integer)
            ml_id      = clean_false(obj.get("odoo_id"))
            move_id    = clean_false(obj.get("move_id"))
            picking_id = clean_false(obj.get("picking_id"))
            product_id = clean_false(obj.get("product_id"))
            loc_src    = clean_false(obj.get("location_id"))
            loc_dst    = clean_false(obj.get("location_dest_id"))
            
            qty        = obj.get("qty")
            odoo_date  = obj.get("date_value")
            write_date = obj.get("write_date")

            wd_dt = parse_odoo_dt(write_date) or parse_odoo_dt(odoo_date)
            if not ml_id or not wd_dt: continue

            # On peuple le champ reference ici pour le staging
            mv_ref = get_movement_ref(obj)

            rows.append((
                wd_dt.date(),
                ml_id, move_id, picking_id,
                odoo_date, write_date, obj.get("state"),
                product_id, loc_src, loc_dst,
                qty, obj.get("uom"),
                obj.get("barcode"), obj.get("unit_sale_price"), obj.get("unit_cost"),
                md5_text(f"{ml_id}|{wd_dt}|{qty}"), 0,
                extract_ts, source_file,
                mv_ref,
                json.dumps(obj, ensure_ascii=False)
            ))

            if len(rows) >= batch_size:
                execute_values(cur, sql, rows, page_size=batch_size)
                inserted += len(rows)
                rows = []

    if rows:
        execute_values(cur, sql, rows, page_size=batch_size)
        inserted += len(rows)
    return inserted

def main():
    stg_table = os.getenv("SM_STG_TABLE", "core.stg_sm_stock_move_line")
    inbox_dir = os.getenv("SM_EXTRACT_OUT_DIR", os.path.join(PROJECT_ROOT, "data/stock_movement/inbox"))
    archive_dir = os.path.join(os.path.dirname(inbox_dir), "archive")
    
    data_files = sorted(glob.glob(os.path.join(inbox_dir, "stock_move_line_*.jsonl.gz")))
    if not data_files:
        LOG.info("Aucun fichier trouvé dans l'inbox.")
        return

    data_file = data_files[-1]
    LOG.info(f"Traitement du fichier : {os.path.basename(data_file)}")

    with db_connect() as conn:
        with conn.cursor() as cur:
            # Nettoyage des 3 derniers jours pour éviter les doublons au chargement
            cur.execute(f"DELETE FROM {stg_table} WHERE sm_part_day >= CURRENT_DATE - INTERVAL '3 days'")
            
            inserted = process_file(cur, stg_table, data_file, datetime.utcnow(), 2000)
            conn.commit()
            LOG.info(f"✅ Insertion terminée : {inserted} lignes dans le Staging.")

    # Archivage
    os.makedirs(archive_dir, exist_ok=True)
    shutil.move(data_file, os.path.join(archive_dir, os.path.basename(data_file)))

if __name__ == "__main__":
    main()