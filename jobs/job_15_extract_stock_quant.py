# jobs/job_15_extract_stock_quant.py
import os
import sys
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# -----------------------------
# Paths / imports
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from odoo_client_odoorpc_fixed import OdooClient  # noqa: E402

from watermark import (  # noqa: E402
    ensure_job,
    get_state,
    mark_running,
    mark_success,
    mark_fail,
    format_odoo_dt,
    compute_new_watermark_ts_id_from_rows,
)

# -----------------------------
# Configuration & Identity
# -----------------------------
JOB_NAME = "job_15_stock_quant"
SOURCE_NAME = "odoo_online"
ENTITY_NAME = "stock.quant"
RUN_LABEL = "job_15_extract_stock_quant"

CONFIG_DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
ROOT_DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# -----------------------------
# Helpers
# -----------------------------
def m2o_id(v) -> Optional[int]:
    return int(v[0]) if isinstance(v, (list, tuple)) and v else None

def parse_odoo_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432")),
    )

def fetch_location_ids_by_usage(odoo: OdooClient, usages: List[str]) -> List[int]:
    """
    Filtrer stock.quant sur un scope de locations déterminé par usage.
    """
    dom = [("usage", "in", usages)]
    rows = odoo.execute(
        "stock.location",
        "search_read",
        dom,
        fields=["id"],
        limit=200000,
        order="id asc",
        _max_retries=6,
    )
    return [int(r["id"]) for r in rows if r.get("id")]

def odoo_extract_incremental_write_date_id(
    odoo: OdooClient,
    model: str,
    base_domain: List[Any],
    fields: List[str],
    since_ts: datetime,
    since_id: int,
    max_rows: int,
    page_size: int,
    pause_s: float,
) -> List[Dict[str, Any]]:
    """
    Incrémental stable: write_date asc, id asc
    Domain:
      base_domain AND (write_date > since_ts OR (write_date = since_ts AND id > since_id))
    """
    out: List[Dict[str, Any]] = []
    cur_ts_str = format_odoo_dt(since_ts)
    cur_id = int(since_id or 0)

    while True:
        domain = ["&"] + base_domain + [
            "|",
            ("write_date", ">", cur_ts_str),
            "&",
            ("write_date", "=", cur_ts_str),
            ("id", ">", cur_id),
        ]

        limit = min(page_size, max_rows - len(out)) if max_rows else page_size
        batch = odoo.execute(
            model,
            "search_read",
            domain,
            fields=fields,
            limit=limit,
            order="write_date asc, id asc",
            _max_retries=6,
        )

        if not batch:
            break

        out.extend(batch)

        last = batch[-1]
        cur_ts_str = last.get("write_date") or cur_ts_str
        cur_id = int(last.get("id") or cur_id)

        if pause_s:
            time.sleep(pause_s)

        if len(batch) < limit or (max_rows and len(out) >= max_rows):
            break

    return out

def dedup_rows_by_id_keep_latest(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Évite psycopg2.errors.CardinalityViolation sur ON CONFLICT:
    dédupliquer le batch sur (id) et garder la version la plus récente (write_date).
    """
    best_by_id: Dict[int, tuple] = {}
    for r in rows:
        rid = r.get("id")
        if rid is None:
            continue
        rid = int(rid)
        wd = parse_odoo_dt(r.get("write_date"))
        key = (wd or datetime(1900, 1, 1), rid)
        prev = best_by_id.get(rid)
        if prev is None or key > prev[0]:
            best_by_id[rid] = (key, r)
    return [v[1] for v in best_by_id.values()]

def resolve_since_ts_id(job_mode: str, wm0, bootstrap_ts: Optional[datetime]) -> tuple:
    """
    Calcule since_ts/since_id en tenant compte:
    - mode full_catchup
    - lookback (wm0.since_timestamp())
    - bootstrap (ex: 2022-01-01)
    """
    if job_mode == "full_catchup":
        return (bootstrap_ts or datetime(2000, 1, 1, 0, 0, 0), 0)

    since_ts = wm0.since_timestamp()  # watermark_ts - lookback
    since_id = wm0.watermark_id

    # si watermark trop ancien, on le relève à bootstrap
    if bootstrap_ts and since_ts < bootstrap_ts:
        since_ts = bootstrap_ts
        since_id = 0

    return (since_ts, since_id)

# -----------------------------
# Main
# -----------------------------
def main():
    load_dotenv(ROOT_DOTENV_PATH)
    if os.path.exists(CONFIG_DB_ENV_PATH):
        load_dotenv(CONFIG_DB_ENV_PATH, override=True)

    job_mode = os.getenv("JOB_MODE", "incremental").lower()
    max_rows = int(os.getenv("MAX_ROWS_PER_RUN", "200000"))
    page_size = int(os.getenv("PAGE_SIZE", "5000"))
    pause_s = float(os.getenv("PAUSE_S", "0.05"))

    # Lookback pour ce job (défaut 30 min conseillé)
    lookback_sec = int(os.getenv("LOOKBACK_SEC", "1800"))

    # Usages à inclure (temp + internal)
    usages_csv = os.getenv("STOCK_QUANT_USAGES", "internal,inventory,production,transit")
    usages = [u.strip() for u in usages_csv.split(",") if u.strip()]

    # Bootstrap start date (recommandé 2021/2022)
    bootstrap_str = os.getenv("STOCK_QUANT_BOOTSTRAP_TS", "2022-01-01 00:00:00").strip()
    bootstrap_ts = parse_odoo_dt(bootstrap_str)

    conn = get_pg_conn()
    started_at_db = None

    try:
        ensure_job(
            conn,
            job_name=JOB_NAME,
            source_name=SOURCE_NAME,
            entity_name=ENTITY_NAME,
            watermark_field="write_date",
            watermark_type="ts",
            lookback_sec=lookback_sec,
        )
        wm0 = get_state(conn, JOB_NAME)
        started_at_db = mark_running(conn, JOB_NAME)

        # Odoo connect
        raw = (os.getenv("ODOO_URL") or "").strip()
        if raw and "://" not in raw:
            raw = "https://" + raw
        parsed = urlparse(raw)
        if not parsed.hostname:
            raise ValueError(f"ODOO_URL invalide: {os.getenv('ODOO_URL')}")

        odoo = OdooClient(
            host=parsed.hostname,
            db=os.getenv("ODOO_DB"),
            user=os.getenv("ODOO_USER"),
            password=os.getenv("ODOO_API_KEY"),
            port=443,
            protocol="jsonrpc+ssl",
            timeout=300,
        )
        odoo.connect()

        # since calc
        since_ts, since_id = resolve_since_ts_id(job_mode, wm0, bootstrap_ts)

        # Locations scope par usage
        location_ids = fetch_location_ids_by_usage(odoo, usages)
        if not location_ids:
            raise RuntimeError(f"Aucun stock.location trouvé pour usages={usages}")

        fields = [
            "id",
            "write_date",
            "create_date",
            "product_id",
            "location_id",
            "company_id",
            "lot_id",
            "quantity",
            "reserved_quantity",
            "in_date",
        ]

        base_domain = [
            ("location_id", "in", location_ids),
        ]

        print(f"🚀 Extraction stock.quant (mode={job_mode}, usages={usages}) since=({since_ts}, {since_id}) ...")
        rows = odoo_extract_incremental_write_date_id(
            odoo=odoo,
            model="stock.quant",
            base_domain=base_domain,
            fields=fields,
            since_ts=since_ts,
            since_id=since_id,
            max_rows=max_rows,
            page_size=page_size,
            pause_s=pause_s,
        )

        if not rows:
            mark_success(
                conn,
                JOB_NAME,
                started_at=started_at_db,
                ended_at=datetime.now(),
                rows=0,
                new_watermark_ts=wm0.watermark_ts,
                new_watermark_id=wm0.watermark_id,
            )
            print("✅ Rien à synchroniser.")
            return

        # DEDUP (fix cardinality violation)
        rows = dedup_rows_by_id_keep_latest(rows)

        # Watermark sur le batch dédupliqué
        best = compute_new_watermark_ts_id_from_rows(rows, ts_field="write_date", id_field="id")
        if best:
            new_wm_ts, new_wm_id = best
        else:
            new_wm_ts, new_wm_id = wm0.watermark_ts, wm0.watermark_id

        # Normalize -> tuples
        stg = []
        now_dt = datetime.now()
        for r in rows:
            quant_id = int(r["id"])
            wd = parse_odoo_dt(r.get("write_date"))
            cd = parse_odoo_dt(r.get("create_date"))
            part_day = (wd.date() if wd else now_dt.date())

            stg.append((
                part_day,
                quant_id,
                wd,
                cd,
                m2o_id(r.get("product_id")),
                m2o_id(r.get("location_id")),
                m2o_id(r.get("company_id")),
                m2o_id(r.get("lot_id")),
                r.get("quantity"),
                r.get("reserved_quantity"),
                parse_odoo_dt(r.get("in_date")),
                json.dumps(r, ensure_ascii=False),
            ))

        sql = """
            INSERT INTO core.stg_sq_stock_quantity (
                sq_part_day,
                sq_odoo_quant_id,
                sq_odoo_write_date,
                sq_odoo_create_date,
                sq_odoo_product_id,
                sq_odoo_location_id,
                sq_odoo_company_id,
                sq_odoo_lot_id,
                sq_quantity,
                sq_reserved_quantity,
                sq_in_date,
                sq_payload
            )
            VALUES %s
            ON CONFLICT (sq_odoo_quant_id) DO UPDATE SET
                sq_part_day = EXCLUDED.sq_part_day,
                sq_odoo_write_date = EXCLUDED.sq_odoo_write_date,
                sq_odoo_create_date = EXCLUDED.sq_odoo_create_date,
                sq_odoo_product_id = EXCLUDED.sq_odoo_product_id,
                sq_odoo_location_id = EXCLUDED.sq_odoo_location_id,
                sq_odoo_company_id = EXCLUDED.sq_odoo_company_id,
                sq_odoo_lot_id = EXCLUDED.sq_odoo_lot_id,
                sq_quantity = EXCLUDED.sq_quantity,
                sq_reserved_quantity = EXCLUDED.sq_reserved_quantity,
                sq_in_date = EXCLUDED.sq_in_date,
                sq_payload = EXCLUDED.sq_payload
        """

        with conn.cursor() as cur:
            execute_values(cur, sql, stg, page_size=2000)
        conn.commit()

        mark_success(
            conn,
            JOB_NAME,
            started_at=started_at_db,
            ended_at=datetime.now(),
            rows=len(stg),
            new_watermark_ts=new_wm_ts,
            new_watermark_id=new_wm_id,
        )

        print(f"✅ SUCCÈS: {len(stg)} quants synchronisés. watermark=({new_wm_ts}, {new_wm_id})")

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        if started_at_db is not None:
            mark_fail(
                conn,
                JOB_NAME,
                started_at=started_at_db,
                ended_at=datetime.now(),
                rows=0,
                error=str(e),
            )
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()