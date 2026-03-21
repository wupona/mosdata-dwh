import os
import sys
import json
import logging
import argparse
import xmlrpc.client
import psycopg2
from psycopg2 import extras
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

# =========================
# CONFIGURATION ET LOGGING
# =========================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("job_expense_etl")

ODOO_URL = os.getenv("ODOO_URL", "https://blissydah.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "")
ODOO_API_KEY = os.getenv("ODOO_API_KEY", "")

if not ODOO_API_KEY:
    LOG.error("❌ Missing ODOO_API_KEY env var")
    raise RuntimeError("Missing ODOO_API_KEY env var")


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432"))
    )


def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange(d1: date, d2: date):
    """Inclusive range day by day."""
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


def odoo_connect():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
    if not uid:
        raise RuntimeError("❌ Odoo authentication failed (uid is falsy). Check user/api key/db.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models


def get_last_odoo_write_date_watermark() -> str:
    """
    Watermark basé sur le write_date Odoo stocké en DB.
    Retourne une string 'YYYY-MM-DD HH:MM:SS'
    """
    last_sync = "2000-01-01 00:00:00"
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(oe_odoo_write_date) FROM core.fct_oe_odoo_expenses;")
            res = cur.fetchone()
            if res and res[0]:
                # safety window: -5 minutes
                last_dt = res[0] - timedelta(minutes=5)
                last_sync = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    return last_sync


def purge_db_from(start_date: date):
    LOG.warning(f"🧹 Purging DB rows where oe_expense_date >= {start_date.isoformat()}")
    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM core.fct_oe_odoo_expenses WHERE oe_expense_date >= %s;",
                (start_date,)
            )
        conn.commit()


def upsert_expenses(records: list[dict]):
    """
    Upsert complet + stockage write_date/create_date Odoo.
    """
    if not records:
        return

    transformed = []
    for exp in records:
        # Champs métier
        oe_expense_id = exp.get("id")
        oe_date = exp.get("date")  # 'YYYY-MM-DD'
        employee_name = exp["employee_id"][1] if exp.get("employee_id") else None
        total_amount = float(exp.get("total_amount") or 0.0)
        status = exp.get("state")

        # Timestamps Odoo (strings 'YYYY-MM-DD HH:MM:SS' ou False)
        odoo_write_date = exp.get("write_date") or None
        odoo_create_date = exp.get("create_date") or None

        transformed.append((
            oe_expense_id,
            oe_date,
            employee_name,
            total_amount,
            status,
            json.dumps(exp, ensure_ascii=False),
            odoo_write_date,
            odoo_create_date
        ))

    upsert_sql = """
        INSERT INTO core.fct_oe_odoo_expenses (
            oe_expense_id_odoo,
            oe_expense_date,
            oe_employee_name,
            oe_total_amount,
            oe_status,
            oe_raw_json,
            oe_odoo_write_date,
            oe_odoo_create_date,
            oe_last_update
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
        ON CONFLICT (oe_expense_id_odoo) DO UPDATE SET
            oe_expense_date      = EXCLUDED.oe_expense_date,
            oe_employee_name     = EXCLUDED.oe_employee_name,
            oe_total_amount      = EXCLUDED.oe_total_amount,
            oe_status            = EXCLUDED.oe_status,
            oe_raw_json          = EXCLUDED.oe_raw_json,
            oe_odoo_write_date   = EXCLUDED.oe_odoo_write_date,
            oe_odoo_create_date  = EXCLUDED.oe_odoo_create_date,
            oe_last_update       = CURRENT_TIMESTAMP;
    """

    with get_pg_conn() as conn:
        with conn.cursor() as cur:
            extras.execute_batch(cur, upsert_sql, transformed, page_size=500)
        conn.commit()

    LOG.info(f"✅ Upserted {len(transformed)} expenses.")


def fetch_expenses(models, uid, domain: list, fields: list[str]) -> list[dict]:
    """
    Attention: search_read peut être limité en volumétrie.
    Si besoin, on peut passer à search + read avec pagination. Pour l’instant, on reste simple.
    """
    return models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "hr.expense", "search_read",
        [domain],
        {"fields": fields, "order": "id asc"}
    )


def run_incremental():
    uid, models = odoo_connect()
    last_sync = get_last_odoo_write_date_watermark()
    LOG.info(f"🔍 Incremental: searching hr.expense with write_date > {last_sync}")

    fields = [
        "id", "date", "employee_id", "name", "total_amount", "state",
        "analytic_distribution", "write_date", "create_date"
    ]
    domain = [("write_date", ">", last_sync)]

    records = fetch_expenses(models, uid, domain, fields)
    if not records:
        LOG.info("✨ No new or updated expenses found.")
        return

    LOG.info(f"📥 Found {len(records)} new/updated records.")
    upsert_expenses(records)


def run_full_reload(start_date: date, end_date: date | None, day_by_day: bool):
    uid, models = odoo_connect()
    fields = [
        "id", "date", "employee_id", "name", "total_amount", "state",
        "analytic_distribution", "write_date", "create_date"
    ]

    if end_date is None:
        end_date = datetime.today().date()

    if day_by_day:
        LOG.info(f"🔁 Full reload DAY-BY-DAY from {start_date} to {end_date}")
        total = 0
        for d in daterange(start_date, end_date):
            domain = [("date", ">=", d.isoformat()), ("date", "<", (d + timedelta(days=1)).isoformat())]
            records = fetch_expenses(models, uid, domain, fields)
            if records:
                upsert_expenses(records)
                total += len(records)
            LOG.info(f"📅 {d.isoformat()} -> {len(records)} records")
        LOG.info(f"🏁 Full reload completed. Total records upserted: {total}")
    else:
        LOG.info(f"🔁 Full reload RANGE from {start_date} to {end_date}")
        domain = [("date", ">=", start_date.isoformat()), ("date", "<=", end_date.isoformat())]
        records = fetch_expenses(models, uid, domain, fields)
        LOG.info(f"📥 Found {len(records)} records in range.")
        upsert_expenses(records)
        LOG.info("🏁 Full reload completed.")


def main():
    parser = argparse.ArgumentParser(description="Odoo hr.expense ETL (incremental or full reload).")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                        help="ETL mode: incremental (default) or full")
    parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD (required for full)")
    parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD (optional for full)")
    parser.add_argument("--purge-db", action="store_true",
                        help="Delete DB rows where oe_expense_date >= start-date before full reload")
    parser.add_argument("--day-by-day", action="store_true",
                        help="Full reload day-by-day (safer, easier to audit)")
    args = parser.parse_args()

    if args.mode == "incremental":
        run_incremental()
        return

    # FULL mode
    if not args.start_date:
        raise ValueError("❌ --start-date is required when --mode=full")

    start_d = parse_ymd(args.start_date)
    end_d = parse_ymd(args.end_date) if args.end_date else None

    if args.purge_db:
        purge_db_from(start_d)

    run_full_reload(start_d, end_d, args.day_by_day)


if __name__ == "__main__":
    main()
