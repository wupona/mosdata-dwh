#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import xmlrpc.client
import logging
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# =========================
# CONFIGURATION (alignée sur le premier code)
# =========================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("job_pos_etl")

# =========================
# PARAMÈTRES ODOO (via environnement comme premier code)
# =========================
ODOO_URL = os.getenv("ODOO_URL", "https://blissydah.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "norbert.wupona@gmail.com")
ODOO_API_KEY = os.getenv("ODOO_API_KEY", "")

if not ODOO_API_KEY:
    LOG.error("❌ Missing ODOO_API_KEY env var")
    raise RuntimeError("Missing ODOO_API_KEY env var")

# =========================
# CONNEXION POSTGRES (comme premier code)
# =========================
def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", "5432"))
    )

# =========================
# HELPERS
# =========================
def parse_odoo_dt(s):
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

def m2o_id(v):
    # Many2one returns [id, name]
    if isinstance(v, list) and len(v) >= 1:
        return str(v[0])
    if isinstance(v, int):
        return str(v)
    return None

def ensure_uniques(cur):
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_ps_session_id_odoo
                   ON core.fct_ps_pos_session (ps_session_id_odoo);""")
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_po_order_id_odoo
                   ON core.fct_po_pos_orders (po_order_id_odoo);""")
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_pl_line_id_odoo
                   ON core.fct_pl_pos_order_line (pl_pos_order_line_id_odoo);""")
    cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_pp_payment_id_odoo
                   ON core.fct_pp_pos_payment (pp_payment_id_odoo);""")

def get_max_ts(cur, table, col):
    cur.execute(f"SELECT MAX({col}) FROM {table};")
    return cur.fetchone()[0]

# =========================
# CONNEXION ODOO XML-RPC
# =========================
def connect_odoo():
    try:
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
        if not uid:
            raise RuntimeError("Odoo authentication failed")
        
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
        LOG.info(f"✅ Odoo connected (uid={uid})")
        return models, uid
    except Exception as e:
        LOG.error(f"❌ Erreur connexion Odoo: {e}")
        raise

def search_read(models, uid, model, domain, fields, order="write_date asc", limit=None):
    kwargs = {"fields": fields, "order": order}
    if limit:
        kwargs["limit"] = limit
    return models.execute_kw(ODOO_DB, uid, ODOO_API_KEY, model, "search_read", [domain], kwargs)

# =========================
# UPSERT SQL
# =========================
UPSERT_ORDERS = """
INSERT INTO core.fct_po_pos_orders (
  po_order_id_odoo, po_session_id_odoo, po_pos_config_id_odoo,
  po_txn_datetime, po_txn_day,
  po_employee_id_odoo, po_partner_id_odoo,
  po_amount_tax, po_amount_total, po_amount_paid, po_amount_return,
  po_state, po_pos_reference, po_uuid,
  po_write_date, po_raw_json, po_etl_loaded_at
) VALUES %s
ON CONFLICT (po_order_id_odoo) DO UPDATE SET
  po_session_id_odoo = EXCLUDED.po_session_id_odoo,
  po_pos_config_id_odoo = EXCLUDED.po_pos_config_id_odoo,
  po_txn_datetime = EXCLUDED.po_txn_datetime,
  po_txn_day = EXCLUDED.po_txn_day,
  po_employee_id_odoo = EXCLUDED.po_employee_id_odoo,
  po_partner_id_odoo = EXCLUDED.po_partner_id_odoo,
  po_amount_tax = EXCLUDED.po_amount_tax,
  po_amount_total = EXCLUDED.po_amount_total,
  po_amount_paid = EXCLUDED.po_amount_paid,
  po_amount_return = EXCLUDED.po_amount_return,
  po_state = EXCLUDED.po_state,
  po_pos_reference = EXCLUDED.po_pos_reference,
  po_uuid = EXCLUDED.po_uuid,
  po_write_date = EXCLUDED.po_write_date,
  po_raw_json = EXCLUDED.po_raw_json,
  po_etl_loaded_at = EXCLUDED.po_etl_loaded_at;
"""

UPSERT_LINES = """
INSERT INTO core.fct_pl_pos_order_line (
  pl_pos_order_line_id_odoo,
  pl_order_id_odoo, pl_pos_config_id_odoo,
  pl_txn_datetime, pl_txn_day,
  pl_product_id_odoo, pl_barcode,
  pl_qty, pl_unit_price, pl_discount_percent,
  pl_subtotal_excl_tax, pl_subtotal_incl_tax,
  pl_write_date, pl_raw_json, pl_etl_loaded_at
) VALUES %s
ON CONFLICT (pl_pos_order_line_id_odoo) DO UPDATE SET
  pl_order_id_odoo = EXCLUDED.pl_order_id_odoo,
  pl_pos_config_id_odoo = EXCLUDED.pl_pos_config_id_odoo,
  pl_txn_datetime = EXCLUDED.pl_txn_datetime,
  pl_txn_day = EXCLUDED.pl_txn_day,
  pl_product_id_odoo = EXCLUDED.pl_product_id_odoo,
  pl_barcode = EXCLUDED.pl_barcode,
  pl_qty = EXCLUDED.pl_qty,
  pl_unit_price = EXCLUDED.pl_unit_price,
  pl_discount_percent = EXCLUDED.pl_discount_percent,
  pl_subtotal_excl_tax = EXCLUDED.pl_subtotal_excl_tax,
  pl_subtotal_incl_tax = EXCLUDED.pl_subtotal_incl_tax,
  pl_write_date = EXCLUDED.pl_write_date,
  pl_raw_json = EXCLUDED.pl_raw_json,
  pl_etl_loaded_at = EXCLUDED.pl_etl_loaded_at;
"""

UPSERT_PAYMENTS = """
INSERT INTO core.fct_pp_pos_payment (
  pp_payment_id_odoo,
  pp_order_id_odoo, pp_pos_config_id_odoo,
  pp_payment_datetime, pp_payment_day,
  pp_payment_method_id_odoo, pp_amount,
  pp_is_change, pp_write_date, pp_raw_json, pp_etl_loaded_at
) VALUES %s
ON CONFLICT (pp_payment_id_odoo) DO UPDATE SET
  pp_order_id_odoo = EXCLUDED.pp_order_id_odoo,
  pp_pos_config_id_odoo = EXCLUDED.pp_pos_config_id_odoo,
  pp_payment_datetime = EXCLUDED.pp_payment_datetime,
  pp_payment_day = EXCLUDED.pp_payment_day,
  pp_payment_method_id_odoo = EXCLUDED.pp_payment_method_id_odoo,
  pp_amount = EXCLUDED.pp_amount,
  pp_is_change = EXCLUDED.pp_is_change,
  pp_write_date = EXCLUDED.pp_write_date,
  pp_raw_json = EXCLUDED.pp_raw_json,
  pp_etl_loaded_at = EXCLUDED.pp_etl_loaded_at;
"""

UPSERT_SESSIONS = """
INSERT INTO core.fct_ps_pos_session (
  ps_session_id_odoo, ps_pos_config_id_odoo,
  ps_start_at, ps_stop_at, ps_state,
  ps_raw_json, ps_etl_loaded_at
) VALUES %s
ON CONFLICT (ps_session_id_odoo) DO UPDATE SET
  ps_pos_config_id_odoo = EXCLUDED.ps_pos_config_id_odoo,
  ps_start_at = EXCLUDED.ps_start_at,
  ps_stop_at = EXCLUDED.ps_stop_at,
  ps_state = EXCLUDED.ps_state,
  ps_raw_json = EXCLUDED.ps_raw_json,
  ps_etl_loaded_at = EXCLUDED.ps_etl_loaded_at;
"""

# =========================
# MAIN LOAD
# =========================
def main():
    conn = None
    try:
        LOG.info("🚀 Début du chargement des données POS...")
        
        # Connexion Odoo
        models, uid = connect_odoo()
        
        # Connexion PostgreSQL
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Configuration via variables d'environnement
        buffer_hours = int(os.getenv("POS_BUFFER_HOURS", "48"))
        lookback_days = int(os.getenv("POS_LOOKBACK_DAYS", "7"))
        
        ensure_uniques(cur)

        now = datetime.now()
        default_from = now - timedelta(days=lookback_days)

        last_orders = get_max_ts(cur, "core.fct_po_pos_orders", "po_write_date") or default_from
        last_lines  = get_max_ts(cur, "core.fct_pl_pos_order_line", "pl_write_date") or default_from
        last_pay    = get_max_ts(cur, "core.fct_pp_pos_payment", "pp_write_date") or default_from

        write_from_orders = last_orders - timedelta(hours=buffer_hours)
        write_from_lines  = last_lines  - timedelta(hours=buffer_hours)
        write_from_pay    = last_pay    - timedelta(hours=buffer_hours)

        # 1. Commandes
        LOG.info("📋 Récupération des commandes depuis Odoo...")
        orders = search_read(
            models, uid,
            "pos.order",
            domain=[["write_date", ">=", write_from_orders.strftime("%Y-%m-%d %H:%M:%S")]],
            fields=["id","session_id","config_id","date_order","employee_id","partner_id",
                    "amount_tax","amount_total","amount_paid","amount_return",
                    "state","pos_reference","uuid","write_date"],
        )
        LOG.info(f"✅ {len(orders)} commandes récupérées")

        order_rows = []
        etl_now = datetime.now()
        for o in orders:
            oid = str(o["id"])
            session_id = m2o_id(o.get("session_id"))
            config_id = m2o_id(o.get("config_id"))
            txn_dt = parse_odoo_dt(o.get("date_order"))
            if not txn_dt:
                continue
            order_rows.append((
                oid, session_id, config_id,
                txn_dt, txn_dt.date(),
                m2o_id(o.get("employee_id")),
                m2o_id(o.get("partner_id")),
                o.get("amount_tax"), o.get("amount_total"), o.get("amount_paid"), o.get("amount_return"),
                o.get("state"), o.get("pos_reference"), o.get("uuid"),
                parse_odoo_dt(o.get("write_date")),
                json.dumps(o, ensure_ascii=False),
                etl_now
            ))
        
        if order_rows:
            LOG.info(f"💾 Insertion de {len(order_rows)} commandes...")
            psycopg2.extras.execute_values(cur, UPSERT_ORDERS, order_rows, page_size=1000)
            LOG.info("✅ Commandes sauvegardées")

        # Map orders => config + txn_dt/day
        cur.execute("SELECT po_order_id_odoo, po_pos_config_id_odoo, po_txn_datetime, po_txn_day FROM core.fct_po_pos_orders;")
        order_map = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

        # 2. Lignes de commande
        LOG.info("📋 Récupération des lignes de commande depuis Odoo...")
        lines = search_read(
            models, uid,
            "pos.order.line",
            domain=[["write_date", ">=", write_from_lines.strftime("%Y-%m-%d %H:%M:%S")]],
            fields=["id","order_id","product_id","qty","price_unit","discount",
                    "price_subtotal","price_subtotal_incl","write_date"],
        )
        LOG.info(f"✅ {len(lines)} lignes récupérées")

        line_rows = []
        for l in lines:
            line_id = str(l["id"])
            order_id = m2o_id(l.get("order_id"))
            if not order_id or order_id not in order_map:
                continue
            config_id, txn_dt, txn_day = order_map[order_id]
            product_id = m2o_id(l.get("product_id"))
            line_rows.append((
                line_id,
                order_id, config_id,
                txn_dt, txn_day,
                product_id, None,
                l.get("qty"), l.get("price_unit"), l.get("discount"),
                l.get("price_subtotal"), l.get("price_subtotal_incl"),
                parse_odoo_dt(l.get("write_date")),
                json.dumps(l, ensure_ascii=False),
                etl_now
            ))
        
        if line_rows:
            LOG.info(f"💾 Insertion de {len(line_rows)} lignes...")
            psycopg2.extras.execute_values(cur, UPSERT_LINES, line_rows, page_size=2000)
            LOG.info("✅ Lignes sauvegardées")

        # 3. Paiements
        LOG.info("📋 Récupération des paiements depuis Odoo...")
        pays = search_read(
            models, uid,
            "pos.payment",
            domain=[["write_date", ">=", write_from_pay.strftime("%Y-%m-%d %H:%M:%S")]],
            fields=["id","pos_order_id","payment_method_id","amount","payment_date","is_change","write_date"],
        )
        LOG.info(f"✅ {len(pays)} paiements récupérés")

        pay_rows = []
        for p in pays:
            pay_id = str(p["id"])
            order_id = m2o_id(p.get("pos_order_id"))
            if not order_id or order_id not in order_map:
                continue
            config_id, _, fallback_day = order_map[order_id]
            pay_dt = parse_odoo_dt(p.get("payment_date")) or datetime.combine(fallback_day, datetime.min.time())
            pay_rows.append((
                pay_id,
                order_id, config_id,
                pay_dt, pay_dt.date(),
                m2o_id(p.get("payment_method_id")),
                p.get("amount"),
                p.get("is_change"),
                parse_odoo_dt(p.get("write_date")),
                json.dumps(p, ensure_ascii=False),
                etl_now
            ))
        
        if pay_rows:
            LOG.info(f"💾 Insertion de {len(pay_rows)} paiements...")
            psycopg2.extras.execute_values(cur, UPSERT_PAYMENTS, pay_rows, page_size=2000)
            LOG.info("✅ Paiements sauvegardés")

        # 4. Enrichissement des codes-barres
        LOG.info("🏷️  Enrichissement des codes-barres...")
        cur.execute("""
            UPDATE core.fct_pl_pos_order_line pl
            SET pl_barcode = p.p_barcode
            FROM core.ref_p_product p
            WHERE pl.pl_barcode IS NULL
              AND pl.pl_product_id_odoo = p.p_product_id_odoo
              AND p.p_barcode IS NOT NULL;
        """)
        LOG.info("✅ Codes-barres enrichis")

        # 5. Sessions
        LOG.info("📋 Récupération des sessions depuis Odoo...")
        sessions = search_read(
            models, uid,
            "pos.session",
            domain=[["write_date", ">=", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")]],
            fields=["id", "config_id", "start_at", "stop_at", "state", "write_date"],
        )
        LOG.info(f"✅ {len(sessions)} sessions récupérées")

        sess_rows = []
        etl_now = datetime.now()
        for s in sessions:
            sid = str(s["id"])
            config_id = m2o_id(s.get("config_id"))
            start_at = parse_odoo_dt(s.get("start_at"))
            stop_at = parse_odoo_dt(s.get("stop_at"))
            state = s.get("state")
            sess_rows.append((
                sid, config_id, start_at, stop_at, state,
                json.dumps(s, ensure_ascii=False), etl_now
            ))

        if sess_rows:
            LOG.info(f"💾 Insertion de {len(sess_rows)} sessions...")
            psycopg2.extras.execute_values(cur, UPSERT_SESSIONS, sess_rows, page_size=1000)
            LOG.info("✅ Sessions sauvegardées")

        conn.commit()
        LOG.info("✅ Données POS chargées avec succès")

    except Exception as e:
        LOG.error(f"❌ Erreur lors du chargement POS: {e}")
        if conn: 
            conn.rollback()
        raise
    finally:
        if conn: 
            conn.close()

if __name__ == "__main__":
    main()