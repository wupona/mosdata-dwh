#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import logging
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras
import xmlrpc.client
from dotenv import load_dotenv
from psycopg2.extras import execute_values



# =========================
# CONFIGURATION (comme job_12)
# =========================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("job_11_pos_stg_hist")

# =========================
# PARAMÈTRES ODOO (comme job_12)
# =========================
ODOO_URL = os.getenv("ODOO_URL", "https://blissydah.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "")
ODOO_API_KEY = os.getenv("ODOO_API_KEY", "")

if not ODOO_API_KEY:
    LOG.error("❌ Missing ODOO_API_KEY env var")
    raise RuntimeError("Missing ODOO_API_KEY env var")

# =========================
# CONNEXION POSTGRES (comme job_12)
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
# CONNEXION ODOO XML-RPC (comme job_12)
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

def search_read(models, uid, model, domain, fields, order="id asc", limit=None):
    kwargs = {"fields": fields, "order": order}
    if limit:
        kwargs["limit"] = limit
    return models.execute_kw(ODOO_DB, uid, ODOO_API_KEY, model, "search_read", [domain], kwargs)

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

def m2o_name(v):
    if isinstance(v, list) and len(v) >= 2:
        return str(v[1])
    return None

def get_model_fields(models, uid, model):
    return models.execute_kw(ODOO_DB, uid, ODOO_API_KEY, model, "fields_get", [], {"attributes": ["string"]})

def keep_existing_fields(models, uid, model, wanted_fields):
    existing = set(get_model_fields(models, uid, model).keys())
    kept = [f for f in wanted_fields if f in existing]
    missing = [f for f in wanted_fields if f not in existing]
    if missing:
        LOG.warning("Champs absents sur %s: %s", model, missing)
    return kept

    wanted = ["id","order_id","product_id","qty","price_unit","discount","price_subtotal_incl","create_date","stock_move_id"]
    line_fields = keep_existing_fields(models, uid, "pos.order.line", wanted)

def pick_payment_datetime(p):
    # selon dispo, on priorise payment_date puis create_date puis write_date
    s = p.get("payment_date") or p.get("create_date") or p.get("write_date")
    return parse_odoo_dt(s) if s else None



# =========================
# TABLES CIBLES
# =========================
STG_LINE_HIST  = "core.stg_po_pos_order_line_hist"
STG_ORDER_HIST = "core.stg_po_pos_orders_hist"

# =========================
# DDL (création tables hist si besoin)
# =========================
DDL = f"""
CREATE TABLE IF NOT EXISTS {STG_LINE_HIST}
(LIKE core.stg_po_pos_order_line INCLUDING ALL);

-- unique index sur line id pour ON CONFLICT simplifié en hist
CREATE UNIQUE INDEX IF NOT EXISTS ux_polh_line_id
ON {STG_LINE_HIST}(po_order_line_id_odoo);

CREATE INDEX IF NOT EXISTS ix_polh_create_date
ON {STG_LINE_HIST}(po_create_date);

CREATE INDEX IF NOT EXISTS ix_polh_order_id
ON {STG_LINE_HIST}(po_order_id_odoo);

CREATE TABLE IF NOT EXISTS {STG_ORDER_HIST} (
  po_order_id_odoo      varchar(50) PRIMARY KEY,
  po_session_id_odoo    varchar(50) NOT NULL,
  po_pos_config_id_odoo varchar(50) NOT NULL,
  po_txn_datetime       timestamp   NOT NULL,
  po_txn_day            date        NOT NULL,
  po_employee_id_odoo   varchar(50),
  po_partner_id_odoo    varchar(50),
  po_amount_tax         numeric(16,4),
  po_amount_total       numeric(16,4),
  po_amount_paid        numeric(16,4),
  po_amount_return      numeric(16,4),
  po_state              varchar(50),
  po_pos_reference      varchar(100),
  po_uuid               varchar(100),
  po_write_date         timestamp,
  po_raw_json           jsonb,
  po_ingestion_timestamp timestamp DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_poh_txn_day
ON {STG_ORDER_HIST}(po_txn_day);

CREATE INDEX IF NOT EXISTS ix_poh_write_date
ON {STG_ORDER_HIST}(po_write_date);
"""

# =========================
# UPSERT SQL
# =========================
UPSERT_ORDERS_HIST = f"""
INSERT INTO {STG_ORDER_HIST} (
  po_order_id_odoo, po_session_id_odoo, po_pos_config_id_odoo,
  po_txn_datetime, po_txn_day,
  po_employee_id_odoo, po_partner_id_odoo,
  po_amount_tax, po_amount_total, po_amount_paid, po_amount_return,
  po_state, po_pos_reference, po_uuid,
  po_write_date, po_raw_json, po_ingestion_timestamp
) VALUES %s
ON CONFLICT (po_order_id_odoo) DO UPDATE SET
  po_session_id_odoo     = EXCLUDED.po_session_id_odoo,
  po_pos_config_id_odoo  = EXCLUDED.po_pos_config_id_odoo,
  po_txn_datetime        = EXCLUDED.po_txn_datetime,
  po_txn_day             = EXCLUDED.po_txn_day,
  po_employee_id_odoo    = EXCLUDED.po_employee_id_odoo,
  po_partner_id_odoo     = EXCLUDED.po_partner_id_odoo,
  po_amount_tax          = EXCLUDED.po_amount_tax,
  po_amount_total        = EXCLUDED.po_amount_total,
  po_amount_paid         = EXCLUDED.po_amount_paid,
  po_amount_return       = EXCLUDED.po_amount_return,
  po_state               = EXCLUDED.po_state,
  po_pos_reference       = EXCLUDED.po_pos_reference,
  po_uuid                = EXCLUDED.po_uuid,
  po_write_date          = EXCLUDED.po_write_date,
  po_raw_json            = EXCLUDED.po_raw_json,
  po_ingestion_timestamp = NOW();
"""

UPSERT_LINES_HIST = f"""
INSERT INTO {STG_LINE_HIST} (
  po_order_line_id_odoo, po_order_id_odoo, po_product_id_odoo,
  po_stock_move_id_odoo, po_qty, po_price_unit, po_discount_percent,
  po_price_subtotal_incl, po_payment_method_id_odoo, po_payment_method_name,
  po_create_date, po_ingestion_timestamp, po_pos_reference
) VALUES %s
ON CONFLICT (po_order_line_id_odoo) DO UPDATE SET
  po_order_id_odoo          = EXCLUDED.po_order_id_odoo,
  po_product_id_odoo        = EXCLUDED.po_product_id_odoo,
  po_stock_move_id_odoo     = EXCLUDED.po_stock_move_id_odoo,
  po_qty                    = EXCLUDED.po_qty,
  po_price_unit             = EXCLUDED.po_price_unit,
  po_discount_percent       = EXCLUDED.po_discount_percent,
  po_price_subtotal_incl    = EXCLUDED.po_price_subtotal_incl,
  po_payment_method_id_odoo = EXCLUDED.po_payment_method_id_odoo,
  po_payment_method_name    = EXCLUDED.po_payment_method_name,
  po_create_date            = EXCLUDED.po_create_date,
  po_pos_reference          = EXCLUDED.po_pos_reference,
  po_ingestion_timestamp    = NOW();
"""

UPSERT_PAYMENTS_HIST = """
INSERT INTO core.stg_pp_pos_payment_hist (
  pp_payment_id_odoo,
  pp_order_id_odoo,
  pp_payment_datetime,
  pp_payment_day,
  pp_payment_method_id_odoo,
  pp_amount,
  pp_write_date,
  pp_raw_json,
  pp_ingestion_timestamp
) VALUES %s
ON CONFLICT (pp_payment_id_odoo) DO UPDATE SET
  pp_order_id_odoo          = EXCLUDED.pp_order_id_odoo,
  pp_payment_datetime       = EXCLUDED.pp_payment_datetime,
  pp_payment_day            = EXCLUDED.pp_payment_day,
  pp_payment_method_id_odoo = EXCLUDED.pp_payment_method_id_odoo,
  pp_amount                 = EXCLUDED.pp_amount,
  pp_write_date             = EXCLUDED.pp_write_date,
  pp_raw_json               = EXCLUDED.pp_raw_json,
  pp_ingestion_timestamp    = NOW();
"""
# =========================
# PARAMÈTRES MODE HIST
# =========================
# Exemples:
# POS_FROM="2025-01-01 00:00:00"
# POS_TO="2026-02-01 00:00:00"   (fin exclusive)
POS_MODE = os.getenv("POS_MODE", "daily").lower()  # daily | hist
POS_FROM = os.getenv("POS_FROM")
POS_TO = os.getenv("POS_TO")
LOOKBACK_HOURS = int(os.getenv("POS_DAILY_LOOKBACK_HOURS", "24"))

def run():
    LOG.info("--- JOB 11 : STAGING HIST (orders + lines) ---")

    # 1) Définir la fenêtre temporelle
    if POS_MODE == "hist":
        if not POS_FROM or not POS_TO:
            raise RuntimeError("POS_MODE=hist nécessite POS_FROM et POS_TO (POS_TO fin exclusive).")
        dt_from = parse_odoo_dt(POS_FROM)
        dt_to = parse_odoo_dt(POS_TO)
    else:
        dt_to = datetime.now()
        dt_from = dt_to - timedelta(hours=LOOKBACK_HOURS)

    LOG.info(f"📌 Fenêtre: [{dt_from} ; {dt_to})  (mode={POS_MODE})")

    # 2) Connexions
    models, uid = connect_odoo()
    conn = get_pg_conn()

    try:
        # 3) Créer tables hist si besoin
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        # 4) Extraction pos.order.line (par create_date comme ton code initial)
        LOG.info("📥 Extraction des lignes pos.order.line ...")
        order_lines = search_read(
            models, uid,
            "pos.order.line",
            domain=[
                ["create_date", ">=", dt_from.strftime("%Y-%m-%d %H:%M:%S")],
                ["create_date", "<",  dt_to.strftime("%Y-%m-%d %H:%M:%S")]
            ],
            fields=["id", "order_id", "product_id", "qty", "price_unit", "discount",
                    "price_subtotal_incl", "create_date"],
            order="id asc"
        )

        if not order_lines:
            LOG.info("ℹ️ Aucune ligne trouvée sur la période.")
            return

        order_ids = sorted({m2o_id(l.get("order_id")) for l in order_lines if m2o_id(l.get("order_id"))})
        LOG.info(f"✅ {len(order_lines)} lignes | {len(order_ids)} orders distincts")

        # 5) Extraction pos.order (headers complets)
        LOG.info("📥 Extraction des orders pos.order ...")
        orders = search_read(
            models, uid,
            "pos.order",
            domain=[["id", "in", [int(x) for x in order_ids]]],
            fields=["id", "name", "pos_reference", "uuid",
                    "session_id", "config_id",
                    "date_order", "write_date",
                    "employee_id", "partner_id",
                    "amount_tax", "amount_total", "amount_paid", "amount_return",
                    "state"],
            order="id asc"
        )
        order_map = {str(o["id"]): o for o in orders}

        # 6) Extraction pos.payment (pour mapper méthode(s) de paiement par order)
        LOG.info("📥 Extraction des paiements pos.payment ...")
        pays = search_read(
            models, uid,
            "pos.payment",
            domain=[["pos_order_id", "in", [int(x) for x in order_ids]]],
            fields=["id", "pos_order_id", "payment_method_id", "amount", "payment_date", "create_date", "write_date"],
            order="id asc"
        )

        pay_by_order = {}
        for p in pays:
            oid = m2o_id(p.get("pos_order_id"))
            if not oid:
                continue
            mid = m2o_id(p.get("payment_method_id"))
            mname = m2o_name(p.get("payment_method_id"))
            amt = p.get("amount") or 0
            pay_by_order.setdefault(oid, []).append((mid, mname, amt))

        def payment_summary(order_id: str):
            items = pay_by_order.get(order_id, [])
            if not items:
                return None, None
            mids = sorted({x[0] for x in items if x[0]})
            one_mid = mids[0] if len(mids) == 1 else None

            # "Cash: 120 | Airtel Money: 50"
            agg = {}
            names = {}
            for mid, nm, amt in items:
                if not mid:
                    continue
                agg[mid] = agg.get(mid, 0) + float(amt or 0)
                if nm:
                    names[mid] = nm
            parts = []
            for mid in sorted(agg.keys(), key=lambda x: int(x) if x.isdigit() else x):
                label = names.get(mid) or f"PM_{mid}"
                parts.append(f"{label}: {agg[mid]}")
            return one_mid, " | ".join(parts) if parts else None

        # 7) Préparer rows orders hist
        order_rows = []
        for oid in order_ids:
            o = order_map.get(oid)
            if not o:
                continue

            sess_id = m2o_id(o.get("session_id")) or "0"
            cfg_id = m2o_id(o.get("config_id")) or "0"
            emp_id = m2o_id(o.get("employee_id"))
            partner_id = m2o_id(o.get("partner_id"))

            txn_dt = parse_odoo_dt(o.get("date_order")) or parse_odoo_dt(o.get("write_date")) or datetime.now()
            txn_day = txn_dt.date()

            pos_ref = o.get("pos_reference") or o.get("name")
            raw_json = json.dumps(o, ensure_ascii=False)
            write_dt = parse_odoo_dt(o.get("write_date"))

            order_rows.append((
                str(o["id"]),
                sess_id,
                cfg_id,
                txn_dt,
                txn_day,
                emp_id,
                partner_id,
                o.get("amount_tax"),
                o.get("amount_total"),
                o.get("amount_paid"),
                o.get("amount_return"),
                o.get("state"),
                pos_ref,
                o.get("uuid"),
                write_dt,
                raw_json,
                datetime.now()
            ))

        # 8) Préparer rows lines hist
        line_rows = []
        for l in order_lines:
            line_id = str(l["id"])
            oid = m2o_id(l.get("order_id"))
            if not oid:
                continue

            prod_id = m2o_id(l.get("product_id"))
            sm_id = None
            create_dt = parse_odoo_dt(l.get("create_date")) or datetime.now()

            pm_id, pm_name = payment_summary(oid)

            o = order_map.get(oid)
            pos_ref = (o.get("pos_reference") or o.get("name")) if o else None

            line_rows.append((
                line_id,
                oid,
                prod_id,
                sm_id,
                l.get("qty"),
                l.get("price_unit"),
                l.get("discount"),
                l.get("price_subtotal_incl"),
                pm_id,
                pm_name,
                create_dt,
                datetime.now(),
                pos_ref
            ))

            amt = p.get("amount") or 0

            # garde-fou overflow numeric(16,4)
            if abs(float(amt)) >= 1e12:
                LOG.warning("AMOUNT OVERFLOW: pay_id=%s order_id=%s method=%s amount=%s raw=%s",
                            p.get("id"),
                            m2o_id(p.get("pos_order_id")),
                            m2o_id(p.get("payment_method_id")),
                            amt,
                            p)
                continue

        # Préparer rows payment hist
        payment_rows = []
        for p in pays:
            pay_id = str(p["id"])

            order_id = m2o_id(p.get("pos_order_id"))  # renvoie str
            method_id = m2o_id(p.get("payment_method_id"))

            amt = p.get("amount") or 0
            pay_dt = pick_payment_datetime(p)
            pay_day = pay_dt.date() if pay_dt else None
            wr_dt = parse_odoo_dt(p.get("write_date")) if p.get("write_date") else None

            payment_rows.append((
                pay_id,
                order_id,
                pay_dt,
                pay_day,
                method_id,
                amt,
                wr_dt,
                json.dumps(p, ensure_ascii=False),
                datetime.now()
            ))

        # 9) Upsert vers Postgres
        with conn.cursor() as cur:
            if order_rows:
                LOG.info(f"💾 Upsert orders_hist: {len(order_rows)}")
                psycopg2.extras.execute_values(cur, UPSERT_ORDERS_HIST, order_rows, page_size=1000)

            if line_rows:
                LOG.info(f"💾 Upsert lines_hist: {len(line_rows)}")
                psycopg2.extras.execute_values(cur, UPSERT_LINES_HIST, line_rows, page_size=2000)
            
            if payment_rows:
                LOG.info(f"💾 Upsert payments_hist: {len(payment_rows)}")
                execute_values(cur, UPSERT_PAYMENTS_HIST, payment_rows, page_size=2000)

        conn.commit()
        LOG.info("✅ JOB 11 terminé avec succès")

    finally:
        conn.close()

if __name__ == "__main__":
    run()