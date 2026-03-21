import os
import uuid
import xmlrpc.client
import logging
from datetime import datetime

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


def m2o_id(v):
    # many2one => [id, name]
    if isinstance(v, list) and len(v) >= 1:
        return int(v[0])
    if isinstance(v, int):
        return v
    return None


def main():

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

    # --- Odoo connect (XML-RPC)
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
    if not uid:
        raise RuntimeError("Odoo authentication failed")

    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

    # --- Extract pos.config (id, name, active, company_id)
    fields = ["id", "name", "active", "company_id", "write_date"]
    configs = models.execute_kw(
        ODOO_DB, uid, ODOO_API_KEY,
        "pos.config", "search_read",
        [[]],
        {"fields": fields, "order": "id asc"}
    )

    print(f"[INFO] pos.config fetched: {len(configs)}")

    now = datetime.now()

    # --- Upsert Postgres
    # Variante A : table avec company_id
    upsert_sql = """
    INSERT INTO core.ref_pc_pos_config (
      pc_id, pc_id_odoo, pc_name, pc_company_id_odoo, pc_is_active,
      pc_is_current, pc_created_at, pc_updated_at
    )
    VALUES %s
    ON CONFLICT (pc_id_odoo) DO UPDATE SET
      pc_name = EXCLUDED.pc_name,
      pc_company_id_odoo = EXCLUDED.pc_company_id_odoo,
      pc_is_active = EXCLUDED.pc_is_active,
      pc_is_current = true,
      pc_updated_at = EXCLUDED.pc_updated_at;
    """

    rows = []
    for c in configs:
        pc_id_odoo = int(c["id"])
        pc_name = c.get("name") or ""
        pc_is_active = bool(c.get("active", True))
        pc_company_id_odoo = m2o_id(c.get("company_id")) or 0  # si tu veux NOT NULL strict

        rows.append((
            str(uuid.uuid4()),
            pc_id_odoo,
            pc_name,
            pc_company_id_odoo,
            pc_is_active,
            True,
            now,
            now
        ))

    with get_pg_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, upsert_sql, rows, page_size=500)
        conn.commit()

    print("[OK] core.ref_pc_pos_config loaded/updated")


if __name__ == "__main__":
    main()
