import os
import xmlrpc.client
import psycopg2
import json
from psycopg2 import extras
import logging
from dotenv import load_dotenv


# =========================
# CONFIGURATION ET LOGGING
# =========================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("job_pos_etl")

ODOO_URL = os.getenv("ODOO_URL", "https://blissydah.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "norbert.wupona@gmail.com")
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

def run_odoo_expense_json_etl():
    try:
        # --- ÉTAPE A : Récupérer la date du dernier enregistrement dans Postgres ---
        last_sync_date = "2000-01-01 00:00:00" # Date par défaut pour le premier run
        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(oe_last_update) FROM core.fct_oe_odoo_expenses")
                res = cur.fetchone()
                if res and res[0]:
                    # On retire quelques minutes par sécurité pour ne rien rater
                    last_sync_date = (res[0]).strftime('%Y-%m-%d %H:%M:%S')

        LOG.info(f"🔍 Searching for changes since: {last_sync_date}")

        # --- ÉTAPE B : Connexion Odoo ---
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

        # --- ÉTAPE C : Extraction Filtrée (write_date > last_sync_date) ---
        fields = ['id', 'date', 'employee_id', 'name', 'total_amount', 'state', 'analytic_distribution', 'write_date']
        
        # Le filtre 'write_date' est la clé de l'incrémental
        domain = [('write_date', '>', last_sync_date)]
        
        expenses = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'hr.expense', 'search_read', [domain], {'fields': fields})
        
        if not expenses:
            LOG.info("✨ No new or updated expenses found.")
            return

        LOG.info(f"📥 {len(expenses)} new/updated records found.")

        # --- ÉTAPE D : Transformation et Insertion (identique à votre code actuel) ---
        transformed_data = []
        for exp in expenses:
            oe_date = exp.get('date') if exp.get('date') else None
            emp_name = exp['employee_id'][1] if exp['employee_id'] else None
            
            transformed_data.append((
                exp['id'],
                oe_date,
                emp_name,
                exp.get('total_amount') or 0.0,
                exp.get('state'),
                json.dumps(exp)
            ))

        upsert_query = """
            INSERT INTO core.fct_oe_odoo_expenses (
                oe_expense_id_odoo, oe_expense_date, oe_employee_name, 
                oe_total_amount, oe_status, oe_raw_json, oe_last_update
            ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (oe_expense_id_odoo) DO UPDATE SET
                oe_status = EXCLUDED.oe_status,
                oe_raw_json = EXCLUDED.oe_raw_json,
                oe_last_update = CURRENT_TIMESTAMP;
        """

        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                extras.execute_batch(cur, upsert_query, transformed_data)
            conn.commit()
            LOG.info(f"🚀 Successfully updated {len(transformed_data)} records.")

    except Exception as e:
        LOG.error(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    run_odoo_expense_json_etl()