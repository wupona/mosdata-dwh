import os
import xmlrpc.client
import psycopg2
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

# =========================
# LOGIQUE D'EXTRACTION ODOO
# =========================
def run_employee_etl():
    try:
        # Connexion Odoo API
        common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
        uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_API_KEY, {})
        models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")

        LOG.info(f"✅ Authenticated to Odoo (UID: {uid})")

        # Extraction des employés
        # On récupère l'ID, le nom, l'email, le poste et le département
        fields = ['id', 'name', 'work_email', 'job_id', 'department_id', 'active']
        employees_data = models.execute_kw(ODOO_DB, uid, ODOO_API_KEY, 'hr.employee', 'search_read', [[]], {'fields': fields})

        LOG.info(f"📥 Extracted {len(employees_data)} employees from Odoo")

        # Transformation simple pour gérer les champs relationnels (Many2one)
        transformed_data = []
        for emp in employees_data:
            transformed_data.append((
                emp['id'],
                emp['name'],
                emp['work_email'] or None,
                emp['job_id'][1] if emp['job_id'] else None, # [id, "Nom du poste"]
                emp['department_id'][1] if emp['department_id'] else None,
                emp['active']
            ))

        # Insertion Postgres
        insert_query = """
            INSERT INTO core.ref_oe_odoo_employees (odoo_id, name, work_email, job_title, department, active, last_update)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (odoo_id) DO UPDATE SET
                name = EXCLUDED.name,
                work_email = EXCLUDED.work_email,
                job_title = EXCLUDED.job_title,
                department = EXCLUDED.department,
                active = EXCLUDED.active,
                last_update = CURRENT_TIMESTAMP;
        """

        with get_pg_conn() as conn:
            with conn.cursor() as cur:
                extras.execute_batch(cur, insert_query, transformed_data)
            conn.commit()
            LOG.info("🚀 Data successfully synced to Postgres table 'core.ref_oe_odoo_employees'")

    except Exception as e:
        LOG.error(f"❌ ETL Error: {str(e)}")

if __name__ == "__main__":
    run_employee_etl()