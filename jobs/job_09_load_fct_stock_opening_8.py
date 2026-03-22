# jobs/job_09_load_fct_stock_opening_5.py
import os
import sys
import logging
import psycopg2
import time
from datetime import datetime
from psycopg2.extras import execute_values
from functools import wraps

# --- CONFIGURATION DES CHEMINS ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

try:
    from odoo_client_odoorpc_fixed import OdooClient
    from security_env import load_project_env, get_odoo_secret, get_db_password
except ImportError:
    print(f"❌ Erreur: odoo_client_odoorpc_fixed.py introuvable dans {SCRIPTS_DIR}")
    sys.exit(1)

# Configuration du Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOG = logging.getLogger("JOB_STOCK_CHEVEUX_AGING")

load_project_env(PROJECT_ROOT)

# --- PARAMÈTRES DE CONNEXION ---
ODOO_HOST = os.getenv("ODOO_HOST", "blissydah.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "")
ODOO_PW = get_odoo_secret(required=False)

PG_HOST = os.getenv("DB_HOST", "localhost")
PG_PORT = int(os.getenv("DB_PORT", "5432"))
PG_NAME = os.getenv("DB_NAME", "blissydah")
PG_USER = os.getenv("DB_USER", "blissydah")
PG_PASS = get_db_password(required=False)

# --- LOGIQUE DE FILTRAGE MÉTIER ---
KEYWORDS = ["perruque", "plante", "lace", "closure"]
EXCLUDE_PREFIXES = ["coiffure"]
EXCLUDE_STARTS_WITH = ["[vieux"]

def is_cheveu(name):
    if not name: return False
    n = name.lower()
    if not any(k in n for k in KEYWORDS): return False
    if any(n.startswith(p) for p in EXCLUDE_PREFIXES): return False
    if any(n.startswith(p) for p in EXCLUDE_STARTS_WITH): return False
    return True

def retry_on_failure(max_retries=3, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        LOG.error(f"Échec après {max_retries} tentatives: {e}")
                        raise
                    LOG.warning(f"Tentative {attempt + 1} échouée. Nouvelle tentative dans {delay}s...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

@retry_on_failure(max_retries=3, delay=2)
def fetch_products_batch(client, batch_ids):
    # On garde votre optimisation de batch
    return client.execute('product.product', 'search_read',
        [('id', 'in', batch_ids), ('active', '=', True)],
        ['id', 'display_name', 'sale_ok', 'standard_price', 'lst_price']
    )

def run():
    LOG.info("--- DÉMARRAGE DU JOB STOCK + AGING OPTIMISÉ ---")
    start_time = time.time()

    # 1. CONNEXION ODOO
    client = OdooClient(host=ODOO_HOST, db=ODOO_DB, user=ODOO_USER, password=ODOO_PW)
    try:
        client.connect()
        LOG.info("✅ Connexion Odoo établie.")
    except Exception as e:
        LOG.error(f"❌ Erreur connexion Odoo : {e}")
        return

    # 2. LECTURE DU STOCK (Avec récupération de in_date)
    LOG.info("📦 ÉTAPE 1 : Lecture du stock physique (quants + in_date)...")
    try:
        quants = client.execute('stock.quant', 'search_read',
            [('location_id.usage', '=', 'internal'), ('quantity', '>', 0)],
            ['product_id', 'location_id', 'quantity', 'in_date'] # 🚀 AJOUT in_date ici
        )
    except Exception as e:
        LOG.error(f"❌ Erreur lors de la récupération des quants: {e}")
        return

    if not quants:
        LOG.info("❌ Aucun stock trouvé.")
        return

    LOG.info(f"📊 {len(quants)} lignes de stock trouvées.")

    # 3. EXTRACTION IDs UNIQUES
    product_ids = {q.get('product_id')[0] for q in quants if q.get('product_id')}

    # 4. CHARGEMENT BATCH DES PRODUITS
    LOG.info(f"📥 ÉTAPE 2 : Chargement de {len(product_ids)} produits en batch...")
    product_cache = {}
    p_ids_list = list(product_ids)
    batch_size = 500

    for i in range(0, len(p_ids_list), batch_size):
        batch_ids = p_ids_list[i:i + batch_size]
        try:
            products = fetch_products_batch(client, batch_ids)
            for p in products:
                product_cache[p['id']] = p
        except Exception as e:
            LOG.error(f"❌ Erreur batch {i}: {e}")
            continue

    # 5. FILTRAGE ET PRÉPARATION
    LOG.info("🧪 ÉTAPE 3 : Filtrage et calcul de l'âge...")
    baseline_date = datetime.now().strftime('%Y-%m-%d')
    db_rows = []
    
    for q in quants:
        p_data = q.get('product_id')
        if not p_data or p_data[0] not in product_cache: continue

        p = product_cache[p_data[0]]
        if not is_cheveu(p['display_name']) or not p.get('sale_ok'): continue

        qty = q.get('quantity', 0.0)
        cost = p.get('standard_price', 0.0)
        sale = p.get('lst_price', 0.0)
        in_date = q.get('in_date') or baseline_date # 🚀 Récupération de l'âge

        db_rows.append((
            baseline_date, p['id'], q.get('location_id')[0],
            qty, cost, sale, qty * cost, qty * sale,
            in_date # 🚀 Nouvelle colonne
        ))

    # 6. INSERTION POSTGRES
    if db_rows:
        LOG.info(f"🗄️ ÉTAPE 4 : Insertion dans Postgres...")
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_NAME, user=PG_USER, password=PG_PASS)
            with conn.cursor() as cur:
                cur.execute("DELETE FROM core.fct_so_stock_opening WHERE so_opening_date = %s", (baseline_date,))
                sql = """
                    INSERT INTO core.fct_so_stock_opening (
                        so_opening_date, so_product_id_odoo, so_location_id_odoo,
                        so_opening_qty, so_unit_cost, so_unit_sale_price,
                        so_opening_value_cost, so_opening_value_sale,
                        so_in_date
                    ) VALUES %s
                """
                execute_values(cur, sql, db_rows)
            conn.commit()
            conn.close()
            LOG.info(f"🚀 Succès : {len(db_rows)} lignes insérées.")
        except Exception as e:
            LOG.error(f"❌ Erreur Database : {e}")

    # 7. PERFORMANCE
    elapsed = time.time() - start_time
    LOG.info(f"⏱️ Temps total : {elapsed:.2f}s ({len(db_rows)/elapsed:.1f} lignes/s)")

if __name__ == "__main__":
    run()
