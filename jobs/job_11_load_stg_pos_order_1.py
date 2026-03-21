import os
import sys
import logging
import psycopg2
import time
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# --- CONFIGURATION DES CHEMINS ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

try:
    from odoo_client_odoorpc_fixed import OdooClient
except ImportError:
    print(f"❌ Erreur: odoo_client_odoorpc_fixed.py introuvable dans {SCRIPTS_DIR}")
    sys.exit(1)

# Configuration du Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOG = logging.getLogger("JOB_11_POS_ORDER")

def run():
    LOG.info("--- DÉMARRAGE DU JOB 11 : STAGING POS + RÉFÉRENCES MÉTIER ---")
    start_time = time.time()
    # 1. CONNEXION ODOO

    odoo_host = os.getenv("ODOO_HOST", "blissydah.odoo.com")
    odoo_db = os.getenv("ODOO_DB", "blissydah")
    odoo_user = os.getenv("ODOO_USER")
    odoo_password = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD")
    if not odoo_user or not odoo_password:
        LOG.error("❌ Missing ODOO_USER and/or ODOO_API_KEY (or ODOO_PASSWORD)")
        return

    client = OdooClient(
        host=odoo_host,
        db=odoo_db,
        user=odoo_user,
        password=odoo_password,
    )
    
    try:
        client.connect()
        LOG.info("✅ Connexion Odoo établie.")
    except Exception as e:
        LOG.error(f"❌ Erreur connexion Odoo : {e}")
        return

    # 2. EXTRACTION DES DONNÉES
    target_date = (datetime.now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
    LOG.info(f"📥 ÉTAPE 1 : Extraction des lignes POS depuis {target_date}...")
    
    try:
        # Récupération des lignes
        order_lines = client.execute('pos.order.line', 'search_read',
            [('create_date', '>=', target_date)],
            ['id', 'order_id', 'product_id', 'qty', 'price_unit', 'discount', 'price_subtotal_incl', 'create_date']
        )

        if not order_lines:
            LOG.info("ℹ️ Aucune nouvelle ligne trouvée.")
            return

        # Récupération des références métier (Lushi/POS/XXXX)
        order_ids = list(set([l['order_id'][0] for l in order_lines]))
        orders = client.execute('pos.order', 'search_read',
            [('id', 'in', order_ids)],
            ['id', 'name']
        )
        ref_map = {o['id']: o['name'] for o in orders}

        # Récupération des paiements multiples
        payments = client.execute('pos.payment', 'search_read',
            [('pos_order_id', 'in', order_ids)],
            ['pos_order_id', 'payment_method_id', 'amount']
        )
        
        pay_agg = {}
        for p in payments:
            oid = p['pos_order_id'][0]
            detail = f"{p['payment_method_id'][1]}: {p['amount']}"
            pay_agg[oid] = f"{pay_agg.get(oid, '')} | {detail}".strip(" | ")

        # 3. PRÉPARATION DES LIGNES (db_rows)
        db_rows = []
        for l in order_lines:
            oid = l['order_id'][0]
            db_rows.append((
                l['id'],                          # po_order_line_id_odoo
                oid,                              # po_order_id_odoo
                l['product_id'][0],               # po_product_id_odoo
                l['qty'],                         # po_qty
                l['price_unit'],                  # po_price_unit
                l['discount'],                    # po_discount_percent
                l['price_subtotal_incl'],         # po_price_subtotal_incl
                pay_agg.get(oid, "NON PAYÉ"),     # po_payment_method_name
                l['create_date'],                 # po_create_date
                ref_map.get(oid, "INCONNU")       # po_pos_reference (Lushi/POS/...)
            ))

        # 4. INSERTION POSTGRES AVEC UPSERT
        LOG.info(f"🗄️ ÉTAPE 2 : Insertion Upsert ({len(db_rows)} lignes)...")
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        with conn.cursor() as cur:
            sql = """
                INSERT INTO core.stg_po_pos_order_line (
                    po_order_line_id_odoo, po_order_id_odoo, po_product_id_odoo,
                    po_qty, po_price_unit, po_discount_percent, po_price_subtotal_incl,
                    po_payment_method_name, po_create_date, po_pos_reference
                ) VALUES %s
                ON CONFLICT (po_order_line_id_odoo, po_create_date) 
                DO UPDATE SET 
                    po_qty = EXCLUDED.po_qty,
                    po_price_unit = EXCLUDED.po_price_unit,
                    po_payment_method_name = EXCLUDED.po_payment_method_name,
                    po_pos_reference = EXCLUDED.po_pos_reference,
                    po_ingestion_timestamp = NOW();
            """
            execute_values(cur, sql, db_rows)
            conn.commit()
            LOG.info("✅ Succès : Staging mis à jour avec références métier.")
        conn.close()

    except Exception as e:
        LOG.error(f"❌ Erreur critique : {e}")

    LOG.info(f"⏱️ Fin du job en {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    run()
