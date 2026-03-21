#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
import logging
from dotenv import load_dotenv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOG = logging.getLogger("job_10_consolidation")

def main():
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=int(os.getenv("DB_PORT", "5432"))
        )
        cur = conn.cursor()

        LOG.info("🚀 Consolidation vers core.fct_som_stock_opening_movement...")

        # 1. Vidage de la table cible
        cur.execute("TRUNCATE TABLE core.fct_som_stock_opening_movement;")

        # 2. Insertion depuis fct_so_stock_opening
        # On utilise le code produit (barcode) en le récupérant depuis le référentiel produit
        LOG.info("📦 Extraction de l'Opening (Fact so)...")
        cur.execute("""
            INSERT INTO core.fct_som_stock_opening_movement (
                som_date_key, som_movement_day, som_location_id_odoo, som_barcode, 
                som_qty, som_signed_qty, som_unit_cost, som_unit_sale_price,
                som_movement_type, som_source_type
            )
            SELECT 
                replace(so.so_opening_date::text, '-', '')::int,
                so.so_opening_date,
                so.so_location_id_odoo,
                p.p_barcode,
                so.so_opening_qty,
                so.so_opening_qty,
                so.so_unit_cost,
                so.so_unit_sale_price,
                'OPENING',
                'INITIAL'
            FROM core.fct_so_stock_opening so
            LEFT JOIN core.ref_p_product p ON so.so_product_id_odoo = p.p_product_id_odoo;
        """)

        # 3. Insertion depuis fct_sm_stock_movement (Flux Odoo)
        LOG.info("🔄 Ajout des mouvements flux (Fact sm)...")
        cur.execute("""
            INSERT INTO core.fct_som_stock_opening_movement (
                som_date_key, 
                som_movement_day, 
                som_location_id_odoo, 
                som_barcode, 
                som_qty, 
                som_signed_qty, 
                som_unit_cost, 
                som_unit_sale_price, 
                som_movement_type, 
                som_source_type, 
                som_odoo_move_line_id
            )
            SELECT 
                sm_date_key, 
                sm_movement_day, 
                sm_location_id_odoo, 
                sm_barcode, 
                sm_signed_qty, 
                sm_signed_qty, 
                COALESCE(sm_unit_cost, 0),       -- Sécurité si nul
                COALESCE(sm_unit_sale_price, 0),  -- Sécurité si nul
                COALESCE(sm_movement_type, 'INCONNU'), 
                'ODOO_MOVE', 
                sm_odoo_move_line_id
            FROM core.fct_sm_stock_movement
            WHERE sm_barcode IS NOT NULL -- On ne prend que ce qui est mappable
                AND sm_is_internal_location = TRUE  -- FILTRE CRUCIAL
                AND sm_location_usage = 'internal'  -- Ne garder que le stock physique       
        """)

        conn.commit()
        LOG.info("✅ Table consolidée mise à jour avec succès.")

    except Exception as e:
        LOG.error(f"❌ Erreur consolidation : {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()