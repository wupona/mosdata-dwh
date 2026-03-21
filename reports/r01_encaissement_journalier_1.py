import pandas as pd
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import shutil

# --- CONFIGURATION DES CHEMINS ---
DB_URL = "postgresql://blissydah:TON_MOT_DE_PASSE@localhost:5432/blissydah"
SQL_FILE_PATH = "reports/queries/q01_daily_revenue.sql"
TEMPLATE_PATH = "reports/templates/Template_Rapport_Encaissement.xlsx"
OUTPUT_DIR = "reports/outputs"
LOOKBACK_DAYS = 3 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_daily_reporting():
    engine = create_engine(DB_URL)
    today = datetime.now()

    # 1. Charger la requête SQL depuis le fichier fourni
    try:
        with open(SQL_FILE_PATH, 'r') as f:
            query = f.read()
    except FileNotFoundError:
        logging.error(f"❌ Fichier SQL introuvable : {SQL_FILE_PATH}")
        return

    for i in range(LOOKBACK_DAYS, 0, -1):
        target_date = today - timedelta(days=i)
        target_date_str = target_date.strftime('%Y-%m-%d')
        sheet_name = target_date.strftime('%d')
        file_name = f"Rapport_Encaissement_{target_date.strftime('%Y_%m')}.xlsx"
        file_path = os.path.join(OUTPUT_DIR, file_name)

        logging.info(f"--- Analyse du jour : {target_date_str} ---")

        try:
            # 2. Exécution de la requête avec les paramètres attendus par ton SQL
            df = pd.read_sql_query(query, engine, params={"target_date": target_date_str})

            if df.empty:
                logging.warning(f"⚠️ Aucune donnée pour le {target_date_str}")
                continue

            # 3. Gestion du fichier Excel à partir du Template
            if not os.path.exists(file_path):
                # Si le fichier du mois n'existe pas, on copie le template
                shutil.copy(TEMPLATE_PATH, file_path)
                logging.info(f"🆕 Nouveau rapport mensuel créé à partir du template.")

            # 4. Écriture dans la feuille correspondante
            with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            logging.info(f"✅ Succès : Feuille {sheet_name} mise à jour.")

        except Exception as e:
            logging.error(f"❌ Erreur sur le jour {target_date_str} : {e}")

if __name__ == "__main__":
    run_daily_reporting()
    print("✅ Processus terminé.")