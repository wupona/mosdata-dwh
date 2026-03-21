import subprocess
import sys
import os
import time
from datetime import datetime

def run_jobs():
    # Détermination de la racine du projet (un niveau au-dessus de jobs/)
    # Si le script est dans /mnt/c/Blissydah/jobs/run_all_jobs.py
    # Alors PROJECT_ROOT sera /mnt/c/Blissydah
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(CURRENT_DIR) == "jobs":
        PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
        JOBS_DIR = CURRENT_DIR
    else:
        PROJECT_ROOT = CURRENT_DIR
        JOBS_DIR = os.path.join(PROJECT_ROOT, "jobs")
    
    # Injection du PROJECT_ROOT dans l'environnement Python pour les imports
    env_vars = os.environ.copy()
    env_vars["PYTHONPATH"] = PROJECT_ROOT + os.pathsep + env_vars.get("PYTHONPATH", "")

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    jobs = [
        "job_02_upsert_ref_p_product_filtered_api_2.py",
        "job_06_extract_sm_move_line_3d_3.py",
        "job_07_load_stg_sm_stock_move_line_3.py",
        "job_11_load_stg_pos_order_1.py",
        "job_12_load_pos_facts.py",
        "job_08_etl_fct_sm_stock_movement_4.py",
        "job_09_load_fct_stock_opening_6.py",
        "job_10_consolidate_opening_movement.py",
        "job_13_load_fct_odoo_expense.py"
    ]
    
    print("\n" + "="*75)
    print(f"⚙️  BLISSYDAH ETL PIPELINE - START AT {now}")
    print(f"📍 Root Project : {PROJECT_ROOT}")
    print(f"📂 Jobs Folder  : {JOBS_DIR}")
    print("="*75)

    start_total = time.time()

    for job_name in jobs:
        job_path = os.path.join(JOBS_DIR, job_name)
        
        if not os.path.exists(job_path):
            print(f"⚠️  MANQUANT : {job_name}")
            continue

        print(f"\n🚀 Lancement de {job_name}...")
        try:
            # On passe env=env_vars pour que le job puisse faire "from scripts.xxx import..."
            result = subprocess.run(
                ["python3", job_path], 
                check=True, 
                cwd=PROJECT_ROOT, # On se place à la racine pour l'exécution
                env=env_vars
            )
            if result.returncode == 0:
                print(f"✅ SUCCÈS")
        except subprocess.CalledProcessError:
            print(f"💥 ERREUR FATALE sur {job_name}. Arrêt du pipeline.")
            sys.exit(1)

    duration = round(time.time() - start_total, 2)
    print("\n" + "="*75)
    print(f"✨ PIPELINE ETL TERMINE AVEC SUCCÈS EN {duration}s")
    print("="*75 + "\n")

if __name__ == "__main__":
    run_jobs()
