import subprocess
import sys
import os
from datetime import datetime

def run_all():
    # Dossier où se trouve ce script Master (normalement /.../Blissydah/reports)
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # FIX: on s'arrête ici, jamais de reports/reports
    REPORTS_DIR = BASE_DIR

    # Date cible
    target_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')

    scripts = [
        "r01_encaissement_journalier.py",
        "r02_stock_opening.py",
        "r03_revenu_detaille.py",
        "r04_stock_exceptions.py",
        "r05_situation_stock.py"
    ]

    print("\n" + "="*60)
    print(f"🚀 BLISSYDAH DATA PIPELINE - SESSION DU {target_date}")
    print(f"📍 Dossier source : {REPORTS_DIR}")
    print("="*60)

    for script_name in scripts:
        script_path = os.path.join(REPORTS_DIR, script_name)

        if not os.path.exists(script_path):
            print(f"❌ SAUTÉ : {script_name} (Introuvable dans {REPORTS_DIR})")
            continue

        print(f"\n▶️ Exécution de {script_name}...")
        try:
            subprocess.run(["python3", script_path, target_date], check=True, cwd=REPORTS_DIR)
            print("✅ Succès")
        except subprocess.CalledProcessError:
            print(f"⚠️ ÉCHEC sur {script_name}")
            continue

    print("\n" + "="*60)
    print("✨ PIPELINE TERMINE")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_all()