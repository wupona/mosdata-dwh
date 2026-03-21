import odoorpc
import os
from dotenv import load_dotenv

from scripts.odoo_client import get_odoo
odoo = get_odoo()

print("1. Démarrage du script...")
load_dotenv()

api_key = os.getenv("ODOO_API_KEY")
print(f"2. Clé API détectée : {len(api_key) if api_key else 0} caractères")

try:
    odoo = odoorpc.ODOO('votre_host', port=443)
    print("3. Instance Odoo initialisée")
    # ... le reste du code ...
except Exception as e:
    print(f"❌ Erreur détectée : {e}")
