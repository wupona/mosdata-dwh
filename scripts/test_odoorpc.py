#!/usr/bin/env python3
"""
Test de connexion Odoo Online avec OdooRPC
"""
import os
import sys

# Ajoute scripts au path
sys.path.append('scripts')

from dotenv import load_dotenv
load_dotenv('.env')

print("🧪 Test OdooRPC avec Odoo Online")
print("=" * 50)

# Affiche la configuration (masque le mot de passe)
print("\n🔧 Configuration chargée:")
print(f"   ODOO_URL: {os.getenv('ODOO_URL')}")
print(f"   ODOO_DB: {os.getenv('ODOO_DB')}")
print(f"   ODOO_USER: {os.getenv('ODOO_USER')}")
print(f"   ODOO_SECRET: {'configured' if os.getenv('ODOO_SECRET') or os.getenv('ODOO_API_KEY') else 'absent'}")

try:
    from odoo_client_odoorpc import OdooClientODOO
    
    # 1. Test connexion
    print("\n1. Test connexion...")
    odoo = OdooClientODOO()
    odoo.connect()
    
    # 2. Test count
    print("\n2. Test count locations...")
    count = odoo.execute("stock.location", "search_count", [])
    print(f"   ✅ Locations totales: {count}")
    
    # 3. Test search_read petit lot
    print("\n3. Test search_read (5 premiers)...")
    locations = odoo.execute(
        "stock.location", 
        "search_read",
        [],  # domain vide = toutes
        fields=["id", "name", "complete_name", "usage"],
        limit=5,
        order="id asc"
    )
    print(f"   ✅ {len(locations)} locations récupérées")
    for i, loc in enumerate(locations, 1):
        print(f"     {i}. ID {loc['id']}: {loc.get('name')} ({loc.get('usage')})")
    
    # 4. Test avec pagination
    print("\n4. Test pagination...")
    batch1 = odoo.execute(
        "stock.location",
        "search_read",
        [],
        fields=["id"],
        limit=3,
        offset=0
    )
    batch2 = odoo.execute(
        "stock.location",
        "search_read",
        [],
        fields=["id"],
        limit=3,
        offset=3
    )
    print(f"   ✅ Batch 1 IDs: {[loc['id'] for loc in batch1]}")
    print(f"   ✅ Batch 2 IDs: {[loc['id'] for loc in batch2]}")
    
    print("\n🎉 Tous les tests réussis!")
    
except ImportError as e:
    print(f"\n❌ Erreur import: {e}")
    print(f"   Path Python: {sys.path}")
    print(f"   Fichier existe-t-il? scripts/odoo_client_odoorpc.py")
    
except Exception as e:
    print(f"\n❌ Erreur: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
