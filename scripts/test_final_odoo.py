# test_final_odoo.py
import os
import sys
sys.path.append('scripts')

from dotenv import load_dotenv
load_dotenv('.env')

print("🧪 Test final Odoo Online avec clé API")
print("=" * 60)

# Affiche info (masquée)
api_key = os.getenv('ODOO_API_KEY', '')
print(f"Clé API: {'configurée' if api_key else 'absente'} ({len(api_key)} caractères)")

try:
    from odoo_client_odoorpc_fixed import OdooClient
    
    print("\n1. Initialisation du client...")
    client = OdooClient()
    
    print("\n2. Connexion à Odoo Online...")
    client.connect()
    
    print("\n3. Test count locations...")
    count = client.execute('stock.location', 'search_count', [])
    print(f"   ✅ {count} locations disponibles")
    
    print("\n4. Test récupération de 3 locations...")
    locations = client.execute(
        'stock.location',
        'search_read',
        [],
        fields=['id', 'name', 'complete_name', 'usage'],
        limit=3,
        order='id asc'
    )
    
    for i, loc in enumerate(locations, 1):
        print(f"   {i}. ID {loc['id']}: {loc.get('name')} - {loc.get('usage', 'N/A')}")
    
    print("\n5. Test pagination...")
    # Batch 1
    batch1 = client.execute(
        'stock.location',
        'search_read',
        [],
        fields=['id'],
        offset=0,
        limit=2
    )
    # Batch 2
    batch2 = client.execute(
        'stock.location',
        'search_read',
        [],
        fields=['id'],
        offset=2,
        limit=2
    )
    
    print(f"   ✅ Batch 1: IDs {[loc['id'] for loc in batch1]}")
    print(f"   ✅ Batch 2: IDs {[loc['id'] for loc in batch2]}")
    
    print("\n🎉 Tous les tests réussis! Vous pouvez maintenant exécuter votre script.")
    
except ImportError as e:
    print(f"\n❌ Erreur import: {e}")
    print(f"   Assurez-vous que scripts/odoo_client_odoorpc_fixed.py existe")
    
except Exception as e:
    print(f"\n❌ Erreur: {type(e).__name__}")
    print(f"   Message: {str(e)[:200]}")
    
    # Aide pour les erreurs courantes
    if "scope and key required" in str(e):
        print(f"\n🔧 Solution:")
        print(f"   1. Votre clé API pourrait être expirée")
        print(f"   2. Générez une nouvelle clé sur Odoo Online")
        print(f"   3. Vérifiez que vous utilisez ODOO_API_KEY dans .env")
    elif "RPC" in str(e) or "connection" in str(e).lower():
        print(f"\n🔧 Vérifiez:")
        print(f"   - Votre connexion internet")
        print(f"   - Que https://blissydah.odoo.com est accessible")
        print(f"   - Que votre clé API est valide")
