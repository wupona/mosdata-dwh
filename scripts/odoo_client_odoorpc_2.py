import odoorpc
import os
from dotenv import load_dotenv

print("1. Démarrage du script...")
load_dotenv()

# Récupération des variables d'environnement
host = os.getenv("ODOO_HOST", "blissydah.odoo.com")
port = int(os.getenv("ODOO_PORT", "443"))
database = os.getenv("ODOO_DB", "blissydah")
username = os.getenv("ODOO_USER", "norbert.wupona@gmail.com")
api_key = os.getenv("ODOO_API_KEY")

print(f"2. Configuration détectée:")
print(f"   - Host: {host}")
print(f"   - Port: {port}")
print(f"   - DB: {database}")
print(f"   - User: {username}")
print(f"   - API Key: {'✓' if api_key else '✗'} ({len(api_key) if api_key else 0} caractères)")

try:
    # Connexion à Odoo
    print(f"3. Tentative de connexion à {host}:{port}...")
    
    odoo = odoorpc.ODOO(
        host=host,
        port=port,
        protocol='jsonrpc+ssl' if port == 443 else 'jsonrpc'
    )
    
    print("4. Client OdooRPC initialisé")
    
    # Méthode 1: Essayons d'abord avec la clé API
    if api_key:
        print("5. Tentative de connexion avec clé API...")
        try:
            odoo.login(database, username, api_key)
            print("✅ Connexion réussie avec clé API!")
        except Exception as api_error:
            print(f"⚠️  Échec avec clé API: {api_error}")
            raise
    
    # Vérification de la connexion
    print("6. Vérification de la connexion...")
    
    # Test 1: Vérifier l'utilisateur courant
    user = odoo.env.user
    print(f"   - Utilisateur connecté: {user.name} ({user.login})")
    print(f"   - ID utilisateur: {user.id}")
    print(f"   - Langue: {user.lang}")
    
    # Test 2: Compter les modèles
    model_count = odoo.env['ir.model'].search_count([])
    print(f"   - Nombre de modèles disponibles: {model_count}")
    
    # Test 3: Lister quelques modèles
    print("   - 5 modèles disponibles:")
    models = odoo.env['ir.model'].search([], limit=5)
    for model_id in models:
        model = odoo.env['ir.model'].browse(model_id)
        print(f"     • {model.model} : {model.name}")
    
    # Test 4: Vérifier les paramètres de l'entreprise
    try:
        company = odoo.env.user.company_id
        print(f"   - Entreprise: {company.name}")
        print(f"   - Devise: {company.currency_id.name}")
    except:
        print("   - Impossible de récupérer les infos entreprise")
    
    print("\n✅ Connexion et tests réussis!")
    
except odoorpc.error.RPCError as rpc_error:
    print(f"❌ Erreur RPC: {rpc_error}")
    print(f"   Code: {rpc_error.args[0] if rpc_error.args else 'Inconnu'}")
    print(f"   Message: {rpc_error.args[1] if len(rpc_error.args) > 1 else 'Inconnu'}")
    
except Exception as e:
    print(f"❌ Erreur générale: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()
