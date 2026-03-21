# scripts/odoo_client_odoorpc_fixed.py
import os
import odoorpc
from dotenv import load_dotenv

# Charge le .env
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

class OdooClient:
    def __init__(self):
        # Lecture des variables avec fallback
        self.url = os.getenv("ODOO_URL", "").rstrip("/")
        self.db = os.getenv("ODOO_DB", "")
        self.username = os.getenv("ODOO_USER", "")
        
        # Essaye ODOO_API_KEY d'abord, puis ODOO_PASSWORD
        self.api_key = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD", "")
        
        print(f"🔧 Configuration Odoo Online:")
        print(f"   URL: {self.url}")
        print(f"   DB: {self.db}")
        print(f"   User: {self.username}")
        print(f"   API Key: {'*' * len(self.api_key) if self.api_key else 'NON DÉFINIE'}")
        
        if not self.api_key:
            raise ValueError("API Key non définie. Utilisez ODOO_API_KEY ou ODOO_PASSWORD dans .env")
        
        self.odoo = None
        
    def connect(self):
        """Connexion à Odoo Online avec API Key"""
        print(f"\n🔗 Connexion à {self.url}...")
        
        try:
            # Extraction du hostname
            if self.url.startswith("https://"):
                host = self.url.replace("https://", "")
            elif self.url.startswith("http://"):
                host = self.url.replace("http://", "")
            else:
                host = self.url
            
            # Connexion avec odoorpc
            self.odoo = odoorpc.ODOO(
                host=host,
                protocol='jsonrpc+ssl',
                port=443,
                timeout=30
            )
            
            # Login avec l'API Key
            self.odoo.login(self.db, self.username, self.api_key)
            
            print(f"✅ Connecté avec succès à Odoo Online!")
            print(f"   UID: {self.odoo.env.uid}")
            print(f"   Version: {self.odoo.version}")
            
            # Test rapide
            try:
                count = self.odoo.env['stock.location'].search_count([])
                print(f"   Test: {count} locations disponibles")
            except:
                print("   Test: Connexion OK (test count non disponible)")
            
        except odoorpc.error.RPCError as e:
            error_msg = str(e)
            print(f"\n❌ Erreur d'authentification RPC:")
            print(f"   Message: {error_msg}")
            
            if "scope and key required" in error_msg:
                print(f"\n🔧 Solution probable:")
                print(f"   1. Votre clé API ({self.api_key[:10]}...) est peut-être expirée")
                print(f"   2. Génerez une NOUVELLE clé API dans Odoo Online:")
                print(f"      - Connectez-vous à https://blissydah.odoo.com")
                print(f"      - Avatar → Préférences → Compte API")
                print(f"      - 'Créer une clé API'")
                print(f"      - Copiez la NOUVELLE clé dans .env")
                print(f"   3. Assurez-vous d'utiliser la clé API, pas le mot de passe")
            
            raise ConnectionError(f"Erreur RPC: {e}")
            
        except Exception as e:
            print(f"❌ Erreur générale: {type(e).__name__}: {e}")
            raise
        
    def execute(self, model, method, *args, **kwargs):
        """Exécute une méthode Odoo"""
        if not self.odoo:
            raise ConnectionError("Client non connecté. Appelez connect() d'abord.")
        
        try:
            model_obj = self.odoo.env[model]
            
            if method == "search_read":
                domain = args[0] if args else []
                return model_obj.search_read(domain, **kwargs)
                
            elif method == "search_count":
                domain = args[0] if args else []
                return model_obj.search_count(domain)
                
            else:
                func = getattr(model_obj, method)
                return func(*args, **kwargs)
                
        except Exception as e:
            raise RuntimeError(f"Erreur {model}.{method}: {e}")
