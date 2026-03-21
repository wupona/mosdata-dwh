# odoo_api_client_short_ver.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

class OdooClient:
    """
    Client Odoo Online via /jsonrpc (service common + object),
    basé sur la logique que tu as déjà validée.
    """
    def __init__(self):
        self.url = os.getenv("ODOO_URL").rstrip("/")
        self.db = os.getenv("ODOO_DB")
        self.user = os.getenv("ODOO_USER")
        self.key = os.getenv("ODOO_API_KEY")
        self.uid = None

        if not all([self.url, self.db, self.user, self.key]):
            raise RuntimeError("Variables .env manquantes: ODOO_URL, ODOO_DB, ODOO_USER, ODOO_API_KEY")

    def _post(self, payload: dict, timeout: int = 60) -> dict:
        r = requests.post(f"{self.url}/jsonrpc", json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        return data

    def connect(self) -> int:
        """
        Authentification: retourne UID.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {"service": "common", "method": "login", "args": [self.db, self.user, self.key]},
            "id": 1
        }
        data = self._post(payload)
        self.uid = data["result"]
        return self.uid

    def execute(self, model: str, method: str, *args, rpc_id: int = 2):
        """
        Exécute une méthode sur un modèle Odoo:
        execute(db, uid, key, model, method, *args)
        """
        if not self.uid:
            raise RuntimeError("Not connected. Call connect() first.")

        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {
                "service": "object",
                "method": "execute",
                "args": [self.db, self.uid, self.key, model, method, *args]
            },
            "id": rpc_id
        }
        data = self._post(payload)
        return data["result"]


# Optionnel: test rapide uniquement si on exécute ce fichier directement
if __name__ == "__main__":
    odoo = OdooClient()
    uid = odoo.connect()
    print("✅ Connected UID:", uid)

    me = odoo.execute("res.users", "read", [uid], ["name", "login", "email"], rpc_id=3)
    print("✅ User:", me)
