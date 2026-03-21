import os
import odoorpc
from dotenv import load_dotenv

def get_odoo():
    load_dotenv()

    host = os.getenv("ODOO_HOST") or os.getenv("ODOO_URL") or "blissydah.odoo.com"
    host = host.replace("https://", "").replace("http://", "").rstrip("/")
    port = int(os.getenv("ODOO_PORT", "443"))

    db = os.getenv("ODOO_DB", "blissydah")
    user = os.getenv("ODOO_USER")
    api_key = os.getenv("ODOO_API_KEY") or os.getenv("ODOO_PASSWORD")

    if not user or not api_key:
        raise RuntimeError("ODOO_USER et ODOO_API_KEY (ou ODOO_PASSWORD) doivent être définis dans .env")

    protocol = "jsonrpc+ssl" if port == 443 else "jsonrpc"
    odoo = odoorpc.ODOO(host=host, port=port, protocol=protocol)
    odoo.login(db, user, api_key)
    return odoo
