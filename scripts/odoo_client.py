import os
import odoorpc
try:
    from .security_env import load_project_env, get_odoo_secret
except ImportError:
    from security_env import load_project_env, get_odoo_secret

def get_odoo():
    load_project_env()

    host = os.getenv("ODOO_HOST") or os.getenv("ODOO_URL") or "blissydah.odoo.com"
    host = host.replace("https://", "").replace("http://", "").rstrip("/")
    port = int(os.getenv("ODOO_PORT", "443"))

    db = os.getenv("ODOO_DB", "blissydah")
    user = os.getenv("ODOO_USER")
    api_key = get_odoo_secret(required=True)

    if not user:
        raise RuntimeError("ODOO_USER doit être défini dans .env")

    protocol = "jsonrpc+ssl" if port == 443 else "jsonrpc"
    odoo = odoorpc.ODOO(host=host, port=port, protocol=protocol)
    odoo.login(db, user, api_key)
    return odoo
