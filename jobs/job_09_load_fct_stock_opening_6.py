# jobs/job_09_load_fct_stock_opening_7.py
# Stock opening + aging (cheveux) — JSON-RPC paginé + config .env et config/db.env
#
# - Charge .env (Odoo) + config/db.env (DB + paramètres)
# - Utilise JSON-RPC (plus stable que XML-RPC sur gros volumes)
# - Lit stock.quant via pattern search(ids) + read(fields) paginé (évite IncompleteRead)
# - Charge product.product en batch (cost/sale) pour calculer les valeurs
# - Exclut des locations via EXCLUDED_LOCATION_IDS (config/db.env)
# - Fail-fast: si une étape critique échoue -> exit(1)

import os
import sys
import json
import time
import logging
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Iterable, Tuple, Optional, Set

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# =========================
# LOAD ENV FILES
# =========================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 1) Odoo config
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)

# 2) DB config (+ paramètres locations)
DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
load_dotenv(DB_ENV_PATH, override=True)


# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOG = logging.getLogger("JOB_09_STOCK_OPENING_JSONRPC")


# =========================
# CONFIG ODOO
# =========================
ODOO_URL = os.getenv("ODOO_URL", "https://blissydah.odoo.com").rstrip("/")
ODOO_DB = os.getenv("ODOO_DB", "blissydah")
ODOO_USER = os.getenv("ODOO_USER", "")
ODOO_API_KEY = os.getenv("ODOO_API_KEY", "")
ODOO_PW = os.getenv("ODOO_PW", "")  # fallback legacy (si tu en utilises encore)


def get_odoo_secret() -> str:
    if ODOO_API_KEY:
        return ODOO_API_KEY
    if ODOO_PW:
        return ODOO_PW
    raise RuntimeError("❌ Missing ODOO_API_KEY (recommandé) ou ODOO_PW. Vérifie .env")


# =========================
# CONFIG DB (config/db.env)
# =========================
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "blissydah")
DB_USER = os.getenv("DB_USER", "blissydah")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Locations config
ONLY_INTERNAL_LOCATIONS = os.getenv("ONLY_INTERNAL_LOCATIONS", "true").strip().lower() == "true"
EXCLUDED_LOCATION_IDS_RAW = os.getenv("EXCLUDED_LOCATION_IDS", "").strip()
EXCLUDED_LOCATION_IDS: Set[int] = set()
if EXCLUDED_LOCATION_IDS_RAW:
    EXCLUDED_LOCATION_IDS = {int(x.strip()) for x in EXCLUDED_LOCATION_IDS_RAW.split(",") if x.strip()}


def get_pg_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


# =========================
# FILTRAGE MÉTIER (Cheveux)
# =========================
KEYWORDS = ["perruque", "plante", "lace", "closure"]
EXCLUDE_PREFIXES = ["coiffure"]
EXCLUDE_STARTS_WITH = ["[vieux"]


def is_cheveu(name: Optional[str]) -> bool:
    if not name:
        return False
    n = name.lower().strip()
    if not any(k in n for k in KEYWORDS):
        return False
    if any(n.startswith(p) for p in EXCLUDE_PREFIXES):
        return False
    if any(n.startswith(p) for p in EXCLUDE_STARTS_WITH):
        return False
    return True


# =========================
# RETRY UTILS
# =========================
def retry_on_failure(max_retries: int = 3, delay: int = 2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt == max_retries - 1:
                        raise
                    LOG.warning(f"⚠️ {func.__name__} tentative {attempt + 1} échouée: {e}. Retry dans {delay}s...")
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


# =========================
# ODOO JSON-RPC CLIENT
# =========================
class OdooJsonRpc:
    def __init__(self, base_url: str, db: str, login: str, secret: str, timeout: int = 240):
        self.base_url = base_url.rstrip("/")
        self.db = db
        self.login = login
        self.secret = secret
        self.timeout = timeout
        self.session = requests.Session()
        self.uid: Optional[int] = None

    @retry_on_failure(max_retries=3, delay=2)
    def _call(self, service: str, method: str, args: list):
        payload = {
            "jsonrpc": "2.0",
            "method": "call",
            "params": {"service": service, "method": method, "args": args},
            "id": int(time.time()),
        }
        resp = self.session.post(
            f"{self.base_url}/jsonrpc",
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        out = resp.json()
        if "error" in out:
            raise RuntimeError(out["error"])
        return out["result"]

    def authenticate(self) -> int:
        uid = self._call("common", "authenticate", [self.db, self.login, self.secret, {}])
        if not uid:
            raise RuntimeError("❌ Odoo auth failed (uid falsy). Vérifie ODOO_USER / ODOO_API_KEY.")
        self.uid = uid
        return uid

    def call_kw(self, model: str, method: str, args=None, kwargs=None):
        if not self.uid:
            raise RuntimeError("Client not authenticated. Call authenticate() first.")
        args = args or []
        kwargs = kwargs or {}
        return self._call("object", "execute_kw", [self.db, self.uid, self.secret, model, method, args, kwargs])

    def search(self, model: str, domain: list, limit: int, offset: int, order: str = "id asc") -> List[int]:
        return self.call_kw(model, "search", args=[domain], kwargs={"limit": limit, "offset": offset, "order": order})

    def read(self, model: str, ids: List[int], fields: List[str]) -> List[Dict[str, Any]]:
        if not ids:
            return []
        return self.call_kw(model, "read", args=[ids], kwargs={"fields": fields})

    def iter_read(self, model: str, domain: list, fields: List[str], batch_size: int = 2000, order: str = "id asc") -> Iterable[List[Dict[str, Any]]]:
        offset = 0
        while True:
            ids = self.search(model, domain, limit=batch_size, offset=offset, order=order)
            if not ids:
                break
            yield self.read(model, ids, fields)
            offset += batch_size


# =========================
# JOB LOGIC
# =========================
@retry_on_failure(max_retries=3, delay=2)
def fetch_products_batch(odoo: OdooJsonRpc, ids_batch: List[int]) -> List[Dict[str, Any]]:
    # search_read sur un batch limité de produits -> OK
    return odoo.call_kw(
        "product.product",
        "search_read",
        args=[[("id", "in", ids_batch), ("active", "=", True)]],
        kwargs={"fields": ["id", "display_name", "sale_ok", "standard_price", "lst_price"]}
    )


def run():
    LOG.info("🚀 DÉMARRAGE job_09 (stock opening + aging) via JSON-RPC")
    t0 = time.time()

    # 1) Connexion Odoo
    try:
        secret = get_odoo_secret()
        odoo = OdooJsonRpc(ODOO_URL, ODOO_DB, ODOO_USER, secret, timeout=240)
        odoo.authenticate()
        LOG.info("✅ Connexion Odoo JSON-RPC établie.")
    except Exception as e:
        LOG.error(f"❌ Connexion Odoo échouée: {e}")
        sys.exit(1)

    # 2) Lire stock.quant paginé (ids -> read)
    try:
        LOG.info("📦 Lecture stock.quant (paginée) ...")
        domain = []
        if ONLY_INTERNAL_LOCATIONS:
            domain.append(("location_id.usage", "=", "internal"))
        domain += [("quantity", ">", 0)]

        quant_fields = ["product_id", "location_id", "quantity", "in_date"]
        batch_size = 2000

        staged_quants: List[Dict[str, Any]] = []
        product_ids: Set[int] = set()
        total = 0

        for batch in odoo.iter_read("stock.quant", domain, quant_fields, batch_size=batch_size):
            staged_quants.extend(batch)
            total += len(batch)
            for q in batch:
                p = q.get("product_id")
                if isinstance(p, list) and p:
                    product_ids.add(int(p[0]))
            if total and total % 5000 == 0:
                LOG.info(f"… quants lus: {total}")

        if not staged_quants:
            LOG.warning("⚠️ Aucun quant trouvé (stock.quant). Fin.")
            return

        LOG.info(f"📊 Quants lus: {total} | Produits uniques: {len(product_ids)}")
    except Exception as e:
        LOG.error(f"❌ Erreur lecture stock.quant: {e}")
        sys.exit(1)

    # 3) Charger produits en batch (cost/sale)
    try:
        LOG.info("📥 Chargement produits (batch) ...")
        product_cache: Dict[int, Dict[str, Any]] = {}
        ids_list = sorted(product_ids)
        prod_batch = 500

        for i in range(0, len(ids_list), prod_batch):
            chunk = ids_list[i:i + prod_batch]
            products = fetch_products_batch(odoo, chunk)
            for p in products:
                product_cache[int(p["id"])] = p

        LOG.info(f"✅ Produits chargés: {len(product_cache)}")
    except Exception as e:
        LOG.error(f"❌ Erreur chargement produits: {e}")
        sys.exit(1)

    # 4) Construire lignes à insérer
    baseline_date = datetime.now().strftime("%Y-%m-%d")
    db_rows: List[Tuple[Any, ...]] = []

    # IMPORTANT: Exclusion location IDs
    excluded_cnt = 0
    filtered_cnt = 0

    for q in staged_quants:
        p_data = q.get("product_id")
        loc_data = q.get("location_id")
        if not (isinstance(p_data, list) and p_data and isinstance(loc_data, list) and loc_data):
            continue

        product_id = int(p_data[0])
        location_id = int(loc_data[0])

        if location_id in EXCLUDED_LOCATION_IDS:
            excluded_cnt += 1
            continue

        p = product_cache.get(product_id)
        if not p:
            continue

        # filtres produit
        if not p.get("sale_ok"):
            continue
        if not is_cheveu(p.get("display_name")):
            continue

        qty = float(q.get("quantity") or 0.0)
        if qty <= 0:
            continue

        unit_cost = float(p.get("standard_price") or 0.0)
        unit_sale = float(p.get("lst_price") or 0.0)

        in_date = q.get("in_date") or baseline_date

        db_rows.append((
            baseline_date,
            product_id,
            location_id,
            qty,
            unit_cost,
            unit_sale,
            qty * unit_cost,
            qty * unit_sale,
            in_date
        ))
        filtered_cnt += 1

    LOG.info(f"🧪 Filtrage terminé: {filtered_cnt} rows retenues | {excluded_cnt} quants exclus (locations)")

    if not db_rows:
        LOG.warning("⚠️ Aucune ligne à insérer après filtrage. Vérifie KEYWORDS/EXCLUDED_LOCATION_IDS.")
        return

    # 5) Insert Postgres
    sql = """
        INSERT INTO core.fct_so_stock_opening (
            so_opening_date,
            so_product_id_odoo,
            so_location_id_odoo,
            so_opening_qty,
            so_unit_cost,
            so_unit_sale_price,
            so_opening_value_cost,
            so_opening_value_sale,
            so_in_date
        ) VALUES %s
    """

    try:
        LOG.info(f"🗄️ Insertion Postgres ({len(db_rows)} lignes) ...")
        conn = get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                # Rebuild snapshot du jour
                cur.execute("DELETE FROM core.fct_so_stock_opening WHERE so_opening_date = %s", (baseline_date,))
                execute_values(cur, sql, db_rows, page_size=5000)
        conn.close()
        LOG.info(f"✅ Insert OK: {len(db_rows)} lignes.")
    except Exception as e:
        LOG.error(f"❌ Erreur insertion DB: {e}")
        sys.exit(1)

    elapsed = time.time() - t0
    LOG.info(f"⏱️ Durée: {elapsed:.2f}s | Débit: {len(db_rows)/max(elapsed,1e-9):.1f} lignes/s")
    LOG.info("✅ JOB TERMINÉ OK.")


if __name__ == "__main__":
    run()
