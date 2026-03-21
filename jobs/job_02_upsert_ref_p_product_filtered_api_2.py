# jobs/job_02_upsert_ref_p_product_filtered_api_1.py
import os
import sys
import uuid
import re
import time
import csv
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from collections import Counter, defaultdict
from scripts.odoo_client import get_odoo

odoo = get_odoo()

# -----------------------------
# Paths / imports
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from odoo_client_odoorpc_fixed import OdooClient  # noqa: E402

from watermark import (  # noqa: E402
    ensure_job,
    get_state,
    mark_running,
    mark_success,
    mark_fail,
    format_odoo_dt,
    compute_new_watermark_ts_id_from_rows,
)

# --- Audit export paths
AUDIT_DIR = os.path.join(PROJECT_ROOT, "reports", "outputs")
os.makedirs(AUDIT_DIR, exist_ok=True)
run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")

# -----------------------------
# Configuration & Identity
# -----------------------------
JOB_NAME = "job_02_ref_p_product"
SOURCE_NAME = "odoo_online"
ENTITY_NAME = "product.product"
RUN_LABEL = "job_02_upsert_ref_p_product_filtered_api_1"

CONFIG_DB_ENV_PATH = os.path.join(PROJECT_ROOT, "config", "db.env")
ROOT_DOTENV_PATH = os.path.join(PROJECT_ROOT, ".env")

# -----------------------------
# Helpers
# -----------------------------
class StepTimer:
    def stamp(self) -> float:
        return time.time()
    def fmt(self, seconds: float) -> str:
        return f"{seconds:,.2f}s"
    def log_step(self, title: str, start: float, end: float):
        print(f"   ⏱️  {title}: {self.fmt(end - start)}")

_space_re = re.compile(r"\s+")
_bracket_re = re.compile(r"\[[^\]]+\]\s*")

def m2o_id(v) -> Optional[int]:
    return int(v[0]) if isinstance(v, (list, tuple)) and v else None

def parse_odoo_dt(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try: return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except: return None

def has_code_prefix_before_dash(name_raw: str) -> bool:
    if not name_raw or "-" not in name_raw: return False
    return len(name_raw.split("-", 1)[0].strip()) > 0

def normalize_product_name_after_code(name_raw: str) -> str:
    if not name_raw: return ""
    s = _bracket_re.sub("", name_raw.strip()).strip()
    if "-" in s:
        parts = s.split("-", 1)
        if len(parts) > 1: s = parts[1].strip()
    return _space_re.sub(" ", s.upper()).strip()

def canon_uom_to_units(uom_name: Optional[str]) -> Optional[str]:
    if not uom_name: return "Units"
    s = uom_name.strip().lower()
    if s in ("unit", "units", "unité", "unites", "pièce", "pieces", "pcs", "pc"):
        return "Units"
    return uom_name.strip()

def build_category_maps(prod_cats: List[Dict[str, Any]]):
    name_by_id = {int(c["id"]): (c.get("name") or "").strip() for c in prod_cats}
    parent_by_id = {int(c["id"]): m2o_id(c.get("parent_id")) for c in prod_cats}
    def full_path(cat_id: int) -> str:
        chain, cur, seen = [], cat_id, set()
        while cur and cur not in seen:
            seen.add(cur)
            chain.append(name_by_id.get(cur, "").strip())
            cur = parent_by_id.get(cur)
        return " / ".join(reversed([x for x in chain if x]))
    return {cid: full_path(cid) for cid in name_by_id.keys()}

def derive_pos_category(cat_full_path: str) -> Optional[str]:
    if not cat_full_path: return None
    s = cat_full_path.upper()
    for keyword in ["PERRUQUE", "LACE", "CLOSURE", "PLANTE"]:
        if keyword in s: return f"VENTE / {keyword}"
    return None

# -----------------------------
# Extraction Logic
# -----------------------------
def odoo_extract_incremental(odoo, model, base_domain, fields, since_ts, since_id, max_rows, page_size, pause_s):
    out = []
    cur_ts_str = format_odoo_dt(since_ts)
    cur_id = int(since_id)
    
    while True:
        domain = ["&"] + base_domain + [
            "|", ("write_date", ">", cur_ts_str),
            "&", ("write_date", "=", cur_ts_str), ("id", ">", cur_id)
        ]
        limit = min(page_size, max_rows - len(out)) if max_rows else page_size
        batch = odoo.execute(model, "search_read", domain, fields=fields, limit=limit, order="write_date asc, id asc", _max_retries=6)
        
        if not batch: break
        out.extend(batch)
        last = batch[-1]
        cur_ts_str = last.get("write_date") or cur_ts_str
        cur_id = int(last.get("id") or cur_id)
        
        if pause_s: time.sleep(pause_s)
        if len(batch) < limit or (max_rows and len(out) >= max_rows): break
        
    return out, parse_odoo_dt(cur_ts_str), cur_id

# -----------------------------
# Main Execution
# -----------------------------
def main():
    timer = StepTimer()
    load_dotenv(ROOT_DOTENV_PATH)
    if os.path.exists(CONFIG_DB_ENV_PATH): load_dotenv(CONFIG_DB_ENV_PATH, override=True)

    job_mode = os.getenv("JOB_MODE", "incremental").lower()
    max_rows = int(os.getenv("MAX_ROWS_PER_RUN", "200000"))
    
    conn = psycopg2.connect(host=os.getenv("DB_HOST"), dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), port=int(os.getenv("DB_PORT", 5432)))
    
    try:
        ensure_job(conn, job_name=JOB_NAME, source_name=SOURCE_NAME, entity_name=ENTITY_NAME)
        wm0 = get_state(conn, JOB_NAME)
        started_at_db = mark_running(conn, JOB_NAME)

        raw = os.getenv("ODOO_URL") or ""
        raw = raw.strip()
        if raw and "://" not in raw:
            raw = "https://" + raw
        parsed = urlparse(raw)
        if not parsed.hostname:
            raise ValueError(f"ODOO_URL invalide: {os.getenv('ODOO_URL')}")

        odoo = OdooClient(host=parsed.hostname, db=os.getenv("ODOO_DB"), user=os.getenv("ODOO_USER"), password=os.getenv("ODOO_API_KEY"), port=443, protocol="jsonrpc+ssl", timeout=300)
        odoo.connect()

        # 1. Categories
        cats = odoo.execute("product.category", "search_read", [], fields=["id", "name", "parent_id"])
        cat_path_map = build_category_maps(cats)
        
        # 2. Products
        print(f"   🚀 Extraction des produits (Mode: {job_mode})...")
        s = timer.stamp()
        since_ts = datetime(2000,1,1) if job_mode == "full_catchup" else wm0.watermark_ts
        products, new_ts, new_id = odoo_extract_incremental(odoo, "product.product", [("active", "=", True), ("barcode", "!=", False)],
            ["id", "name", "barcode", "active", "product_tmpl_id", "write_date", "create_date"],
            since_ts, wm0.watermark_id, max_rows, 5000, 0.05)
        timer.log_step("Extract products", s, timer.stamp())

        if not products:
            mark_success(conn, JOB_NAME, started_at=started_at_db, rows=0, new_watermark_ts=wm0.watermark_ts, new_watermark_id=wm0.watermark_id)
            return

        # 3. BULK Extraction: Templates (Solution anti-déconnexion)
        print(f"   🚀 Chargement massif des templates...")
        s = timer.stamp()
        tmpl_categ, tmpl_uom, offset = {}, {}, 0
        while True:
            t_batch = odoo.execute("product.template", "search_read", [("active","=",True)], fields=["id", "categ_id", "uom_id"], limit=15000, offset=offset)
            if not t_batch: break
            for t in t_batch:
                tid = int(t["id"])
                tmpl_categ[tid], tmpl_uom[tid] = m2o_id(t.get("categ_id")), m2o_id(t.get("uom_id"))
            offset += len(t_batch)
            print(f"      … templates: {len(tmpl_categ)}", end="\r")
        print()
        timer.log_step("Bulk Extract templates", s, timer.stamp())

        # 4. Global Extraction: UoM
        uoms = odoo.execute("uom.uom", "search_read", [], fields=["id", "name"])
        uom_map = {int(u["id"]): u["name"] for u in uoms}

        # 5. Normalization
        print("   🚀 Normalisation...")
        stg = []
        for p in products:
            name_raw = p.get("name") or ""
            if not has_code_prefix_before_dash(name_raw): continue
            tid = m2o_id(p.get("product_tmpl_id"))
            pos_cat = derive_pos_category(cat_path_map.get(tmpl_categ.get(tid), ""))
            if not pos_cat: continue
            
            uom_name = uom_map.get(tmpl_uom.get(tid), "Units")
            stg.append((str(uuid.uuid4()), int(p["id"]), pos_cat, normalize_product_name_after_code(name_raw),
                cat_path_map.get(tmpl_categ.get(tid), ""), bool(p.get("active")), canon_uom_to_units(uom_name),
                parse_odoo_dt(p.get("create_date")) or datetime.now(), parse_odoo_dt(p.get("write_date")) or datetime.now(),
                True, p.get("barcode"), name_raw))

        # 6. Upsert DB
        sql = """INSERT INTO core.ref_p_product (p_product_id, p_product_id_odoo, p_pos_category_norm, p_product_name_norm,
                p_pos_category_raw, p_active, p_uom, p_created_at, p_updated_at, p_is_current, p_barcode, p_product_name_raw) 
                VALUES %s ON CONFLICT (p_product_id_odoo) DO UPDATE SET 
                p_pos_category_norm = EXCLUDED.p_pos_category_norm, p_product_name_norm = EXCLUDED.p_product_name_norm,
                p_active = EXCLUDED.p_active, p_updated_at = EXCLUDED.p_updated_at, p_barcode = COALESCE(EXCLUDED.p_barcode, core.ref_p_product.p_barcode)"""
        

        # index dans stg:
        # (uuid, p_product_id_odoo, ..., barcode, name_raw)
        KEY_IDX = 1

        IDX_ID = 1
        IDX_UPDATED = 8
        IDX_BARCODE = 10
        IDX_NAME_RAW = 11
        IDX_UUID = 0

        best = {}
        for r in stg:
            pid = r[IDX_ID]
            # garder le "dernier" vu (ou le plus récent si vous préférez comparer updated_at)
            best[pid] = r

        audit_file = os.path.join(AUDIT_DIR, f"audit_job02_products_touched_{run_tag}.csv")
        with open(audit_file, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["p_product_id_odoo", "updated_at", "barcode", "name_raw"])
            for pid, r in sorted(best.items(), key=lambda x: (x[1][IDX_UPDATED], x[0])):
                w.writerow([pid, r[IDX_UPDATED], r[IDX_BARCODE], r[IDX_NAME_RAW]])

        print(f"📄 Audit CSV généré: {audit_file} | produits uniques: {len(best):,}")

        # 2) Export des DOUBLONS (pour expliquer l’échec technique)
        counts = Counter(r[IDX_ID] for r in stg)
        dup_ids = [k for k, v in counts.items() if v > 1]

        if dup_ids:
            dup_file = os.path.join(AUDIT_DIR, f"audit_job02_duplicates_{run_tag}.csv")
            with open(dup_file, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["p_product_id_odoo", "occurrences", "updated_at", "barcode", "name_raw"])
                for pid in sorted(dup_ids):
                    # on écrit toutes les occurrences pour transparence
                    rows = [r for r in stg if r[IDX_ID] == pid]
                    for r in rows:
                        w.writerow([pid, counts[pid], r[IDX_UPDATED], r[IDX_BARCODE], r[IDX_NAME_RAW]])

            print(f"📄 Doublons CSV généré: {dup_file} | ids dupliqués: {len(dup_ids):,}")

        # --- 5bis. Audit doublons + déduplication (ne pas casser le job)

        # Indices selon stg.append(...) :contentReference[oaicite:2]{index=2}
        IDX_UUID = 0
        IDX_ID = 1          # p_product_id_odoo
        IDX_UPDATED = 8     # p_updated_at
        IDX_BARCODE = 10
        IDX_NAME_RAW = 11

        counts = Counter(r[IDX_ID] for r in stg)
        dup_ids = [pid for pid, n in counts.items() if n > 1]

        if dup_ids:
            # 1) Générer un CSV détaillé pour audit
            dup_file = os.path.join(AUDIT_DIR, f"audit_job02_duplicates_{run_tag}.csv")
            with open(dup_file, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["p_product_id_odoo", "occurrences", "p_updated_at", "barcode", "name_raw"])
                for pid in sorted(dup_ids):
                    rows = [r for r in stg if r[IDX_ID] == pid]
                    for r in rows:
                        w.writerow([pid, counts[pid], r[IDX_UPDATED], r[IDX_BARCODE], r[IDX_NAME_RAW]])

            print(f"📄 Audit doublons généré: {dup_file} | ids dupliqués: {len(dup_ids):,}")

            # 2) Déduplication : garder la ligne la plus récente (p_updated_at)
            #    si égalité parfaite, garder la dernière rencontrée.
            best = {}
            for r in stg:
                pid = r[IDX_ID]
                if pid not in best:
                    best[pid] = r
                    continue
                cur = best[pid]
                # Comparaison sur updated_at (None-safe)
                r_dt = r[IDX_UPDATED]
                cur_dt = cur[IDX_UPDATED]
                if (cur_dt is None and r_dt is not None) or (r_dt is not None and cur_dt is not None and r_dt > cur_dt):
                    best[pid] = r
                elif r_dt == cur_dt:
                    # même timestamp: garder la dernière occurrence (utile si d'autres champs diffèrent)
                    best[pid] = r

            before = len(stg)
            stg = list(best.values())
            print(f"✅ Déduplication appliquée: {before:,} -> {len(stg):,} lignes (keep=dernier p_updated_at)")

        with conn.cursor() as cur:
            execute_values(cur, sql, stg, page_size=2000)
        
        mark_success(conn, JOB_NAME, started_at=started_at_db, ended_at=datetime.now(), rows=len(stg), 
                     new_watermark_ts=new_ts or wm0.watermark_ts, new_watermark_id=new_id or wm0.watermark_id)
        print(f"✅ SUCCÈS: {len(stg)} produits synchronisés.")

    except Exception as e:
        mark_fail(conn, JOB_NAME, started_at=started_at_db, ended_at=datetime.now(), rows=0, error=str(e))
        raise
    finally:
        conn.close()

if __name__ == "__main__": main()
