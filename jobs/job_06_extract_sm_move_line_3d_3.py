#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
job_06_extract_sm_move_line_3d_2.py

Extract Odoo stock.move.line (last N days) and write JSONL.GZ + manifest
to be consumed by job_07_load_stg_sm_stock_move_line_1.py.

Output schema per JSON line MUST match job_07 expectations:
  odoo_id, move_id, picking_id, product_id, location_id, location_dest_id,
  qty, uom, state, date_value, write_date,
  barcode, unit_sale_price, unit_cost,
  raw (full odoo record dict + optional normalized fields)

Files written to:
  {SM_EXTRACT_OUT_DIR}/stock_move_line_<timestamp>.jsonl.gz
  {SM_EXTRACT_OUT_DIR}/stock_move_line_<timestamp>.manifest.json

Env (.env at project root):
  ODOO_URL, ODOO_DB, ODOO_USER, ODOO_SECRET (alias: ODOO_API_KEY)
Optional:
  SM_EXTRACT_DAYS=3
  SM_EXTRACT_OUT_DIR=/mnt/c/Blissydah/data/stock_movement/inbox
  SM_EXTRACT_LIMIT=2000
  SM_PRODUCT_CHUNK=500
  SM_FILTER_DONE=1 (default 1)
  LOG_LEVEL=INFO
"""

import os
import sys
import json
import gzip
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from typing import Any, Dict, List, Optional


# -------------------------------------------------------------------
# PATH FIX: allow importing scripts/ modules when running as script
# -------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

try:
    from odoo_client_odoorpc_fixed import OdooClient
    from security_env import load_project_env, get_odoo_secret
except ImportError as e:
    print(f"❌ ImportError: {e}")
    print(f"Expected odoo_client_odoorpc_fixed.py in: {SCRIPTS_DIR}")
    sys.exit(1)


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
LOG = logging.getLogger("job_06_extract_sm_move_line_3d")

load_project_env(PROJECT_ROOT)


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def build_odoo_client() -> OdooClient:
    url = env("ODOO_URL")
    parsed = urlparse(url)

    password = get_odoo_secret(required=True)

    return OdooClient(
        host=parsed.hostname,
        db=env("ODOO_DB"),
        user=env("ODOO_USER"),
        password=password,
        port=443,
        protocol="jsonrpc+ssl",
        timeout=300,
    )


def get_display_name(m2o: Any) -> Optional[str]:
    # Odoo many2one is usually [id, display_name]
    if isinstance(m2o, list) and len(m2o) >= 2:
        return str(m2o[1])
    return None


def get_id(m2o: Any) -> Optional[int]:
    if isinstance(m2o, list) and len(m2o) >= 1:
        try:
            return int(m2o[0])
        except Exception:
            return None
    if isinstance(m2o, int):
        return m2o
    return None


def write_manifest(manifest_path: str, payload: Dict[str, Any]) -> None:
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main():
    setup_logging()

    client = build_odoo_client()
    client.connect()

    days = int(os.getenv("SM_EXTRACT_DAYS", "3"))
    out_dir = os.getenv("SM_EXTRACT_OUT_DIR", os.path.join(PROJECT_ROOT, "data", "stock_movement", "inbox"))
    limit = int(os.getenv("SM_EXTRACT_LIMIT", "2000"))
    prod_chunk = int(os.getenv("SM_PRODUCT_CHUNK", "500"))
    filter_done = os.getenv("SM_FILTER_DONE", "1") == "1"

    os.makedirs(out_dir, exist_ok=True)

    t_start = datetime.utcnow() - timedelta(days=days)
    since_str = t_start.strftime("%Y-%m-%d %H:%M:%S")

    domain = [("write_date", ">", since_str)]
    if filter_done:
        domain.append(("state", "=", "done"))

    fields = [
        "id",
        "move_id",
        "picking_id",
        "product_id",
        "location_id",
        "location_dest_id",
        "qty_done",
        "product_uom_id",
        "state",
        "date",
        "write_date",
        # some Odoo setups have 'reference' on stock.move.line; if not, it returns False/None
        "reference",
    ]

    LOG.info("STOCK MOVE LINE extract | since=%s | filter_done=%s | page=%s", since_str, filter_done, limit)

    # ------------------------------------------------------------
    # 1) Extract stock.move.line with pagination
    # ------------------------------------------------------------
    all_move_lines: List[Dict[str, Any]] = []
    offset = 0

    while True:
        batch = client.execute(
            "stock.move.line",
            "search_read",
            domain,
            fields=fields,
            limit=limit,
            offset=offset,
            order="id asc",
        )
        if not batch:
            break

        all_move_lines.extend(batch)
        offset += len(batch)

        # log every few pages only (avoid redundant noise)
        if len(all_move_lines) % (limit * 3) < len(batch):
            LOG.info("... extracted rows=%s", len(all_move_lines))

    if not all_move_lines:
        LOG.info("No data to extract.")
        return

    LOG.info("Extracted total rows=%s", len(all_move_lines))

    # ------------------------------------------------------------
    # 2) Enrich products: barcode, standard_price, list_price
    # ------------------------------------------------------------
    product_ids = sorted({get_id(m.get("product_id")) for m in all_move_lines if m.get("product_id")})
    product_ids = [pid for pid in product_ids if pid is not None]

    LOG.info("Enriching products unique=%s (chunk=%s)", len(product_ids), prod_chunk)

    prod_map: Dict[int, Dict[str, Any]] = {}
    for i in range(0, len(product_ids), prod_chunk):
        subset = product_ids[i : i + prod_chunk]
        prods = client.execute("product.product", "read", subset, fields=["barcode", "standard_price", "list_price"])
        for p in prods:
            pid = p.get("id")
            if pid:
                prod_map[int(pid)] = p

        # light progress (not too verbose)
        if (i // prod_chunk) % 10 == 0:
            LOG.info("... products enriched %s/%s", min(i + prod_chunk, len(product_ids)), len(product_ids))

    # ------------------------------------------------------------
    # 3) Write JSONL.GZ + manifest (job_07 compatible)
    # ------------------------------------------------------------
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = f"stock_move_line_{ts}"
    data_path = os.path.join(out_dir, f"{base}.jsonl.gz")
    manifest_path = os.path.join(out_dir, f"{base}.manifest.json")

    created_at_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    rows_written = 0

    LOG.info("Writing data file: %s", data_path)

    with gzip.open(data_path, "wt", encoding="utf-8") as f:
        for ml in all_move_lines:
            pid = get_id(ml.get("product_id"))
            p_info = prod_map.get(pid or -1, {})

            # Movement ref: prefer picking display_name, fallback to ml.reference
            movement_ref = get_display_name(ml.get("picking_id")) or ml.get("reference")

            row = {
                # job_07 expected keys
                "odoo_id": ml.get("id"),
                "move_id": get_id(ml.get("move_id")),
                "picking_id": get_id(ml.get("picking_id")),
                "product_id": pid,
                "location_id": get_id(ml.get("location_id")),
                "location_dest_id": get_id(ml.get("location_dest_id")),
                "qty": ml.get("qty_done"),
                "uom": get_display_name(ml.get("product_uom_id")),
                "state": ml.get("state"),
                "date_value": ml.get("date"),
                "write_date": ml.get("write_date"),

                # enrichments expected by job_07
                "barcode": p_info.get("barcode"),
                "unit_sale_price": p_info.get("list_price", 0),
                "unit_cost": p_info.get("standard_price", 0),

                # payload for job_07 -> sm_payload (append-only)
                "raw": {
                    **ml,
                    # keep a normalized reference too (optional, convenient)
                    "movement_ref": movement_ref,
                },
            }

            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            rows_written += 1

    manifest = {
        "job_name": "job_06_extract_sm_move_line_3d_2",
        "model": "stock.move.line",
        "created_at_utc": created_at_utc,
        "extract_since_utc": since_str,
        "filter_done": filter_done,
        "rows": rows_written,
        "data_file": os.path.basename(data_path),
        "schema_hint": "job_07_expected",
    }
    write_manifest(manifest_path, manifest)

    LOG.info("DONE: rows_written=%s | manifest=%s", rows_written, manifest_path)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        LOG.exception("FAILED: %s", e)
        raise
