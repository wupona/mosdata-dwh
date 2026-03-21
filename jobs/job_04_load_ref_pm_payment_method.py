#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
jobs/job_04_load_ref_pm_payment_method.py

Load core.ref_pm_payment_method from Odoo model pos.payment.method using existing OdooClient (OdooRPC).

Key points:
- Odoo field for journal differs by version; we detect it dynamically via fields_get.
- core.ref_pm_payment_method has UNIQUE(pm_payment_method_id_odoo) => SCD2 inserts are NOT possible.
  We use UPSERT (SCD1): INSERT ... ON CONFLICT(pm_payment_method_id_odoo) DO UPDATE ...

Env:
- DWH_DSN=postgresql://user:pass@host:port/db
- (Odoo env is handled by scripts/odoo_client_odoorpc_fixed.py via .env)
Optional:
- PM_JOURNAL_FIELD (force journal field name if you already know it, e.g. "journal_id")
"""

import os
import time
import uuid
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import psycopg2

from scripts.odoo_client_odoorpc_fixed import OdooClient


# -------------------------------------------------------------------
# ENV
# -------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(ENV_PATH)

LOG = logging.getLogger("job_load_core_ref_pm_payment_method")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def parse_odoo_dt(v: Any) -> Optional[datetime]:
    """
    Odoo often returns 'YYYY-MM-DD HH:MM:SS' (string), or False/None.
    """
    if not v:
        return None
    if isinstance(v, datetime):
        return v
    return datetime.strptime(str(v), "%Y-%m-%d %H:%M:%S")


def pg_connect():
    return psycopg2.connect(env("DWH_DSN"))


# -------------------------------------------------------------------
# ODOO HELPERS
# -------------------------------------------------------------------
def get_model_fields(client: OdooClient, model: str) -> List[str]:
    """
    Return list of field names for a model using fields_get().
    """
    # OdooRPC: model.fields_get() returns dict: {field: {...}}
    fields_def = client.execute(model, "fields_get", [])
    if isinstance(fields_def, dict):
        return list(fields_def.keys())
    return []


def detect_journal_field(model_fields: List[str]) -> Optional[str]:
    """
    Detect the journal field on pos.payment.method depending on Odoo version/modules.
    Common candidates: journal_id, cash_journal_id, account_journal_id.
    """
    # If user forces it via env, prefer that.
    forced = os.getenv("PM_JOURNAL_FIELD")
    if forced and forced in model_fields:
        return forced

    candidates = [
        "journal_id",
        "cash_journal_id",
        "account_journal_id",
        "payment_journal_id",
    ]
    for c in candidates:
        if c in model_fields:
            return c
    return None


def extract_payment_methods(client: OdooClient) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Extract pos.payment.method records and also return the journal field used (or None).
    """
    model = "pos.payment.method"
    fields = get_model_fields(client, model)
    journal_field = detect_journal_field(fields)

    read_fields = ["id", "name", "active", "create_date", "write_date"]
    if journal_field:
        read_fields.append(journal_field)

    LOG.info("pos.payment.method journal field detected: %s", journal_field or "NONE")

    recs = client.execute(model, "search_read", [], fields=read_fields)
    return (recs or []), journal_field


def extract_journals_by_ids(client: OdooClient, journal_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Read account.journal basic info for journal_ids.
    Return dict[journal_id] -> {name, type}
    """
    if not journal_ids:
        return {}

    model = "account.journal"
    # Some DBs restrict access; handle gracefully
    try:
        recs = client.execute(
            model,
            "search_read",
            [["id", "in", journal_ids]],
            fields=["id", "name", "type"],
        ) or []
        out = {}
        for r in recs:
            out[int(r["id"])] = {
                "name": r.get("name"),
                "type": r.get("type"),
            }
        return out
    except Exception as e:
        LOG.warning("Could not read account.journal (permissions or module). Continuing without journal type. Error: %s", e)
        return {}


def parse_m2o(value: Any) -> Tuple[Optional[int], Optional[str]]:
    """
    Odoo M2O often returns [id, display_name] or False/None.
    """
    if not value:
        return None, None
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return int(value[0]), str(value[1])
    # Sometimes returns just an int
    if isinstance(value, int):
        return int(value), None
    return None, None


# -------------------------------------------------------------------
# LOAD (UPSERT)
# -------------------------------------------------------------------
def upsert_payment_method(
    cur,
    odoo_id: int,
    name: str,
    journal_id: Optional[int],
    journal_name: Optional[str],
    journal_type: Optional[str],
    is_current: bool,
    created_at: datetime,
    updated_at: Optional[datetime],
) -> None:
    """
    UPSERT into core.ref_pm_payment_method using UNIQUE(pm_payment_method_id_odoo).
    """
    cur.execute(
        """
        INSERT INTO core.ref_pm_payment_method (
            pm_payment_method_id,
            pm_payment_method_id_odoo,
            pm_payment_method_name,
            pm_journal_id_odoo,
            pm_journal_name,
            pm_journal_type,
            pm_is_current,
            pm_created_at,
            pm_updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (pm_payment_method_id_odoo)
        DO UPDATE SET
            pm_payment_method_name = EXCLUDED.pm_payment_method_name,
            pm_journal_id_odoo     = EXCLUDED.pm_journal_id_odoo,
            pm_journal_name        = EXCLUDED.pm_journal_name,
            pm_journal_type        = EXCLUDED.pm_journal_type,
            pm_is_current          = EXCLUDED.pm_is_current,
            pm_updated_at          = EXCLUDED.pm_updated_at
        """,
        (
            str(uuid.uuid4()),  # only used on INSERT
            odoo_id,
            name,
            journal_id,
            journal_name,
            journal_type,
            is_current,
            created_at,
            updated_at,
        ),
    )


def set_not_current_missing(cur, present_odoo_ids: List[int]) -> int:
    """
    Mark methods not present in the latest extract as not current.
    Useful if methods are deleted/archived in Odoo or filtered out.
    """
    if not present_odoo_ids:
        cur.execute("UPDATE core.ref_pm_payment_method SET pm_is_current = FALSE, pm_updated_at = CURRENT_TIMESTAMP WHERE pm_is_current = TRUE;")
        return cur.rowcount

    cur.execute(
        """
        UPDATE core.ref_pm_payment_method
        SET pm_is_current = FALSE,
            pm_updated_at = CURRENT_TIMESTAMP
        WHERE pm_is_current = TRUE
          AND pm_payment_method_id_odoo <> ALL(%s)
        """,
        (present_odoo_ids,),
    )
    return cur.rowcount


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    setup_logging()
    start = time.time()

    LOG.info("Loaded .env from: %s", ENV_PATH)
    LOG.info("DWH_DSN present: %s", "YES" if os.getenv("DWH_DSN") else "NO")

    # 1) Connect Odoo
    odoo = OdooClient()
    odoo.connect()

    # 2) Extract payment methods + detect journal field
    methods, journal_field = extract_payment_methods(odoo)
    LOG.info("Fetched %s pos.payment.method records.", len(methods))

    # 3) Collect journal ids if we have journal field
    journal_ids: List[int] = []
    if journal_field:
        for r in methods:
            jid, _ = parse_m2o(r.get(journal_field))
            if jid:
                journal_ids.append(jid)
        journal_ids = sorted(list(set(journal_ids)))

    journals_info = extract_journals_by_ids(odoo, journal_ids)

    # 4) Load into DWH (UPSERT)
    present_odoo_ids: List[int] = []
    upserted = 0

    with pg_connect() as conn:
        with conn.cursor() as cur:
            for r in methods:
                odoo_id = int(r["id"])
                present_odoo_ids.append(odoo_id)

                name = (r.get("name") or "").strip()
                active = bool(r.get("active")) if r.get("active") is not None else True

                # is_current: we can decide to align with active flag
                is_current = True if active else False

                created_at = parse_odoo_dt(r.get("create_date")) or datetime.utcnow()
                updated_at = parse_odoo_dt(r.get("write_date"))

                journal_id = None
                journal_name = None
                journal_type = None

                if journal_field:
                    journal_id, journal_name = parse_m2o(r.get(journal_field))
                    if journal_id and journal_id in journals_info:
                        # prefer authoritative journal name/type if available
                        journal_name = journals_info[journal_id].get("name") or journal_name
                        journal_type = journals_info[journal_id].get("type")

                upsert_payment_method(
                    cur=cur,
                    odoo_id=odoo_id,
                    name=name,
                    journal_id=journal_id,
                    journal_name=journal_name,
                    journal_type=journal_type,
                    is_current=is_current,
                    created_at=created_at,
                    updated_at=updated_at,
                )
                upserted += 1

            # Mark missing ones as not current
            missing_marked = set_not_current_missing(cur, present_odoo_ids)

        conn.commit()

    dur = round(time.time() - start, 2)
    LOG.info("DONE in %ss | upserted=%s | missing_marked_not_current=%s", dur, upserted, missing_marked)


if __name__ == "__main__":
    main()
