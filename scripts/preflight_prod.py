#!/usr/bin/env python3
"""Production preflight checks for MOS Data DWH."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

import psycopg2
from dotenv import load_dotenv


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def fail(msg: str) -> None:
    print(f"[FAIL] {msg}")


def ok(msg: str) -> None:
    print(f"[OK]   {msg}")


def check_env() -> list[str]:
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
    load_dotenv(os.path.join(PROJECT_ROOT, "config", "db.env"), override=True)

    required = [
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "ODOO_URL",
        "ODOO_DB",
        "ODOO_USER",
        "ODOO_API_KEY",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        fail(f"Missing env vars: {', '.join(missing)}")
    else:
        ok("Required env vars are present")
    return missing


def connect_db():
    dsn = os.getenv("DWH_DSN")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def add_months(d: date, months: int) -> date:
    month0 = (d.year * 12 + (d.month - 1)) + months
    year = month0 // 12
    month = (month0 % 12) + 1
    return date(year, month, 1)


def check_db_auth(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("select current_user, current_database()")
        current_user, current_db = cur.fetchone()
    ok(f"DB connection ok as user={current_user} db={current_db}")
    return True


def check_privileges(conn) -> bool:
    checks = [
        ("core.stg_po_pos_order_line", "INSERT"),
        ("core.fct_ps_pos_session", "INSERT"),
        ("core.fct_po_pos_orders", "INSERT"),
        ("core.fct_pl_pos_order_line", "INSERT"),
        ("core.fct_pp_pos_payment", "INSERT"),
    ]
    all_ok = True
    with conn.cursor() as cur:
        for table_name, privilege in checks:
            cur.execute(
                "select has_table_privilege(current_user, %s, %s)",
                (table_name, privilege),
            )
            allowed = bool(cur.fetchone()[0])
            if allowed:
                ok(f"{privilege} privilege on {table_name}")
            else:
                fail(f"Missing {privilege} privilege on {table_name}")
                all_ok = False
    return all_ok


def check_pos_partitions(conn, days_ahead: int) -> bool:
    all_ok = True
    parent = "core.stg_po_pos_order_line"
    with conn.cursor() as cur:
        cur.execute("select to_regclass(%s) is not null", (parent,))
        exists = bool(cur.fetchone()[0])
        if not exists:
            fail(f"Parent table missing: {parent}")
            return False

    for i in range(0, days_ahead + 1):
        day = date.today() + timedelta(days=i)
        suffix = day.strftime("%Y%m%d")
        child = f"core.stg_po_pos_order_line_{suffix}"
        with conn.cursor() as cur:
            cur.execute("select to_regclass(%s) is not null", (child,))
            exists = bool(cur.fetchone()[0])
        if exists:
            ok(f"Partition exists for {day.isoformat()} ({child})")
        else:
            fail(f"Missing partition for {day.isoformat()} ({child})")
            all_ok = False
    return all_ok


def check_stg_sm_partitions(conn, start_offset_days: int, days_ahead: int) -> bool:
    all_ok = True
    parent = "core.stg_sm_stock_move_line"
    with conn.cursor() as cur:
        cur.execute("select to_regclass(%s) is not null", (parent,))
        exists = bool(cur.fetchone()[0])
        if not exists:
            fail(f"Parent table missing: {parent}")
            return False

    for i in range(start_offset_days, days_ahead + 1):
        day = date.today() + timedelta(days=i)
        suffix = day.strftime("%Y%m%d")
        child = f"core.stg_sm_stock_move_line_{suffix}"
        with conn.cursor() as cur:
            cur.execute("select to_regclass(%s) is not null", (child,))
            exists = bool(cur.fetchone()[0])
        if exists:
            ok(f"Partition exists for {day.isoformat()} ({child})")
        else:
            fail(f"Missing partition for {day.isoformat()} ({child})")
            all_ok = False
    return all_ok


def check_fct_sm_partitions(conn, months_ahead: int) -> bool:
    all_ok = True
    parent = "core.fct_sm_stock_movement"
    with conn.cursor() as cur:
        cur.execute("select to_regclass(%s) is not null", (parent,))
        exists = bool(cur.fetchone()[0])
        if not exists:
            fail(f"Parent table missing: {parent}")
            return False

    this_month = date.today().replace(day=1)
    for i in range(0, months_ahead + 1):
        month_date = add_months(this_month, i)
        suffix = month_date.strftime("%Y%m")
        child = f"core.fct_sm_stock_movement_{suffix}"
        with conn.cursor() as cur:
            cur.execute("select to_regclass(%s) is not null", (child,))
            exists = bool(cur.fetchone()[0])
        if exists:
            ok(f"Partition exists for {month_date.strftime('%Y-%m')} ({child})")
        else:
            fail(f"Missing partition for {month_date.strftime('%Y-%m')} ({child})")
            all_ok = False
    return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Run DWH production preflight checks")
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=None,
        help="Legacy option: if set, reused for both POS and SM daily horizons",
    )
    parser.add_argument("--days-ahead-pos", type=int, default=5, help="Required POS partition horizon")
    parser.add_argument("--days-ahead-sm", type=int, default=7, help="Required stock STG partition horizon")
    parser.add_argument(
        "--start-offset-sm",
        type=int,
        default=1,
        help="Stock STG partition start offset (days from today); default=1 for future-only checks",
    )
    parser.add_argument("--months-ahead-fct", type=int, default=3, help="Required stock FACT monthly horizon")
    args = parser.parse_args()

    days_ahead_pos = args.days_ahead_pos if args.days_ahead is None else args.days_ahead
    days_ahead_sm = args.days_ahead_sm if args.days_ahead is None else args.days_ahead

    missing = check_env()
    if missing:
        return 1

    try:
        conn = connect_db()
    except Exception as exc:
        fail(f"DB connection failed: {exc}")
        return 1

    try:
        checks = [
            check_db_auth(conn),
            check_privileges(conn),
            check_pos_partitions(conn, days_ahead_pos),
            check_stg_sm_partitions(conn, args.start_offset_sm, days_ahead_sm),
            check_fct_sm_partitions(conn, args.months_ahead_fct),
        ]
    finally:
        conn.close()

    if all(checks):
        ok("Preflight PASSED")
        return 0
    fail("Preflight FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
