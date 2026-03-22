# Security Secret Inventory (DWH)

Last update: 2026-03-22

## Canonical environment variables

- `ODOO_SECRET` (preferred Odoo credential)
- `ODOO_API_KEY` (backward-compatible alias, auto-normalized)
- `BLISSYDAH_DB_PASSWORD` (preferred Blissydah DB password)
- `DB_PASSWORD` (backward-compatible alias, auto-normalized)

## Secret loading source of truth

- `scripts/security_env.py`

This module loads `.env` and `config/db.env`, then normalizes aliases so key rotation can happen from one variable value.

## Odoo credential usage (code references)

- `jobs/job_06_extract_sm_move_line_3d_3.py`
- `jobs/job_11_load_stg_pos_order_1.py`
- `jobs/job_09_load_fct_stock_opening_8.py`
- `scripts/odoo_client.py`
- `scripts/odoo_client_odoorpc.py`
- `jobs/run_all_jobs.py` (normalization for child jobs)

## DB password usage (code references)

- `jobs/job_11_load_stg_pos_order_1.py`
- `jobs/job_09_load_fct_stock_opening_8.py`
- `jobs/run_all_jobs.py` (normalization for child jobs)
- other jobs continue to read `DB_PASSWORD`, now fed by alias normalization.

## Git safety

- `.env` and `config/db.env` are ignored by git in `.gitignore`.
- Never commit real values in `.env.example` or `config/db.env.example`.
