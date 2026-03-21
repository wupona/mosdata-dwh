# MOS Data DWH Deployment

## 1) Server bootstrap

```bash
sudo mkdir -p /opt/mosdata-dwh
sudo chown -R $USER:$USER /opt/mosdata-dwh
cd /opt/mosdata-dwh
git clone <your-repo-url> .
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Environment setup

```bash
cp .env.example .env
cp config/db.env.example config/db.env
```

Fill real credentials in `.env` and `config/db.env`.

## 3) Manual smoke tests

```bash
source .venv/bin/activate
python -m jobs.job_00_ref_odoo_employees
python -m jobs.job_02_upsert_ref_p_product_filtered_api_2
python -m reports.run_all_reports
```

## 4) Cron examples

```bash
# Product reference refresh
0 2 * * * cd /opt/mosdata-dwh && /opt/mosdata-dwh/orchestration/run_job.sh jobs.job_02_upsert_ref_p_product_filtered_api_2 >> /var/log/mosdata/job02.log 2>&1

# Daily partition maintenance for POS staging
15 2 * * * cd /opt/mosdata-dwh && /opt/mosdata-dwh/orchestration/ensure_pos_partitions.sh -2 7 >> /var/log/mosdata/ensure_pos_partitions.log 2>&1

# Full production-safe ETL pipeline (includes preflight checks)
30 2 * * * cd /opt/mosdata-dwh && /opt/mosdata-dwh/orchestration/run_pipeline_prod.sh >> /var/log/mosdata/pipeline_prod.log 2>&1

# Daily report dispatch (optional)
0 6 * * * cd /opt/mosdata-dwh && /opt/mosdata-dwh/orchestration/send_daily_opening_summary_report.sh >> /var/log/mosdata/reports.log 2>&1
```

## 5) Production guardrails

Before manual execution, run:

```bash
cd /opt/mosdata-dwh
source .venv/bin/activate
python scripts/preflight_prod.py --days-ahead 5
```

Expected result: `Preflight PASSED`.
