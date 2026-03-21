#!/bin/bash
set -euo pipefail

PROJECT_HOME=/mnt/c/Blissydah
VENV_PATH=$PROJECT_HOME/venv
LOG_DIR=$PROJECT_HOME/logs
JOB=job_02_load_ref_p_product
LOG_FILE="$LOG_DIR/$JOB.log"

mkdir -p "$LOG_DIR"
source "$VENV_PATH/bin/activate"

{
  echo "[$(date -Is)] START $JOB"
  python -u "$PROJECT_HOME/jobs/job_02_load_ref_p_product.py"
  rc=$?
  echo "[$(date -Is)] END $JOB (exit=$rc)"
  exit $rc
} >> "$LOG_FILE" 2>&1
