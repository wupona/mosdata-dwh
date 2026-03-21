#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VENV_PATH="$PROJECT_ROOT/.venv/bin/activate"
if [[ ! -f "$VENV_PATH" ]]; then
  VENV_PATH="$PROJECT_ROOT/venv/bin/activate"
fi
if [[ ! -f "$VENV_PATH" ]]; then
  echo "[FAIL] Python virtualenv not found (.venv or venv)"
  exit 1
fi

if [[ -d "/var/log/mosdata" && -w "/var/log/mosdata" ]]; then
  LOG_DIR="/var/log/mosdata"
else
  LOG_DIR="$PROJECT_ROOT/logs"
fi
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/dwh_pipeline_$(date +%Y%m%d_%H%M%S).log"

echo "[INFO] Project root: $PROJECT_ROOT" | tee -a "$LOG_FILE"
echo "[INFO] Log file: $LOG_FILE" | tee -a "$LOG_FILE"

# shellcheck disable=SC1090
source "$VENV_PATH"
cd "$PROJECT_ROOT"

echo "[INFO] Step 1/3 - Ensure partitions" | tee -a "$LOG_FILE"
"$PROJECT_ROOT/orchestration/ensure_pos_partitions.sh" -2 7 | tee -a "$LOG_FILE"

echo "[INFO] Step 2/3 - Preflight checks" | tee -a "$LOG_FILE"
python "$PROJECT_ROOT/scripts/preflight_prod.py" --days-ahead 5 | tee -a "$LOG_FILE"

echo "[INFO] Step 3/3 - Run ETL pipeline" | tee -a "$LOG_FILE"
python -m jobs.run_all_jobs | tee -a "$LOG_FILE"

echo "[OK] Production pipeline finished" | tee -a "$LOG_FILE"
