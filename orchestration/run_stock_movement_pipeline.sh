#!/usr/bin/env bash
set -euo pipefail

# -------------------------------------------------------------------
# Blissydah - Stock Movement mini-pipeline
# Runs:
#  1) job_06_extract_sm_move_line_3d_2
#  2) job_07_load_stg_sm_stock_move_line_2
#  3) job_08_etl_fct_sm_stock_movement_3
# With sleep between steps and centralized logs.
# -------------------------------------------------------------------

PROJECT_DIR="/mnt/c/Blissydah"
VENV_DIR="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs"
SLEEP_SECS=10

mkdir -p "$LOG_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/stock_movement_pipeline_$TS.log"

cd "$PROJECT_DIR"

# Load venv
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') | $*" | tee -a "$LOG_FILE"
}

run_step() {
  local step_name="$1"
  local module_name="$2"

  log "============================================================"
  log "START: $step_name  (python -m $module_name)"
  log "============================================================"

  # Run and append all output to the same log file
  if python -m "$module_name" 2>&1 | tee -a "$LOG_FILE" ; then
    log "DONE : $step_name"
  else
    log "FAIL : $step_name (see logs above)"
    exit 1
  fi
}

log "Pipeline started. Log file: $LOG_FILE"
log "Project: $PROJECT_DIR"
log "Sleep between steps: ${SLEEP_SECS}s"

run_step "JOB 06 - Extract stock.move.line (3d)" "jobs.job_06_extract_sm_move_line_3d_2"
log "Sleeping ${SLEEP_SECS}s..."
sleep "$SLEEP_SECS"

run_step "JOB 07 - Load STG stock move lines" "jobs.job_07_load_stg_sm_stock_move_line_2"
log "Sleeping ${SLEEP_SECS}s..."
sleep "$SLEEP_SECS"

run_step "JOB 08 - ETL FACT stock movements" "jobs.job_08_etl_fct_sm_stock_movement_3"

log "✅ Pipeline finished successfully."
