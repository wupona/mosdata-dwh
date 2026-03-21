#!/usr/bin/env bash
# orchestration/run_stock_opening.sh
set -euo pipefail

# Détermination des chemins
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_ROOT/venv"
LOG_DIR="$PROJECT_ROOT/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/stock_opening_$(date +%Y%m%d).log"

echo "------------------------------------------------------------" | tee -a "$LOG_FILE"
echo "$(date '+%Y-%m-%d %H:%M:%S') | START Stock Opening" | tee -a "$LOG_FILE"
echo "------------------------------------------------------------" | tee -a "$LOG_FILE"

# Activation environnement
cd "$PROJECT_ROOT"
source "$VENV_DIR/bin/activate"

# Lancement du job (python -m respecte la structure des packages)
if python -m jobs.job_09_load_fct_stock_opening_3 2>&1 | tee -a "$LOG_FILE"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') | ✅ Terminé avec succès" | tee -a "$LOG_FILE"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') | ❌ ÉCHEC" | tee -a "$LOG_FILE"
    exit 1
fi