#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <python.module.path>"
  echo "Example: $0 jobs.job_02_upsert_ref_p_product_filtered_api_2"
  exit 1
fi

MODULE_PATH="$1"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "${PROJECT_DIR}"

if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
elif [[ -f "venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "venv/bin/activate"
fi

python -m "${MODULE_PATH}"
