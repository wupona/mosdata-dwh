#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$PROJECT_ROOT/config/db.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/config/db.env"
  set +a
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-blissydah}"
DB_USER="${DB_USER:-blissydah}"
DB_PASSWORD="${DB_PASSWORD:-}"

START_OFFSET="${1:--2}" # days from today
END_OFFSET="${2:-7}"    # days from today

if [[ -z "$DB_PASSWORD" ]]; then
  echo "[FAIL] DB_PASSWORD is empty in config/db.env"
  exit 1
fi

export PGPASSWORD="$DB_PASSWORD"

for i in $(seq "$START_OFFSET" "$END_OFFSET"); do
  d="$(date -d "$i day" +%F)"
  dn="$(date -d "$i day +1 day" +%F)"
  n="$(date -d "$i day" +%Y%m%d)"
  table_name="core.stg_po_pos_order_line_${n}"
  sql="CREATE TABLE IF NOT EXISTS ${table_name} PARTITION OF core.stg_po_pos_order_line FOR VALUES FROM ('${d}') TO ('${dn}');"
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -c "$sql" >/dev/null
  echo "[OK] partition ${table_name}"
done

unset PGPASSWORD
