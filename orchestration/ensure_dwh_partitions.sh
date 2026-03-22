#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/.env"
  set +a
fi

if [[ -f "$PROJECT_ROOT/config/db.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$PROJECT_ROOT/config/db.env"
  set +a
fi

DWH_DSN="${DWH_DSN:-}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-blissydah}"
DB_USER="${DB_USER:-blissydah}"
DB_PASSWORD="${DB_PASSWORD:-}"

PROFILE="${1:-dev}" # dev|prod|custom

# POS (daily partitions)
POS_START_OFFSET_DAYS="${POS_START_OFFSET_DAYS:--2}"
POS_END_OFFSET_DAYS="${POS_END_OFFSET_DAYS:-7}"

# STG stock movement (daily partitions)
SM_STG_START_OFFSET_DAYS="${SM_STG_START_OFFSET_DAYS:-1}"
SM_STG_END_OFFSET_DAYS="${SM_STG_END_OFFSET_DAYS:-14}"
SM_STG_KEEP_DAYS="${SM_STG_KEEP_DAYS:-90}"

# FACT stock movement (monthly partitions)
SM_FACT_START_OFFSET_MONTHS="${SM_FACT_START_OFFSET_MONTHS:--1}"
SM_FACT_END_OFFSET_MONTHS="${SM_FACT_END_OFFSET_MONTHS:-12}"
SM_FACT_KEEP_MONTHS="${SM_FACT_KEEP_MONTHS:-12}"

APPLY_RETENTION="${APPLY_RETENTION:-1}"

if [[ "$PROFILE" == "prod" ]]; then
  POS_START_OFFSET_DAYS="${POS_START_OFFSET_DAYS_PROD:--2}"
  POS_END_OFFSET_DAYS="${POS_END_OFFSET_DAYS_PROD:-21}"
  SM_STG_START_OFFSET_DAYS="${SM_STG_START_OFFSET_DAYS_PROD:-1}"
  SM_STG_END_OFFSET_DAYS="${SM_STG_END_OFFSET_DAYS_PROD:-30}"
  SM_FACT_START_OFFSET_MONTHS="${SM_FACT_START_OFFSET_MONTHS_PROD:--1}"
  SM_FACT_END_OFFSET_MONTHS="${SM_FACT_END_OFFSET_MONTHS_PROD:-18}"
elif [[ "$PROFILE" == "dev" ]]; then
  :
elif [[ "$PROFILE" == "custom" ]]; then
  :
else
  echo "[FAIL] Unknown profile: $PROFILE (expected dev|prod|custom)"
  exit 1
fi

if [[ -z "$DWH_DSN" && -z "$DB_PASSWORD" ]]; then
  echo "[FAIL] DB_PASSWORD is empty in config/db.env"
  exit 1
fi

export PGPASSWORD="$DB_PASSWORD"

psql_cmd() {
  if [[ -n "$DWH_DSN" ]]; then
    psql "$DWH_DSN" -v ON_ERROR_STOP=1 -X "$@"
  else
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -X "$@"
  fi
}

psql_exec() {
  local sql="$1"
  psql_cmd -q -c "$sql"
}

check_parent_exists() {
  local rel="$1"
  local exists
  exists="$(psql_cmd -Atqc "SELECT to_regclass('$rel') IS NOT NULL;")"
  if [[ "$exists" != "t" ]]; then
    echo "[FAIL] Missing parent object: $rel"
    exit 1
  fi
}

echo "[INFO] ensure_dwh_partitions profile=$PROFILE"
if [[ -n "$DWH_DSN" ]]; then
  echo "[INFO] DB=via DWH_DSN"
else
  echo "[INFO] DB=${DB_HOST}:${DB_PORT}/${DB_NAME} user=${DB_USER}"
fi

check_parent_exists "core.stg_po_pos_order_line"
check_parent_exists "core.stg_sm_stock_move_line"
check_parent_exists "core.fct_sm_stock_movement"

echo "[INFO] Ensuring POS daily partitions: offsets ${POS_START_OFFSET_DAYS}..${POS_END_OFFSET_DAYS}"
for i in $(seq "$POS_START_OFFSET_DAYS" "$POS_END_OFFSET_DAYS"); do
  d="$(date -d "$i day" +%F)"
  dn="$(date -d "$i day +1 day" +%F)"
  n="$(date -d "$i day" +%Y%m%d)"
  table_name="core.stg_po_pos_order_line_${n}"
  sql="CREATE TABLE IF NOT EXISTS ${table_name} PARTITION OF core.stg_po_pos_order_line FOR VALUES FROM ('${d}') TO ('${dn}');"
  psql_exec "$sql" >/dev/null
  echo "[OK] POS partition ${table_name}"
done

sm_stg_from="$(date -d "${SM_STG_START_OFFSET_DAYS} day" +%F)"
sm_stg_to="$(date -d "${SM_STG_END_OFFSET_DAYS} day" +%F)"
echo "[INFO] Ensuring STG stock daily partitions: ${sm_stg_from}..${sm_stg_to}"
psql_exec "CALL core.sp_crt_partitions_stg_stockmov('${sm_stg_from}'::date, '${sm_stg_to}'::date);" >/dev/null
echo "[OK] STG stock partitions ensured"

sm_fact_from="$(date -d "$(date +%Y-%m-01) ${SM_FACT_START_OFFSET_MONTHS} month" +%F)"
sm_fact_to="$(date -d "$(date +%Y-%m-01) ${SM_FACT_END_OFFSET_MONTHS} month" +%F)"
echo "[INFO] Ensuring FACT stock monthly partitions: ${sm_fact_from}..${sm_fact_to}"

cursor="$sm_fact_from"
while [[ "$cursor" < "$(date -d "$sm_fact_to +1 day" +%F)" ]]; do
  month_start="$(date -d "$cursor" +%F)"
  next_month="$(date -d "$cursor +1 month" +%F)"
  suffix="$(date -d "$cursor" +%Y%m)"
  table_name="core.fct_sm_stock_movement_${suffix}"

  bound_exists="$(
    psql_cmd -Atqc "
      SELECT EXISTS (
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'core.fct_sm_stock_movement'::regclass
          AND pg_get_expr(c.relpartbound, c.oid) =
              format('FOR VALUES FROM (''%s'') TO (''%s'')', '${month_start}', '${next_month}')
      );
    "
  )"

  if [[ "$bound_exists" == "t" ]]; then
    echo "[OK] FACT partition bound exists for ${month_start}..${next_month}"
  else
    psql_exec "CREATE TABLE IF NOT EXISTS ${table_name} PARTITION OF core.fct_sm_stock_movement FOR VALUES FROM ('${month_start}') TO ('${next_month}');" >/dev/null
    psql_exec "CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_sm_stock_movement_${suffix}_natkey ON ${table_name} (sm_odoo_move_line_id, sm_movement_side, sm_location_id_odoo);" >/dev/null
    psql_exec "CREATE INDEX IF NOT EXISTS ix_fct_sm_stock_movement_${suffix}_prod_loc_date ON ${table_name} (sm_product_id_odoo, sm_location_id_odoo, sm_date_key);" >/dev/null
    psql_exec "CREATE INDEX IF NOT EXISTS ix_fct_sm_stock_movement_${suffix}_write_date ON ${table_name} (sm_odoo_write_date);" >/dev/null
    echo "[OK] FACT partition created ${table_name}"
  fi

  cursor="$next_month"
done

echo "[OK] FACT stock partitions ensured"

if [[ "$APPLY_RETENTION" == "1" ]]; then
  echo "[INFO] Applying retention (STG keep days=${SM_STG_KEEP_DAYS}, FACT keep months=${SM_FACT_KEEP_MONTHS})"
  psql_exec "CALL core.sp_drop_partitions_stg_stockmv(${SM_STG_KEEP_DAYS});" >/dev/null
  psql_exec "CALL core.sp_drop_part_fct_sm_stock_movement(${SM_FACT_KEEP_MONTHS});" >/dev/null
  echo "[OK] Retention applied"
else
  echo "[INFO] Retention skipped (APPLY_RETENTION=${APPLY_RETENTION})"
fi

unset PGPASSWORD || true
echo "[OK] Partition automation complete"
