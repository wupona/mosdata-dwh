#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

START_OFFSET="${1:--2}" # days from today
END_OFFSET="${2:-7}"    # days from today

POS_START_OFFSET_DAYS="$START_OFFSET" \
POS_END_OFFSET_DAYS="$END_OFFSET" \
APPLY_RETENTION="${APPLY_RETENTION:-0}" \
"$PROJECT_ROOT/orchestration/ensure_dwh_partitions.sh" custom
