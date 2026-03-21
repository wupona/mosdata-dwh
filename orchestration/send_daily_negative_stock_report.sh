#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Script: send_daily_negative_stock_report.sh
# But: Envoyer le dernier rapport CSV ODOO_STOCK_NEGATIF_*.csv par email
#      en forçant un expéditeur autorisé (From + envelope-from).
# ==============================================================================

BASE_DIR="/mnt/c/Blissydah"
REPORT_DIR="${BASE_DIR}/daily_reports"
RECIPIENTS_FILE="${BASE_DIR}/config/report_recipients.txt"
LOG_FILE="${BASE_DIR}/logs/send_mail.log"

# IMPORTANT: doit être une adresse "owned by user" sur le SMTP blissydah
FROM_ADDR="norbert.wupona@blissydah.com"

SUBJECT_PREFIX="[Blissydah] Rapport stock négatif"

TMP_BODY="$(mktemp)"
TMP_MIME="$(mktemp)"

mkdir -p "${BASE_DIR}/logs"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

cleanup() {
  rm -f "${TMP_BODY}" "${TMP_MIME}" 2>/dev/null || true
}
trap cleanup EXIT

# -----------------------
# 1) Vérifications
# -----------------------
if [[ ! -d "${REPORT_DIR}" ]]; then
  log "ERREUR: Dossier introuvable: ${REPORT_DIR}"
  exit 1
fi

if [[ ! -f "${RECIPIENTS_FILE}" ]]; then
  log "ERREUR: Fichier destinataires introuvable: ${RECIPIENTS_FILE}"
  exit 1
fi

# Charger destinataires (ignore vides + commentaires), retirer CR Windows
mapfile -t RECIPIENTS < <(grep -Ev '^\s*($|#)' "${RECIPIENTS_FILE}" | tr -d '\r')

if (( ${#RECIPIENTS[@]} == 0 )); then
  log "ERREUR: Aucun destinataire dans ${RECIPIENTS_FILE}"
  exit 1
fi

# Trouver le dernier fichier
LATEST_FILE="$(ls -1t "${REPORT_DIR}"/ODOO_STOCK_NEGATIF_*.csv 2>/dev/null | head -n 1 || true)"
if [[ -z "${LATEST_FILE}" ]]; then
  log "ERREUR: Aucun fichier ODOO_STOCK_NEGATIF_*.csv trouvé dans ${REPORT_DIR}"
  exit 1
fi

BASENAME="$(basename "${LATEST_FILE}")"
FILE_SIZE="$(stat -c%s "${LATEST_FILE}" 2>/dev/null || echo "NA")"
NOW="$(date '+%Y-%m-%d %H:%M:%S')"

SUBJECT="${SUBJECT_PREFIX} - ${BASENAME}"

# -----------------------
# 2) Corps du mail
# -----------------------
cat > "${TMP_BODY}" <<EOF
Bonjour,

Veuillez trouver ci-joint le rapport "Stock négatif" généré automatiquement.

- Fichier : ${BASENAME}
- Chemin  : ${LATEST_FILE}
- Taille  : ${FILE_SIZE} octets
- Date    : ${NOW}

Cordialement,
Blissydah - Controle Interne
EOF
# -----------------------
# 3) Construire un MIME multipart avec pièce jointe (base64)
#    + Forcer envelope-from via sendmail -f
# -----------------------
BOUNDARY="BOUNDARY_$(date +%s)_$$"

{
  echo "From: ${FROM_ADDR}"
  echo "To: ${RECIPIENTS[*]}"
  echo "Subject: ${SUBJECT}"
  echo "MIME-Version: 1.0"
  echo "Content-Type: multipart/mixed; boundary=\"${BOUNDARY}\""
  echo
  echo "--${BOUNDARY}"
  echo "Content-Type: text/plain; charset=\"utf-8\""
  echo "Content-Transfer-Encoding: 8bit"
  echo
  cat "${TMP_BODY}"
  echo
  echo "--${BOUNDARY}"
  echo "Content-Type: text/csv; name=\"${BASENAME}\""
  echo "Content-Transfer-Encoding: base64"
  echo "Content-Disposition: attachment; filename=\"${BASENAME}\""
  echo
  base64 "${LATEST_FILE}"
  echo
  echo "--${BOUNDARY}--"
} > "${TMP_MIME}"

log "Envoi du mail à: ${RECIPIENTS[*]}"
log "Expéditeur (From + envelope-from): ${FROM_ADDR}"
log "Pièce jointe: ${LATEST_FILE}"

# Envoi réel
/usr/sbin/sendmail -f "${FROM_ADDR}" "${RECIPIENTS[@]}" < "${TMP_MIME}"

log "OK: Message remis à sendmail (Postfix). Vérifier /var/log/mail.log pour status=sent."
