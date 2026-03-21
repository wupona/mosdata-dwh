-- Situation journaliere de stock
WITH params AS (
  SELECT
    %(opening_prev)s::DATE AS opening_prev_date,
    %(movement_day)s::DATE AS movement_day,
    %(opening_curr)s::DATE AS opening_curr_date
),
loc AS (
  SELECT l_location_id_odoo, l_name_norm
  FROM core.ref_l_location
  WHERE l_name_norm IN (
    'PARTNER | CUSTOMERS',
    'DEPOT | CENTRAL NEW | STOCK',
    'BOUTIQUE | B24 | STOCK',
    'BOUTIQUE | JUSTICE | STOCK',
    'BOUTIQUE | KINTAMBO | STOCK',
    'BOUTIQUE | METEO | STOCK',
    'BOUTIQUE | LUSHI | STOCK',
    'VIRTUAL | PRODUCTION',
    'PARTNER | VENDORS',
    'OTHER_INTERNAL | EMPLACEMENT TEMPORAIRE',
    'VIRTUAL | INVENTORY_ADJUSTMENT'
  )
),
opening_prev AS (
  SELECT so.so_location_id_odoo AS location_id,
         SUM(so.so_opening_qty) AS open_qty_prev
  FROM core.fct_so_stock_opening so
  JOIN params p ON so.so_opening_date = p.opening_prev_date
  GROUP BY 1
),
opening_curr AS (
  SELECT so.so_location_id_odoo AS location_id,
         SUM(so.so_opening_qty) AS open_qty_curr
  FROM core.fct_so_stock_opening so
  JOIN params p ON so.so_opening_date = p.opening_curr_date
  GROUP BY 1
),
movement_io AS (
  SELECT
    sm.sm_location_id_odoo AS location_id,
    SUM(CASE WHEN sm.sm_movement_side = 'DST' THEN ABS(sm.sm_qty) ELSE 0 END) AS in_qty,
    SUM(CASE WHEN sm.sm_movement_side = 'SRC' THEN ABS(sm.sm_qty) ELSE 0 END) AS out_qty,
    SUM(
      CASE
        WHEN sm.sm_movement_side = 'SRC' THEN -ABS(sm.sm_qty)
        WHEN sm.sm_movement_side = 'DST' THEN  ABS(sm.sm_qty)
        ELSE 0
      END
    ) AS net_movement
  FROM core.fct_sm_stock_movement sm
  JOIN params p ON sm.sm_movement_day = p.movement_day
  WHERE sm.sm_barcode IS NOT NULL
    AND sm.sm_barcode ~ '^[A-Za-z]'
    AND sm.sm_barcode <> 'false'
  GROUP BY 1
)
SELECT
  p.opening_curr_date AS "Date Ouverture",
  l.l_name_norm AS "Emplacement",
  COALESCE(op.open_qty_prev, 0) AS "Stock Ouverture (Prec)",
  COALESCE(m.in_qty, 0)         AS "Qte Entree",
  COALESCE(-m.out_qty, 0)        AS "Qte Sortie",
  COALESCE(m.net_movement, 0)   AS "Mouvement Net",
  COALESCE(op.open_qty_prev, 0) + COALESCE(m.net_movement, 0) AS "Stock Cloture Calc",
  COALESCE(oc.open_qty_curr, 0) AS "Stock Ouverture (Cour)",
  COALESCE(oc.open_qty_curr, 0) - (COALESCE(op.open_qty_prev, 0) + COALESCE(m.net_movement, 0)) AS "Ecart Qte"
FROM loc l
CROSS JOIN params p
LEFT JOIN opening_prev op ON op.location_id = l.l_location_id_odoo
LEFT JOIN opening_curr oc ON oc.location_id = l.l_location_id_odoo
LEFT JOIN movement_io m   ON m.location_id  = l.l_location_id_odoo
ORDER BY 2 DESC;
