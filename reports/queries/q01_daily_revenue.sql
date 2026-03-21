WITH sales AS (
  SELECT
      pc.pc_name AS shops,
      p.pp_payment_day AS date_operation,
      SUM(p.pp_amount) AS vente_odoo,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'POS MAKUTA')     AS makuta,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'ECOBANK')        AS ecobank,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'UBA')            AS uba,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'Orange money')   AS orange,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'M-PESA')         AS m_pesa,
      SUM(p.pp_amount) FILTER (WHERE pm.pm_payment_method_name = 'Caisse RECETTE') AS caisse_recette
  FROM core.fct_pp_pos_payment p
  JOIN core.ref_pc_pos_config pc
    ON pc.pc_id_odoo::varchar = p.pp_pos_config_id_odoo::varchar
  LEFT JOIN core.ref_pm_payment_method pm
    ON p.pp_payment_method_id_odoo::varchar = pm.pm_payment_method_id_odoo::varchar
  WHERE p.pp_payment_day = %(target_date)s
  GROUP BY pc.pc_name, p.pp_payment_day
),
sales_norm AS (
  SELECT
    *,
    REGEXP_REPLACE(UPPER(TRIM(shops)), '[\s\-_]+', '', 'g') AS shop_key
  FROM sales
),
expenses_shop AS (
  SELECT
      e.oe_expense_date AS date_operation,
      m.shop_key        AS shop_key,
      SUM(e.oe_total_amount) AS depenses
  FROM core.fct_oe_odoo_expenses e
  JOIN core.ref_shop_employee_map m
    ON m.is_active = true
   AND m.employee_name = (e.oe_raw_json->'employee_id'->>1)
   AND e.oe_expense_date BETWEEN m.valid_from AND m.valid_to
  WHERE e.oe_expense_date = %(target_date)s
    AND COALESCE(e.oe_total_amount, 0) <> 0
  GROUP BY e.oe_expense_date, m.shop_key
)
SELECT
    ROW_NUMBER() OVER (ORDER BY s.shops, s.date_operation) AS "Numero",
    s.shops                                                AS "Shops",
    s.date_operation                                       AS "Date Operation",
    s.vente_odoo                                           AS "Vente Odoo",
    COALESCE(s.makuta, 0)         AS "Makuta",
    COALESCE(s.ecobank, 0)        AS "Ecobank",
    COALESCE(s.uba, 0)            AS "UBA",
    COALESCE(s.orange, 0)         AS "Orange",
    COALESCE(s.m_pesa, 0)         AS "M-Pesa",
    COALESCE(s.caisse_recette, 0) AS "Caisse Recette",

    COALESCE(e.depenses, 0)       AS "Depenses",
    (s.caisse_recette - COALESCE(e.depenses, 0)) AS "A verser"
FROM sales_norm s
LEFT JOIN expenses_shop e
  ON e.date_operation = s.date_operation
 AND e.shop_key = s.shop_key
ORDER BY "Numero";
