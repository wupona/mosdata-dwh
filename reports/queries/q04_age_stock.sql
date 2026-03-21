 WITH stock_age AS (
         SELECT so.so_opening_date,
            so.so_location_id_odoo,
            ref.p_product_name_norm,
            so.so_opening_qty,
            so.so_opening_value_cost,
            CURRENT_DATE - so.so_in_date AS days_old
           FROM core.fct_so_stock_opening so
             JOIN core.ref_p_product ref ON so.so_product_id_odoo = ref.p_product_id_odoo
          WHERE so.so_opening_date = %(opening_curr)s
        )
 SELECT sa.so_opening_date,l.l_complete_name AS emplacement,
    sum(
        CASE
            WHEN sa.days_old < 30 THEN sa.so_opening_qty
            ELSE 0::numeric
        END) AS "stock_<30j",
    sum(
        CASE
            WHEN sa.days_old >= 30 AND sa.days_old < 90 THEN sa.so_opening_qty
            ELSE 0::numeric
        END) AS "stock_30-90j",
    sum(
        CASE
            WHEN sa.days_old >= 90 AND sa.days_old < 180 THEN sa.so_opening_qty
            ELSE 0::numeric
        END) AS "stock_90-180j",
    sum(
        CASE
            WHEN sa.days_old >= 180 AND sa.days_old < 360 THEN sa.so_opening_qty
            ELSE 0::numeric
        END) AS "stock_180_360j",
    sum(
        CASE
            WHEN sa.days_old > 360 THEN sa.so_opening_qty
            ELSE 0::numeric
        END) AS "stock_>360"
   FROM stock_age sa
     JOIN core.ref_l_location l ON sa.so_location_id_odoo = l.l_location_id_odoo
  GROUP BY sa.so_opening_date, l.l_complete_name
  ORDER BY 2;
