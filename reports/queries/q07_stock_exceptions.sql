WITH raw_data AS (
    SELECT
        s.so_opening_date,
        p.p_barcode AS barcode,
        l.l_complete_name AS emplacement,
        p.p_product_name_norm AS product_name,
        p.p_pos_category_norm AS category,
        s.so_opening_qty,
        s.so_unit_cost,
        s.so_unit_sale_price,
        CASE
            WHEN s.so_opening_qty < 0 THEN 'STOCK NEGATIF'
            WHEN s.so_opening_qty > 1 THEN 'STOCK > 1'
            ELSE 'OK'
        END AS status_qty,
        CASE
            WHEN s.so_unit_sale_price = 0 AND s.so_unit_cost = 0 THEN 'PRIX & COUT NULS'
            WHEN s.so_unit_sale_price > 0 AND s.so_unit_cost = 0 THEN 'COUT MANQUANT'
            WHEN s.so_unit_sale_price = 0 AND s.so_unit_cost > 0 THEN 'PRIX DE VENTE MANQUANT'
            ELSE 'OK'
        END AS status_financial
    FROM core.fct_so_stock_opening s
    JOIN core.ref_p_product p ON s.so_product_id_odoo = p.p_product_id_odoo
    JOIN core.ref_l_location l ON s.so_location_id_odoo = l.l_location_id_odoo
    WHERE p.p_is_current = true
      AND s.so_opening_date = %(target_date)s
)
SELECT * FROM raw_data
WHERE status_qty <> 'OK'
   OR status_financial <> 'OK'
ORDER BY emplacement, category;
