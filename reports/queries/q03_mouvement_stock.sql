-- Mouvement journalier par Emplacement

SELECT
    sm.sm_movement_day,
    sm_location_id_odoo,
    l_name_norm,
    sm.sm_movement_side,
    sm.sm_location_usage,
    sm.sm_is_internal_location,
    sm.sm_movement_type,
    SUM(
        CASE 
            WHEN sm.sm_movement_side = 'SRC' THEN -ABS(sm.sm_qty)
            WHEN sm.sm_movement_side = 'DST' THEN  ABS(sm.sm_qty)
            ELSE 0
        END
    ) AS sm_qty_final
FROM core.fct_sm_stock_movement sm
JOIN core.ref_l_location l
    ON sm.sm_location_id_odoo = l.l_location_id_odoo
JOIN core.ref_p_product p
    ON sm.sm_product_id_odoo = p.p_product_id_odoo
WHERE sm.sm_movement_day = %(movement_day)s
    AND sm_barcode IS NOT NULL
    AND sm_barcode ~ '^[A-Za-z]'
    AND sm_barcode <> 'false'
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1,4,5,7,3;
