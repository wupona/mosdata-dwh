SELECT
	so.so_product_id_odoo,
	p.p_barcode,
	p.p_product_name_norm,
        l.l_name_norm,
	so.so_opening_qty,
	so.so_unit_cost,
	so.so_unit_sale_price
FROM core.fct_so_stock_opening so
JOIN core.ref_l_location l
  ON so.so_location_id_odoo = l.l_location_id_odoo
JOIN core.ref_p_product p
  ON so.so_product_id_odoo = p.p_product_id_odoo
WHERE so.so_opening_date = %(target_date)s
    AND p.p_barcode IS NOT NULL
    AND p.p_barcode ~ '^[A-Za-z]'
    AND p.p_barcode <> 'false';
