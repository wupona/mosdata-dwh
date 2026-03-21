SELECT 
    pl_txn_day,
    po_raw_json->'config_id'->>1 AS pos_name,
	pl_order_id_odoo,
    pl_raw_json->'product_id'->>1 AS product_name,
    pl_barcode,
    pl_qty,
    pl_unit_price,
    pl_subtotal_excl_tax,
	po_raw_json->'employee_id'->>1 AS agent_blissydah
FROM core.fct_pl_pos_order_line pl
JOIN core.fct_po_pos_orders po
  ON pl_order_id_odoo = po_order_id_odoo
WHERE pl_txn_day = %(target_date)s
ORDER BY 2,3;
