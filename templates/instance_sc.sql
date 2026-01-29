-- Get statistical code from each instance record

SELECT
    ins.id AS id,
    sc.name AS instance_stat_codes
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(ins.jsonb, 'statisticalCodeIds')) AS object ON true
LEFT JOIN folio_inventory.statistical_code__t sc ON sc.id = object::uuid