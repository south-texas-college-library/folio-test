-- Get statistical code from each instance record

SELECT
    ins.id AS instance_id,
    instance_sc.name AS instance_stat_codes
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(ins.jsonb, 'statisticalCodeIds')) AS code_id (element)
JOIN folio_inventory.statistical_code__t instance_sc ON instance_sc.id = code_id.element::uuid