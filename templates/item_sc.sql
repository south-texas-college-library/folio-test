-- Get statistical code from each item record

SELECT
    it.id AS item_id,
    item_sc.name AS item_stat_code
FROM folio_inventory.item it
CROSS JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(it.jsonb, 'statisticalCodeIds')) AS code_id (element)
JOIN folio_inventory.statistical_code__t item_sc ON item_sc.id = code_id::uuid