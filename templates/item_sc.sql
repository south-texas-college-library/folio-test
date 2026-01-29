-- Get statistical code from each item record

SELECT
    it.id AS id,
    sc.name AS item_stat_codes
FROM folio_inventory.item it
LEFT JOIN LATERAL jsonb_array_elements_text(jsonb_extract_path(it.jsonb, 'statisticalCodeIds')) AS object ON true
LEFT JOIN folio_inventory.statistical_code__t sc ON sc.id = object::uuid