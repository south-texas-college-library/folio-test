-- Get all identifiers from each instance record

SELECT
    ins.id AS id,
    string_agg(jsonb_extract_path_text(object, 'value'), ', ') AS identifiers
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'identifiers')) AS object ON TRUE
GROUP BY
    ins.id