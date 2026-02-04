-- Get all identifiers from each instance record

SELECT
    ins.id AS instance_id,
    string_agg(jsonb_extract_path_text(identifier.element, 'value'), ', ') AS identifiers
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'identifiers')) AS identifier (element)
GROUP BY
    ins.id