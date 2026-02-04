-- Get all publishers from each instance record

SELECT
    ins.id AS instance_id,
    string_agg(jsonb_extract_path_text(publisher.element, 'publisher'), ', ') AS publishers
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'publication')) AS publisher (element)
GROUP BY 
    ins.id