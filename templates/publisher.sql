-- Get primary publisher from each instance record

SELECT
    ins.id AS instance_id,
    jsonb_extract_path_text(publisher.element, 'publisher') AS publisher
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'publication')) WITH ordinality AS publisher (element, ordinal)
WHERE 
    publisher.ordinal = 1