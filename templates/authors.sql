-- Get all authors from each instance record

SELECT
    ins.id AS instance_id,
    string_agg(jsonb_extract_path_text(author.element, 'name'), ', ') AS authors
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'contributors')) AS author (element)
GROUP BY 
    ins.id