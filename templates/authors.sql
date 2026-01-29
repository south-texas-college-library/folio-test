-- Get all authors from each instance record

SELECT
    ins.id AS id,
    string_agg(jsonb_extract_path_text(c.jsonb, 'name'), ', ') AS authors
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'contributors')) AS c ON TRUE
GROUP BY 
    ins.id