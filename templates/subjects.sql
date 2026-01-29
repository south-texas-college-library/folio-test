-- Get subjects from each instance record

SELECT
    ins.id AS id,
    string_agg(jsonb_extract_path_text(s.jsonb, 'value'), ', ') AS subjects
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'subjects')) AS s ON TRUE
GROUP BY 
    ins.id