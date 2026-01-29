-- Get primary publisher from each instance record

SELECT
    ins.id AS id,
    jsonb_extract_path_text(p.jsonb, 'publisher') AS publisher
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'publication')) WITH ORDINALITY AS p (jsonb) ON TRUE
WHERE 
    p.ordinality = 1