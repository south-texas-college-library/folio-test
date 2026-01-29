-- Get primary author/contributor from each instance record

SELECT
    ins.id AS id,
    jsonb_extract_path_text(c.jsonb, 'name') AS author
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'contributors')) WITH ORDINALITY AS c (jsonb) ON TRUE
WHERE 
    c.ordinality = 1