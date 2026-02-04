-- Get primary author/contributor from each instance record

SELECT
    ins.id AS instance_id,
    jsonb_extract_path_text(author.element, 'name') AS author
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'contributors')) WITH ordinality AS author (element, ordinal)
WHERE 
    author.ordinal = 1