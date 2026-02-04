-- Get subjects from each instance record

SELECT
    ins.id AS instance_id,
    string_agg(jsonb_extract_path_text(subject.element, 'value'), ', ') AS subjects
FROM folio_inventory.instance ins
CROSS JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'subjects')) AS subject (element)
GROUP BY 
    ins.id