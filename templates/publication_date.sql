-- Get publication date from each instance record in production

SELECT
	ins.id AS id,
	GREATEST(jsonb_extract_path_text(ins.jsonb, 'dates', 'date1'), jsonb_extract_path_text(ins.jsonb, 'dates', 'date2')) AS pub_date
FROM folio_inventory.instance ins
LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'publication')) AS p ON TRUE

-- Get publication date from each instance record in test (They have different publication date structures)

-- SELECT
--     ins.id AS id,
--     GREATEST(jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'start'), jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'end')) AS pub_date
-- FROM folio_inventory.instance ins
-- LEFT JOIN LATERAL jsonb_array_elements(jsonb_extract_path(ins.jsonb, 'publication')) AS p ON TRUE