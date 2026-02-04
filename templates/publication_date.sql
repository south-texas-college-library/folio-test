-- Get publication date from each instance record in production

SELECT
	ins.id AS instance_id,
	GREATEST(jsonb_extract_path_text(ins.jsonb, 'dates', 'date1'), jsonb_extract_path_text(ins.jsonb, 'dates', 'date2')) AS pub_date
FROM folio_inventory.instance ins

-- Get publication date from each instance record in test (They have different publication date structures)

-- SELECT
--     ins.id AS instance_id,
--     GREATEST(jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'start'), jsonb_extract_path_text(ins.jsonb, 'publicationPeriod', 'end')) AS pub_date
-- FROM folio_inventory.instance ins