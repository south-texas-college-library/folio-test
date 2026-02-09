--metadb:function get_hotspot_checkouts

DROP FUNCTION IF EXISTS get_hotspot_checkouts;

CREATE FUNCTION get_hotspot_checkouts(
    service_point text DEFAULT NULL,
    status text DEFAULT NULL,
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    "A - Title" text,
    "B - Patron Barcode" text,
    "C - Loan Status" text,
    "D - Patron Name" text,
    "E - Copy Number" text,
    "F - Item Status" text,
    "G - Loan Date" timestamptz,
    "H - Due Date" timestamptz,
    "I - Item Barcode" text,
    "J - Home Location" text,
    "K - Current Location" text,
    "L - Campus Name" text,
    "M - Price" text,
    "N - PO Number" text,
    "O - Staff Notes" text
)
AS $$
SELECT
    jsonb_extract_path_text(ins.jsonb, 'title'),
    jsonb_extract_path_text(u.jsonb, 'barcode'),
    jsonb_extract_path_text(fl.jsonb, 'status', 'name'),
    NULLIF(CONCAT(jsonb_extract_path_text(u.jsonb, 'personal', 'firstName'), ' ', jsonb_extract_path_text(u.jsonb, 'personal', 'lastName')), ' '),
    jsonb_extract_path_text(it.jsonb, 'copyNumber'),
    jsonb_extract_path_text(fl.jsonb, 'itemStatus'),
    jsonb_extract_path_text(fl.jsonb, 'loanDate')::timestamptz,
    jsonb_extract_path_text(fl.jsonb, 'dueDate')::timestamptz,
    jsonb_extract_path_text(it.jsonb, 'barcode'),
    hl.name,
    il.name,
    lc.name,
    NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), ''),
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[\[\]"]', '', 'g'), ''),
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g'), '')
FROM 
    folio_inventory.instance ins
    JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
    JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    LEFT JOIN folio_circulation.loan fl on jsonb_extract_path_text(fl.jsonb, 'itemId')::uuid = it.id
    LEFT JOIN folio_users.users u on u.id = jsonb_extract_path_text(fl.jsonb, 'userId')::uuid
    JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	JOIN folio_inventory.loccampus__t lc ON lc.id = il.campus_id
	JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
	JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
WHERE 
    insc.name = 'Hotspot'
    AND (status = 'All' OR jsonb_extract_path_text(fl.jsonb, 'status', 'name') = status)
    AND jsonb_extract_path_text(fl.jsonb, 'loanDate')::timestamptz BETWEEN start_date AND end_date
    AND	(service_point = 'All' OR sp.name = service_point)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;