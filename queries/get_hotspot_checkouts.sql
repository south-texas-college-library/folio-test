--metadb:function get_hotspot_checkouts

DROP FUNCTION IF EXISTS get_hotspot_checkouts;

CREATE FUNCTION get_hotspot_checkouts(
    service_point text DEFAULT NULL,
    status text DEFAULT NULL,
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    "1 - Title" text,
    "2 - Patron Barcode" text,
    "3 - Loan Status" text,
    "4 - Patron Name" text,
    "5 - Copy Number" text,
    "6 - Item Status" text,
    "7 - Loan Date" timestamptz,
    "8 - Due Date" timestamptz,
    "9 - Item Barcode" text,
    "10 - Home Location" text,
    "11 - Current Location" text,
    "12 - Campus Name" text,
    "13 - Price" text,
    "14 - PO Number" text,
    "15 - Staff Notes" text
)
AS $$
SELECT 
    ins.jsonb ->> 'title',
    u.jsonb ->> 'barcode',
    fl.jsonb -> 'status' ->> 'name',
    NULLIF(CONCAT(u.jsonb -> 'personal' ->> 'firstName', ' ', u.jsonb -> 'personal' ->> 'lastName'), ' '),
    it.jsonb ->> 'copyNumber',
    fl.jsonb ->> 'itemStatus',
    (fl.jsonb ->> 'loanDate')::timestamptz,
    (fl.jsonb ->> 'dueDate')::timestamptz,
    it.jsonb ->> 'barcode',
    hl.name,
    il.name,
    lc.name,
    NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), ''),
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[\[\]"]', '', 'g'), ''),
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g'), '')
FROM 
    folio_inventory.instance ins
    LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
    LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    LEFT JOIN folio_circulation.loan fl on (fl.jsonb ->> 'itemId')::uuid = it.id
    LEFT JOIN folio_users.users u on u.id = (fl.jsonb ->> 'userId')::uuid
    LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	LEFT JOIN folio_inventory.loccampus__t lc ON lc.id = il.campus_id
	LEFT JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
	LEFT JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
WHERE 
    insc.name = 'Hotspot'
    AND (status = 'All' OR fl.jsonb -> 'status' ->> 'name' = status)
    AND (fl.jsonb ->> 'loanDate')::timestamptz BETWEEN start_date AND end_date
    AND	(service_point = 'All' OR sp.name = service_point)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;