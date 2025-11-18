--metadb:function get_hotspot_checkouts

DROP FUNCTION IF EXISTS get_hotspot_checkouts;

CREATE FUNCTION get_hotspot_checkouts(
    service_point text DEFAULT NULL,
    status text DEFAULT NULL,
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    title text,
    user_barcode text,
    loan_status text,
    name text,
    copy_number text,
    item_status text,
    loan_date timestamptz,
    loan_due_date timestamptz,
    item_barcode text,
    home_location text,
    current_location text,
    campus_name text,
    price text,
    po_number text,
    staff_notes text
)
AS $$
SELECT 
    ins.jsonb ->> 'title' as "Title",
    u.jsonb ->> 'barcode' as "User ID",
    fl.jsonb -> 'status' ->> 'name' as "Loan Status",
    NULLIF(CONCAT(u.jsonb -> 'personal' ->> 'firstName', ' ', u.jsonb -> 'personal' ->> 'lastName'), ' ') as "Name",
    it.jsonb ->> 'copyNumber' as "Copy Number",
    fl.jsonb ->> 'itemStatus' as "Item Status",
    fl.jsonb ->> 'dueDate' as "Due Date",
    fl.jsonb ->> 'loanDate' as "Loan Date",
    it.jsonb ->> 'barcode' as "Barcode",
    hl.name as "Home Location",
    il.name as "Current Location",
    lc.name as "Owning Library",
    NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g'), '') AS price,
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[\[\]"]', '', 'g'), '') AS po_number,
	NULLIF(REGEXP_REPLACE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g'), '') AS staff_notes
FROM 
    folio_inventory.instance ins
    LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
    LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
    LEFT JOIN folio_circulation.loan fl on (fl.jsonb ->> 'itemId')::uuid = it.id
    LEFT JOIN folio_users.users u on u.id = (fl.jsonb ->> 'userId')::uuid
    LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	LEFT JOIN folio_inventory.loccampus__t lc ON lc.id = il.campus_id
	LEFT JOIN folio_inventory.service_point__t sp ON sp.id = (fl.jsonb ->> 'checkoutServicePointId')::uuid
	LEFT JOIN folio_inventory.statistical_code__t insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
WHERE 
    insc.name = 'Hotspot'
    AND (status = 'All' OR fl.jsonb ->> 'itemStatus' = status)
    AND fl.jsonb ->> 'loanDate' BETWEEN start_date AND end_date
    AND	(service_point = 'All' OR sp.name = service_point)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;