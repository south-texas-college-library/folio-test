--metadb:function get_item_details

DROP FUNCTION IF EXISTS get_item_details;

CREATE FUNCTION get_item_details(
    service_point text DEFAULT NULL,
    holdings_location text DEFAULT NULL,
    item_location text DEFAULT NULL,
    material_type text DEFAULT NULL,
    item_status text DEFAULT NULL,
    permanent_loan_type text DEFAULT NULL,
    temporary_loan_type text DEFAULT NULL,
    subtype text DEFAULT NULL
)
RETURNS TABLE(
    title TEXT,
    call_number TEXT,
    identifiers TEXT,
    publication_date TEXT,
	item_barcode TEXT,
    holdings_location TEXT,
    item_location TEXT,
    material_type TEXT,
    service_point TEXT,
    item_status TEXT,
    permanent_loan_type TEXT,
    temporary_loan_type TEXT,
    circulation_note TEXT,
    public_note TEXT,
    staff_notes TEXT,
    ownership TEXT,
    price TEXT,
    inventory_date TEXT,
    po_number TEXT,
    invoice TEXT,
	subtype TEXT,
    fund TEXT
)
AS $$
SELECT
	ins.jsonb ->> 'title' AS title,
	hr.call_number AS call_number,
	REGEXP_REPLACE(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', ' :.*?\$\d+\.\d{2}', '', 'g'), '[\[\]"]', '', 'g') AS identifiers,
    GREATEST(ins.jsonb -> 'publicationPeriod' ->> 'start', ins.jsonb -> 'publicationPeriod' ->> 'end') AS publication_date,
    it.jsonb ->> 'barcode' AS item_barcode,
    hl.name AS holdings_location,
    il.name AS item_location,
    mt.name AS material_type,
    sp.name AS service_point,
    it.jsonb -> 'status' ->> 'name' AS item_status,
    plt.name AS permanent_loan_type,
    tlt.name AS temporary_loan_type,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5366d4d4-8775-4cf4-a00f-77c82f0ca3bf").note') #>> '{}', '[\[\]"]', '', 'g') AS circulation_note,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "b6b35579-ee2b-4973-8e0d-ebc05bab0dab").note') #>> '{}', '[\[\]"]', '', 'g') AS public_note,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[\[\]"]', '', 'g') AS staff_notes,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "34207e4e-5cd7-4eab-801b-b0326cd5c66a").note') #>> '{}', '[\[\]"]', '', 'g') AS ownership,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[\[\]"]', '', 'g') AS price,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "e1f34ba3-6d37-462e-878c-17f922b13d93").note') #>> '{}', '[\[\]"]', '', 'g') AS inventory_date,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[\[\]"]', '', 'g') AS po_number,
	REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "8f99bd3a-706c-45d2-89d8-8eca7fa1c03f").note') #>> '{}', '[\[\]"]', '', 'g') AS invoice,
    insc.jsonb ->> 'name' AS subtype,
	itsc.jsonb ->> 'name' AS fund
FROM 
	folio_inventory.instance ins
	LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
	LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
	LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	LEFT JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
	LEFT JOIN folio_inventory.loan_type__t plt ON plt.id = it.permanentloantypeid
	LEFT JOIN folio_inventory.loan_type__t tlt ON tlt.id = it.temporaryloantypeid
	LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	LEFT JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
	LEFT JOIN folio_inventory.statistical_code insc ON insc.id = (jsonb_path_query_first(ins.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
	LEFT JOIN folio_inventory.statistical_code itsc ON itsc.id = (jsonb_path_query_first(it.jsonb, '$.statisticalCodeIds[*]') #>> '{}')::uuid
WHERE
	(service_point = 'All' OR sp.name = service_point)
	AND (holdings_location = 'All' OR hl.name = holdings_location)
	AND (item_location = 'All' OR il.name = item_location)
	AND (material_type = 'All' OR mt.name = material_type)
	AND (permanent_loan_type = 'All' OR plt.name = permanent_loan_type)
	AND (temporary_loan_type = 'All' OR tlt.name = temporary_loan_type)
	AND (subtype = 'All' OR insc.jsonb ->> 'name' = subtype)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;