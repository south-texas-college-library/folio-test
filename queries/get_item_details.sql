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
    identifier TEXT,
    publication_date TEXT,
    holdings_permanent_location TEXT,
    item_barcode TEXT,
    location_name TEXT,
    material_type_name TEXT,
    service_point_name TEXT,
    status_name TEXT,
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
    fund TEXT,
    subtype TEXT
)
AS $$
WITH fi AS (
	SELECT 
		id,
		STRING_AGG(distinct SPLIT_PART(i ->> 'value', ' : ', 1), ', ') AS identifiers
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'identifiers') AS i
	GROUP BY
		ins.id
),
fp AS (
	SELECT
		ins.id,
		MAX(r[1]::text) AS pub_date
	FROM folio_inventory.instance ins
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'publication') AS p
	CROSS JOIN LATERAL REGEXP_MATCHES(p.jsonb ->> 'dateOfPublication', '\d{4}', 'g') AS r
	GROUP BY
		ins.id
),
fn AS (
	SELECT 
		it.id,
		STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Circulation Note') AS circulation_note,
		STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Public Note') AS public_note,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Staff Note') AS staff_notes,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Ownership') AS ownership,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Price') AS price,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Inventory Date') AS inventory_date,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'PO Number') AS po_number,
	    STRING_AGG(DISTINCT n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Invoice') AS invoice
	FROM folio_inventory.item it
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(it.jsonb -> 'notes') AS n
	LEFT JOIN folio_inventory.item_note_type nt on nt.id = (n ->> 'itemNoteTypeId')::uuid
	GROUP BY
		it.id
),
insc AS (
	SELECT
		id,
		(statcodes.jsonb #>> '{}')::uuid AS code
	FROM 
		folio_inventory.instance AS ins
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'statisticalCodeIds') AS statcodes
),
itsc AS (
	select
		id,
		(statcodes.jsonb #>> '{}')::uuid AS code
	FROM 
		folio_inventory.item AS it
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(it.jsonb -> 'statisticalCodeIds') AS statcodes
)
SELECT
    ins.jsonb ->> 'title' AS "Title",
    hr.call_number AS "Call Number",
    fi.identifiers AS "Identifiers",
    fp.pub_date AS "Publication Date",
    it.jsonb ->> 'barcode' AS "Barcode",
    hl.name AS "Holdings Location",
    il.name AS "Items Location",
    mt.name AS "Material Type",
    sp.name AS "Service Point",
    it.jsonb -> 'status' ->> 'name' AS "Item Status",
    plt.name AS "Permanent Loan Type",
    tlt.name AS "Temporary Loan Type",
    fn.circulation_note AS "Circulation Notes",
    fn.public_note AS "Public Notes",
    fn.staff_notes AS "Staff Notes",
    fn.ownership AS "Ownership",
    fn.price AS "Price",
    fn.inventory_date AS "Inventory Date",
    fn.po_number AS "PO Number",
    fn.invoice AS "Invoice",
    finsc.jsonb ->> 'name' AS "Subtype",
    fitsc.jsonb ->> 'name' AS "Fund"
FROM
	folio_inventory.instance ins
	LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
	LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
	LEFT JOIN folio_inventory.loan_type__t plt ON plt.id = it.permanentloantypeid
	LEFT JOIN folio_inventory.loan_type__t tlt ON tlt.id = it.temporaryloantypeid
	LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
	LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
	LEFT JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid::uuid
	LEFT JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point::uuid
	LEFT JOIN fi ON fi.id = ins.id
	LEFT JOIN fp ON fp.id = ins.id
	LEFT JOIN fn ON fn.id = it.id
	LEFT JOIN insc ON insc.id = ins.id
	LEFT JOIN itsc ON itsc.id = it.id
	LEFT JOIN folio_inventory.statistical_code finsc ON finsc.id = insc.code
	LEFT JOIN folio_inventory.statistical_code fitsc ON fitsc.id = itsc.code
WHERE
	(service_point = 'All' OR sp.name = service_point)
    AND (holdings_location = 'All' OR hl.name = holdings_location)
	AND (item_location = 'All' OR il.name = item_location)
    AND (material_type = 'All' OR mt.name = material_type)
	AND (item_status = 'All' OR it.jsonb -> 'status' ->> 'name' = item_status)
    AND (permanent_loan_type = 'All' OR plt.name = permanent_loan_type)
	AND (temporary_loan_type = 'All' OR tlt.name = temporary_loan_type)
    AND (subtype = 'All' OR finsc.jsonb ->> 'name' = subtype)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;