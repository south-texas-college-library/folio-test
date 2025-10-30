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
    fund TEXT,
    subtype TEXT
)
AS $$
WITH identifiers AS (
	SELECT 
		ins.id AS id,
		STRING_AGG(distinct SPLIT_PART(i ->> 'value', ' : ', 1), ', ') AS identifier
	FROM 
		folio_inventory.instance ins
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'identifiers') AS i
	GROUP BY
		ins.id
),
notes AS MATERIALIZED (
	SELECT
		it.id AS id,
		STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Circulation Note') AS circulation_note,
		STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Public Note') AS public_note,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Staff Note') AS staff_notes,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Ownership') AS ownership,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Price') AS price,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Inventory Date') AS inventory_date,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'PO Number') AS po_number,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Invoice') AS invoice
	FROM
		folio_inventory.item it
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(it.jsonb -> 'notes') AS n
		LEFT JOIN folio_inventory.item_note_type nt ON nt.id = (n ->> 'itemNoteTypeId')::uuid
	GROUP BY
		it.id
),
codes AS MATERIALIZED (
	SELECT
		ins.id AS id,
		it.id AS item_id,
		insc.name AS instance_code,
		itsc.name AS item_code
	FROM 
		folio_inventory.instance ins
		LEFT JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
		LEFT JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
		CROSS JOIN LATERAL ROWS FROM (
			JSONB_ARRAY_ELEMENTS(ins.jsonb -> 'statisticalCodeIds'), 
			JSONB_ARRAY_ELEMENTS(it.jsonb -> 'statisticalCodeIds')
		) x(y, z)
		LEFT JOIN folio_inventory.statistical_code__t insc ON insc.id = (y #>> '{}')::uuid
		LEFT JOIN folio_inventory.statistical_code__t itsc ON itsc.id = (z #>> '{}')::uuid
)
SELECT
	ins.jsonb ->> 'title' AS title,
	hr.call_number AS call_number,
    fi.identifier AS identifiers,
    GREATEST(ins.jsonb -> 'publicationPeriod' ->> 'start', ins.jsonb -> 'publicationPeriod' ->> 'end') AS publication_date,
    it.jsonb ->> 'barcode' AS item_barcode,
    hl.name AS holdings_location,
    il.name AS item_location,
    mt.name AS material_type,
    sp.name AS service_point,
    it.jsonb -> 'status' ->> 'name' AS item_status,
    plt.name AS permanent_loan_type,
    tlt.name AS temporary_loan_type,
    fn.circulation_note,
    fn.public_note,
    fn.staff_notes,
    fn.ownership,
    fn.price,
    fn.inventory_date,
    fn.po_number,
    fn.invoice,
    fc.instance_code AS fund,
    fc.item_code AS subtype
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
	LEFT JOIN identifiers fi ON fi.id = ins.id
	LEFT JOIN notes fn ON fn.id = it.id
	LEFT JOIN codes fc ON fc.item_id = it.id
WHERE
	(service_point = 'All' OR sp.name = service_point)
	AND (holdings_location = 'All' OR hl.name = holdings_location)
	AND (item_location = 'All' OR il.name = item_location)
	AND (material_type = 'All' OR mt.name = material_type)
	AND (permanent_loan_type = 'All' OR plt.name = permanent_loan_type)
	AND (temporary_loan_type = 'All' OR tlt.name = temporary_loan_type)
	AND (subtype = 'All' OR fc.instance_code = subtype)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;