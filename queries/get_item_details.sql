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
WITH inventory as (
	SELECT
		ins.id as instance_id,
		ins.jsonb as instance_jsonb,
		hr.id as hr_id,
		hr.call_number AS call_number,
		hl.name AS holdings_location,
		sp.name AS service_point,
		it.id as item_id,
	    it.jsonb as item_jsonb,
	    il.name AS item_location,
	    mt.name AS material_type,
	    plt.name AS permanent_loan_type,
    	tlt.name AS temporary_loan_type
	FROM 
		folio_inventory.instance ins
		LEFT JOIN folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
		LEFT JOIN folio_inventory.item it on it.holdingsrecordid = hr.id
		LEFT JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
		LEFT JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
		LEFT JOIN folio_inventory.loan_type__t plt ON plt.id = it.permanentloantypeid
		LEFT JOIN folio_inventory.loan_type__t tlt ON tlt.id = it.temporaryloantypeid
		LEFT JOIN folio_inventory.location__t il ON il.id = it.effectivelocationid
		LEFT JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
	WHERE
		(service_point = 'All' OR sp.name = service_point)
		AND (holdings_location = 'All' OR hl.name = holdings_location)
		(item_location = 'All' OR il.name = item_location)
		AND (material_type = 'All' OR mt.name = material_type)
		AND (permanent_loan_type = 'All' OR plt.name = permanent_loan_type)
		AND (temporary_loan_type = 'All' OR tlt.name = temporary_loan_type)
),
identifiers AS (
	SELECT 
		inv.instance_id,
		STRING_AGG(DISTINCT SPLIT_PART(i ->> 'value', ' : ', 1), ', ') AS identifier
	FROM 
		inventory inv
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(inv.instance_jsonb -> 'identifiers') AS i
	GROUP BY
		inv.instance_id
),
publication AS (
	SELECT
		ins.id,
		GREATEST(ins.jsonb -> 'publicationPeriod' ->> 'start', ins.jsonb -> 'publicationPeriod' ->> 'end') AS pub_date	
	FROM 
		folio_inventory.instance ins
),
notes AS (
	SELECT 
		inv.item_id,
		STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Circulation Note') AS circulation_note,
		STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Public Note') AS public_note,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Staff Note') AS staff_notes,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Ownership') AS ownership,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Price') AS price,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Inventory Date') AS inventory_date,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'PO Number') AS po_number,
	    STRING_AGG(n ->> 'note', ', ') FILTER (WHERE nt.jsonb ->> 'name' = 'Invoice') AS invoice
	FROM
		inventory inv
		CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(inv.item_jsonb -> 'notes') AS n
		LEFT JOIN folio_inventory.item_note_type nt on nt.id = (n ->> 'itemNoteTypeId')::uuid
	GROUP BY
		inv.item_id
),
codes AS (
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
	WHERE
	    (subtype = 'All' OR insc.name = subtype)
)
SELECT
    inv.instance_jsonb ->> 'title'  AS title,
    inv.call_number AS call_number,
    fi.identifier AS identifiers,
    fp.pub_date AS publication_date,
    inv.item_jsonb ->> 'barcode' AS item_barcode,
    inv.holdings_location AS holdings_location,
    inv.item_location AS item_location,
    inv.material_type AS material_type,
    inv.service_point AS service_point,
    inv.item_jsonb -> 'status' ->> 'name' AS item_status,
    inv.permanent_loan_type AS permanent_loan_type,
    inv.temporary_loan_type AS temporary_loan_type,
    fn.circulation_note AS circulation_note,
    fn.public_note AS public_note,
    fn.staff_notes AS staff_notes,
    fn.ownership AS ownership,
    fn.price AS price,
    fn.inventory_date AS inventory_date,
    fn.po_number AS po_number,
    fn.invoice AS invoice,
    fc.instance_code AS fund,
    fc.item_code AS subtype
FROM 
	inventory inv
	LEFT JOIN identifiers fi ON fi.instance_id = inv.instance_id
	LEFT JOIN publication fp ON fp.id = inv.instance_id
	LEFT JOIN notes fn ON fn.item_id = inv.item_id
	LEFT JOIN codes fc ON fc.item_id = inv.item_id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;