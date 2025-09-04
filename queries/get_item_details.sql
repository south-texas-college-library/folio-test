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
SELECT
    folio_derived.items_holdings_instances.title as title,
    folio_derived.items_holdings_instances.call_number as call_number,
    string_agg(DISTINCT split_part(folio_derived.instance_identifiers.identifier, ' : ', 1), ', ') AS identifier,
	(SELECT MAX(val[1]::text) from regexp_matches(folio_derived.instance_publication.date_of_publication, '\d{4}', 'g') as val) as publication_date,
	folio_derived.holdings_ext.permanent_location_name as holdings_permanent_location,
    folio_derived.items_holdings_instances.barcode as item_barcode,
    folio_derived.locations_libraries.location_name as location_name,
    folio_derived.item_ext.material_type_name as material_type_name,
    folio_derived.locations_service_points.service_point_name as service_point_name,
    folio_derived.item_ext.status_name as status_name,
    folio_derived.item_ext.permanent_loan_type_name as permanent_loan_type,
    folio_derived.item_ext.temporary_loan_type_name as temporary_loan_type,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Circulation Note') AS circulation_note,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Public Note') AS public_note,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Staff Note') AS staff_notes,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Ownership') AS ownership,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Price') AS price,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Inventory Date') AS inventory_date,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'PO Number') AS po_number,
    string_agg(DISTINCT folio_derived.item_notes.note, ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Invoice') AS invoice,
    folio_derived.item_statistical_codes.statistical_code_name as fund,
    folio_derived.instance_statistical_codes.statistical_code_name as subtype
FROM
	folio_derived.items_holdings_instances
    LEFT JOIN folio_derived.item_ext ON folio_derived.item_ext.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.holdings_ext ON folio_derived.holdings_ext.holdings_id = folio_derived.items_holdings_instances.holdings_id
    LEFT JOIN folio_derived.item_notes ON folio_derived.item_notes.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.instance_identifiers ON folio_derived.instance_identifiers.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.instance_publication ON folio_derived.instance_publication.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.locations_libraries ON folio_derived.locations_libraries.location_id = folio_derived.item_ext.effective_location_id
    LEFT JOIN folio_derived.locations_service_points ON folio_derived.locations_service_points.location_id = folio_derived.item_ext.effective_location_id
    LEFT JOIN folio_derived.instance_statistical_codes ON folio_derived.instance_statistical_codes.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.item_statistical_codes ON folio_derived.item_statistical_codes.item_id = folio_derived.items_holdings_instances.item_id
WHERE
	(service_point = 'All' OR folio_derived.locations_service_points.service_point_name = service_point)
    AND (holdings_location = 'All' OR folio_derived.holdings_ext.permanent_location_name = holdings_location)
	AND (item_location = 'All' OR folio_derived.locations_libraries.location_name = item_location)
    AND (material_type = 'All' OR folio_derived.item_ext.material_type_name = material_type)
	AND (item_status = 'All' OR folio_derived.item_ext.status_name = item_status)
    AND (permanent_loan_type = 'All' OR folio_derived.item_ext.permanent_loan_type_name = permanent_loan_type)
	AND (temporary_loan_type = 'All' OR folio_derived.item_ext.temporary_loan_type_name = temporary_loan_type)
    AND (subtype = 'All' OR folio_derived.instance_statistical_codes.statistical_code_name = subtype)
    AND (folio_derived.instance_publication.publication_ordinality = 1 OR folio_derived.instance_publication.publication_ordinality IS NULL)
GROUP BY
    folio_derived.items_holdings_instances.title,
    folio_derived.items_holdings_instances.barcode,
    folio_derived.items_holdings_instances.call_number,
    folio_derived.item_ext.material_type_name,
    folio_derived.item_ext.status_name,
    folio_derived.holdings_ext.permanent_location_name,
    folio_derived.locations_libraries.location_name,
    folio_derived.locations_service_points.service_point_name,
    folio_derived.instance_publication.date_of_publication,
    folio_derived.item_ext.permanent_loan_type_name,
    folio_derived.item_ext.temporary_loan_type_name,
    folio_derived.instance_publication.publication_ordinality,
    folio_derived.instance_statistical_codes.statistical_code_name,
    folio_derived.item_statistical_codes.statistical_code_name
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;