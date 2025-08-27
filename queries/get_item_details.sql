--metadb:function get_item_details

DROP FUNCTION IF EXISTS get_item_details;

CREATE FUNCTION get_item_details(
    service_point text DEFAULT NULL
    item_location text DEFAULT NULL
    material_type text DEFAULT NULL
    item_status text DEFAULT NULL
)
RETURNS TABLE(
    title TEXT,
    call_number TEXT,
    item_barcode TEXT,
    isbn TEXT,
    status_name TEXT,
    material_type_name TEXT,
    service_point_name TEXT,
    location_name TEXT,
    price TEXT
    inventory_date TEXT,
    po_number TEXT,
    staff_notes TEXT,
    invoice TEXT
)
AS $$
SELECT
    folio_derived.items_holdings_instances.title as title,
    folio_derived.items_holdings_instances.call_number as call_number,
    folio_derived.items_holdings_instances.barcode as item_barcode,
    string_agg(DISTINCT split_part(folio_derived.instance_identifiers.identifier, ' : ', 1), ', ') AS isbn,
    folio_derived.item_ext.status_name as status_name,
    folio_derived.item_ext.material_type_name as material_type_name,
    folio_derived.locations_service_points.service_point_name as service_point_name,
    folio_derived.locations_libraries.location_name as location_name,
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Price') AS price,
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Inventory Date') AS inventory_date,
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'PO Number') AS po_number,
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Staff Note') AS staff_notes,
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Invoice') AS invoice
FROM
	folio_derived.items_holdings_instances
    LEFT JOIN folio_derived.item_ext ON folio_derived.item_ext.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.item_notes ON folio_derived.item_notes.item_id = folio_derived.items_holdings_instances.item_id
    LEFT JOIN folio_derived.instance_identifiers ON folio_derived.instance_identifiers.instance_id = folio_derived.items_holdings_instances.instance_id
    LEFT JOIN folio_derived.locations_libraries ON folio_derived.locations_libraries.location_id = folio_derived.item_ext.effective_location_id
    LEFT JOIN folio_derived.locations_service_points ON folio_derived.locations_service_points.location_id = folio_derived.item_ext.effective_location_id
WHERE
	folio_derived.locations_service_points.service_point_name = service_point
	AND folio_derived.locations_libraries.location_name = item_location
    AND folio_derived.item_ext.material_type_name = material_type
	AND folio_derived.item_ext.status_name = item_status
GROUP by
    folio_derived.item_ext.material_type_name,
    folio_derived.item_ext.status_name,
    folio_derived.items_holdings_instances.title,
    folio_derived.items_holdings_instances.barcode,
    folio_derived.items_holdings_instances.call_number,
    folio_derived.locations_libraries.location_name,
    folio_derived.locations_service_points.service_point_name
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;