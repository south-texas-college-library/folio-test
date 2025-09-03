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
    folio_inventory.instance__t.title as "Title",
    folio_users.users__t.barcode as "User ID",
    folio_derived.loans_items.loan_status as "Loan Status",
    CONCAT(folio_derived.users_groups.user_first_name, ' ', folio_derived.users_groups.user_last_name) as "Name",
    folio_derived.loans_items.copy_number as "Copy Number",
    folio_derived.loans_items.item_status as "Item Status",
    folio_derived.loans_items.loan_date as "Loan Date",
    folio_derived.loans_items.loan_due_date as "Loan Due Date",
    folio_inventory.item__t.barcode as "Item ID",
    folio_derived.holdings_ext.permanent_location_name as "Home Location",
    folio_derived.item_ext.effective_location_name as "Current Location",
    folio_derived.locations_libraries.campus_name as "Owning Library",
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Price') AS "Price",
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'PO Number') AS "PO Number",
    string_agg(DISTINCT split_part(folio_derived.item_notes.note, ' : ', 1), ', ') FILTER (WHERE folio_derived.item_notes.note_type_name = 'Staff Note') AS "Staff Notes"
FROM 
    folio_inventory.instance__t
    LEFT JOIN folio_inventory.holdings_record__t ON folio_inventory.holdings_record__t.instance_id = folio_inventory.instance__t.id
    LEFT JOIN folio_inventory.item__t ON folio_inventory.item__t.holdings_record_id = folio_inventory.holdings_record__t.id
    LEFT JOIN folio_source_record.marc__t ON folio_source_record.marc__t.instance_id = folio_inventory.instance__t.id
    LEFT JOIN folio_derived.loans_items on folio_derived.loans_items.item_id = folio_inventory.item__t.id
    LEFT JOIN folio_users.users__t on folio_users.users__t.id = folio_derived.loans_items.user_id
    LEFT JOIN folio_derived.users_groups on folio_derived.users_groups.user_id = folio_users.users__t.id
    LEFT JOIN folio_derived.instance_statistical_codes ON folio_derived.instance_statistical_codes.instance_id = folio_inventory.instance__t.id
    LEFT JOIN folio_derived.holdings_ext ON folio_derived.holdings_ext.holdings_id = folio_inventory.holdings_record__t.id
    LEFT JOIN folio_derived.item_ext ON folio_derived.item_ext.item_id = folio_inventory.item__t.id
    LEFT JOIN folio_derived.item_notes ON folio_derived.item_notes.item_id = folio_inventory.item__t.id
    LEFT JOIN folio_derived.locations_libraries on folio_derived.loans_items.current_item_effective_location_id = folio_derived.locations_libraries.location_id
    LEFT JOIN folio_derived.loans_renewal_dates on folio_derived.loans_renewal_dates.loan_id = folio_derived.loans_items.loan_id
WHERE 
    folio_derived.instance_statistical_codes.statistical_code_name = 'Hotspot'
    AND (status = 'All' OR folio_derived.loans_items.loan_status = status)
    AND folio_derived.loans_items.loan_date BETWEEN start_date AND end_date
    AND	(service_point = 'All' OR folio_derived.loans_items.checkout_service_point_name = service_point)
GROUP BY
    folio_inventory.instance__t.id,
    folio_inventory.instance__t.title,
    folio_users.users__t.barcode,
    folio_derived.locations_libraries.campus_name,
    folio_derived.loans_items.loan_status,
    folio_derived.users_groups.user_first_name,
    folio_derived.users_groups.user_last_name,
    folio_derived.loans_items.checkout_service_point_name,
    folio_derived.loans_items.copy_number,
    folio_derived.loans_items.item_status,
    folio_derived.loans_items.loan_date,
    folio_derived.loans_items.loan_return_date,
    folio_derived.loans_items.loan_due_date,
    folio_derived.loans_items.system_return_date,
    folio_derived.loans_renewal_dates.loan_action_date,
    folio_inventory.item__t.barcode,
    folio_derived.holdings_ext.permanent_location_name,
    folio_derived.item_ext.effective_location_name,
    folio_derived.locations_libraries.location_name,
    folio_inventory.item__t.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;