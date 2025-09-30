--metadb:function get_callnumber

DROP FUNCTION IF EXISTS get_callnumber;

CREATE FUNCTION get_callnumber(
    start_cn text default null,
    end_cn text default null
)
RETURNS TABLE(
    created_date timestamptz,
    effective_location_name text,
    call_number text,
    barcode text,
    title text,
    material_type_name text,
    cataloged_date text,
    statistical_code_name text,
    item_status text,
    loan_date timestamptz,
    loan_return_date timestamptz,
    renewal_count integer,
    total_checkouts integer,
    item_notes text,
    instance_identifiers text,
    instance_publication text,
    subjects text
)
AS $$
    select
        item_ext.created_date, effective_location_name, 
        items_holdings_instances.call_number, items_holdings_instances.barcode, title, items_holdings_instances.material_type_name, cataloged_date,
        item_statistical_codes.statistical_code_name,
        loans_items.item_status, loans_items.loan_date, loans_items.loan_return_date, loans_items.renewal_count,
        (SELECT COUNT(*) FROM folio_derived.loans_items WHERE folio_derived.loans_items.item_id = folio_derived.items_holdings_instances.item_id) AS total_checkouts,
        string_agg(distinct folio_derived.item_notes.note, ', ') filter (where folio_derived.item_notes.note_type_name = 'Staff Note') as "item_notes",
        string_agg(distinct folio_derived.instance_identifiers.identifier, ', ') as "instance_identifiers",
        string_agg(distinct folio_derived.instance_publication.date_of_publication, ', ') as "instance_publication",
        string_agg(DISTINCT LOWER(jsonb_extract_path_text(folio_derived.instance_subjects.subjects::jsonb, 'value')), ', ') AS subjects
    from
        folio_derived.item_ext
        join 
        folio_derived.items_holdings_instances on folio_derived.items_holdings_instances.item_id = folio_derived.item_ext.item_id
        join
        folio_derived.item_statistical_codes on folio_derived.item_statistical_codes.item_id = folio_derived.item_ext.item_id
        join
        folio_derived.instance_subjects on folio_derived.instance_subjects.instance_id = folio_derived.items_holdings_instances.instance_id
        join
        folio_derived.instance_publication on folio_derived.instance_publication.instance_id = folio_derived.instance_subjects.instance_id
        join
        folio_derived.instance_identifiers on folio_derived.instance_identifiers.instance_id = folio_derived.instance_publication.instance_id
        join
        folio_derived.item_notes on folio_derived.item_notes.item_id = folio_derived.item_statistical_codes.item_id
        join
        folio_derived.loans_renewal_count on folio_derived.loans_renewal_count.item_id = folio_derived.item_ext.item_id
        join
        folio_derived.loans_items on folio_derived.loans_items.item_id = folio_derived.item_ext.item_id
    where items_holdings_instances.call_number between start_cn and end_cn
    group by 
    item_ext.created_date,
    effective_location_name, 
    items_holdings_instances.call_number, 
    items_holdings_instances.barcode, 
    title, 
    items_holdings_instances.material_type_name, 
    cataloged_date, 
    item_statistical_codes.statistical_code_name, 
    loans_items.item_status, 
    loans_items.loan_date, 
    loans_items.loan_return_date, 
    loans_items.renewal_count, 
    items_holdings_instances.item_id
    order by call_number;
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;