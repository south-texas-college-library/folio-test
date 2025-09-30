--metadb:function get_loan_items

DROP FUNCTION IF EXISTS get_loan_items;

CREATE FUNCTION get_loan_items(
    item_barcode text DEFAULT NULL,
    item_status text DEFAULT NULL,
    start_date date DEFAULT '2000-01-01',
    end_date date DEFAULT '2050-01-01'
)
RETURNS TABLE(
    user_id uuid,
    item_id uuid,
    barcode text,
    item_status text,
    loan_date timestamptz,
    loan_due_date timestamptz,
    loan_return_date timestamptz,
    renewal_count integer,
    material_type_name text
)
AS $$
    SELECT 
        folio_derived.loans_items.user_id,
        folio_derived.loans_items.item_id,
        folio_derived.loans_items.barcode,
        folio_derived.loans_items.item_status,
        folio_derived.loans_items.loan_date,
        folio_derived.loans_items.loan_due_date,
        folio_derived.loans_items.loan_return_date,
        folio_derived.loans_items.renewal_count,
        folio_derived.loans_items.material_type_name
    FROM folio_derived.loans_items
    WHERE 
        (item_barcode IS NULL OR folio_derived.loans_items.barcode = item_barcode)
        AND (item_status = 'All' OR folio_derived.loans_items.item_status = item_status)
        AND (folio_derived.loans_items.loan_date BETWEEN start_date AND end_date)
        AND (folio_derived.loans_items.patron_group_name = 'Employee')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;
