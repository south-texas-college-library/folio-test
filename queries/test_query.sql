--metadb:function test_query

DROP FUNCTION IF EXISTS test_query;

CREATE FUNCTION test_query(
    start_cn text DEFAULT 'A',
    end_cn text DEFAULT 'Z 9999.99'
)
RETURNS TABLE(
    "A - Title" text,
    "B - Barcode" text,
    "C - Call Number" text,
    "D - Cataloged Date" text,
    "E - Identifiers" text,
    "F - Staff Notes" text,
    "G - Statistical Codes" text
)
AS $$
    with loans as materialized (
        select
            folio_circulation.loan__t.item_id as item_id,
            count(folio_circulation.loan__t.id) as checkouts
        from
            folio_circulation.loan__t
        left join folio_inventory.item__t on folio_inventory.item__t.id = folio_circulation.loan__t.item_id
        group by folio_circulation.loan__t.item_id
    ),
    identifiers as materialized (
        select
            i.id as id,
            string_agg(distinct object ->> 'value', ', ') AS values
        from folio_inventory.instance i
        cross join lateral jsonb_array_elements(i.jsonb -> 'identifiers') AS object
        group by
            i.id
    ),
    notes as materialized (
        select
            it.id as id,
            string_agg(distinct object ->> 'note', ', ') filter (where folio_inventory.item_note_type__t.name = 'Staff Note') as values
        from folio_inventory.item it
        cross join lateral jsonb_array_elements(it.jsonb -> 'notes') as object
        left join folio_inventory.item_note_type__t on folio_inventory.item_note_type__t.id = (object ->> 'itemNoteTypeId')::uuid
        group by
            it.id
    ),
    codes as materialized (
        select
            it.id as id,
            folio_inventory.statistical_code__t.name as values
        from folio_inventory.item it
        left join lateral jsonb_array_elements_text(it.jsonb -> 'statisticalCodeIds') as object on true
        left join folio_inventory.statistical_code__t on folio_inventory.statistical_code__t.id = object::uuid
    )
    select
        folio_inventory.instance.jsonb ->> 'title' as "A - Title",
        folio_inventory.item__t.barcode as "B - Barcode",
        folio_inventory.holdings_record__t.call_number as "C - Call Number",
        folio_inventory.instance.jsonb ->> 'catalogedDate' as "D - Cataloged Date",
        identifiers.values as "E - Identifiers",
        notes.values as "F- Staff Notes",
        codes.values as "G - Statistical Codes"
    from folio_inventory.instance
        join folio_inventory.holdings_record__t on folio_inventory.holdings_record__t.instance_id = folio_inventory.instance.id
        join folio_inventory.item on folio_inventory.item.holdingsrecordid = folio_inventory.holdings_record__t.id
        join folio_inventory.item__t on folio_inventory.item__t.id = folio_inventory.item.id
        join loans on loans.item_id = folio_inventory.item.id
        join identifiers on identifiers.id = folio_inventory.instance.id
        join notes on notes.id = folio_inventory.item.id
        join codes on codes.id = folio_inventory.item.id
    where folio_inventory.holdings_record__t.call_number between start_cn and end_cn
    order by call_number
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;