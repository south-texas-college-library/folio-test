--metadb:function items

DROP FUNCTION IF EXISTS items;

CREATE FUNCTION items()

RETURNS TABLE(
    "Instance ID" TEXT,
    "Title" TEXT,
    "HR ID" TEXT,
    "Call Number" TEXT,
    "Item ID" TEXT,
    "Item Barcode" TEXT
)
AS $$
    select
        ins.id,
        ins.TITLE,
        hr.id,
        hr.call_number,
        it.id,
        it.barcode
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
    join folio_inventory.item__t it on it.HOLDINGS_RECORD_ID = hr.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;