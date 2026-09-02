--metadb:function items

DROP FUNCTION IF EXISTS items;

CREATE FUNCTION items()

RETURNS TABLE(
    "Instance ID" TEXT,
    "Title" TEXT,
    "Discovery Suppress" TEXT,
    "Staff Suppress" TEXT,
    "Deleted" TEXT,
    "HR ID" TEXT,
    "Call Number" TEXT,
    "Holdings Discovery Suppress" TEXT,
    "Copy Number" TEXT,
    "Item ID" TEXT,
    "Item Barcode" TEXT,
    "Item Discovery Suppress" TEXT,
    "Chronology" TEXT,
    "Enumeration" TEXT,
    "Item Identifier" TEXT,
    "Material Type ID" TEXT,
    "Missing Pieces" TEXT,
    "Volume" TEXT
)
AS $$
    select
        ins.id,
        ins.TITLE,
        ins.DISCOVERY_SUPPRESS,
        ins.STAFF_SUPPRESS,
        ins.DELETED,
        hr.id,
        hr.call_number,
        hr.DISCOVERY_SUPPRESS,
        hr.COPY_NUMBER,
        it.id,
        it.barcode,
        it.DISCOVERY_SUPPRESS,
        it.CHRONOLOGY,
        it.ENUMERATION,
        it.ITEM_IDENTIFIER,
        it.MATERIAL_TYPE_ID,
        it.MISSING_PIECES,
        it.VOLUME
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
    join folio_inventory.item__t it on it.HOLDINGS_RECORD_ID = hr.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;