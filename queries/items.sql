--metadb:function items

DROP FUNCTION IF EXISTS items;

CREATE FUNCTION items()

RETURNS TABLE(
    "Instance ID" TEXT,
    "Title" TEXT,
    "Discovery Suppress" TEXT,
    "Staff Suppress" TEXT,
    "Deleted" TEXT,
    "Index Title" TEXT,
    "Cataloged Date" TEXT,
    "Status ID" TEXT,
    "HR ID" TEXT,
    "Call Number" TEXT,
    "Holdings Discovery Suppress" TEXT,
    "Holdings Permanent Location" TEXT,
    "Holdings Temporary Location" TEXT,
    "Effective Location" TEXT,
    "Copy Number" TEXT,
    "Item ID" TEXT,
    "Item Barcode" TEXT,
    "Item Discovery Suppress" TEXT,
    "Chronology" TEXT,
    "Enumeration" TEXT,
    "Item Identifier" TEXT,
    "Material Type ID" TEXT,
    "Missing Pieces" TEXT,
    "Item Permanent Location" TEXT,
    "Item Temporary Location" TEXT,
    "Item Effective Location" TEXT,
    "Volume" TEXT
)
AS $$
    select
        ins.id,
        ins.TITLE,
        ins.DISCOVERY_SUPPRESS,
        ins.STAFF_SUPPRESS,
        ins.DELETED,
        ins.INDEX_TITLE,
        ins.CATALOGED_DATE,
        ins.STATUS_ID,
        hr.id,
        hr.call_number,
        hr.DISCOVERY_SUPPRESS,
        hr.PERMANENT_LOCATION_ID,
        hr.TEMPORARY_LOCATION_ID,
        hr.EFFECTIVE_LOCATION_ID,
        hr.COPY_NUMBER,
        it.id,
        it.barcode,
        it.DISCOVERY_SUPPRESS,
        it.CHRONOLOGY,
        it.ENUMERATION,
        it.ITEM_IDENTIFIER,
        it.MATERIAL_TYPE_ID,
        it.MISSING_PIECES,
        it.PERMANENT_LOCATION_ID,
        it.TEMPORARY_LOCATION_ID,
        it.EFFECTIVE_LOCATION_ID,
        it.VOLUME
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
    join folio_inventory.item__t it on it.HOLDINGS_RECORD_ID = hr.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;