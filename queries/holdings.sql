--metadb:function holdings

DROP FUNCTION IF EXISTS holdings;

CREATE FUNCTION holdings()

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
    "Discovery Suppress" TEXT,
    "Holdings Permanent Location" TEXT,
    "Holdings Temporary Location" TEXT,
    "Holdings Effective Location" TEXT,
    "Copy Number" TEXT
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
        hr.COPY_NUMBER
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;