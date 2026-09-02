--metadb:function holdings

DROP FUNCTION IF EXISTS holdings;

CREATE FUNCTION holdings()

RETURNS TABLE(
    "Instance ID" TEXT,
    "Title" TEXT,
    "Instance Discovery Suppress" TEXT,
    "Staff Suppress" TEXT,
    "Deleted" TEXT,
    "HR ID" TEXT,
    "Call Number" TEXT
)
AS $$
    select 
        LEFT(ins.id, 1),
        LEFT(ins.title, 1),
        LEFT(ins.discovery_suppress, 1),
        LEFT(ins.staff_suppress, 1),
        LEFT(ins.deleted, 1),
        LEFT(hr.id, 1),
        LEFT(hr.call_number, 1)
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;