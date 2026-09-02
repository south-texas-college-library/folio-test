--metadb:function holdings

DROP FUNCTION IF EXISTS holdings;

CREATE FUNCTION holdings()

RETURNS TABLE(
    "Instance ID" TEXT,
    "HR ID" TEXT
)
AS $$
    select 
        ins.id::text,
        hr.id::text
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;