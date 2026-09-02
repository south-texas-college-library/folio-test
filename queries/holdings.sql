--metadb:function holdings

DROP FUNCTION IF EXISTS holdings;

CREATE FUNCTION holdings()

RETURNS TABLE(
    "Title" TEXT,
    "Call Number" TEXT
)
AS $$
    select 
        ins.title,
        hr.call_number
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;