--metadb:function holdings

DROP FUNCTION IF EXISTS holdings;

CREATE FUNCTION holdings()

RETURNS TABLE(
    "HR ID" UUID
)
AS $$
    select 
        hr.id
    from folio_inventory.instance__t ins
    join folio_inventory.holdings_record__t hr on hr.instance_id = ins.id
$$
LANGUAGE SQL;