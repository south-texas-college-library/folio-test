--metadb:function test

DROP FUNCTION IF EXISTS test;

CREATE FUNCTION test(
  item_barcode text[]
)
RETURNS TABLE(
  instance_id uuid
  )
AS $$
select distinct it.id as instance_id
from 
folio_inventory.instance__t it
left join folio_inventory.holdings_record__t hrt on (it.id = hrt.instance_id)
left join folio_inventory.item__t im on (im.holdings_record_id = hrt.id) 
where im.barcode in (item_barcode)
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;