--metadb:function link

DROP FUNCTION IF EXISTS link;

CREATE FUNCTION link(
  item_barcode text default null,
  item_status text default null
)
RETURNS TABLE(
  link text
)
AS $$
    select concat('<a href="https://stc.folio.indexdata.com/inventory?filters=staffSuppress.false&qindex=items.barcode&query=', barcode, '&segment=items&sort=title">', barcode, '</a>')
    from folio_inventory.item__t
    where barcode = item_barcode
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;