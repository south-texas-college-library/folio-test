--metadb:function link

DROP FUNCTION IF EXISTS link;

CREATE FUNCTION link(
  item_barcode text
)
RETURNS TABLE(
  link text
)
AS $$
    select concat('<a href="https://stc.folio.indexdata.com/inventory?filters=staffSuppress.false&qindex=items.barcode&query="', barcode, '"&segment=items&sort=title" target="_blank" rel="noopener">', barcode, '</a>')
    from folio_inventory.item__t
    where barcode = item_barcode
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;