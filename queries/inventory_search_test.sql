--metadb:function inventory_search_test

DROP FUNCTION IF EXISTS inventory_search_test;

CREATE FUNCTION inventory_search_test()
RETURNS TABLE(
    "Barcode" TEXT
)
AS $$
    SELECT
        jsonb_extract_path_text(it.jsonb, 'barcode') as "Barcode",
    FROM folio_inventory.item it ON it.holdingsrecordid = hr.id
    ORDER BY
        jsonb_extract_path_text(it.jsonb, 'barcode')
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;