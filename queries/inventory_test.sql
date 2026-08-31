--metadb:function inventory_test

DROP FUNCTION IF EXISTS inventory_test;

CREATE FUNCTION inventory_test()

RETURNS TABLE(
    "Barcode" TEXT,
    "Call Number" TEXT,
    "Publication Date" TEXT,
    "Title" TEXT,
    "Author" TEXT,
	"Publisher" TEXT,
    "Cataloged Date" TEXT,
    "Created Date" TEXT,
    "Status" TEXT
)
AS $$
    select
        jsonb_extract_path_text(it.jsonb , 'barcode') as barcode,
        hr.call_number as call_number,
        COALESCE(
            GREATEST(jsonb_extract_path_text(ins.jsonb, 'dates', 'date1'), jsonb_extract_path_text(ins.jsonb, 'dates', 'date2')),
            REGEXP_REPLACE(jsonb_path_query_first(ins.jsonb, '$.publication[*].dateOfPublication') #>> '{}', '[^0-9?,\s-]', '', 'g')
        ) as publication_date,
        jsonb_extract_path_text(ins.jsonb, 'title') as title,
        jsonb_path_query_first(ins.jsonb, '$.contributors[*].name') #>> '{}' as author,	
        jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' as publisher,
        jsonb_extract_path_text(ins.jsonb, 'catalogedDate') as cataloged_date,
        COALESCE(
            TO_DATE(split_part(jsonb_path_query_first(it.jsonb, '$.administrativeNotes[*]') #>> '{}', ': ', 2), 'YYYYMMDD')::text,
            jsonb_extract_path_text(it.jsonb, 'metadata', 'createdDate')::date::text
        ) as created_date,
        jsonb_extract_path_text(it.jsonb , 'status', 'name') as status
    FROM
        folio_inventory.instance ins
        JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;