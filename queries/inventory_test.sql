--metadb:function inventory_test

DROP FUNCTION IF EXISTS inventory_test;

CREATE FUNCTION inventory_test()

RETURNS TABLE(
    "Barcode" TEXT,
    "Call Number" TEXT,
    "Publication Date" TEXT,
    "Title" TEXT,
    "Author" TEXT,
    "ISBN" TEXT,
	"Publisher" TEXT,
    "Subjects" TEXT,
    "Cataloged Date" TEXT,
    "Created Date" TEXT,
    "Inventory Date" TEXT,
    "PO Number" TEXT,
    "Invoice" TEXT,
    "Ownership" TEXT,
    "Price" TEXT,
    "Public Notes" TEXT,
    "Circulation Notes" TEXT,
    "Staff Notes" TEXT,
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
        NULLIF(REGEXP_REPLACE(jsonb_path_query_array(ins.jsonb, '$.identifiers[*].value') #>> '{}', '\[|\]|"| :.*?\$\d+\.\d{2}', '', 'g'), '') as isbn,
        jsonb_path_query_first(ins.jsonb, '$.publication[*].publisher') #>> '{}' as publisher,
        NULLIF(TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '') as subjects,
        jsonb_extract_path_text(ins.jsonb, 'catalogedDate') as cataloged_date,
        COALESCE(
            TO_DATE(split_part(jsonb_path_query_first(it.jsonb, '$.administrativeNotes[*]') #>> '{}', ': ', 2), 'YYYYMMDD')::text,
            jsonb_extract_path_text(it.jsonb, 'metadata', 'createdDate')::date::text
        ) as created_date,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "e1f34ba3-6d37-462e-878c-17f922b13d93").note') #>> '{}', '[]"', ''), '') AS inventory_date,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5ec4ca65-aacc-4f16-aa9d-395efd89f850").note') #>> '{}', '[]"', ''), '') AS po_number,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "8f99bd3a-706c-45d2-89d8-8eca7fa1c03f").note') #>> '{}', '[]"', ''), '') AS invoice,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "34207e4e-5cd7-4eab-801b-b0326cd5c66a").note') #>> '{}', '[]"', ''), '') AS ownership,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "1fceb11c-7a89-49d6-8ef0-2a42c58556a2").note') #>> '{}', '[]"', ''), '') AS price,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "b6b35579-ee2b-4973-8e0d-ebc05bab0dab").note') #>> '{}', '[]"', ''), '') AS public_notes,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "5366d4d4-8775-4cf4-a00f-77c82f0ca3bf").note') #>> '{}', '[]"', ''), '') AS circulation_notes,
        NULLIF(TRANSLATE(jsonb_path_query_array(it.jsonb, '$.notes[*] ? (@.itemNoteTypeId == "86e6410d-4c8b-4853-8054-bd5e563e9760").note') #>> '{}', '[]"', ''), '') AS staff_notes,
        jsonb_extract_path_text(it.jsonb , 'status', 'name') as status
    FROM
        folio_inventory.instance ins
        JOIN folio_inventory.holdings_record__t hr ON hr.instance_id = ins.id
        JOIN folio_inventory.item it ON it.holdingsrecordid = hr.id
$$
LANGUAGE SQL
STABLE
PARALLEL SAFE;