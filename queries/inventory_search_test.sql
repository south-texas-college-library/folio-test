--metadb:function inventory_search_test

DROP FUNCTION IF EXISTS inventory_search_test;

CREATE FUNCTION inventory_search_test(
    start_cn TEXT DEFAULT '0',
    end_cn TEXT DEFAULT 'ZZZZZZZZ',
    subjects TEXT DEFAULT NULL,
    title TEXT DEFAULT NULL,
    search_type TEXT DEFAULT NULL,
    material_type TEXT DEFAULT NULL,
    sub_type TEXT DEFAULT NULL,
    campus TEXT DEFAULT NULL,
    department TEXT DEFAULT NULL
)
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
    "Content" TEXT,
    "Subtype" TEXT,
    "Department" TEXT,
    "Campus" TEXT,
    "Location" TEXT,
    "Temporary Location" TEXT,
    "Material Type" TEXT,
    "Created Date" TEXT,
    "Fund" TEXT,
    "Inventory Date" TEXT,
    "PO Number" TEXT,
    "Invoice" TEXT,
    "Ownership" TEXT,
    "Price" TEXT,
    "Public Notes" TEXT,
    "Circulation Notes" TEXT,
    "Staff Notes" TEXT,
    "Checkouts" INTEGER,
    "Renewals" INTEGER,
    "Status" TEXT,
    "Loan Date" TEXT,
    "Due Date" TEXT
)
AS $$
    WITH inventory as materialized (
        select
            ins.id as instance_id,
            it.id as item_id,
            ins.jsonb as instance_jsonb,
            it.jsonb as item_jsonb,
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
            CASE
                WHEN hl.name ~* 'CLE' THEN 'CLE'
                WHEN hl.name ~* 'Open Lab' THEN 'Open Lab'
                ELSE 'Library'
            END AS department,
            lc.name as campus,
            hl.name as location,
            tl.name as temporary_location,
            mt.name as material_type,
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
            JOIN folio_inventory.location__t hl ON hl.id = hr.permanent_location_id
            JOIN folio_inventory.material_type__t mt ON mt.id = it.materialtypeid
            JOIN folio_inventory.service_point__t sp ON sp.id = hl.primary_service_point
            JOIN folio_inventory.loccampus__t lc ON lc.id = hl.campus_id
            LEFT JOIN folio_inventory.location__t tl ON tl.id = it.temporarylocationid
            WHERE
                (subjects IS NULL OR TO_TSVECTOR('english', NULLIF(TRANSLATE(jsonb_path_query_array(ins.jsonb, '$.subjects[*].value') #>> '{}', '[]"', ''), '')) @@ WEBSEARCH_TO_TSQUERY('english', subjects))
                AND (material_type = 'All' OR mt.name = material_type)
                AND (campus = 'All' OR lc.name = campus)
                AND CASE department
                    WHEN 'Library' THEN COALESCE(hr.call_number, '') ~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name !~* '(CLE|Open Lab)'
                    WHEN 'CLE' THEN COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name ~* '(CLE)'
                    WHEN 'Open Lab' THEN COALESCE(hr.call_number, '') !~ '^[A-Z]{1,3}\s*[0-9]' AND hl.name ~* '(Open Lab)'
                    ELSE TRUE
                END
                AND CASE search_type
                    WHEN 'Contains All' THEN (title IS NULL OR TO_TSVECTOR('english', jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('english', title))
                    WHEN 'Exact Match' THEN (title IS NULL OR TO_TSVECTOR(jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('"' || title || '"'))
                    WHEN 'Contains Any' THEN (title IS NULL OR TO_TSVECTOR('english', jsonb_extract_path_text(ins.jsonb, 'title')) @@ WEBSEARCH_TO_TSQUERY('english', replace(title, ' ', ' OR ' )))
                    WHEN 'Starts With' THEN (title IS NULL OR jsonb_extract_path_text(ins.jsonb, 'title') ILIKE (title || '%'))
                    WHEN 'Ends With' THEN (title IS NULL OR jsonb_extract_path_text(ins.jsonb, 'title') ILIKE ('%' || title))
                    ELSE TRUE
                END
                AND (hr.call_number IS NULL OR hr.call_number between start_cn and end_cn)
    ),
    loans AS (
        SELECT
            jsonb_extract_path_text(l.jsonb, 'itemId')::uuid AS item_id,
            jsonb_extract_path_text(l.jsonb, 'loanDate') AS loan_date,
            jsonb_extract_path_text(l.jsonb, 'dueDate') AS due_date
        FROM inventory inv 
        JOIN folio_circulation.loan l on jsonb_extract_path_text(l.jsonb, 'itemId')::uuid = inv.item_id
        WHERE jsonb_extract_path_text(l.jsonb, 'status', 'name') = 'Open'
    ),
    stats as (
        SELECT
            l.item_id AS item_id,
            COUNT(l.id) AS checkouts,
            SUM(l.renewal_count) AS renewals
        FROM inventory inv 
        JOIN folio_circulation.loan__t l on l.item_id = inv.item_id
        GROUP BY 
            l.item_id
    ),
    codes AS (
        SELECT
            inv.instance_id AS instance_id,
            MAX(sct.name) FILTER (WHERE sctt.name = 'CONTENT') AS content,
            MAX(sct.name) FILTER (WHERE sctt.name = 'SUBTYPE') AS subtype,
            MAX(sct.name) FILTER (WHERE sctt.name = 'FUND') AS fund
        FROM inventory inv
        CROSS JOIN LATERAL jsonb_array_elements_text(
            jsonb_path_query_array(inv.instance_jsonb, '$.statisticalCodeIds[*]') || jsonb_path_query_array(inv.item_jsonb, '$.statisticalCodeIds[*]')
        ) stat_id
        JOIN folio_inventory.statistical_code__t sct ON sct.id = stat_id::uuid
        JOIN folio_inventory.statistical_code_type__t sctt ON sctt.id = sct.statistical_code_type_id
        GROUP BY 
            inv.instance_id
    )
    SELECT
        inventory.barcode as "Barcode",
        inventory.call_number as "Call Number",
        inventory.publication_date as "Publication Date",
        inventory.title as "Title",
        inventory.author as "Author",
        inventory.isbn as "ISBN",
        inventory.publisher as "Publisher",
        inventory.subjects as "Subjects",
        inventory.cataloged_date,
        codes.content as "Content",
        codes.subtype as "Subtype",
        inventory.department as "Department",
        inventory.campus as "Campus",
        inventory.location as "Location",
        inventory.temporary_location as "Temporary Location",
        inventory.material_type as "Material Type",
        inventory.created_date as "Created Date",
        codes.fund as "Fund",
        inventory.inventory_date as "Inventory Date",
        inventory.po_number as "PO Number",
        inventory.invoice as "Invoice",
        inventory.ownership as "Ownership",
        inventory.price as "Price",
        inventory.public_notes as "Public Notes",
        inventory.circulation_notes as "Circulation Notes",
        inventory.staff_notes as "Staff Notes",
        COALESCE(stats.checkouts, 0) as "Checkouts",
        COALESCE(stats.renewals, 0) as "Renewals",
        inventory.status as "Status",
        loans.loan_date as "Loan Date",
        loans.due_date as "Due Date"
    FROM inventory
    LEFT JOIN loans ON loans.item_id = inventory.item_id
    LEFT JOIN stats ON stats.item_id = inventory.item_id
    LEFT JOIN codes ON codes.instance_id = inventory.instance_id
    WHERE
        (sub_type = 'All' OR codes.subtype = sub_type)
    ORDER BY
        inventory.call_number, inventory.barcode
    $$
LANGUAGE SQL
STABLE
PARALLEL SAFE;